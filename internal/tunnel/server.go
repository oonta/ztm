package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"

	"github.com/quic-go/quic-go"

	"ztm/internal/identity"
	"ztm/internal/policy"
	"ztm/internal/proxy"
	"ztm/internal/registry"
	"ztm/internal/router"
)

// Server accepts client QUIC connections and proxies TCP flows.
type Server struct {
	nodeID   string
	registry *registry.Registry
	policy   *policy.Policy
	mesh     PeerRelay
	listener *quic.Listener
	addr     string
}

// ServerConfig configures the client tunnel server.
type ServerConfig struct {
	NodeID   string
	Registry *registry.Registry
	Policy   *policy.Policy
	Mesh     PeerRelay
	TLS      *tls.Config
}

// Listen starts the client QUIC listener.
func (s *Server) Listen(ctx context.Context, addr string, cfg ServerConfig) error {
	if cfg.Policy == nil {
		cfg.Policy = policy.Permissive()
	}
	s.nodeID = cfg.NodeID
	s.registry = cfg.Registry
	s.policy = cfg.Policy
	s.mesh = cfg.Mesh

	ln, err := quic.ListenAddr(addr, cfg.TLS, nil)
	if err != nil {
		return err
	}
	s.listener = ln
	s.addr = ln.Addr().String()

	go s.acceptLoop(ctx)
	return nil
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("tunnel: accept: %v", err)
			return
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.handleStream(ctx, conn, stream)
	}
}

func (s *Server) handleStream(ctx context.Context, conn *quic.Conn, stream *quic.Stream) {
	defer stream.Close()

	clientID, err := ClientIDFromConn(conn)
	if err != nil {
		log.Printf("tunnel: client id: %v", err)
		return
	}

	reader := bufio.NewReader(stream)
	req, err := ReadConnectRequest(reader)
	if err != nil {
		log.Printf("tunnel: read connect: %v", err)
		return
	}

	service := req.TargetService
	if service == "" {
		service = proxy.ServiceNameFromHost(req.TargetHost)
	}
	if service == "" {
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "SERVICE_REQUIRED"})
		return
	}

	if !s.policy.Allow(clientID, service) {
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "ACL_DENIED"})
		return
	}

	peers := []string(nil)
	if s.mesh != nil {
		peers = s.mesh.PeerIDs()
	}
	target, ok := router.Select(s.registry.FindByName(service), s.nodeID, peers)
	if !ok {
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "SERVICE_NOT_FOUND"})
		return
	}

	if target.NodeID == s.nodeID {
		s.relayLocal(stream, service, target)
		return
	}

	if s.mesh == nil {
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "ROUTE_FAILED"})
		return
	}

	relayReq := ConnectRequest{
		TargetService:  service,
		TargetHost:     req.TargetHost,
		TargetPort:     req.TargetPort,
		ClientIdentity: clientID,
	}
	peerStream, err := s.mesh.OpenRelay(ctx, target.NodeID, relayReq)
	if err != nil {
		log.Printf("tunnel: mesh relay %s via %s: %v", service, target.NodeID, err)
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "ROUTE_FAILED"})
		return
	}
	defer peerStream.Close()

	if err := WriteConnectResponse(stream, ConnectResponse{OK: true, ResolvedNode: target.NodeID}); err != nil {
		return
	}

	if err := Relay(stream, peerStream); err != nil && err != io.EOF {
		log.Printf("tunnel: relay %s via %s: %v", service, target.NodeID, err)
	}
}

func (s *Server) relayLocal(stream *quic.Stream, service string, target registry.Service) {
	backend, err := proxy.DialTCP(target.Host, target.Port)
	if err != nil {
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "BACKEND_UNREACHABLE"})
		return
	}
	defer backend.Close()

	if err := WriteConnectResponse(stream, ConnectResponse{OK: true, ResolvedNode: s.nodeID}); err != nil {
		return
	}

	if err := Relay(stream, backend); err != nil && err != io.EOF {
		log.Printf("tunnel: relay %s: %v", service, err)
	}
}

// ClientIDFromConn extracts client id from QUIC connection TLS state.
func ClientIDFromConn(conn *quic.Conn) (string, error) {
	st := conn.ConnectionState().TLS
	if len(st.PeerCertificates) == 0 {
		return "", fmt.Errorf("no client certificate")
	}
	cert := st.PeerCertificates[0]
	for _, u := range cert.URIs {
		if id, ok := identity.ParseClientID(u.Host, cert.URIs); ok {
			return id, nil
		}
	}
	return "", fmt.Errorf("invalid client identity")
}
