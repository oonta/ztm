package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/quic-go/quic-go"

	"ztm/internal/identity"
	"ztm/internal/metrics"
	"ztm/internal/policy"
	"ztm/internal/proxy"
	"ztm/internal/registry"
	"ztm/internal/router"
)

// Server accepts client QUIC connections and proxies TCP flows.
type Server struct {
	nodeID   string
	registry *registry.Registry
	finder   ServiceFinder
	policy   *policy.Policy
	mesh     PeerRelay
	metrics  *metrics.Collector
	listener *quic.Listener
	addr     string

	active sync.WaitGroup
}

// ServiceFinder resolves logical service names to instances.
type ServiceFinder interface {
	FindByName(ctx context.Context, name string) []registry.Service
}

// ServerConfig configures the client tunnel server.
type ServerConfig struct {
	NodeID        string
	Registry      *registry.Registry
	ServiceFinder ServiceFinder
	Policy        *policy.Policy
	Mesh          PeerRelay
	Metrics       *metrics.Collector
	TLS           *tls.Config
}

// Listen starts the client QUIC listener.
func (s *Server) Listen(ctx context.Context, addr string, cfg ServerConfig) error {
	if cfg.Policy == nil {
		cfg.Policy = policy.Permissive()
	}
	s.nodeID = cfg.NodeID
	s.registry = cfg.Registry
	s.finder = cfg.ServiceFinder
	s.policy = cfg.Policy
	s.mesh = cfg.Mesh
	s.metrics = cfg.Metrics

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

// Close stops accepting new client connections.
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// Shutdown stops accepting and waits for active streams to finish or ctx cancellation.
func (s *Server) Shutdown(ctx context.Context) error {
	_ = s.Close()

	done := make(chan struct{})
	go func() {
		s.active.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if s.metrics != nil {
		s.metrics.TunnelConnOpened()
		defer s.metrics.TunnelConnClosed()
	}
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go s.handleStream(ctx, conn, stream)
	}
}

func (s *Server) handleStream(ctx context.Context, conn *quic.Conn, stream *quic.Stream) {
	s.active.Add(1)
	defer s.active.Done()
	defer stream.Close()
	if s.metrics != nil {
		s.metrics.TunnelStreamOpened()
		defer s.metrics.TunnelStreamClosed()
	}

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
		if s.metrics != nil {
			s.metrics.IncACLDenied()
		}
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "ACL_DENIED"})
		return
	}

	peers := []string(nil)
	if s.mesh != nil {
		peers = s.mesh.PeerIDs()
	}
	candidates := router.Candidates(s.findServices(ctx, service), s.nodeID, peers)
	if len(candidates) == 0 {
		_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "SERVICE_NOT_FOUND"})
		return
	}

	relayReq := ConnectRequest{
		TargetService:  service,
		TargetHost:     req.TargetHost,
		TargetPort:     req.TargetPort,
		ClientIdentity: clientID,
	}

	for _, target := range candidates {
		if target.NodeID == s.nodeID {
			if s.tryRelayLocal(stream, service, target) {
				return
			}
			continue
		}
		if s.mesh == nil {
			continue
		}
		peerStream, err := s.mesh.OpenRelay(ctx, target.NodeID, relayReq)
		if err != nil {
			log.Printf("tunnel: mesh relay %s via %s: %v", service, target.NodeID, err)
			continue
		}
		if err := WriteConnectResponse(stream, ConnectResponse{OK: true, ResolvedNode: target.NodeID}); err != nil {
			peerStream.Close()
			return
		}
		if err := metrics.Relay(stream, peerStream, s.metrics); err != nil && err != io.EOF {
			log.Printf("tunnel: relay %s via %s: %v", service, target.NodeID, err)
		}
		return
	}

	_ = WriteConnectResponse(stream, ConnectResponse{OK: false, Reason: "ROUTE_FAILED"})
}

func (s *Server) findServices(ctx context.Context, name string) []registry.Service {
	if s.finder != nil {
		return s.finder.FindByName(ctx, name)
	}
	if s.registry != nil {
		return s.registry.FindByName(name)
	}
	return nil
}

func (s *Server) tryRelayLocal(stream *quic.Stream, service string, target registry.Service) bool {
	backend, err := proxy.DialTCP(target.Host, target.Port)
	if err != nil {
		return false
	}

	if err := WriteConnectResponse(stream, ConnectResponse{OK: true, ResolvedNode: s.nodeID}); err != nil {
		backend.Close()
		return false
	}

	if err := metrics.Relay(stream, backend, s.metrics); err != nil && err != io.EOF {
		log.Printf("tunnel: relay %s: %v", service, err)
	}
	return true
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
