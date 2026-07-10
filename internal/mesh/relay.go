package mesh

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/quic-go/quic-go"

	"ztm/internal/metrics"
	"ztm/internal/policy"
	"ztm/internal/protocol"
	"ztm/internal/proxy"
	"ztm/internal/registry"
)

// RelayHandler serves mesh relay streams from peer nodes.
type RelayHandler struct {
	NodeID   string
	Registry *registry.Registry
	Policy   *policy.Policy
	Metrics  *metrics.Collector
}

// HandleStream proxies a mesh relay request to a local backend.
func (h *RelayHandler) HandleStream(peerID string, stream *quic.Stream) {
	defer stream.Close()

	reader := bufio.NewReader(stream)
	req, err := protocol.ReadConnectRequest(reader)
	if err != nil {
		log.Printf("mesh relay: read connect from %s: %v", peerID, err)
		return
	}

	service := req.TargetService
	if service == "" {
		service = proxy.ServiceNameFromHost(req.TargetHost)
	}
	if service == "" {
		_ = protocol.WriteConnectResponse(stream, protocol.ConnectResponse{OK: false, Reason: "SERVICE_REQUIRED"})
		return
	}

	clientID := req.ClientIdentity
	if clientID == "" {
		clientID = "mesh:" + peerID
	}
	if !h.Policy.Allow(clientID, service) {
		if h.Metrics != nil {
			h.Metrics.IncACLDenied()
		}
		_ = protocol.WriteConnectResponse(stream, protocol.ConnectResponse{OK: false, Reason: "ACL_DENIED"})
		return
	}

	host, port, ok := proxy.ResolveLocal(h.Registry, h.NodeID, service)
	if !ok {
		_ = protocol.WriteConnectResponse(stream, protocol.ConnectResponse{OK: false, Reason: "SERVICE_NOT_FOUND"})
		return
	}

	backend, err := proxy.DialTCP(host, port)
	if err != nil {
		_ = protocol.WriteConnectResponse(stream, protocol.ConnectResponse{OK: false, Reason: "BACKEND_UNREACHABLE"})
		return
	}
	defer backend.Close()

	if err := protocol.WriteConnectResponse(stream, protocol.ConnectResponse{OK: true, ResolvedNode: h.NodeID}); err != nil {
		return
	}

	if err := metrics.Relay(stream, backend, h.Metrics); err != nil && err != io.EOF {
		log.Printf("mesh relay: %s via %s: %v", service, peerID, err)
	}
}

// OpenRelay opens a relay stream to a peer and returns it after a successful connect ack.
func (t *Transport) OpenRelay(ctx context.Context, peerID string, req protocol.ConnectRequest) (io.ReadWriteCloser, error) {
	conn, ok := t.peer(peerID)
	if !ok {
		return nil, fmt.Errorf("peer %q not connected", peerID)
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	if err := protocol.WriteConnectRequest(stream, req); err != nil {
		stream.Close()
		return nil, err
	}

	resp, err := protocol.ReadConnectResponse(bufio.NewReader(stream))
	if err != nil {
		stream.Close()
		return nil, err
	}
	if !resp.OK {
		stream.Close()
		return nil, fmt.Errorf("relay denied: %s", resp.Reason)
	}
	return stream, nil
}

func (t *Transport) peer(id string) (*quic.Conn, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	conn, ok := t.peers[id]
	return conn, ok
}

func (t *Transport) serveRelayStreams(ctx context.Context, peerID string, conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		if t.relayHandler == nil {
			stream.Close()
			continue
		}
		go t.relayHandler.HandleStream(peerID, stream)
	}
}
