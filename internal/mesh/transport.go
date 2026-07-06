package mesh

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

// Hello is the first message exchanged on a mesh stream.
type Hello struct {
	Type    string `json:"type"`
	NodeID  string `json:"node_id"`
	Cluster string `json:"cluster"`
}

// Transport manages QUIC mesh connections between nodes.
type Transport struct {
	nodeID  string
	cluster string
	tlsConf *tls.Config

	listener *quic.Listener
	addr     string

	onDisconnect func(peerID string)

	relayHandler *RelayHandler

	mu    sync.RWMutex
	peers map[string]*quic.Conn
}

// NewTransport creates a mesh transport. tlsConf must use ALPN ztm-mesh/1.
func NewTransport(nodeID, cluster string, tlsConf *tls.Config) *Transport {
	return &Transport{
		nodeID:  nodeID,
		cluster: cluster,
		tlsConf: tlsConf,
		peers:   make(map[string]*quic.Conn),
	}
}

// SetRelayHandler handles inbound mesh relay streams.
func (t *Transport) SetRelayHandler(h *RelayHandler) {
	t.relayHandler = h
}

// SetOnDisconnect sets a callback when a peer disconnects.
func (t *Transport) SetOnDisconnect(fn func(peerID string)) {
	t.onDisconnect = fn
}

// Listen starts the QUIC mesh listener.
func (t *Transport) Listen(ctx context.Context, addr string) error {
	ln, err := quic.ListenAddr(addr, t.tlsConf, quicConfig())
	if err != nil {
		return fmt.Errorf("mesh listen: %w", err)
	}
	t.listener = ln
	t.addr = ln.Addr().String()

	go t.acceptLoop(ctx)
	return nil
}

// Addr returns the bound mesh listen address.
func (t *Transport) Addr() string {
	return t.addr
}

// Connect dials a peer mesh address and completes the hello handshake.
func (t *Transport) Connect(ctx context.Context, addr string) error {
	conn, err := quic.DialAddr(ctx, addr, t.tlsConf, quicConfig())
	if err != nil {
		return fmt.Errorf("mesh dial %s: %w", addr, err)
	}

	peerID, err := t.handshake(ctx, conn, true)
	if err != nil {
		_ = conn.CloseWithError(1, "handshake failed")
		return err
	}

	t.addPeer(peerID, conn)
	go t.monitorPeer(ctx, peerID, conn)
	go t.serveRelayStreams(ctx, peerID, conn)
	log.Printf("mesh: connected to peer %s at %s", peerID, addr)
	return nil
}

// PeerIDs returns connected peer node IDs.
func (t *Transport) PeerIDs() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids := make([]string, 0, len(t.peers))
	for id := range t.peers {
		ids = append(ids, id)
	}
	return ids
}

// Close shuts down the listener and all peers.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.peers {
		_ = conn.CloseWithError(0, "shutdown")
		delete(t.peers, id)
	}
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

func (t *Transport) acceptLoop(ctx context.Context) {
	for {
		conn, err := t.listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("mesh: accept error: %v", err)
			return
		}
		go t.handleInbound(ctx, conn)
	}
}

func (t *Transport) handleInbound(ctx context.Context, conn *quic.Conn) {
	peerID, err := t.handshake(ctx, conn, false)
	if err != nil {
		log.Printf("mesh: inbound handshake failed: %v", err)
		_ = conn.CloseWithError(1, "handshake failed")
		return
	}

	t.addPeer(peerID, conn)
	go t.monitorPeer(ctx, peerID, conn)
	go t.serveRelayStreams(ctx, peerID, conn)
	log.Printf("mesh: accepted peer %s from %s", peerID, conn.RemoteAddr())
}

func (t *Transport) handshake(ctx context.Context, conn *quic.Conn, initiator bool) (string, error) {
	var stream *quic.Stream
	var err error
	if initiator {
		stream, err = conn.OpenStreamSync(ctx)
	} else {
		stream, err = conn.AcceptStream(ctx)
	}
	if err != nil {
		return "", fmt.Errorf("stream: %w", err)
	}
	defer stream.Close()

	reader := bufio.NewReader(stream)
	if initiator {
		if err := writeHello(stream, Hello{Type: "hello", NodeID: t.nodeID, Cluster: t.cluster}); err != nil {
			return "", err
		}
		ack, err := readHello(reader)
		if err != nil {
			return "", err
		}
		if ack.Type != "hello_ack" {
			return "", fmt.Errorf("unexpected message type %q", ack.Type)
		}
		if ack.Cluster != t.cluster {
			return "", fmt.Errorf("cluster mismatch: got %q want %q", ack.Cluster, t.cluster)
		}
		return ack.NodeID, nil
	}

	msg, err := readHello(reader)
	if err != nil {
		return "", err
	}
	if msg.Type != "hello" {
		return "", fmt.Errorf("unexpected message type %q", msg.Type)
	}
	if msg.Cluster != t.cluster {
		return "", fmt.Errorf("cluster mismatch: got %q want %q", msg.Cluster, t.cluster)
	}
	if err := writeHello(stream, Hello{Type: "hello_ack", NodeID: t.nodeID, Cluster: t.cluster}); err != nil {
		return "", err
	}
	return msg.NodeID, nil
}

func writeHello(w io.Writer, h Hello) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func readHello(r *bufio.Reader) (Hello, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return Hello{}, err
	}
	var h Hello
	if err := json.Unmarshal(line, &h); err != nil {
		return Hello{}, err
	}
	return h, nil
}

func (t *Transport) addPeer(id string, conn *quic.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, ok := t.peers[id]; ok {
		_ = old.CloseWithError(0, "replaced")
	}
	t.peers[id] = conn
}

func (t *Transport) monitorPeer(_ context.Context, peerID string, conn *quic.Conn) {
	<-conn.Context().Done()
	t.mu.Lock()
	if cur, ok := t.peers[peerID]; ok && cur == conn {
		delete(t.peers, peerID)
	}
	t.mu.Unlock()
	if t.onDisconnect != nil {
		t.onDisconnect(peerID)
	}
	log.Printf("mesh: peer %s disconnected", peerID)
}

// ResolveHost ensures join addresses work when binding to localhost.
func ResolveHost(hostport string) (string, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", err
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return hostport, nil
	}
	return net.JoinHostPort("127.0.0.1", port), nil
}
