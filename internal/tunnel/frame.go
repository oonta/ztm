package tunnel

import (
	"bufio"
	"context"
	"io"

	"ztm/internal/protocol"
)

// ConnectRequest is a client or mesh relay connect frame.
type ConnectRequest = protocol.ConnectRequest

// ConnectResponse is the connect acknowledgement frame.
type ConnectResponse = protocol.ConnectResponse

func WriteConnectRequest(w io.Writer, req ConnectRequest) error {
	return protocol.WriteConnectRequest(w, req)
}

func ReadConnectResponse(r *bufio.Reader) (ConnectResponse, error) {
	return protocol.ReadConnectResponse(r)
}

func WriteConnectResponse(w io.Writer, resp ConnectResponse) error {
	return protocol.WriteConnectResponse(w, resp)
}

func ReadConnectRequest(r *bufio.Reader) (ConnectRequest, error) {
	return protocol.ReadConnectRequest(r)
}

func Relay(a, b io.ReadWriteCloser) error {
	return protocol.Relay(a, b)
}

// PeerRelay opens cross-node mesh relay streams.
type PeerRelay interface {
	PeerIDs() []string
	OpenRelay(ctx context.Context, peerID string, req ConnectRequest) (io.ReadWriteCloser, error)
}
