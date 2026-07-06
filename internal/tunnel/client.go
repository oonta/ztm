package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"

	"github.com/quic-go/quic-go"
)

// Client maintains a QUIC session to an edge node.
type Client struct {
	conn    *quic.Conn
	tlsConf *tls.Config
}

// Dial connects to an edge node client listener.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config) (*Client, error) {
	conn, err := quic.DialAddr(ctx, addr, tlsConf, nil)
	if err != nil {
		return nil, fmt.Errorf("quic dial: %w", err)
	}
	return &Client{conn: conn, tlsConf: tlsConf}, nil
}

// Close closes the QUIC connection.
func (c *Client) Close() error {
	return c.conn.CloseWithError(0, "client shutdown")
}

// OpenStream dials a target service through the tunnel.
func (c *Client) OpenStream(ctx context.Context, clientIdentity, service string, port uint32) (*quic.Stream, error) {
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	req := ConnectRequest{
		TargetService:  service,
		TargetPort:     port,
		ClientIdentity: clientIdentity,
	}
	if err := WriteConnectRequest(stream, req); err != nil {
		stream.Close()
		return nil, err
	}

	resp, err := ReadConnectResponse(bufio.NewReader(stream))
	if err != nil {
		stream.Close()
		return nil, err
	}
	if !resp.OK {
		stream.Close()
		return nil, fmt.Errorf("connect denied: %s", resp.Reason)
	}
	return stream, nil
}
