package metrics

import (
	"io"

	"ztm/internal/protocol"
)

type countingRW struct {
	io.ReadWriteCloser
	onRead func(int64)
}

func (c *countingRW) Read(p []byte) (int, error) {
	n, err := c.ReadWriteCloser.Read(p)
	if n > 0 && c.onRead != nil {
		c.onRead(int64(n))
	}
	return n, err
}

// Relay copies between endpoints and counts bytes read on both sides.
func Relay(a, b io.ReadWriteCloser, c *Collector) error {
	if c == nil {
		return protocol.Relay(a, b)
	}
	wrappedA := &countingRW{ReadWriteCloser: a, onRead: c.AddRelayBytes}
	wrappedB := &countingRW{ReadWriteCloser: b, onRead: c.AddRelayBytes}
	return protocol.Relay(wrappedA, wrappedB)
}
