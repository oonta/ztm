package metrics

import (
	"io"
	"sync/atomic"

	"ztm/internal/protocol"
)

// RelaySnapshot holds relay load metrics exposed via RelayStats RPC.
type RelaySnapshot struct {
	BytesIn       uint64
	BytesOut      uint64
	ActiveStreams uint32
	ErrorRate     float32
}

func (c *Collector) SnapshotRelay() RelaySnapshot {
	if c == nil {
		return RelaySnapshot{}
	}
	success := atomic.LoadUint64(&c.relaySuccess)
	failures := atomic.LoadUint64(&c.relayFailures)
	total := success + failures
	var rate float32
	if total > 0 {
		rate = float32(failures) / float32(total)
	}
	return RelaySnapshot{
		BytesIn:       atomic.LoadUint64(&c.relayBytesIn),
		BytesOut:      atomic.LoadUint64(&c.relayBytesOut),
		ActiveStreams: uint32(atomic.LoadInt32(&c.tunnelStreamsActive) + atomic.LoadInt32(&c.meshRelayActive)),
		ErrorRate:     rate,
	}
}

func (c *Collector) RecordRelaySuccess() {
	if c != nil {
		atomic.AddUint64(&c.relaySuccess, 1)
	}
}

func (c *Collector) RecordRelayFailure() {
	if c != nil {
		atomic.AddUint64(&c.relayFailures, 1)
	}
}

func (c *Collector) MeshRelayStarted() {
	if c != nil {
		atomic.AddInt32(&c.meshRelayActive, 1)
	}
}

func (c *Collector) MeshRelayFinished() {
	if c != nil {
		atomic.AddInt32(&c.meshRelayActive, -1)
	}
}

func (c *Collector) addRelayBytesIn(n int64) {
	if c != nil && n > 0 {
		atomic.AddUint64(&c.relayBytesIn, uint64(n))
		c.RelayBytes.Add(float64(n))
	}
}

func (c *Collector) addRelayBytesOut(n int64) {
	if c != nil && n > 0 {
		atomic.AddUint64(&c.relayBytesOut, uint64(n))
		c.RelayBytes.Add(float64(n))
	}
}

// Relay copies between endpoints and tracks directional relay bytes.
func Relay(client, backend io.ReadWriteCloser, c *Collector) error {
	if c == nil {
		return protocol.Relay(client, backend)
	}
	errCh := make(chan error, 2)
	go func() {
		_, err := copyCount(backend, client, c.addRelayBytesOut)
		errCh <- err
	}()
	go func() {
		_, err := copyCount(client, backend, c.addRelayBytesIn)
		errCh <- err
	}()
	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil && err != io.EOF && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		c.RecordRelaySuccess()
	} else {
		c.RecordRelayFailure()
	}
	return firstErr
}

func copyCount(dst io.Writer, src io.Reader, add func(int64)) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
				add(int64(nw))
			}
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			return written, er
		}
	}
}
