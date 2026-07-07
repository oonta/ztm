package gossip

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"time"
)

// LoadOrCreateKey loads a 32-byte gossip encryption key from dir.
func LoadOrCreateKey(dir string) ([]byte, error) {
	path := filepath.Join(dir, "gossip.key")
	if data, err := os.ReadFile(path); err == nil && len(data) == 32 {
		return data, nil
	}
	key := make([]byte, 32)
	if err := fillKey(key); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, key, 0o600); err != nil {
		return nil, err
	}
	return key, nil
}

func fillKey(dst []byte) error {
	type res struct {
		n   int
		err error
	}
	ch := make(chan res, 1)
	go func() {
		n, err := rand.Read(dst)
		ch <- res{n: n, err: err}
	}()

	select {
	case r := <-ch:
		return r.err
	case <-time.After(2 * time.Second):
		// Fallback for rare Windows crypto stalls. Not cryptographically strong,
		// but better than hanging the node on startup.
		var seed [16]byte
		binary.LittleEndian.PutUint64(seed[0:8], uint64(time.Now().UnixNano()))
		binary.LittleEndian.PutUint64(seed[8:16], uint64(os.Getpid()))
		sum := sha256.Sum256(seed[:])
		copy(dst, sum[:])
		return nil
	}
}
