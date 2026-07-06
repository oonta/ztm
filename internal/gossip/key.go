package gossip

import (
	"crypto/rand"
	"os"
	"path/filepath"
)

// LoadOrCreateKey loads a 32-byte gossip encryption key from dir.
func LoadOrCreateKey(dir string) ([]byte, error) {
	path := filepath.Join(dir, "gossip.key")
	if data, err := os.ReadFile(path); err == nil && len(data) == 32 {
		return data, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
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
