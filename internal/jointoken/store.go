package jointoken

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const defaultRole = "node"

type entry struct {
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Used      bool      `json:"used"`
}

// Store manages one-time join tokens on disk.
type Store struct {
	path string
	mu   sync.Mutex
	data map[string]entry
}

// Open loads or creates a token store in dataDir.
func Open(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	s := &Store{
		path: filepath.Join(dataDir, "join_tokens.json"),
		data: make(map[string]entry),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Create issues a new one-time token valid for ttl.
func (s *Store) Create(role string, ttl time.Duration) (string, error) {
	if role == "" {
		role = defaultRole
	}
	if ttl <= 0 {
		ttl = time.Hour
	}

	token, err := randomToken()
	if err != nil {
		return "", err
	}

	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[token] = entry{
		Role:      role,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	if err := s.saveLocked(); err != nil {
		return "", err
	}
	return token, nil
}

// Consume validates and marks a token as used.
func (s *Store) Consume(token string) error {
	if token == "" {
		return fmt.Errorf("token is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[token]
	if !ok {
		return fmt.Errorf("invalid token")
	}
	if e.Used {
		return fmt.Errorf("token already used")
	}
	if time.Now().After(e.ExpiresAt) {
		return fmt.Errorf("token expired")
	}
	e.Used = true
	s.data[token] = e
	return s.saveLocked()
}

func (s *Store) load() error {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(raw, &s.data)
}

func (s *Store) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, raw, 0o600)
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
