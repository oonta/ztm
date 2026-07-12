package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
)

const gossipSignKeyFile = "gossip_sign.key"

// GossipSigner signs gossip registry payloads with an Ed25519 node key.
type GossipSigner struct {
	nodeID string
	key    ed25519.PrivateKey
}

// EnsureGossipSigner loads or creates the per-node Ed25519 gossip signing key.
func (s *Store) EnsureGossipSigner(nodeID string) (*GossipSigner, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("node id is required")
	}
	nodeDir := filepath.Join(s.dir, "nodes", nodeID)
	if err := os.MkdirAll(nodeDir, 0o700); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(nodeDir, gossipSignKeyFile)

	if data, err := os.ReadFile(keyPath); err == nil {
		if len(data) != ed25519.PrivateKeySize {
			return nil, fmt.Errorf("invalid gossip signing key size for %s", nodeID)
		}
		key := ed25519.PrivateKey(data)
		return &GossipSigner{nodeID: nodeID, key: key}, nil
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		return nil, err
	}
	return &GossipSigner{nodeID: nodeID, key: key}, nil
}

func (g *GossipSigner) NodeID() string { return g.nodeID }

func (g *GossipSigner) PublicKeyBase64() string {
	return base64.StdEncoding.EncodeToString(g.key.Public().(ed25519.PublicKey))
}

func (g *GossipSigner) Sign(payload []byte) []byte {
	return ed25519.Sign(g.key, payload)
}

// ParseGossipPublicKey decodes a base64 Ed25519 public key from node metadata.
func ParseGossipPublicKey(encoded string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode gossip public key: %w", err)
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid gossip public key length")
	}
	return ed25519.PublicKey(raw), nil
}

// EncodeGossipPublicKey encodes an Ed25519 public key for node metadata.
func EncodeGossipPublicKey(pub ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(pub)
}

// VerifyGossipSignature checks an Ed25519 signature over payload.
func VerifyGossipSignature(pub ed25519.PublicKey, payload, sig []byte) bool {
	return ed25519.Verify(pub, payload, sig)
}
