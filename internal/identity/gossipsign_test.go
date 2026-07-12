package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureGossipSigner(t *testing.T) {
	dir := t.TempDir()
	store, err := Open("test", dir)
	if err != nil {
		t.Fatal(err)
	}

	s1, err := store.EnsureGossipSigner("node1")
	if err != nil {
		t.Fatal(err)
	}
	s2, err := store.EnsureGossipSigner("node1")
	if err != nil {
		t.Fatal(err)
	}
	if s1.PublicKeyBase64() != s2.PublicKeyBase64() {
		t.Fatal("expected stable gossip signing key")
	}

	payload := []byte("registry-state")
	sig := s1.Sign(payload)
	pub, err := ParseGossipPublicKey(s1.PublicKeyBase64())
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyGossipSignature(pub, payload, sig) {
		t.Fatal("signature verification failed")
	}

	keyPath := filepath.Join(dir, "nodes", "node1", gossipSignKeyFile)
	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("expected key file at %s: %v", keyPath, err)
	}
}
