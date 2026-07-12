package gossip

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"

	"ztm/internal/identity"
	"ztm/internal/registry"
)

type testSigner struct {
	nodeID string
	key    ed25519.PrivateKey
}

func (s *testSigner) NodeID() string { return s.nodeID }

func (s *testSigner) PublicKeyBase64() string {
	return identity.EncodeGossipPublicKey(s.key.Public().(ed25519.PublicKey))
}

func (s *testSigner) Sign(payload []byte) []byte {
	return ed25519.Sign(s.key, payload)
}

func newTestSigner(t *testing.T, nodeID string) *testSigner {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &testSigner{nodeID: nodeID, key: key}
}

func TestSignAndVerifyEnvelope(t *testing.T) {
	signer := newTestSigner(t, "node1")
	reg := registry.New("node1")
	reg.RegisterLocal("api", "127.0.0.1", 8080, nil)
	payload, err := reg.MarshalLocalState()
	if err != nil {
		t.Fatal(err)
	}

	wire, err := signEnvelope("test", payload, signer)
	if err != nil {
		t.Fatal(err)
	}

	pub, _ := identity.ParseGossipPublicKey(signer.PublicKeyBase64())
	got, err := unwrapEnvelope("test", wire, func(nodeID string) (ed25519.PublicKey, bool) {
		if nodeID == "node1" {
			return pub, true
		}
		return nil, false
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestRejectUnsignedLegacyJSON(t *testing.T) {
	state := registry.LocalState{NodeID: "node2", Services: []registry.Service{{NodeID: "node2", Name: "api"}}}
	raw, _ := json.Marshal(state)
	_, err := unwrapEnvelope("test", raw, func(string) (ed25519.PublicKey, bool) { return nil, false })
	if err == nil {
		t.Fatal("expected unsigned legacy payload to be rejected")
	}
}

func TestRejectBadSignature(t *testing.T) {
	signer := newTestSigner(t, "node1")
	other := newTestSigner(t, "node2")
	reg := registry.New("node1")
	reg.RegisterLocal("api", "127.0.0.1", 8080, nil)
	payload, _ := reg.MarshalLocalState()
	wire, err := signEnvelope("test", payload, signer)
	if err != nil {
		t.Fatal(err)
	}

	otherPub, _ := identity.ParseGossipPublicKey(other.PublicKeyBase64())
	_, err = unwrapEnvelope("test", wire, func(nodeID string) (ed25519.PublicKey, bool) {
		return otherPub, true
	})
	if err == nil {
		t.Fatal("expected signature verification to fail")
	}
}
