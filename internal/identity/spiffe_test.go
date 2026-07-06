package identity_test

import (
	"net/url"
	"testing"

	"ztm/internal/identity"
)

func TestNodeURIRoundTrip(t *testing.T) {
	raw := identity.NodeURI("prod", "edge-1")
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	id, ok := identity.ParseNodeID("prod", []*url.URL{u})
	if !ok || id != "edge-1" {
		t.Fatalf("ParseNodeID = %q, %v", id, ok)
	}
}

func TestEnsureNodeCert(t *testing.T) {
	dir := t.TempDir()
	store, err := identity.Open("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureNodeCert("node1"); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureNodeCert("node1"); err != nil {
		t.Fatal(err)
	}
	cert, err := store.NodeTLSCertificate("node1")
	if err != nil || len(cert.Certificate) == 0 {
		t.Fatalf("node cert: %v", err)
	}
}
