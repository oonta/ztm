package router

import (
	"testing"

	"ztm/internal/registry"
)

func TestSelectPrefersLocal(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Host: "10.0.0.2", Port: 80},
		{NodeID: "node1", Name: "api", Host: "127.0.0.1", Port: 8080},
	}
	got, ok := Select(services, "node1", []string{"node2"})
	if !ok || got.NodeID != "node1" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestSelectRemoteOnConnectedPeer(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Host: "127.0.0.1", Port: 9000},
	}
	got, ok := Select(services, "node1", []string{"node2"})
	if !ok || got.NodeID != "node2" || got.Port != 9000 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestSelectSkipsUnreachablePeer(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Host: "127.0.0.1", Port: 9000},
	}
	_, ok := Select(services, "node1", nil)
	if ok {
		t.Fatal("expected no route when peer not connected")
	}
}

func TestCandidatesOrdersLocalThenRemotes(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node3", Name: "api", Port: 3},
		{NodeID: "node1", Name: "api", Port: 1},
		{NodeID: "node2", Name: "api", Port: 2},
	}
	got := Candidates(services, "node1", []string{"node2", "node3"})
	if len(got) != 3 {
		t.Fatalf("got %d candidates: %+v", len(got), got)
	}
	if got[0].NodeID != "node1" || got[1].NodeID != "node2" || got[2].NodeID != "node3" {
		t.Fatalf("order: %+v", got)
	}
}

func TestCandidatesExcludesUnreachable(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Port: 2},
		{NodeID: "node3", Name: "api", Port: 3},
	}
	got := Candidates(services, "node1", []string{"node3"})
	if len(got) != 1 || got[0].NodeID != "node3" {
		t.Fatalf("got %+v", got)
	}
}
