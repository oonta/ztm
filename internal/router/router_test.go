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
	got, ok := Select(services, "node1", []string{"node2"}, nil)
	if !ok || got.NodeID != "node1" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestSelectRemoteOnConnectedPeer(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Host: "127.0.0.1", Port: 9000},
	}
	got, ok := Select(services, "node1", []string{"node2"}, nil)
	if !ok || got.NodeID != "node2" || got.Port != 9000 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestSelectSkipsUnreachablePeer(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Host: "127.0.0.1", Port: 9000},
	}
	_, ok := Select(services, "node1", nil, nil)
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
	got := Candidates(services, "node1", []string{"node2", "node3"}, nil)
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
	got := Candidates(services, "node1", []string{"node3"}, nil)
	if len(got) != 1 || got[0].NodeID != "node3" {
		t.Fatalf("got %+v", got)
	}
}

func TestCandidatesWeightedByErrorRate(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Port: 2},
		{NodeID: "node3", Name: "api", Port: 3},
	}
	loads := map[string]NodeLoad{
		"node2": {ErrorRate: 0.8, ActiveStreams: 1},
		"node3": {ErrorRate: 0.1, ActiveStreams: 1},
	}
	got := Candidates(services, "node1", []string{"node2", "node3"}, loads)
	if len(got) != 2 || got[0].NodeID != "node3" || got[1].NodeID != "node2" {
		t.Fatalf("got %+v", got)
	}
}

func TestCandidatesWeightedByActiveStreams(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Port: 2},
		{NodeID: "node3", Name: "api", Port: 3},
	}
	loads := map[string]NodeLoad{
		"node2": {ErrorRate: 0.1, ActiveStreams: 50},
		"node3": {ErrorRate: 0.1, ActiveStreams: 2},
	}
	got := Candidates(services, "node1", []string{"node2", "node3"}, loads)
	if len(got) != 2 || got[0].NodeID != "node3" || got[1].NodeID != "node2" {
		t.Fatalf("got %+v", got)
	}
}

func TestSelectWeightedRemote(t *testing.T) {
	services := []registry.Service{
		{NodeID: "node2", Name: "api", Port: 2},
		{NodeID: "node3", Name: "api", Port: 3},
	}
	loads := map[string]NodeLoad{
		"node2": {ErrorRate: 0.9},
		"node3": {ErrorRate: 0.0},
	}
	got, ok := Select(services, "node1", []string{"node2", "node3"}, loads)
	if !ok || got.NodeID != "node3" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}
