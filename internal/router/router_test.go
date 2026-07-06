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
