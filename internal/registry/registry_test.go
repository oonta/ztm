package registry

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMergeRemoteState(t *testing.T) {
	r := New("node1")
	peer := New("node2")

	svc := peer.RegisterLocal("api", "10.0.0.2", 8080, nil)
	data, err := peer.MarshalLocalState()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.MergeRemoteState(data); err != nil {
		t.Fatal(err)
	}

	list := r.List()
	if len(list) != 1 || list[0].Name != "api" || list[0].NodeID != "node2" {
		t.Fatalf("unexpected list: %+v", list)
	}

	newer := peer.RegisterLocal("api", "10.0.0.3", 9090, nil)
	if newer.Version <= svc.Version {
		t.Fatal("expected version bump")
	}
	data, _ = peer.MarshalLocalState()
	_ = r.MergeRemoteState(data)
	list = r.List()
	if len(list) != 1 || list[0].Port != 9090 {
		t.Fatalf("expected updated port, got %+v", list)
	}
}

func TestPruneExpired(t *testing.T) {
	r := New("node1")
	state := LocalState{
		NodeID: "node2",
		Services: []Service{{
			NodeID:    "node2",
			Name:      "api",
			Host:      "10.0.0.2",
			Port:      8080,
			Version:   1,
			TTL:       time.Second,
			TTLSec:    1,
			Announced: time.Now().Add(-2 * time.Second),
		}},
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	_ = r.MergeRemoteState(data)
	if removed := r.Prune(); removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if len(r.List()) != 0 {
		t.Fatal("expected empty list")
	}
}
