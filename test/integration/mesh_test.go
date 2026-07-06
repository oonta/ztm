package integration

import (
	"context"
	"testing"
	"time"

	"ztm/internal/agent"
)

func TestTwoNodeMesh(t *testing.T) {
	dataDir := t.TempDir()
	cluster := "test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID:   "node1",
		Cluster:  cluster,
		DataDir:  dataDir,
		MeshBind: "127.0.0.1:0",
	})

	waitMeshAddr(t, node1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	node2 := startAgent(t, ctx2, agent.Config{
		NodeID:   "node2",
		Cluster:  cluster,
		DataDir:  dataDir,
		MeshBind: "127.0.0.1:0",
		Join:     node1.MeshAddr(),
	})
	_ = ctx2

	waitForPeers(t, node1, node2)

	cancel1()
	cancel2()
}

func startAgent(t *testing.T, ctx context.Context, cfg agent.Config) *agent.Agent {
	t.Helper()
	a, err := agent.New(cfg)
	if err != nil {
		t.Fatalf("agent new: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("agent shutdown timeout")
		}
	})
	return a
}

func waitMeshAddr(t *testing.T, a *agent.Agent) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.MeshAddr() != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("mesh address not ready")
}

func waitForPeers(t *testing.T, node1, node2 *agent.Agent) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p1 := node1.PeerIDs()
		p2 := node2.PeerIDs()
		if len(p1) == 1 && len(p2) == 1 && contains(p1, "node2") && contains(p2, "node1") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peers not connected: node1=%v node2=%v", node1.PeerIDs(), node2.PeerIDs())
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
