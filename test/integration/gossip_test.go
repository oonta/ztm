package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"ztm/internal/admin"
	"ztm/internal/agent"
	"ztm/internal/registry"
)

func TestTwoNodeMeshViaGossip(t *testing.T) {
	dataDir := t.TempDir()
	cluster := "test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID:     "node1",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
	})
	waitAgent(t, node1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	node2 := startAgent(t, ctx2, agent.Config{
		NodeID:     "node2",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
		Join:       node1.GossipAddr(),
	})
	waitAgent(t, node2)

	waitForMembers(t, node1, 2)
	waitForMembers(t, node2, 2)
	waitForPeers(t, node1, node2)

	cancel1()
	cancel2()
}

func TestThreeNodeGossipAndServiceRegistry(t *testing.T) {
	dataDir := t.TempDir()
	cluster := "test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID:     "node1",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
	})
	waitAgent(t, node1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	_ = startAgent(t, ctx2, agent.Config{
		NodeID:     "node2",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
		Join:       node1.GossipAddr(),
	})

	ctx3, cancel3 := context.WithCancel(context.Background())
	node3 := startAgent(t, ctx3, agent.Config{
		NodeID:     "node3",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
		Join:       node1.GossipAddr(),
	})
	waitAgent(t, node3)

	waitForMembers(t, node1, 3)
	waitForMembers(t, node3, 3)

	if err := registerService(node1.AdminURL(), "api", "10.0.0.1", 8080); err != nil {
		t.Fatalf("register: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if services, err := fetchServices(node3.AdminURL()); err == nil && hasService(services, "api", "node1") {
			cancel1()
			cancel2()
			cancel3()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	services, _ := fetchServices(node3.AdminURL())
	t.Fatalf("node3 did not learn service api; services=%v", services)

	cancel1()
	cancel2()
	cancel3()
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

func waitAgent(t *testing.T, a *agent.Agent) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.MeshAddr() != "" && a.GossipAddr() != "" && a.AdminAddr() != "" {
			if err := admin.WaitReady(a.AdminURL(), time.Second); err == nil {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("agent not ready: mesh=%s gossip=%s admin=%s", a.MeshAddr(), a.GossipAddr(), a.AdminAddr())
}

func waitForMembers(t *testing.T, a *agent.Agent, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if a.MemberCount() >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("member count on %s: got %d want >= %d", a.AdminAddr(), a.MemberCount(), want)
}

func waitForPeers(t *testing.T, node1, node2 *agent.Agent) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		p1 := node1.PeerIDs()
		p2 := node2.PeerIDs()
		if len(p1) >= 1 && len(p2) >= 1 && contains(p1, "node2") && contains(p2, "node1") {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peers not connected: node1=%v node2=%v", node1.PeerIDs(), node2.PeerIDs())
}

func registerService(baseURL, name, host string, port uint32) error {
	payload := map[string]any{"name": name, "host": host, "port": port}
	data, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+"/v1/services/register", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func fetchServices(baseURL string) ([]registry.Service, error) {
	resp, err := http.Get(baseURL + "/v1/services")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var services []registry.Service
	if err := json.Unmarshal(body, &services); err != nil {
		return nil, err
	}
	return services, nil
}

func hasService(services []registry.Service, name, nodeID string) bool {
	for _, s := range services {
		if s.Name == name && s.NodeID == nodeID {
			return true
		}
	}
	return false
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
