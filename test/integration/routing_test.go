package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"ztm/internal/agent"
	"ztm/internal/client"
	"ztm/internal/identity"
)

func TestRoutingFailoverToSecondInstance(t *testing.T) {
	echo, echoAddr := startEchoServer(t)
	defer echo.Close()
	_, portStr, _ := net.SplitHostPort(echoAddr)
	echoPort := atoi(portStr)

	dataDir := t.TempDir()
	cluster := "test"

	store, err := identity.Open(cluster, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureClientCert("alice"); err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID: "node1", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
	})
	waitAgent(t, node1)
	waitForClientAddr(t, node1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	node2 := startAgent(t, ctx2, agent.Config{
		NodeID: "node2", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
		Join: node1.GossipAddr(),
	})
	waitAgent(t, node2)

	ctx3, cancel3 := context.WithCancel(context.Background())
	node3 := startAgent(t, ctx3, agent.Config{
		NodeID: "node3", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
		Join: node1.GossipAddr(),
	})
	waitAgent(t, node3)

	waitForMembers(t, node1, 3)
	waitForPeers(t, node1, node2)
	waitForPeers(t, node1, node3)

	// node2 is tried first (lexicographic); backend is down.
	if err := registerService(node2.AdminURL(), "api.ha", "127.0.0.1", 59999); err != nil {
		t.Fatalf("register node2: %v", err)
	}
	if err := registerService(node3.AdminURL(), "api.ha", "127.0.0.1", echoPort); err != nil {
		t.Fatalf("register node3: %v", err)
	}
	waitForService(t, node1, "api.ha", "node2")
	waitForService(t, node1, "api.ha", "node3")

	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	go func() {
		c := client.New(client.Config{
			NodeAddr:    node1.ClientAddr(),
			Cluster:     cluster,
			DataDir:     dataDir,
			ClientID:    "alice",
			SocksListen: "127.0.0.1:19182",
		})
		_ = c.Run(clientCtx)
	}()
	waitForTCP(t, "127.0.0.1:19182")

	conn, err := net.Dial("tcp", "127.0.0.1:19182")
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	if err := socks5Connect(conn, "api.ha", uint16(echoPort)); err != nil {
		t.Fatalf("socks connect: %v", err)
	}
	if _, err := io.WriteString(conn, "failover\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "failover\n" {
		t.Fatalf("echo got %q", line)
	}

	clientCancel()
	cancel1()
	cancel2()
	cancel3()
}

func TestGracefulLeaveRemovesServices(t *testing.T) {
	dataDir := t.TempDir()
	cluster := "test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID: "node1", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
	})
	waitAgent(t, node1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	node2 := startAgent(t, ctx2, agent.Config{
		NodeID: "node2", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
		Join: node1.GossipAddr(),
	})
	waitAgent(t, node2)
	waitForMembers(t, node1, 2)

	if err := registerService(node2.AdminURL(), "bye.local", "127.0.0.1", 9000); err != nil {
		t.Fatalf("register: %v", err)
	}
	waitForService(t, node1, "bye.local", "node2")

	cancel2()
	waitForServiceGone(t, node1, "bye.local", "node2")

	cancel1()
}

func TestDeregisterAPI(t *testing.T) {
	dataDir := t.TempDir()
	cluster := "test"

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID: "node1", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
	})
	waitAgent(t, node1)

	if err := registerService(node1.AdminURL(), "tmp.local", "127.0.0.1", 9000); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := deregisterService(node1.AdminURL(), "tmp.local"); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	services, err := fetchServices(node1.AdminURL())
	if err != nil {
		t.Fatal(err)
	}
	if hasService(services, "tmp.local", "node1") {
		t.Fatalf("service still listed: %+v", services)
	}

	cancel1()
}

func deregisterService(baseURL, name string) error {
	payload := map[string]any{"name": name}
	data, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+"/v1/services/deregister", "application/json", bytes.NewReader(data))
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

func waitForServiceGone(t *testing.T, a *agent.Agent, name, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		services, err := fetchServices(a.AdminURL())
		if err == nil && !hasService(services, name, nodeID) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("service %s on %s still visible on %s", name, nodeID, a.AdminAddr())
}

func TestCrossNodeServiceViaMeshRelay(t *testing.T) {
	echo, echoAddr := startEchoServer(t)
	defer echo.Close()

	dataDir := t.TempDir()
	cluster := "test"

	store, err := identity.Open(cluster, dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureClientCert("alice"); err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	node1 := startAgent(t, ctx1, agent.Config{
		NodeID:     "node1",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
	})
	waitAgent(t, node1)
	waitForClientAddr(t, node1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	node2 := startAgent(t, ctx2, agent.Config{
		NodeID:     "node2",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
		Join:       node1.GossipAddr(),
	})
	waitAgent(t, node2)

	waitForMembers(t, node1, 2)
	waitForPeers(t, node1, node2)

	_, portStr, _ := net.SplitHostPort(echoAddr)
	port := atoi(portStr)
	if err := registerService(node2.AdminURL(), "echo.remote", "127.0.0.1", port); err != nil {
		t.Fatalf("register: %v", err)
	}

	waitForService(t, node1, "echo.remote", "node2")

	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	go func() {
		c := client.New(client.Config{
			NodeAddr:    node1.ClientAddr(),
			Cluster:     cluster,
			DataDir:     dataDir,
			ClientID:    "alice",
			SocksListen: "127.0.0.1:19181",
		})
		_ = c.Run(clientCtx)
	}()

	waitForTCP(t, "127.0.0.1:19181")

	conn, err := net.Dial("tcp", "127.0.0.1:19181")
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	if err := socks5Connect(conn, "echo.remote", uint16(port)); err != nil {
		t.Fatalf("socks connect: %v", err)
	}

	if _, err := io.WriteString(conn, "cross-node\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "cross-node\n" {
		t.Fatalf("echo got %q", line)
	}

	clientCancel()
	cancel1()
	cancel2()
}

func waitForClientAddr(t *testing.T, a *agent.Agent) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a.ClientAddr() != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("client tunnel address not ready on %s", a.AdminAddr())
}

func waitForService(t *testing.T, a *agent.Agent, name, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		services, err := fetchServices(a.AdminURL())
		if err == nil && hasService(services, name, nodeID) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("service %s on %s not visible on %s", name, nodeID, a.AdminAddr())
}

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("tcp %s not listening", addr)
}
