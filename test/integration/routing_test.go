package integration

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"ztm/internal/agent"
	"ztm/internal/client"
	"ztm/internal/identity"
)

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
