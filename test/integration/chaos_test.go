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

// TestChaosAbruptNodeKillFailover verifies routing survives an abrupt node death
// by failing over to a healthy peer instance.
func TestChaosAbruptNodeKillFailover(t *testing.T) {
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
			SocksListen: "127.0.0.1:19201",
		})
		_ = c.Run(clientCtx)
	}()
	waitForTCP(t, "127.0.0.1:19201")

	// Abruptly kill node2 (no graceful leave).
	cancel2()
	waitForPeerGone(t, node1, "node2")

	conn, err := net.Dial("tcp", "127.0.0.1:19201")
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	if err := socks5Connect(conn, "api.ha", uint16(echoPort)); err != nil {
		t.Fatalf("socks connect: %v", err)
	}
	if _, err := io.WriteString(conn, "after-kill\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "after-kill\n" {
		t.Fatalf("echo got %q", line)
	}

	cancel1()
	cancel3()
}

// TestChaosNodeRestartRejoinsMesh verifies a replaced node can rejoin the mesh.
func TestChaosNodeRestartRejoinsMesh(t *testing.T) {
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
	waitForPeers(t, node1, node2)

	cancel2()
	waitForPeerGone(t, node1, "node2")
	waitForMemberCount(t, node1, 1)

	ctx2b, cancel2b := context.WithCancel(context.Background())
	node2b := startAgent(t, ctx2b, agent.Config{
		NodeID: "node2", Cluster: cluster, DataDir: dataDir,
		MeshBind: "127.0.0.1:0", ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0", AdminBind: "127.0.0.1:0",
		Join: node1.GossipAddr(),
	})
	waitAgent(t, node2b)
	waitForMemberCount(t, node1, 2)
	waitForPeersDeadline(t, node1, node2b, "node2", "node1", 45*time.Second)

	cancel2b()
	cancel1()
}

func waitForPeerGone(t *testing.T, a *agent.Agent, peerID string) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if !contains(a.PeerIDs(), peerID) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("peer %s still connected on %s: %v", peerID, a.AdminAddr(), a.PeerIDs())
}

func waitForMemberCount(t *testing.T, a *agent.Agent, want int) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if a.MemberCount() == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("member count on %s: got %d want %d", a.AdminAddr(), a.MemberCount(), want)
}

func waitForPeersDeadline(t *testing.T, node1, node2 *agent.Agent, peerOn1, peerOn2 string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p1 := node1.PeerIDs()
		p2 := node2.PeerIDs()
		if contains(p1, peerOn1) && contains(p2, peerOn2) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("peers not connected: node1=%v node2=%v", node1.PeerIDs(), node2.PeerIDs())
}
