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

func TestClientTunnelSOCKS5(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := startAgent(t, ctx, agent.Config{
		NodeID:     "node1",
		Cluster:    cluster,
		DataDir:    dataDir,
		MeshBind:   "127.0.0.1:0",
		ClientBind: "127.0.0.1:0",
		GossipBind: "127.0.0.1:0",
		AdminBind:  "127.0.0.1:0",
	})
	waitAgent(t, node)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if node.ClientAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if node.ClientAddr() == "" {
		t.Fatal("client tunnel address not ready")
	}

	_, portStr, _ := net.SplitHostPort(echoAddr)
	port := atoi(portStr)
	if err := registerService(node.AdminURL(), "echo.local", "127.0.0.1", port); err != nil {
		t.Fatalf("register: %v", err)
	}

	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	clientDone := make(chan error, 1)
	go func() {
		c := client.New(client.Config{
			NodeAddr:    node.ClientAddr(),
			Cluster:     cluster,
			DataDir:     dataDir,
			ClientID:    "alice",
			SocksListen: "127.0.0.1:19180",
		})
		clientDone <- c.Run(clientCtx)
	}()

	socksDeadline := time.Now().Add(10 * time.Second)
	var socksOK bool
	for time.Now().Before(socksDeadline) {
		conn, err := net.Dial("tcp", "127.0.0.1:19180")
		if err == nil {
			conn.Close()
			socksOK = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !socksOK {
		t.Fatal("socks5 not listening")
	}

	conn, err := net.Dial("tcp", "127.0.0.1:19180")
	if err != nil {
		t.Fatalf("dial socks: %v", err)
	}
	defer conn.Close()

	if err := socks5Connect(conn, "echo.local", uint16(port)); err != nil {
		t.Fatalf("socks connect: %v", err)
	}

	if _, err := io.WriteString(conn, "ping\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "ping\n" {
		t.Fatalf("echo got %q", line)
	}

	clientCancel()
	cancel()
}

func startEchoServer(t *testing.T) (net.Listener, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()
	return ln, ln.Addr().String()
}

func socks5Connect(conn net.Conn, host string, port uint16) error {
	req := []byte{0x05, 0x01, 0x00}
	_, err := conn.Write(req)
	if err != nil {
		return err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	buf := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	buf = append(buf, host...)
	p := make([]byte, 2)
	p[0] = byte(port >> 8)
	p[1] = byte(port)
	buf = append(buf, p...)
	if _, err := conn.Write(buf); err != nil {
		return err
	}
	reply := make([]byte, 10)
	_, err = io.ReadFull(conn, reply)
	return err
}

func atoi(s string) uint32 {
	var n uint32
	for i := 0; i < len(s); i++ {
		n = n*10 + uint32(s[i]-'0')
	}
	return n
}
