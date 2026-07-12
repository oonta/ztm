package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"ztm/internal/client"
)

func main() {
	args := os.Args[1:]
	if len(args) < 1 || args[0] != "connect" {
		fmt.Fprintln(os.Stderr, "usage: ztm-client connect --node <host:port> --data-dir <dir> --client-id <id>")
		os.Exit(1)
	}
	args = args[1:]

	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	nodeAddr := fs.String("node", "", "edge node client address host:port (required)")
	cluster := fs.String("cluster", "default", "cluster name")
	clientID := fs.String("client-id", "", "client identity (required with --data-dir)")
	dataDir := fs.String("data-dir", "", "directory with CA and client certificates")
	certFile := fs.String("cert", "", "client certificate PEM")
	keyFile := fs.String("key", "", "client private key PEM")
	caFile := fs.String("ca", "", "cluster CA certificate PEM")
	socksListen := fs.String("socks-listen", "127.0.0.1:1080", "local SOCKS5 listen address")
	tunMode := fs.Bool("tun", false, "Linux TUN mode (requires root/CAP_NET_ADMIN)")
	tunDevice := fs.String("tun-device", "ztun0", "TUN interface name")
	tunCIDR := fs.String("tun-cidr", "100.127.0.0/24", "route CIDR through TUN")
	tunDNS := fs.String("tun-dns", "100.127.0.1", "DNS listen address for *.ztm resolution")
	_ = fs.Parse(args)

	if *nodeAddr == "" {
		fmt.Fprintln(os.Stderr, "error: --node is required")
		os.Exit(1)
	}
	if *clientID == "" && *certFile == "" {
		fmt.Fprintln(os.Stderr, "error: --client-id or --cert is required")
		os.Exit(1)
	}

	a := client.New(client.Config{
		NodeAddr:    *nodeAddr,
		Cluster:     *cluster,
		ClientID:    *clientID,
		DataDir:     *dataDir,
		CertFile:    *certFile,
		KeyFile:     *keyFile,
		CAFile:      *caFile,
		SocksListen: *socksListen,
		TUN:         *tunMode,
		TunDevice:   *tunDevice,
		TunCIDR:     *tunCIDR,
		TunDNS:      *tunDNS,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *tunMode {
		log.Printf("ztm-client: node=%s tun=%s cidr=%s", *nodeAddr, *tunDevice, *tunCIDR)
	} else {
		log.Printf("ztm-client: node=%s socks=%s", *nodeAddr, *socksListen)
	}
	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("client: %v", err)
	}
	log.Println("disconnecting")
}
