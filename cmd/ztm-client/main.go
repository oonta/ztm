package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		nodeAddr    = flag.String("node", "", "edge node address host:port (required)")
		certFile    = flag.String("cert", "", "client certificate PEM")
		keyFile     = flag.String("key", "", "client private key PEM")
		caFile      = flag.String("ca", "", "cluster CA certificate PEM")
		socksListen = flag.String("socks-listen", "127.0.0.1:1080", "local SOCKS5 listen address")
	)
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 || args[0] != "connect" {
		fmt.Fprintln(os.Stderr, "usage: ztm-client connect --node <host:port> [--cert ...] [--key ...] [--ca ...]")
		os.Exit(1)
	}

	if *nodeAddr == "" {
		fmt.Fprintln(os.Stderr, "error: --node is required")
		os.Exit(1)
	}

	log.Printf("ztm-client connect: node=%s socks=%s cert=%s key=%s ca=%s",
		*nodeAddr, *socksListen, *certFile, *keyFile, *caFile)
	log.Println("status: skeleton only — SOCKS5 tunnel not implemented yet")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("disconnecting")
}
