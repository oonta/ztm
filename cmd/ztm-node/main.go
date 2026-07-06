package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"ztm/internal/agent"
)

func main() {
	var (
		nodeID     = flag.String("node-id", "", "unique node identifier (required)")
		dataDir    = flag.String("data-dir", "./data", "directory for CA and node certificates")
		meshBind   = flag.String("mesh-bind", ":7444", "inter-node QUIC listen address")
		join       = flag.String("join", "", "seed node mesh address host:port")
		cluster    = flag.String("cluster", "default", "cluster name")
	)
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --node-id is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := agent.Config{
		NodeID:   *nodeID,
		Cluster:  *cluster,
		DataDir:  *dataDir,
		MeshBind: *meshBind,
		Join:     *join,
	}

	a, err := agent.New(cfg)
	if err != nil {
		log.Fatalf("agent: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := a.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("agent: %v", err)
	}
	log.Println("shutting down")
}
