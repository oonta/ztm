package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"ztm/internal/agent"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "run":
			runServer(os.Args[2:])
			return
		case "members":
			cliGet(os.Args[2:], "/v1/members")
			return
		case "status":
			cliGet(os.Args[2:], "/v1/status")
			return
		case "services":
			cliGet(os.Args[2:], "/v1/services")
			return
		case "register":
			cliRegister(os.Args[2:])
			return
		case "help", "-h", "--help":
			usage()
			return
		}
		if os.Args[1][0] != '-' {
			fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
			usage()
			os.Exit(1)
		}
	}
	runServer(os.Args[1:])
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  ztm-node [run] [flags]                 start node (default)
  ztm-node members  --admin-url <url>    list gossip members
  ztm-node status   --admin-url <url>    node status
  ztm-node services --admin-url <url>    list services
  ztm-node register --admin-url <url> --name <n> --port <p> [--host <h>]

Run flags:
`)
	flag.CommandLine.SetOutput(os.Stderr)
	flag.PrintDefaults()
}

func runServer(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	nodeID := fs.String("node-id", "", "unique node identifier (required)")
	dataDir := fs.String("data-dir", "./data", "directory for CA and node certificates")
	meshBind := fs.String("mesh-bind", ":7444", "inter-node QUIC listen address")
	gossipBind := fs.String("gossip-bind", ":7946", "gossip (memberlist) bind address")
	adminBind := fs.String("admin-bind", "127.0.0.1:0", "admin HTTP API listen address (:0 = random free port)")
	join := fs.String("join", "", "seed node gossip address host:port")
	cluster := fs.String("cluster", "default", "cluster name")
	_ = fs.Parse(args)

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --node-id is required")
		fs.Usage()
		os.Exit(1)
	}

	cfg := agent.Config{
		NodeID:     *nodeID,
		Cluster:    *cluster,
		DataDir:    *dataDir,
		MeshBind:   *meshBind,
		GossipBind: *gossipBind,
		AdminBind:  *adminBind,
		Join:       *join,
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

func cliGet(args []string, path string) {
	url := parseAdminURL(args)
	resp, err := http.Get(url + path)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var out bytes.Buffer
	if err := json.Indent(&out, body, "", "  "); err != nil {
		fmt.Println(string(body))
		return
	}
	fmt.Println(out.String())
}

func cliRegister(args []string) {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	url := fs.String("admin-url", "http://127.0.0.1:8080", "admin API base URL")
	name := fs.String("name", "", "service name (required)")
	host := fs.String("host", "127.0.0.1", "backend host")
	port := fs.Uint("port", 0, "backend port (required)")
	_ = fs.Parse(args)

	if *name == "" || *port == 0 {
		fmt.Fprintln(os.Stderr, "error: --name and --port are required")
		os.Exit(1)
	}

	payload := map[string]any{
		"name": *name,
		"host": *host,
		"port": *port,
	}
	data, _ := json.Marshal(payload)
	resp, err := http.Post(*url+"/v1/services/register", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	var out bytes.Buffer
	_ = json.Indent(&out, body, "", "  ")
	fmt.Println(out.String())
}

func parseAdminURL(args []string) string {
	fs := flag.NewFlagSet("cli", flag.ExitOnError)
	url := fs.String("admin-url", "http://127.0.0.1:8080", "admin API base URL")
	_ = fs.Parse(args)
	return *url
}
