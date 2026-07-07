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
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"

	ztmv1 "ztm/api/proto/ztm/v1"
	"ztm/internal/agent"
	"ztm/internal/identity"
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
		case "deregister":
			cliDeregister(os.Args[2:])
			return
		case "enroll-client":
			enrollClient(os.Args[2:])
			return
		case "push-policy":
			cliPushPolicy(os.Args[2:])
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
  ztm-node deregister --admin-url <url> --name <n>
  ztm-node enroll-client --data-dir <dir> --client-id <id>
  ztm-node push-policy --rpc-addr <host:port> --node-id <id> --data-dir <dir> [--allow-service ...] [--deny-service ...]

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
	clientBind := fs.String("client-bind", ":7443", "client QUIC listen address")
	gossipBind := fs.String("gossip-bind", ":7946", "gossip (memberlist) bind address")
	adminBind := fs.String("admin-bind", "127.0.0.1:0", "admin HTTP API listen address (:0 = random free port)")
	rpcBind := fs.String("rpc-bind", "127.0.0.1:0", "inter-node gRPC listen address (:0 = random free port)")
	join := fs.String("join", "", "seed node gossip address host:port")
	cluster := fs.String("cluster", "default", "cluster name")
	allowService := fs.String("allow-service", "", "comma-separated allowed service patterns (empty = allow all)")
	_ = fs.Parse(args)

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --node-id is required")
		fs.Usage()
		os.Exit(1)
	}

	cfg := agent.Config{
		NodeID:       *nodeID,
		Cluster:      *cluster,
		DataDir:      *dataDir,
		MeshBind:     *meshBind,
		ClientBind:   *clientBind,
		GossipBind:   *gossipBind,
		AdminBind:    *adminBind,
		RPCBind:      *rpcBind,
		Join:         *join,
		AllowService: splitCSV(*allowService),
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

func cliDeregister(args []string) {
	fs := flag.NewFlagSet("deregister", flag.ExitOnError)
	url := fs.String("admin-url", "http://127.0.0.1:8080", "admin API base URL")
	name := fs.String("name", "", "service name (required)")
	_ = fs.Parse(args)

	if *name == "" {
		fmt.Fprintln(os.Stderr, "error: --name is required")
		os.Exit(1)
	}

	payload := map[string]any{"name": *name}
	data, _ := json.Marshal(payload)
	resp, err := http.Post(*url+"/v1/services/deregister", "application/json", bytes.NewReader(data))
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

func enrollClient(args []string) {
	fs := flag.NewFlagSet("enroll-client", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "cluster data directory")
	clientID := fs.String("client-id", "", "client identifier (required)")
	cluster := fs.String("cluster", "default", "cluster name")
	_ = fs.Parse(args)

	if *clientID == "" {
		fmt.Fprintln(os.Stderr, "error: --client-id is required")
		os.Exit(1)
	}

	store, err := identity.Open(*cluster, *dataDir)
	if err != nil {
		log.Fatalf("identity: %v", err)
	}
	if err := store.EnsureClientCert(*clientID); err != nil {
		log.Fatalf("enroll: %v", err)
	}
	cert, key, ca := identity.ClientCertPaths(*dataDir, *clientID)
	fmt.Printf("enrolled client %q\n", *clientID)
	fmt.Printf("  cert: %s\n", cert)
	fmt.Printf("  key:  %s\n", key)
	fmt.Printf("  ca:   %s\n", ca)
}

func cliPushPolicy(args []string) {
	fs := flag.NewFlagSet("push-policy", flag.ExitOnError)
	rpcAddr := fs.String("rpc-addr", "", "target node RPC address host:port (required)")
	nodeID := fs.String("node-id", "", "local node id for client cert (required)")
	dataDir := fs.String("data-dir", "./data", "cluster data directory")
	cluster := fs.String("cluster", "default", "cluster name")
	allowService := fs.String("allow-service", "", "comma-separated allowed service patterns")
	denyService := fs.String("deny-service", "", "comma-separated denied service patterns")
	version := fs.Uint64("version", uint64(time.Now().Unix()), "policy bundle version")
	_ = fs.Parse(args)

	if *rpcAddr == "" || *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: --rpc-addr and --node-id are required")
		os.Exit(1)
	}

	store, err := identity.Open(*cluster, *dataDir)
	if err != nil {
		log.Fatalf("identity: %v", err)
	}
	if err := store.EnsureNodeCert(*nodeID); err != nil {
		log.Fatalf("node cert: %v", err)
	}
	tlsConf, err := store.RPCTLSConfig(*nodeID, false)
	if err != nil {
		log.Fatalf("rpc tls: %v", err)
	}

	bundle := &ztmv1.PolicyBundle{
		Version:       *version,
		Cluster:       *cluster,
		UpdatedAtUnix: time.Now().Unix(),
		Rules: []*ztmv1.PolicyRule{{
			Subject: "*",
		}},
	}
	r := bundle.Rules[0]
	for _, s := range splitCSV(*allowService) {
		r.Allow = append(r.Allow, &ztmv1.PolicyMatch{Target: &ztmv1.PolicyMatch_Service{Service: s}})
	}
	for _, s := range splitCSV(*denyService) {
		r.Deny = append(r.Deny, &ztmv1.PolicyMatch{Target: &ztmv1.PolicyMatch_Service{Service: s}})
	}
	data, err := proto.Marshal(bundle)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}

	conn, err := grpc.Dial(*rpcAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := ztmv1.NewNodeRPCClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.PushPolicy(ctx, &ztmv1.PushPolicyRequest{PolicyBundle: data, MinVersion: 0})
	if err != nil {
		log.Fatalf("push: %v", err)
	}
	fmt.Printf("{\"accepted_version\": %d}\n", resp.GetAcceptedVersion())
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
