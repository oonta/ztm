package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"ztm/internal/gossip"
	"ztm/internal/metrics"
	"ztm/internal/registry"
)

// Status is returned by GET /v1/status.
type Status struct {
	NodeID       string `json:"node_id"`
	Cluster      string `json:"cluster"`
	MeshAddr     string `json:"mesh_addr"`
	GossipAddr   string `json:"gossip_addr"`
	RPCAddr      string `json:"rpc_addr"`
	MemberCount  int    `json:"member_count"`
	MeshPeers    []string `json:"mesh_peers"`
	ServiceCount int    `json:"service_count"`
}

// Server exposes a local admin HTTP API.
type Server struct {
	nodeID     string
	cluster    string
	meshAddr   string
	gossipAddr string
	rpcAddr    string
	memberCount func() int
	meshPeers   func() []string
	registry    *registry.Registry
	gossip      *gossip.Cluster
	onRegister   func(name, host string, port uint32, labels map[string]string) error
	onDeregister func(name string) error

	metrics *metrics.Collector

	httpServer *http.Server
	boundAddr  string
}

type Options struct {
	NodeID      string
	Cluster     string
	MeshAddr    string
	GossipAddr  string
	RPCAddr     string
	Registry    *registry.Registry
	Gossip      *gossip.Cluster
	MemberCount func() int
	MeshPeers   func() []string
	OnRegister   func(name, host string, port uint32, labels map[string]string) error
	OnDeregister func(name string) error
	Metrics      *metrics.Collector
}

func New(opts Options) *Server {
	m := opts.Metrics
	if m == nil {
		m = metrics.New()
	}

	return &Server{
		nodeID:      opts.NodeID,
		cluster:     opts.Cluster,
		meshAddr:    opts.MeshAddr,
		gossipAddr:  opts.GossipAddr,
		rpcAddr:     opts.RPCAddr,
		registry:    opts.Registry,
		gossip:      opts.Gossip,
		memberCount: opts.MemberCount,
		meshPeers:   opts.MeshPeers,
		onRegister:   opts.OnRegister,
		onDeregister: opts.OnDeregister,
		metrics:      m,
	}
}

func (s *Server) Listen(addr string) (string, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/members", s.handleMembers)
	mux.HandleFunc("GET /v1/services", s.handleServices)
	mux.HandleFunc("POST /v1/services/register", s.handleRegister)
	mux.HandleFunc("POST /v1/services/deregister", s.handleDeregister)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Reg, promhttp.HandlerOpts{}))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	s.boundAddr = ln.Addr().String()
	s.httpServer = &http.Server{Handler: mux}
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("admin: %v", err)
		}
	}()
	return s.boundAddr, nil
}

func (s *Server) Addr() string {
	return s.boundAddr
}

func (s *Server) ListenAndServe(addr string) error {
	_, err := s.Listen(addr)
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := Status{
		NodeID:       s.nodeID,
		Cluster:      s.cluster,
		MeshAddr:     s.meshAddr,
		GossipAddr:   s.gossipAddr,
		RPCAddr:      s.rpcAddr,
		MemberCount:  s.memberCount(),
		MeshPeers:    s.meshPeers(),
		ServiceCount: len(s.registry.List()),
	}
	if s.metrics != nil {
		s.metrics.Members.Set(float64(st.MemberCount))
		s.metrics.Peers.Set(float64(len(st.MeshPeers)))
		s.metrics.Services.Set(float64(st.ServiceCount))
	}
	writeJSON(w, st)
}

func (s *Server) handleMembers(w http.ResponseWriter, _ *http.Request) {
	if s.gossip == nil {
		writeJSON(w, []gossip.Member{})
		return
	}
	writeJSON(w, s.gossip.Members())
}

func (s *Server) handleServices(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.registry.List())
}

type registerRequest struct {
	Name   string            `json:"name"`
	Host   string            `json:"host"`
	Port   uint32            `json:"port"`
	Labels map[string]string `json:"labels"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Port == 0 {
		http.Error(w, "name and port are required", http.StatusBadRequest)
		return
	}
	if req.Host == "" {
		req.Host = "127.0.0.1"
	}
	if s.onRegister == nil {
		http.Error(w, "register not configured", http.StatusInternalServerError)
		return
	}
	if err := s.onRegister(req.Name, req.Host, req.Port, req.Labels); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, s.registry.List())
}

type deregisterRequest struct {
	Name string `json:"name"`
}

func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	var req deregisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if s.onDeregister == nil {
		http.Error(w, "deregister not configured", http.StatusInternalServerError)
		return
	}
	if err := s.onDeregister(req.Name); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, s.registry.List())
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		http.Error(w, fmt.Sprintf("encode: %v", err), http.StatusInternalServerError)
	}
}

// URL returns the admin base URL for a bound address.
func URL(addr string) string {
	return fmt.Sprintf("http://%s", addr)
}

// WaitReady polls status endpoint until reachable.
func WaitReady(baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/v1/status")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("admin not ready at %s", baseURL)
}
