package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"ztm/internal/admin"
	"ztm/internal/gossip"
	"ztm/internal/identity"
	"ztm/internal/mesh"
	"ztm/internal/policy"
	"ztm/internal/rpc"
	"ztm/internal/registry"
	"ztm/internal/tunnel"
)

// Config holds ztm-node runtime configuration.
type Config struct {
	NodeID     string
	Cluster    string
	DataDir    string
	MeshBind   string
	ClientBind string
	GossipBind string
	AdminBind  string
	RPCBind    string
	Join       string // gossip seed host:port
	AllowService []string
}

// Agent coordinates node subsystems.
type Agent struct {
	cfg      Config
	identity *identity.Store
	mesh     *mesh.Transport
	tunnel   *tunnel.Server
	gossip   *gossip.Cluster
	registry *registry.Registry
	admin    *admin.Server
	rpc      *rpc.Server

	meshAddr   string
	clientAddr string
	gossipAddr string
	adminAddr  string
	rpcAddr    string

	mu          sync.Mutex
	meshConnected map[string]struct{}
}

// New creates an agent from config.
func New(cfg Config) (*Agent, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("node-id is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}
	if cfg.Cluster == "" {
		cfg.Cluster = "default"
	}
	if cfg.MeshBind == "" {
		cfg.MeshBind = ":7444"
	}
	if cfg.ClientBind == "" {
		cfg.ClientBind = ":7443"
	}
	if cfg.GossipBind == "" {
		cfg.GossipBind = ":7946"
	}
	if cfg.AdminBind == "" {
		cfg.AdminBind = "127.0.0.1:0"
	}
	if cfg.RPCBind == "" {
		cfg.RPCBind = "127.0.0.1:0"
	}

	store, err := identity.Open(cfg.Cluster, cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	if err := store.EnsureNodeCert(cfg.NodeID); err != nil {
		return nil, fmt.Errorf("node cert: %w", err)
	}

	tlsConf, err := store.TLSConfig(cfg.NodeID, true)
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}

	reg := registry.New(cfg.NodeID)

	return &Agent{
		cfg:           cfg,
		identity:      store,
		mesh:          mesh.NewTransport(cfg.NodeID, cfg.Cluster, tlsConf),
		registry:      reg,
		meshConnected: make(map[string]struct{}),
	}, nil
}

func (a *Agent) wireMeshCallbacks() {
	a.mesh.SetOnDisconnect(a.onMeshPeerDisconnected)
}

// Run starts the agent until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	a.wireMeshCallbacks()

	pol := policy.New()
	if len(a.cfg.AllowService) == 0 {
		pol = policy.Permissive()
	} else {
		for _, svc := range a.cfg.AllowService {
			pol.AllowService(svc)
		}
	}

	a.mesh.SetRelayHandler(&mesh.RelayHandler{
		NodeID:   a.cfg.NodeID,
		Registry: a.registry,
		Policy:   pol,
	})

	if err := a.mesh.Listen(ctx, a.cfg.MeshBind); err != nil {
		return err
	}
	a.meshAddr = a.mesh.Addr()
	log.Printf("agent: node=%s cluster=%s mesh=%s", a.cfg.NodeID, a.cfg.Cluster, a.meshAddr)

	rpcTLS, err := a.identity.RPCTLSConfig(a.cfg.NodeID, true)
	if err != nil {
		return fmt.Errorf("rpc tls: %w", err)
	}
	a.rpc = &rpc.Server{}
	rpcAddr, err := a.rpc.Listen(a.cfg.RPCBind, rpc.Options{
		NodeID:   a.cfg.NodeID,
		Registry: a.registry,
		TLS:      rpcTLS,
	})
	if err != nil {
		return fmt.Errorf("rpc: %w", err)
	}
	a.rpcAddr = rpcAddr
	log.Printf("agent: rpc %s", a.rpcAddr)

	tunnelTLS, err := a.identity.TunnelServerTLSConfig(a.cfg.NodeID)
	if err != nil {
		return fmt.Errorf("tunnel tls: %w", err)
	}
	a.tunnel = &tunnel.Server{}
	if err := a.tunnel.Listen(ctx, a.cfg.ClientBind, tunnel.ServerConfig{
		NodeID:   a.cfg.NodeID,
		Registry: a.registry,
		Policy:   pol,
		Mesh:     a.mesh,
		TLS:      tunnelTLS,
	}); err != nil {
		return fmt.Errorf("tunnel listen: %w", err)
	}
	a.clientAddr = a.tunnel.Addr()
	log.Printf("agent: client tunnel listening on %s", a.clientAddr)

	key, err := gossip.LoadOrCreateKey(a.cfg.DataDir)
	if err != nil {
		return fmt.Errorf("gossip key: %w", err)
	}

	var join []string
	if a.cfg.Join != "" {
		joinAddr, err := mesh.ResolveHost(a.cfg.Join)
		if err != nil {
			return fmt.Errorf("join address: %w", err)
		}
		join = []string{joinAddr}
	}

	gossipCluster, err := gossip.Start(gossip.Config{
		NodeID:    a.cfg.NodeID,
		Cluster:   a.cfg.Cluster,
		MeshAddr:  a.meshAddr,
		BindAddr:  a.cfg.GossipBind,
		Registry:  a.registry,
		SecretKey: key,
		OnMember: func(meta gossip.NodeMeta) {
			a.connectMeshPeer(ctx, meta)
		},
	}, join)
	if err != nil {
		return fmt.Errorf("gossip: %w", err)
	}
	a.gossip = gossipCluster
	a.gossipAddr = gossipCluster.Addr()
	log.Printf("agent: gossip listening on %s", a.gossipAddr)

	for _, member := range a.gossip.Members() {
		if member.Meta.NodeID != "" && member.Meta.NodeID != a.cfg.NodeID {
			a.connectMeshPeer(ctx, member.Meta)
		}
	}

	a.admin = admin.New(admin.Options{
		NodeID:     a.cfg.NodeID,
		Cluster:    a.cfg.Cluster,
		MeshAddr:   a.meshAddr,
		GossipAddr: a.gossipAddr,
		RPCAddr:    a.rpcAddr,
		Registry:   a.registry,
		Gossip:     a.gossip,
		MemberCount: func() int {
			if a.gossip == nil {
				return 0
			}
			return a.gossip.MemberCount()
		},
		MeshPeers: func() []string { return a.mesh.PeerIDs() },
		OnRegister:   a.registerService,
		OnDeregister: a.deregisterService,
	})
	adminAddr, err := a.admin.Listen(a.cfg.AdminBind)
	if err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	a.adminAddr = adminAddr
	log.Printf("agent: admin http://%s", a.adminAddr)

	go a.backgroundLoops(ctx)
	go a.maintainMeshLinks(ctx)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a.leaveCluster(shutdownCtx)
	return nil
}

func (a *Agent) leaveCluster(shutdownCtx context.Context) {
	a.deregisterAllLocal()
	time.Sleep(200 * time.Millisecond)

	_ = a.admin.Shutdown(shutdownCtx)
	if err := a.tunnel.Shutdown(shutdownCtx); err != nil {
		log.Printf("agent: tunnel shutdown: %v", err)
	}
	_ = a.rpc.Shutdown(shutdownCtx)
	_ = a.gossip.Shutdown()
	_ = a.mesh.Close()
}

func (a *Agent) deregisterAllLocal() {
	if a.gossip == nil {
		return
	}
	names := a.registry.LocalNames()
	for _, name := range names {
		if err := a.deregisterService(name); err != nil {
			log.Printf("agent: deregister %s: %v", name, err)
		}
	}
}

func (a *Agent) propagateRegistryState(data []byte) {
	if a.gossip == nil {
		return
	}
	a.gossip.BroadcastState()
	if len(data) > 0 {
		a.gossip.PropagateState(data)
	}
}

func (a *Agent) backgroundLoops(ctx context.Context) {
	reannounce := time.NewTicker(30 * time.Second)
	prune := time.NewTicker(10 * time.Second)
	defer reannounce.Stop()
	defer prune.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reannounce.C:
			a.gossip.BroadcastState()
		case <-prune.C:
			a.registry.Prune()
		}
	}
}

func (a *Agent) connectMeshPeer(ctx context.Context, meta gossip.NodeMeta) {
	if meta.NodeID == a.cfg.NodeID || meta.MeshAddr == "" {
		return
	}
	// Avoid duplicate QUIC dials: only the lexicographically smaller node_id initiates.
	if a.cfg.NodeID > meta.NodeID {
		return
	}
	a.mu.Lock()
	if _, ok := a.meshConnected[meta.NodeID]; ok {
		a.mu.Unlock()
		return
	}
	a.meshConnected[meta.NodeID] = struct{}{}
	a.mu.Unlock()

	addr, err := mesh.ResolveHost(meta.MeshAddr)
	if err != nil {
		log.Printf("agent: mesh resolve %s: %v", meta.MeshAddr, err)
		return
	}
	if err := a.mesh.Connect(ctx, addr); err != nil {
		log.Printf("agent: mesh connect %s (%s): %v", meta.NodeID, addr, err)
		a.mu.Lock()
		delete(a.meshConnected, meta.NodeID)
		a.mu.Unlock()
		return
	}
	log.Printf("agent: mesh linked to %s via gossip", meta.NodeID)
}

func (a *Agent) maintainMeshLinks(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if a.gossip == nil {
				continue
			}
			connected := make(map[string]struct{}, len(a.mesh.PeerIDs()))
			for _, id := range a.mesh.PeerIDs() {
				connected[id] = struct{}{}
			}
			for _, member := range a.gossip.Members() {
				peerID := member.Meta.NodeID
				if peerID == "" || peerID == a.cfg.NodeID {
					continue
				}
				if a.cfg.NodeID > peerID {
					continue
				}
				if _, ok := connected[peerID]; ok {
					continue
				}
				a.connectMeshPeer(ctx, member.Meta)
			}
		}
	}
}

func (a *Agent) onMeshPeerDisconnected(peerID string) {
	a.mu.Lock()
	delete(a.meshConnected, peerID)
	a.mu.Unlock()
}

func (a *Agent) registerService(name, host string, port uint32, labels map[string]string) error {
	a.registry.RegisterLocal(name, host, port, labels)
	if data, err := a.registry.MarshalLocalState(); err == nil {
		a.propagateRegistryState(data)
	}
	return nil
}

func (a *Agent) deregisterService(name string) error {
	tombstone, ok := a.registry.DeregisterLocal(name)
	if !ok {
		return fmt.Errorf("service %q not found locally", name)
	}
	state := registry.LocalState{
		NodeID:   a.cfg.NodeID,
		Services: []registry.Service{tombstone},
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	a.propagateRegistryState(data)
	return nil
}

// RegisterService registers a local service and gossips it.
func (a *Agent) RegisterService(name, host string, port uint32, labels map[string]string) error {
	if a.gossip == nil {
		return fmt.Errorf("agent not running")
	}
	return a.registerService(name, host, port, labels)
}

// DeregisterService removes a local service and gossips tombstones.
func (a *Agent) DeregisterService(name string) error {
	if a.gossip == nil {
		return fmt.Errorf("agent not running")
	}
	return a.deregisterService(name)
}

// ClientAddr returns the client QUIC listen address.
func (a *Agent) ClientAddr() string {
	return a.clientAddr
}

// MeshAddr returns the bound mesh listen address.
func (a *Agent) MeshAddr() string {
	if a.meshAddr != "" {
		return a.meshAddr
	}
	return a.mesh.Addr()
}

// GossipAddr returns the gossip listen address.
func (a *Agent) GossipAddr() string {
	return a.gossipAddr
}

// AdminAddr returns the admin HTTP listen address.
func (a *Agent) AdminAddr() string {
	return a.adminAddr
}

// AdminURL returns the admin base URL.
func (a *Agent) AdminURL() string {
	return admin.URL(a.adminAddr)
}

// PeerIDs returns connected mesh peers.
func (a *Agent) PeerIDs() []string {
	return a.mesh.PeerIDs()
}

// MemberCount returns gossip member count.
func (a *Agent) MemberCount() int {
	if a.gossip == nil {
		return 0
	}
	return a.gossip.MemberCount()
}

// Members returns gossip members.
func (a *Agent) Members() []gossip.Member {
	if a.gossip == nil {
		return nil
	}
	return a.gossip.Members()
}

// Services returns the merged service registry.
func (a *Agent) Services() []registry.Service {
	return a.registry.List()
}
