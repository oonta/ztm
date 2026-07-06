package agent

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"ztm/internal/admin"
	"ztm/internal/gossip"
	"ztm/internal/identity"
	"ztm/internal/mesh"
	"ztm/internal/registry"
)

// Config holds ztm-node runtime configuration.
type Config struct {
	NodeID     string
	Cluster    string
	DataDir    string
	MeshBind   string
	GossipBind string
	AdminBind  string
	Join       string // gossip seed host:port
}

// Agent coordinates node subsystems.
type Agent struct {
	cfg      Config
	identity *identity.Store
	mesh     *mesh.Transport
	gossip   *gossip.Cluster
	registry *registry.Registry
	admin    *admin.Server

	meshAddr   string
	gossipAddr string
	adminAddr  string

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
	if cfg.GossipBind == "" {
		cfg.GossipBind = ":7946"
	}
	if cfg.AdminBind == "" {
		cfg.AdminBind = "127.0.0.1:0"
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
	if err := a.mesh.Listen(ctx, a.cfg.MeshBind); err != nil {
		return err
	}
	a.meshAddr = a.mesh.Addr()
	log.Printf("agent: node=%s cluster=%s mesh=%s", a.cfg.NodeID, a.cfg.Cluster, a.meshAddr)

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
		Registry:   a.registry,
		Gossip:     a.gossip,
		MemberCount: func() int {
			if a.gossip == nil {
				return 0
			}
			return a.gossip.MemberCount()
		},
		MeshPeers: func() []string { return a.mesh.PeerIDs() },
		OnRegister: a.registerService,
	})
	adminAddr, err := a.admin.Listen(a.cfg.AdminBind)
	if err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	a.adminAddr = adminAddr
	log.Printf("agent: admin http://%s", a.adminAddr)

	go a.backgroundLoops(ctx)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.admin.Shutdown(shutdownCtx)
	_ = a.gossip.Shutdown()
	return a.mesh.Close()
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

func (a *Agent) onMeshPeerDisconnected(peerID string) {
	a.mu.Lock()
	delete(a.meshConnected, peerID)
	a.mu.Unlock()
}

func (a *Agent) registerService(name, host string, port uint32, labels map[string]string) error {
	a.registry.RegisterLocal(name, host, port, labels)
	a.gossip.BroadcastState()
	if data, err := a.registry.MarshalLocalState(); err == nil {
		a.gossip.PropagateState(data)
	}
	return nil
}

// RegisterService registers a local service and gossips it.
func (a *Agent) RegisterService(name, host string, port uint32, labels map[string]string) error {
	if a.gossip == nil {
		return fmt.Errorf("agent not running")
	}
	return a.registerService(name, host, port, labels)
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
