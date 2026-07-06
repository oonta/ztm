package agent

import (
	"context"
	"fmt"
	"log"
	"time"

	"ztm/internal/identity"
	"ztm/internal/mesh"
)

// Config holds ztm-node runtime configuration.
type Config struct {
	NodeID   string
	Cluster  string
	DataDir  string
	MeshBind string
	Join     string
}

// Agent coordinates node subsystems.
type Agent struct {
	cfg       Config
	identity  *identity.Store
	mesh      *mesh.Transport
	meshAddr  string
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

	return &Agent{
		cfg:      cfg,
		identity: store,
		mesh:     mesh.NewTransport(cfg.NodeID, cfg.Cluster, tlsConf),
	}, nil
}

// Run starts the agent until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.mesh.Listen(ctx, a.cfg.MeshBind); err != nil {
		return err
	}
	a.meshAddr = a.mesh.Addr()
	log.Printf("agent: node=%s cluster=%s mesh listening on %s", a.cfg.NodeID, a.cfg.Cluster, a.meshAddr)

	if a.cfg.Join != "" {
		joinAddr, err := mesh.ResolveHost(a.cfg.Join)
		if err != nil {
			return fmt.Errorf("join address: %w", err)
		}
		// Brief delay so listener is ready when peers reflect back.
		deadline := time.Now().Add(10 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = a.mesh.Connect(ctx, joinAddr)
			if lastErr == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if lastErr != nil {
			return fmt.Errorf("join %s: %w", joinAddr, lastErr)
		}
	}

	<-ctx.Done()
	return a.mesh.Close()
}

// MeshAddr returns the bound mesh listen address.
func (a *Agent) MeshAddr() string {
	if a.meshAddr != "" {
		return a.meshAddr
	}
	return a.mesh.Addr()
}

// PeerIDs returns connected mesh peers.
func (a *Agent) PeerIDs() []string {
	return a.mesh.PeerIDs()
}
