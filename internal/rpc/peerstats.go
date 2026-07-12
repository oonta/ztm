package rpc

import (
	"context"
	"crypto/tls"
	"log"
	"sync"
	"time"

	"ztm/internal/gossip"
	"ztm/internal/identity"
	"ztm/internal/router"
)

// PeerStatsCache fetches and caches RelayStats from mesh peers for routing.
type PeerStatsCache struct {
	NodeID   string
	Identity *identity.Store
	Members  func() []gossip.Member
	TTL      time.Duration

	mu    sync.RWMutex
	cache map[string]cachedPeerStats
}

type cachedPeerStats struct {
	load      router.NodeLoad
	fetchedAt time.Time
}

func (c *PeerStatsCache) Loads(ctx context.Context, nodeIDs []string) map[string]router.NodeLoad {
	if c == nil || c.Identity == nil || len(nodeIDs) == 0 {
		return nil
	}
	ttl := c.TTL
	if ttl <= 0 {
		ttl = 3 * time.Second
	}

	now := time.Now()
	out := make(map[string]router.NodeLoad, len(nodeIDs))
	var missing []string

	c.mu.RLock()
	for _, id := range nodeIDs {
		if id == "" || id == c.NodeID {
			continue
		}
		if ent, ok := c.cache[id]; ok && now.Sub(ent.fetchedAt) < ttl {
			out[id] = ent.load
		} else {
			missing = append(missing, id)
		}
	}
	c.mu.RUnlock()

	if len(missing) == 0 {
		return out
	}

	rpcAddr := c.memberRPCAddrs()
	tlsConf, err := c.Identity.RPCTLSConfig(c.NodeID, false)
	if err != nil {
		log.Printf("peer stats: tls: %v", err)
		return out
	}

	for _, id := range missing {
		addr, ok := rpcAddr[id]
		if !ok || addr == "" {
			continue
		}
		load, ok := c.fetchOne(ctx, addr, id, tlsConf)
		if !ok {
			continue
		}
		out[id] = load
		c.mu.Lock()
		if c.cache == nil {
			c.cache = make(map[string]cachedPeerStats)
		}
		c.cache[id] = cachedPeerStats{load: load, fetchedAt: now}
		c.mu.Unlock()
	}
	return out
}

func (c *PeerStatsCache) memberRPCAddrs() map[string]string {
	addrs := make(map[string]string)
	if c.Members == nil {
		return addrs
	}
	for _, m := range c.Members() {
		if m.Meta.NodeID != "" && m.Meta.RPCAddr != "" {
			addrs[m.Meta.NodeID] = m.Meta.RPCAddr
		}
	}
	return addrs
}

func (c *PeerStatsCache) fetchOne(ctx context.Context, addr, nodeID string, tlsConf *tls.Config) (router.NodeLoad, bool) {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	client, err := Dial(reqCtx, ClientOptions{Target: addr, TLS: tlsConf, Timeout: 2 * time.Second})
	cancel()
	if err != nil {
		log.Printf("peer stats: dial %s (%s): %v", nodeID, addr, err)
		return router.NodeLoad{}, false
	}
	defer client.Close()

	reqCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
	resp, err := client.RelayStats(reqCtx, nodeID)
	cancel()
	if err != nil {
		log.Printf("peer stats: relay stats %s: %v", nodeID, err)
		return router.NodeLoad{}, false
	}
	return router.NodeLoad{
		ActiveStreams: resp.GetActiveStreams(),
		ErrorRate:     resp.GetErrorRate(),
	}, true
}
