package rpc

import (
	"context"
	"encoding/json"
	"log"
	"sort"
	"time"

	ztmv1 "ztm/api/proto/ztm/v1"
	"ztm/internal/gossip"
	"ztm/internal/identity"
	"ztm/internal/registry"
)

// ServiceResolver looks up services locally and falls back to peer RPC ResolveService.
type ServiceResolver struct {
	NodeID    string
	Registry  *registry.Registry
	Identity  *identity.Store
	Members   func() []gossip.Member
	MeshPeers func() []string
}

// FindByName returns service instances, querying peers over RPC when local state is empty.
func (r *ServiceResolver) FindByName(ctx context.Context, name string) []registry.Service {
	if r.Registry == nil {
		return nil
	}
	if found := r.Registry.FindByName(name); len(found) > 0 {
		return found
	}
	if r.Identity == nil || r.Members == nil {
		return nil
	}

	tlsConf, err := r.Identity.RPCTLSConfig(r.NodeID, false)
	if err != nil {
		log.Printf("rpc resolver: tls: %v", err)
		return nil
	}

	for _, member := range r.peerOrder() {
		addr := member.Meta.RPCAddr
		if addr == "" || member.Meta.NodeID == "" || member.Meta.NodeID == r.NodeID {
			continue
		}

		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		client, err := Dial(reqCtx, ClientOptions{Target: addr, TLS: tlsConf, Timeout: 2 * time.Second})
		cancel()
		if err != nil {
			log.Printf("rpc resolver: dial %s (%s): %v", member.Meta.NodeID, addr, err)
			continue
		}

		reqCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
		resp, err := client.ResolveService(reqCtx, name, nil)
		cancel()
		_ = client.Close()
		if err != nil {
			log.Printf("rpc resolver: resolve %s on %s: %v", name, member.Meta.NodeID, err)
			continue
		}
		if len(resp.GetServices()) == 0 {
			continue
		}

		mergeAnnouncements(r.Registry, resp.GetServices())
		if found := r.Registry.FindByName(name); len(found) > 0 {
			return found
		}
	}

	return nil
}

func (r *ServiceResolver) peerOrder() []gossip.Member {
	members := r.Members()
	if len(members) == 0 {
		return nil
	}

	connected := make(map[string]struct{})
	if r.MeshPeers != nil {
		for _, id := range r.MeshPeers() {
			connected[id] = struct{}{}
		}
	}

	var preferred, rest []gossip.Member
	for _, m := range members {
		if m.Meta.NodeID == "" || m.Meta.NodeID == r.NodeID {
			continue
		}
		if _, ok := connected[m.Meta.NodeID]; ok {
			preferred = append(preferred, m)
		} else {
			rest = append(rest, m)
		}
	}

	sort.Slice(preferred, func(i, j int) bool { return preferred[i].Meta.NodeID < preferred[j].Meta.NodeID })
	sort.Slice(rest, func(i, j int) bool { return rest[i].Meta.NodeID < rest[j].Meta.NodeID })
	return append(preferred, rest...)
}

func mergeAnnouncements(reg *registry.Registry, anns []*ztmv1.ServiceAnnouncement) {
	byNode := make(map[string][]registry.Service)
	for _, ann := range anns {
		if ann == nil || ann.GetNodeId() == "" {
			continue
		}
		byNode[ann.GetNodeId()] = append(byNode[ann.GetNodeId()], announcementToService(ann))
	}

	for nodeID, services := range byNode {
		state := registry.LocalState{NodeID: nodeID, Services: services}
		data, err := json.Marshal(state)
		if err != nil {
			continue
		}
		if err := reg.MergeRemoteState(data); err != nil {
			log.Printf("rpc resolver: merge %s: %v", nodeID, err)
		}
	}
}

func announcementToService(ann *ztmv1.ServiceAnnouncement) registry.Service {
	svc := registry.Service{
		ServiceID: ann.GetServiceId(),
		NodeID:    ann.GetNodeId(),
		Name:      ann.GetName(),
		Labels:    ann.GetLabels(),
		Version:   ann.GetVersion(),
		TTLSec:    ann.GetTtlSec(),
	}
	if ts := ann.GetAnnouncedAtUnix(); ts > 0 {
		svc.Announced = time.Unix(ts, 0)
	} else {
		svc.Announced = time.Now()
	}
	if svc.TTLSec > 0 {
		svc.TTL = time.Duration(svc.TTLSec) * time.Second
	}
	if eps := ann.GetEndpoints(); len(eps) > 0 {
		svc.Host = eps[0].GetHost()
		svc.Port = eps[0].GetPort()
	}
	return svc
}
