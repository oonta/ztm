package router

import (
	"sort"

	"ztm/internal/registry"
)

// NodeLoad summarizes relay load for weighted routing.
type NodeLoad struct {
	ActiveStreams uint32
	ErrorRate     float32
}

// Candidates returns routable service instances in priority order:
// local first, then remote instances on connected mesh peers.
// When loads is non-nil, remotes are ordered by error_rate, active_streams, then node_id.
func Candidates(services []registry.Service, localNodeID string, connectedPeers []string, loads map[string]NodeLoad) []registry.Service {
	if len(services) == 0 {
		return nil
	}

	connected := make(map[string]struct{}, len(connectedPeers))
	for _, id := range connectedPeers {
		connected[id] = struct{}{}
	}

	sorted := append([]registry.Service(nil), services...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NodeID < sorted[j].NodeID
	})

	var local []registry.Service
	var remote []registry.Service
	for _, svc := range sorted {
		if svc.NodeID == localNodeID {
			local = append(local, svc)
			continue
		}
		if _, ok := connected[svc.NodeID]; ok {
			remote = append(remote, svc)
		}
	}

	if len(loads) > 0 && len(remote) > 1 {
		sort.SliceStable(remote, func(i, j int) bool {
			li := loads[remote[i].NodeID]
			lj := loads[remote[j].NodeID]
			if li.ErrorRate != lj.ErrorRate {
				return li.ErrorRate < lj.ErrorRate
			}
			if li.ActiveStreams != lj.ActiveStreams {
				return li.ActiveStreams < lj.ActiveStreams
			}
			return remote[i].NodeID < remote[j].NodeID
		})
	}

	out := make([]registry.Service, 0, len(local)+len(remote))
	out = append(out, local...)
	out = append(out, remote...)
	return out
}

// Select picks the highest-priority routable service instance.
func Select(services []registry.Service, localNodeID string, connectedPeers []string, loads map[string]NodeLoad) (registry.Service, bool) {
	candidates := Candidates(services, localNodeID, connectedPeers, loads)
	if len(candidates) == 0 {
		return registry.Service{}, false
	}
	return candidates[0], true
}
