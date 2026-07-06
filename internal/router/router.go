package router

import (
	"sort"

	"ztm/internal/registry"
)

// Candidates returns routable service instances in priority order:
// local first, then remote instances on connected mesh peers (stable node_id order).
func Candidates(services []registry.Service, localNodeID string, connectedPeers []string) []registry.Service {
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

	var out []registry.Service
	for _, svc := range sorted {
		if svc.NodeID == localNodeID {
			out = append(out, svc)
		}
	}
	for _, svc := range sorted {
		if svc.NodeID == localNodeID {
			continue
		}
		if _, ok := connected[svc.NodeID]; ok {
			out = append(out, svc)
		}
	}
	return out
}

// Select picks the highest-priority routable service instance.
func Select(services []registry.Service, localNodeID string, connectedPeers []string) (registry.Service, bool) {
	candidates := Candidates(services, localNodeID, connectedPeers)
	if len(candidates) == 0 {
		return registry.Service{}, false
	}
	return candidates[0], true
}
