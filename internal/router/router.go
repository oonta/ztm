package router

import (
	"sort"

	"ztm/internal/registry"
)

// Select picks a service instance for routing.
// Prefers a local instance, then a remote instance on a connected mesh peer.
func Select(services []registry.Service, localNodeID string, connectedPeers []string) (registry.Service, bool) {
	if len(services) == 0 {
		return registry.Service{}, false
	}

	connected := make(map[string]struct{}, len(connectedPeers))
	for _, id := range connectedPeers {
		connected[id] = struct{}{}
	}

	sorted := append([]registry.Service(nil), services...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].NodeID < sorted[j].NodeID
	})

	for _, svc := range sorted {
		if svc.NodeID == localNodeID {
			return svc, true
		}
	}
	for _, svc := range sorted {
		if _, ok := connected[svc.NodeID]; ok {
			return svc, true
		}
	}
	return registry.Service{}, false
}
