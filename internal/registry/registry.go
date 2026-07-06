package registry

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"
)

const DefaultTTL = 90 * time.Second

// Service is a registered backend behind a node.
type Service struct {
	ServiceID string            `json:"service_id"`
	NodeID    string            `json:"node_id"`
	Name      string            `json:"name"`
	Host      string            `json:"host"`
	Port      uint32            `json:"port"`
	Labels    map[string]string `json:"labels,omitempty"`
	Version   uint64            `json:"version"`
	TTL       time.Duration     `json:"-"`
	TTLSec    int64             `json:"ttl_sec"`
	Announced time.Time         `json:"announced_at"`
}

// LocalState is gossip-synced per-node service state.
type LocalState struct {
	NodeID   string    `json:"node_id"`
	Services []Service `json:"services"`
}

// Registry stores local and gossip-merged remote services.
type Registry struct {
	nodeID string
	ttl    time.Duration

	mu      sync.RWMutex
	local   map[string]*Service
	remote  map[string]*Service
	version map[string]uint64
}

// New creates a registry for a node.
func New(nodeID string) *Registry {
	return &Registry{
		nodeID:  nodeID,
		ttl:     DefaultTTL,
		local:   make(map[string]*Service),
		remote:  make(map[string]*Service),
		version: make(map[string]uint64),
	}
}

// RegisterLocal adds or updates a service on this node.
func (r *Registry) RegisterLocal(name, host string, port uint32, labels map[string]string) Service {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.version[name]++
	svc := &Service{
		ServiceID: serviceID(r.nodeID, name, r.version[name]),
		NodeID:    r.nodeID,
		Name:      name,
		Host:      host,
		Port:      port,
		Labels:    copyLabels(labels),
		Version:   r.version[name],
		TTL:       r.ttl,
		TTLSec:    int64(r.ttl.Seconds()),
		Announced: time.Now(),
	}
	r.local[name] = svc
	return *svc
}

// DeregisterLocal removes a local service via tombstone announce.
func (r *Registry) DeregisterLocal(name string) (Service, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.local[name]; !ok {
		return Service{}, false
	}
	delete(r.local, name)
	r.version[name]++
	return Service{
		ServiceID: serviceID(r.nodeID, name, r.version[name]),
		NodeID:    r.nodeID,
		Name:      name,
		Version:   r.version[name],
		TTL:       0,
		TTLSec:    0,
		Announced: time.Now(),
	}, true
}

// LocalSnapshot returns serializable local state for gossip.
func (r *Registry) LocalSnapshot() LocalState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	services := make([]Service, 0, len(r.local))
	for _, svc := range r.local {
		services = append(services, *svc)
	}
	sort.Slice(services, func(i, j int) bool { return services[i].Name < services[j].Name })
	return LocalState{NodeID: r.nodeID, Services: services}
}

// MarshalLocalState encodes local services for memberlist.
func (r *Registry) MarshalLocalState() ([]byte, error) {
	return json.Marshal(r.LocalSnapshot())
}

// MergeRemoteState merges gossip state from a peer node.
func (r *Registry) MergeRemoteState(data []byte) error {
	var state LocalState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.NodeID == "" || state.NodeID == r.nodeID {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	active := make(map[string]struct{}, len(state.Services))
	for _, svc := range state.Services {
		if svc.TTLSec == 0 {
			r.deleteRemoteLocked(state.NodeID, svc.Name)
			continue
		}
		svc.NodeID = state.NodeID
		svc.TTL = time.Duration(svc.TTLSec) * time.Second
		if svc.Announced.IsZero() {
			svc.Announced = time.Now()
		}
		key := remoteKey(state.NodeID, svc.Name)
		if cur, ok := r.remote[key]; ok && !svcIsNewer(svc, *cur) {
			continue
		}
		copy := svc
		r.remote[key] = &copy
		active[key] = struct{}{}
	}

	for key, svc := range r.remote {
		if svc.NodeID != state.NodeID {
			continue
		}
		if _, ok := active[key]; !ok {
			delete(r.remote, key)
		}
	}
	return nil
}

// FindByName returns all non-expired instances of a logical service name.
func (r *Registry) FindByName(name string) []Service {
	var out []Service
	for _, svc := range r.List() {
		if svc.Name == name {
			out = append(out, svc)
		}
	}
	return out
}

// List returns all non-expired services (local + remote).
func (r *Registry) List() []Service {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	out := make([]Service, 0, len(r.local)+len(r.remote))
	for _, svc := range r.local {
		out = append(out, *svc)
	}
	for key, svc := range r.remote {
		if svc.TTL > 0 && now.After(svc.Announced.Add(svc.TTL)) {
			delete(r.remote, key)
			continue
		}
		out = append(out, *svc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].NodeID < out[j].NodeID
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Prune removes expired remote entries.
func (r *Registry) Prune() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	removed := 0
	for key, svc := range r.remote {
		if svc.TTL > 0 && now.After(svc.Announced.Add(svc.TTL)) {
			delete(r.remote, key)
			removed++
		}
	}
	return removed
}

func (r *Registry) deleteRemoteLocked(nodeID, name string) {
	delete(r.remote, remoteKey(nodeID, name))
}

func serviceID(nodeID, name string, version uint64) string {
	return nodeID + "/" + name + "#" + strconv.FormatUint(version, 10)
}

func remoteKey(nodeID, name string) string {
	return nodeID + "/" + name
}

func svcIsNewer(a, b Service) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return a.NodeID > b.NodeID
}

func copyLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
