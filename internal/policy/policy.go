package policy

import (
	"strings"
	"sync"
	"time"

	ztmv1 "ztm/api/proto/ztm/v1"
)

// Policy evaluates client access to services.
type Policy struct {
	mu sync.RWMutex

	allowServices []string
	denyServices  []string
	permissive    bool

	version   uint64
	cluster   string
	updatedAt time.Time
}

// New returns a default-deny policy with no rules.
func New() *Policy {
	return &Policy{}
}

// Permissive allows all clients to reach all services.
func Permissive() *Policy {
	return &Policy{permissive: true}
}

// AllowService adds an allowed service pattern (* suffix supported).
func (p *Policy) AllowService(pattern string) *Policy {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowServices = append(p.allowServices, pattern)
	return p
}

// DenyService adds a denied service pattern.
func (p *Policy) DenyService(pattern string) *Policy {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.denyServices = append(p.denyServices, pattern)
	return p
}

// Allow checks whether clientID may access service.
func (p *Policy) Allow(_ string, service string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.permissive {
		return true
	}
	for _, pattern := range p.denyServices {
		if matchPattern(pattern, service) {
			return false
		}
	}
	if len(p.allowServices) == 0 {
		return len(p.denyServices) == 0
	}
	for _, pattern := range p.allowServices {
		if matchPattern(pattern, service) {
			return true
		}
	}
	return false
}

func (p *Policy) Version() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.version
}

// ApplyBundle atomically replaces the policy rules with a PolicyBundle.
// Returns accepted version or an error when rejected.
func (p *Policy) ApplyBundle(b *ztmv1.PolicyBundle, minVersion uint64) (uint64, error) {
	if b == nil {
		return 0, nil
	}
	if b.GetVersion() < minVersion {
		return p.Version(), nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if b.GetCluster() != "" && p.cluster != "" && b.GetCluster() != p.cluster {
		return p.version, nil
	}
	if b.GetVersion() <= p.version {
		return p.version, nil
	}

	var allow []string
	var deny []string
	var permissive bool

	for _, rule := range b.GetRules() {
		// MVP: only global subject "*" rules (or empty) are applied.
		sub := rule.GetSubject()
		if sub != "" && sub != "*" {
			continue
		}
		for _, m := range rule.GetAllow() {
			if s := m.GetService(); s != "" {
				allow = append(allow, s)
			}
		}
		for _, m := range rule.GetDeny() {
			if s := m.GetService(); s != "" {
				deny = append(deny, s)
			}
		}
	}
	if len(allow) == 0 && len(deny) == 0 {
		// Empty bundle means default-deny (not permissive).
		permissive = false
	}

	p.allowServices = allow
	p.denyServices = deny
	p.permissive = permissive
	p.version = b.GetVersion()
	p.cluster = b.GetCluster()
	if ts := b.GetUpdatedAtUnix(); ts > 0 {
		p.updatedAt = time.Unix(ts, 0)
	} else {
		p.updatedAt = time.Now()
	}

	return p.version, nil
}

func matchPattern(pattern, value string) bool {
	if pattern == "*" || pattern == value {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		return strings.HasSuffix(value, strings.TrimPrefix(pattern, "*"))
	}
	if strings.HasSuffix(pattern, ".*") {
		return strings.HasPrefix(value, strings.TrimSuffix(pattern, ".*"))
	}
	return false
}
