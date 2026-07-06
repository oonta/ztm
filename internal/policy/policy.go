package policy

import (
	"strings"
)

// Policy evaluates client access to services.
type Policy struct {
	allowServices []string
	denyServices  []string
	permissive    bool
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
	p.allowServices = append(p.allowServices, pattern)
	return p
}

// DenyService adds a denied service pattern.
func (p *Policy) DenyService(pattern string) *Policy {
	p.denyServices = append(p.denyServices, pattern)
	return p
}

// Allow checks whether clientID may access service.
func (p *Policy) Allow(_ string, service string) bool {
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
