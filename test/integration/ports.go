package integration

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"ztm/internal/agent"
)

var (
	portMu   sync.Mutex
	nextPort = 32000
)

// ensureAgentBinds replaces ":0" binds with explicit ports in a range that avoids
// common Windows Hyper-V/WSL excluded UDP/TCP blocks.
func ensureAgentBinds(cfg agent.Config) agent.Config {
	if needsPort(cfg.MeshBind) {
		cfg.MeshBind = allocUDPAddr()
	}
	if needsPort(cfg.ClientBind) {
		cfg.ClientBind = allocUDPAddr()
	}
	if needsPort(cfg.GossipBind) {
		cfg.GossipBind = allocUDPAddr()
	}
	if needsPort(cfg.AdminBind) {
		cfg.AdminBind = allocTCPAddr()
	}
	if needsPort(cfg.RPCBind) {
		cfg.RPCBind = allocTCPAddr()
	}
	return cfg
}

func needsPort(bind string) bool {
	if bind == "" {
		return true
	}
	_, port, err := net.SplitHostPort(bind)
	if err != nil {
		return strings.HasSuffix(bind, ":0")
	}
	return port == "0"
}

func allocUDPAddr() string {
	return allocAddr("udp")
}

func allocTCPAddr() string {
	return allocAddr("tcp")
}

func allocAddr(network string) string {
	portMu.Lock()
	defer portMu.Unlock()
	for i := 0; i < 300; i++ {
		nextPort++
		if nextPort > 61000 {
			nextPort = 32000
		}
		addr := fmt.Sprintf("127.0.0.1:%d", nextPort)
		if network == "udp" {
			pc, err := net.ListenPacket("udp", addr)
			if err != nil {
				continue
			}
			_ = pc.Close()
			return addr
		}
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			continue
		}
		_ = ln.Close()
		return addr
	}
	panic("integration: failed to allocate listen port")
}
