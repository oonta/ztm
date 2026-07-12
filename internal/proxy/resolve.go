package proxy

import (
	"net"
	"strconv"

	"ztm/internal/registry"
)

// ResolveLocal finds a service registered on this node.
func ResolveLocal(reg *registry.Registry, nodeID, name string) (host string, port uint32, ok bool) {
	for _, svc := range reg.List() {
		if svc.Name == name && svc.NodeID == nodeID {
			return svc.Host, svc.Port, true
		}
	}
	return "", 0, false
}

// DialTCP connects to host:port.
func DialTCP(host string, port uint32) (net.Conn, error) {
	return net.Dial("tcp", net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10)))
}

// ServiceNameFromHost maps SOCKS target host to logical service name.
func ServiceNameFromHost(host string) string {
	const suffix = ".ztm"
	if name, ok := stringsCutSuffix(host, suffix); ok {
		return name
	}
	return host
}

func stringsCutSuffix(s, suffix string) (string, bool) {
	if len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix {
		return s[:len(s)-len(suffix)], true
	}
	return s, false
}
