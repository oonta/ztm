package client

import (
	"fmt"
	"net"
	"sync"
)

var (
	virtualServiceMu       sync.RWMutex
	virtualServiceToIP     map[string]string
	virtualIPToServiceName map[string]string
)

func serviceFromVirtualIP(host string) (string, bool) {
	ip := net.ParseIP(host)
	if ip == nil {
		return "", false
	}
	v4 := ip.To4()
	if v4 == nil || v4[0] != 100 || v4[1] != 127 {
		return "", false
	}
	name, ok := virtualIPToService(v4)
	return name, ok
}

func virtualIPToService(v4 net.IP) (string, bool) {
	virtualServiceMu.RLock()
	defer virtualServiceMu.RUnlock()
	name, ok := virtualIPToServiceName[virtualIPKey(v4)]
	return name, ok
}

func virtualIPKey(v4 net.IP) string {
	return fmt.Sprintf("%d.%d.%d.%d", v4[0], v4[1], v4[2], v4[3])
}

func registerVirtualService(name string, ip net.IP) {
	virtualServiceMu.Lock()
	defer virtualServiceMu.Unlock()
	if virtualServiceToIP == nil {
		virtualServiceToIP = make(map[string]string)
		virtualIPToServiceName = make(map[string]string)
	}
	key := virtualIPKey(ip.To4())
	virtualServiceToIP[name] = key
	virtualIPToServiceName[key] = name
}

func virtualIPForService(name string) net.IP {
	h := fnv1a32(name)
	return net.IPv4(100, 127, byte((h>>8)&0xff), byte((h&0xff)|2))
}

func fnv1a32(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}
