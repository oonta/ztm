//go:build linux

package client

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/xjasonlyu/tun2socks/v2/engine"
	"ztm/internal/socks5"
)

func (a *Agent) runTUN(ctx context.Context, open func(context.Context, string, uint16) (net.Conn, error)) error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("tun socks loopback: %w", err)
	}
	defer ln.Close()

	socksAddr := ln.Addr().String()
	handler := &socks5.Handler{Open: open}
	go func() {
		if err := handler.Serve(ctx, ln); err != nil && ctx.Err() == nil {
			log.Printf("tun: socks loopback: %v", err)
		}
	}()

	dnsStop, err := startTUNDNS(ctx, a.cfg.TunDNS)
	if err != nil {
		return fmt.Errorf("tun dns: %w", err)
	}
	defer dnsStop()

	dev := a.cfg.TunDevice
	cidr := a.cfg.TunCIDR
	tunIP, prefix, err := tunGateway(cidr)
	if err != nil {
		return err
	}

	postUp := fmt.Sprintf(
		"ip addr add %s/%d dev %s && ip link set %s up && ip route add %s dev %s",
		tunIP, prefix, dev, dev, cidr, dev,
	)
	engine.Insert(&engine.Key{
		Device:     fmt.Sprintf("tun://%s", dev),
		Proxy:      fmt.Sprintf("socks5://%s", socksAddr),
		LogLevel:   "warn",
		TUNPostUp:  postUp,
	})
	go engine.Start()

	log.Printf("tun: device=%s cidr=%s dns=%s socks=%s", dev, cidr, a.cfg.TunDNS, socksAddr)
	<-ctx.Done()
	engine.Stop()
	return ctx.Err()
}

func tunGateway(cidr string) (ip string, prefix int, err error) {
	n, bits, err := net.ParseCIDR(cidr)
	if err != nil {
		return "", 0, fmt.Errorf("tun cidr: %w", err)
	}
	v4 := n.To4()
	if v4 == nil {
		return "", 0, fmt.Errorf("tun cidr: IPv4 required")
	}
	ones, _ := bits.Mask.Size()
	return fmt.Sprintf("%d.%d.%d.1", v4[0], v4[1], v4[2]), ones, nil
}
