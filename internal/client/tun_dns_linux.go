//go:build linux

package client

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/miekg/dns"
)

func startTUNDNS(ctx context.Context, listenIP string) (func(), error) {
	addr := net.JoinHostPort(listenIP, "53")
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen dns %s: %w", addr, err)
	}

	srv := &dns.Server{
		PacketConn: pc,
		Net:        "udp",
		Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
			m := new(dns.Msg)
			m.SetReply(r)
			m.Authoritative = true
			for _, q := range r.Question {
				if q.Qtype != dns.TypeA || !strings.HasSuffix(q.Name, ".ztm.") {
					continue
				}
				name := strings.TrimSuffix(q.Name, ".")
				ip := virtualIPForService(name)
				registerVirtualService(name, ip)
				m.Answer = append(m.Answer, &dns.A{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
					A:   ip,
				})
			}
			_ = w.WriteMsg(m)
		}),
	}

	go func() {
		_ = srv.ActivateAndServe()
	}()

	go func() {
		<-ctx.Done()
		_ = srv.Shutdown()
	}()

	return func() {
		_ = srv.Shutdown()
	}, nil
}
