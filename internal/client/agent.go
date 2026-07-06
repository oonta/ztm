package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"

	"ztm/internal/identity"
	"ztm/internal/proxy"
	"ztm/internal/socks5"
	"ztm/internal/tunnel"
)

// Config holds ztm-client runtime settings.
type Config struct {
	NodeAddr    string
	Cluster     string
	ClientID    string
	DataDir     string
	CertFile    string
	KeyFile     string
	CAFile      string
	SocksListen string
}

// Agent runs the client tunnel and SOCKS5 proxy.
type Agent struct {
	cfg Config
}

// New creates a client agent.
func New(cfg Config) *Agent {
	if cfg.Cluster == "" {
		cfg.Cluster = "default"
	}
	if cfg.SocksListen == "" {
		cfg.SocksListen = "127.0.0.1:1080"
	}
	return &Agent{cfg: cfg}
}

// Run starts SOCKS5 and the QUIC tunnel until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	tlsConf, clientIdentity, err := a.tlsConfig()
	if err != nil {
		return err
	}

	tc, err := tunnel.Dial(ctx, a.cfg.NodeAddr, tlsConf)
	if err != nil {
		return err
	}
	defer tc.Close()

	ln, err := net.Listen("tcp", a.cfg.SocksListen)
	if err != nil {
		return err
	}
	defer ln.Close()

	handler := &socks5.Handler{
		Open: func(ctx context.Context, host string, port uint16) (net.Conn, error) {
			service := proxy.ServiceNameFromHost(host)
			stream, err := tc.OpenStream(ctx, clientIdentity, service, uint32(port))
			if err != nil {
				return nil, err
			}
			return &streamConn{Stream: stream}, nil
		},
	}
	return handler.Serve(ctx, ln)
}

func (a *Agent) tlsConfig() (*tls.Config, string, error) {
	certFile := a.cfg.CertFile
	keyFile := a.cfg.KeyFile
	caFile := a.cfg.CAFile
	if certFile == "" && a.cfg.DataDir != "" && a.cfg.ClientID != "" {
		certFile, keyFile, caFile = identity.ClientCertPaths(a.cfg.DataDir, a.cfg.ClientID)
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, "", fmt.Errorf("cert, key, and ca are required (or --data-dir with --client-id)")
	}

	cert, pool, err := identity.LoadClientTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		return nil, "", err
	}

	clientID := a.cfg.ClientID
	if clientID == "" {
		if len(cert.Certificate) > 0 {
			if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
				for _, u := range leaf.URIs {
					if id, ok := identity.ParseClientID(u.Host, leaf.URIs); ok {
						clientID = id
						break
					}
				}
			}
		}
	}
	if clientID == "" {
		return nil, "", fmt.Errorf("client-id is required")
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"ztm/1"},
	}
	return cfg, identity.ClientURI(a.cfg.Cluster, clientID), nil
}

type streamConn struct {
	*quic.Stream
}

func (c *streamConn) LocalAddr() net.Addr              { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (c *streamConn) RemoteAddr() net.Addr             { return &net.TCPAddr{IP: net.IPv4zero, Port: 0} }
func (c *streamConn) SetDeadline(t time.Time) error      { return c.Stream.SetDeadline(t) }
func (c *streamConn) SetReadDeadline(t time.Time) error  { return c.Stream.SetReadDeadline(t) }
func (c *streamConn) SetWriteDeadline(t time.Time) error { return c.Stream.SetWriteDeadline(t) }
