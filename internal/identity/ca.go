package identity

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	caCertFile = "ca/cert.pem"
	caKeyFile  = "ca/key.pem"
)

// Store manages cluster CA and issued node certificates on disk.
type Store struct {
	cluster string
	dir     string

	mu     sync.Mutex
	caCert *x509.Certificate
	caKey  *rsa.PrivateKey
}

// Open loads or creates a CA in dir and prepares node certificate issuance.
func Open(cluster, dir string) (*Store, error) {
	if cluster == "" {
		return nil, fmt.Errorf("cluster name is required")
	}
	if dir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if err := os.MkdirAll(filepath.Join(dir, "ca"), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "nodes"), 0o700); err != nil {
		return nil, err
	}

	s := &Store{cluster: cluster, dir: dir}
	if err := s.loadOrCreateCA(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) loadOrCreateCA() error {
	certPath := filepath.Join(s.dir, caCertFile)
	keyPath := filepath.Join(s.dir, caKeyFile)

	if _, err := os.Stat(certPath); err == nil {
		certPEM, err := os.ReadFile(certPath)
		if err != nil {
			return err
		}
		keyPEM, err := os.ReadFile(keyPath)
		if err != nil {
			return err
		}
		cert, key, err := parseCertKey(certPEM, keyPEM)
		if err != nil {
			return err
		}
		s.caCert = cert
		s.caKey = key
		return nil
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"ZTM CA"}, CommonName: s.cluster},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return err
	}

	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return err
	}
	if err := writePEM(keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key)); err != nil {
		return err
	}

	s.caCert = cert
	s.caKey = key
	return nil
}

// EnsureNodeCert loads or issues a node certificate for nodeID.
func (s *Store) EnsureNodeCert(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodeDir := filepath.Join(s.dir, "nodes", nodeID)
	if err := os.MkdirAll(nodeDir, 0o700); err != nil {
		return err
	}

	certPath := filepath.Join(nodeDir, "cert.pem")
	if _, err := os.Stat(certPath); err == nil {
		return nil
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	spiffeURI, err := url.Parse(NodeURI(s.cluster, nodeID))
	if err != nil {
		return err
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName:   "node:" + nodeID,
			Organization: []string{"ZTM"},
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().AddDate(0, 0, 30),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{spiffeURI},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, s.caCert, &key.PublicKey, s.caKey)
	if err != nil {
		return err
	}

	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return err
	}
	return writePEM(filepath.Join(nodeDir, "key.pem"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}

// NodeTLSCertificate returns the TLS certificate for a node.
func (s *Store) NodeTLSCertificate(nodeID string) (tls.Certificate, error) {
	certPEM, err := os.ReadFile(filepath.Join(s.dir, "nodes", nodeID, "cert.pem"))
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(s.dir, "nodes", nodeID, "key.pem"))
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// CAPool returns a cert pool with the cluster CA.
func (s *Store) CAPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(s.caCert)
	return pool
}

// TLSConfig builds a mTLS config for mesh QUIC.
func (s *Store) TLSConfig(nodeID string, server bool) (*tls.Config, error) {
	cert, err := s.NodeTLSCertificate(nodeID)
	if err != nil {
		return nil, err
	}

	pool := s.CAPool()
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"ztm-mesh/1"},
	}

	if server {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("no peer certificate")
		}
		peer, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		peerID, ok := ParseNodeID(s.cluster, peer.URIs)
		if !ok {
			return fmt.Errorf("peer missing SPIFFE node URI for cluster %q", s.cluster)
		}
		if peerID == nodeID {
			return fmt.Errorf("peer connected to itself")
		}
		return nil
	}

	return cfg, nil
}

func parseCertKey(certPEM, keyPEM []byte) (*x509.Certificate, *rsa.PrivateKey, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, nil, fmt.Errorf("decode certificate PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, fmt.Errorf("decode key PEM")
	}
	key, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func writePEM(path, typ string, der []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600)
}
