package identity

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const clientTunnelALPN = "ztm/1"

// ParseClientID extracts client ID from a peer certificate URI SAN.
func ParseClientID(cluster string, uris []*url.URL) (string, bool) {
	const prefix = "/client/"
	for _, u := range uris {
		if u.Scheme != scheme || u.Host != cluster {
			continue
		}
		if id, ok := strings.CutPrefix(u.Path, prefix); ok && id != "" {
			return id, true
		}
	}
	return "", false
}

// EnsureClientCert loads or issues a client certificate.
func (s *Store) EnsureClientCert(clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Join(s.dir, "clients", clientID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	certPath := filepath.Join(dir, "cert.pem")
	if _, err := os.Stat(certPath); err == nil {
		return nil
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	spiffeURI, err := url.Parse(ClientURI(s.cluster, clientID))
	if err != nil {
		return err
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName:   "client:" + clientID,
			Organization: []string{"ZTM"},
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().AddDate(0, 0, 1),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        []*url.URL{spiffeURI},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, s.caCert, &key.PublicKey, s.caKey)
	if err != nil {
		return err
	}
	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return err
	}
	return writePEM(filepath.Join(dir, "key.pem"), "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
}

// ClientTLSCertificate returns TLS cert for a client.
func (s *Store) ClientTLSCertificate(clientID string) (tls.Certificate, error) {
	dir := filepath.Join(s.dir, "clients", clientID)
	certPEM, err := os.ReadFile(filepath.Join(dir, "cert.pem"))
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, "key.pem"))
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// TunnelServerTLSConfig is mTLS for client-facing QUIC listener on a node.
func (s *Store) TunnelServerTLSConfig(nodeID string) (*tls.Config, error) {
	cert, err := s.NodeTLSCertificate(nodeID)
	if err != nil {
		return nil, err
	}
	pool := s.CAPool()
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{clientTunnelALPN},
	}
	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		_, err := verifyClientCert(s.cluster, rawCerts)
		return err
	}
	return cfg, nil
}

// ClientTunnelTLSConfig is mTLS for ztm-client dialing a node.
func (s *Store) ClientTunnelTLSConfig(clientID string) (*tls.Config, error) {
	cert, err := s.ClientTLSCertificate(clientID)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      s.CAPool(),
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{clientTunnelALPN},
	}, nil
}

// LoadClientTLSConfig loads client TLS materials from PEM files.
func LoadClientTLSConfig(certFile, keyFile, caFile string) (tls.Certificate, *x509.CertPool, error) {
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, fmt.Errorf("parse CA PEM")
	}
	return cert, pool, nil
}

// ClientIDFromCert returns client id from a TLS certificate.
func ClientIDFromCert(cluster string, cert *x509.Certificate) (string, bool) {
	return ParseClientID(cluster, cert.URIs)
}

func verifyClientCert(cluster string, rawCerts [][]byte) (string, error) {
	if len(rawCerts) == 0 {
		return "", fmt.Errorf("no peer certificate")
	}
	peer, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return "", err
	}
	id, ok := ParseClientID(cluster, peer.URIs)
	if !ok {
		return "", fmt.Errorf("peer missing SPIFFE client URI for cluster %q", cluster)
	}
	return id, nil
}

// ClientCertPaths returns default PEM paths for a client under data dir.
func ClientCertPaths(dataDir, clientID string) (cert, key, ca string) {
	dir := filepath.Join(dataDir, "clients", clientID)
	return filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dataDir, "ca", "cert.pem")
}
