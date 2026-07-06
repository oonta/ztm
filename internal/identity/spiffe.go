package identity

import (
	"fmt"
	"net/url"
	"strings"
)

const scheme = "spiffe"

// NodeURI returns a SPIFFE URI for an edge node.
func NodeURI(cluster, nodeID string) string {
	return fmt.Sprintf("%s://%s/node/%s", scheme, cluster, nodeID)
}

// ClientURI returns a SPIFFE URI for a client identity.
func ClientURI(cluster, clientID string) string {
	return fmt.Sprintf("%s://%s/client/%s", scheme, cluster, clientID)
}

// ParseNodeID extracts node ID from a peer certificate URI SAN.
func ParseNodeID(cluster string, uris []*url.URL) (string, bool) {
	const prefix = "/node/"
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
