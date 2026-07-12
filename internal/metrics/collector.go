package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Collector holds ZTM Prometheus metrics.
type Collector struct {
	Reg *prometheus.Registry

	Members  prometheus.Gauge
	Peers    prometheus.Gauge
	Services prometheus.Gauge

	TunnelConnections prometheus.Gauge
	TunnelStreams     prometheus.Gauge
	RelayBytes        prometheus.Counter
	ACLDenied         prometheus.Counter
	RPCRequests       *prometheus.CounterVec
}

// New creates and registers ZTM metrics.
func New() *Collector {
	reg := prometheus.NewRegistry()
	c := &Collector{
		Reg: reg,
		Members: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ztm",
			Name:      "member_count",
			Help:      "Number of gossip members visible to this node.",
		}),
		Peers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ztm",
			Name:      "mesh_peer_count",
			Help:      "Number of connected mesh peers.",
		}),
		Services: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ztm",
			Name:      "service_count",
			Help:      "Number of services visible to this node (local + remote).",
		}),
		TunnelConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ztm",
			Name:      "tunnel_connections_active",
			Help:      "Active client QUIC connections.",
		}),
		TunnelStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ztm",
			Name:      "tunnel_streams_active",
			Help:      "Active client tunnel streams.",
		}),
		RelayBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ztm",
			Name:      "relay_bytes_total",
			Help:      "Total bytes relayed through tunnels and mesh.",
		}),
		ACLDenied: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ztm",
			Name:      "acl_denied_total",
			Help:      "Total ACL denials.",
		}),
		RPCRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ztm",
			Name:      "rpc_requests_total",
			Help:      "NodeService requests by method and gRPC status code.",
		}, []string{"method", "code"}),
	}
	for _, col := range []prometheus.Collector{
		c.Members, c.Peers, c.Services,
		c.TunnelConnections, c.TunnelStreams,
		c.RelayBytes, c.ACLDenied, c.RPCRequests,
	} {
		_ = reg.Register(col)
	}
	return c
}

func (c *Collector) IncACLDenied() {
	if c != nil {
		c.ACLDenied.Inc()
	}
}

func (c *Collector) AddRelayBytes(n int64) {
	if c != nil && n > 0 {
		c.RelayBytes.Add(float64(n))
	}
}

func (c *Collector) TunnelConnOpened() {
	if c != nil {
		c.TunnelConnections.Inc()
	}
}

func (c *Collector) TunnelConnClosed() {
	if c != nil {
		c.TunnelConnections.Dec()
	}
}

func (c *Collector) TunnelStreamOpened() {
	if c != nil {
		c.TunnelStreams.Inc()
	}
}

func (c *Collector) TunnelStreamClosed() {
	if c != nil {
		c.TunnelStreams.Dec()
	}
}

func (c *Collector) ObserveRPC(method, code string) {
	if c != nil {
		c.RPCRequests.WithLabelValues(method, code).Inc()
	}
}
