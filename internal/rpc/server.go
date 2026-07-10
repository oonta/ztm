package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	ztmv1 "ztm/api/proto/ztm/v1"
	"ztm/internal/identity"
	"ztm/internal/jointoken"
	"ztm/internal/metrics"
	"ztm/internal/policy"
	"ztm/internal/registry"

	"google.golang.org/protobuf/proto"
)

type Server struct {
	nodeID   string
	registry *registry.Registry

	grpcServer *grpc.Server
	ln         net.Listener
	addr       string
}

type Options struct {
	NodeID            string
	Cluster           string
	Registry          *registry.Registry
	Policy            *policy.Policy
	Identity          *identity.Store
	JoinTokens        *jointoken.Store
	GossipSecret      func() []byte
	NodeIDTaken       func(string) bool
	MeshPeers         func() []string
	GossipMemberCount func() int
	Metrics           *metrics.Collector
	TLS               *tls.Config
}

func (s *Server) Listen(addr string, opts Options) (string, error) {
	if opts.NodeID == "" {
		return "", fmt.Errorf("node id is required")
	}
	if opts.Registry == nil {
		return "", fmt.Errorf("registry is required")
	}
	if opts.TLS == nil {
		return "", fmt.Errorf("tls config is required")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}

	s.nodeID = opts.NodeID
	s.registry = opts.Registry
	s.ln = ln
	s.addr = ln.Addr().String()
	s.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(opts.TLS)),
		grpc.UnaryInterceptor(rpcMetricsInterceptor(opts.Metrics)),
	)

	ztmv1.RegisterNodeRPCServer(s.grpcServer, &nodeRPC{
		nodeID:            opts.NodeID,
		cluster:           opts.Cluster,
		registry:          opts.Registry,
		policy:            opts.Policy,
		identity:          opts.Identity,
		joinTokens:        opts.JoinTokens,
		gossipSecret:      opts.GossipSecret,
		nodeIDTaken:       opts.NodeIDTaken,
		meshPeers:         opts.MeshPeers,
		gossipMemberCount: opts.GossipMemberCount,
	})

	go func() {
		_ = s.grpcServer.Serve(ln)
	}()

	return s.addr, nil
}

func rpcMetricsInterceptor(m *metrics.Collector) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if m != nil {
			code := codes.OK
			if err != nil {
				code = status.Code(err)
			}
			m.ObserveRPC(info.FullMethod, code.String())
		}
		return resp, err
	}
}

func (s *Server) Addr() string { return s.addr }

func (s *Server) Shutdown(ctx context.Context) error {
	if s.grpcServer == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.grpcServer.Stop()
		return ctx.Err()
	}
}

type nodeRPC struct {
	ztmv1.UnimplementedNodeRPCServer
	nodeID            string
	cluster           string
	registry          *registry.Registry
	policy            *policy.Policy
	identity          *identity.Store
	joinTokens        *jointoken.Store
	gossipSecret      func() []byte
	nodeIDTaken       func(string) bool
	meshPeers         func() []string
	gossipMemberCount func() int
}

func (n *nodeRPC) requireNodePeer(ctx context.Context) error {
	if n.identity == nil {
		return status.Error(codes.Internal, "identity not configured")
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "no peer info")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.PeerCertificates) == 0 {
		return status.Error(codes.Unauthenticated, "client certificate required")
	}
	if _, err := n.identity.VerifyPeerNode(ti.State.PeerCertificates[0]); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	return nil
}

func (n *nodeRPC) HealthCheck(ctx context.Context, req *ztmv1.HealthCheckRequest) (*ztmv1.HealthCheckResponse, error) {
	if err := n.requireNodePeer(ctx); err != nil {
		return nil, err
	}
	start := time.Now()

	if tid := req.GetTargetNodeId(); tid != "" && tid != n.nodeID {
		return nil, status.Error(codes.NotFound, "unknown target node")
	}

	meshOK := true
	if n.meshPeers != nil {
		meshOK = len(n.meshPeers()) > 0
	}
	// If meshPeers isn't wired, treat mesh_ok as true (not applicable).

	gossipOK := true
	if n.gossipMemberCount != nil {
		gossipOK = n.gossipMemberCount() >= 1
	}
	// If gossipMemberCount isn't wired, treat gossip_ok as true (not applicable).

	return &ztmv1.HealthCheckResponse{
		Reachable: true,
		LatencyMs: uint32(time.Since(start) / time.Millisecond),
		MeshOk:    meshOK,
		GossipOk:  gossipOK,
	}, nil
}

func (n *nodeRPC) ResolveService(ctx context.Context, req *ztmv1.ResolveServiceRequest) (*ztmv1.ResolveServiceResponse, error) {
	if err := n.requireNodePeer(ctx); err != nil {
		return nil, err
	}
	_ = req.GetLabels() // reserved for future filtering
	instances := n.registry.FindByName(req.GetName())
	out := make([]*ztmv1.ServiceAnnouncement, 0, len(instances))
	for _, s := range instances {
		out = append(out, &ztmv1.ServiceAnnouncement{
			ServiceId: s.ServiceID,
			NodeId:    s.NodeID,
			Name:      s.Name,
			Endpoints: []*ztmv1.Endpoint{{
				Host:     s.Host,
				Port:     s.Port,
				Protocol: "tcp",
			}},
			Labels:  s.Labels,
			Version: s.Version,
			TtlSec:  s.TTLSec,
		})
	}
	return &ztmv1.ResolveServiceResponse{Services: out}, nil
}

func (n *nodeRPC) JoinCluster(_ context.Context, req *ztmv1.JoinClusterRequest) (*ztmv1.JoinClusterResponse, error) {
	if n.joinTokens == nil || n.identity == nil {
		return nil, status.Error(codes.FailedPrecondition, "join not configured")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.GetJoinToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "join_token is required")
	}
	if err := n.joinTokens.Consume(req.GetJoinToken()); err != nil {
		return nil, status.Error(codes.PermissionDenied, "invalid token")
	}
	if n.nodeIDTaken != nil && n.nodeIDTaken(req.GetNodeId()) {
		return nil, status.Error(codes.AlreadyExists, "node id already taken")
	}
	if n.identity.NodeCertExists(req.GetNodeId()) {
		return nil, status.Error(codes.AlreadyExists, "node id already taken")
	}

	certPEM, keyPEM, err := n.identity.GenerateNodeCert(req.GetNodeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issue cert: %v", err)
	}
	caPEM, err := n.identity.CACertPEM()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ca cert: %v", err)
	}
	secret := []byte(nil)
	if n.gossipSecret != nil {
		secret = n.gossipSecret()
	}
	if len(secret) != 32 {
		return nil, status.Error(codes.FailedPrecondition, "gossip secret unavailable")
	}

	cluster := n.cluster
	if cluster == "" {
		cluster = n.identity.Cluster()
	}
	return &ztmv1.JoinClusterResponse{
		NodeCertPem:   certPEM,
		NodeKeyPem:    keyPEM,
		CaCertPem:     caPEM,
		Cluster:       cluster,
		GossipSecret:  secret,
	}, nil
}

func (n *nodeRPC) PushPolicy(ctx context.Context, req *ztmv1.PushPolicyRequest) (*ztmv1.PushPolicyResponse, error) {
	if err := n.requireNodePeer(ctx); err != nil {
		return nil, err
	}
	if n.policy == nil {
		return nil, status.Error(codes.FailedPrecondition, "policy not configured")
	}
	if len(req.GetPolicyBundle()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "policy_bundle is required")
	}
	var b ztmv1.PolicyBundle
	if err := proto.Unmarshal(req.GetPolicyBundle(), &b); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid policy bundle")
	}
	accepted, _ := n.policy.ApplyBundle(&b, req.GetMinVersion())
	return &ztmv1.PushPolicyResponse{AcceptedVersion: accepted}, nil
}

