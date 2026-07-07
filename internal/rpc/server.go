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
	"google.golang.org/grpc/status"

	ztmv1 "ztm/api/proto/ztm/v1"
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
	NodeID   string
	Registry *registry.Registry
	Policy   *policy.Policy
	MeshPeers func() []string
	GossipMemberCount func() int
	TLS      *tls.Config
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
	s.grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(opts.TLS)))

	ztmv1.RegisterNodeRPCServer(s.grpcServer, &nodeRPC{
		nodeID:            opts.NodeID,
		registry:          opts.Registry,
		policy:            opts.Policy,
		meshPeers:         opts.MeshPeers,
		gossipMemberCount: opts.GossipMemberCount,
	})

	go func() {
		_ = s.grpcServer.Serve(ln)
	}()

	return s.addr, nil
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
	nodeID   string
	registry *registry.Registry
	policy   *policy.Policy
	meshPeers func() []string
	gossipMemberCount func() int
}

func (n *nodeRPC) HealthCheck(_ context.Context, req *ztmv1.HealthCheckRequest) (*ztmv1.HealthCheckResponse, error) {
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

func (n *nodeRPC) ResolveService(_ context.Context, req *ztmv1.ResolveServiceRequest) (*ztmv1.ResolveServiceResponse, error) {
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

func (n *nodeRPC) JoinCluster(context.Context, *ztmv1.JoinClusterRequest) (*ztmv1.JoinClusterResponse, error) {
	return nil, status.Error(codes.Unimplemented, "JoinCluster not implemented")
}

func (n *nodeRPC) PushPolicy(_ context.Context, req *ztmv1.PushPolicyRequest) (*ztmv1.PushPolicyResponse, error) {
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

