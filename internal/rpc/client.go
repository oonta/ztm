package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	ztmv1 "ztm/api/proto/ztm/v1"
)

// Client is a small mTLS gRPC client for NodeRPC.
type Client struct {
	target string
	conn   *grpc.ClientConn
	rpc    ztmv1.NodeRPCClient
}

type ClientOptions struct {
	Target string
	TLS    *tls.Config
	Timeout time.Duration
}

func Dial(ctx context.Context, opts ClientOptions) (*Client, error) {
	if opts.Target == "" {
		return nil, fmt.Errorf("target is required")
	}
	if opts.TLS == nil {
		return nil, fmt.Errorf("tls config is required")
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, opts.Target, grpc.WithTransportCredentials(credentials.NewTLS(opts.TLS)))
	if err != nil {
		return nil, err
	}
	return &Client{
		target: opts.Target,
		conn:   conn,
		rpc:    ztmv1.NewNodeRPCClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) JoinCluster(ctx context.Context, req *ztmv1.JoinClusterRequest) (*ztmv1.JoinClusterResponse, error) {
	resp, err := c.rpc.JoinCluster(ctx, req)
	if err != nil {
		return nil, status.Convert(err).Err()
	}
	return resp, nil
}

func (c *Client) HealthCheck(ctx context.Context, targetNodeID string) (*ztmv1.HealthCheckResponse, error) {
	resp, err := c.rpc.HealthCheck(ctx, &ztmv1.HealthCheckRequest{TargetNodeId: targetNodeID})
	if err != nil {
		return nil, status.Convert(err).Err()
	}
	return resp, nil
}

func (c *Client) ResolveService(ctx context.Context, name string, labels map[string]string) (*ztmv1.ResolveServiceResponse, error) {
	resp, err := c.rpc.ResolveService(ctx, &ztmv1.ResolveServiceRequest{Name: name, Labels: labels})
	if err != nil {
		return nil, status.Convert(err).Err()
	}
	return resp, nil
}

