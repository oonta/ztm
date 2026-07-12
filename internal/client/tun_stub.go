//go:build !linux

package client

import (
	"context"
	"fmt"
	"net"
)

func (a *Agent) runTUN(ctx context.Context, open func(context.Context, string, uint16) (net.Conn, error)) error {
	_ = ctx
	_ = open
	return fmt.Errorf("TUN mode requires Linux (build with GOOS=linux)")
}
