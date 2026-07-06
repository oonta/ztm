package mesh

import (
	"time"

	"github.com/quic-go/quic-go"
)

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:  5 * time.Minute,
		KeepAlivePeriod: 15 * time.Second,
	}
}
