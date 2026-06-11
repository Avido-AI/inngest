package connect

import (
	"context"
	"net"
	"testing"
	"time"

	connectConfig "github.com/inngest/inngest/pkg/config/connect"
	"github.com/stretchr/testify/require"
)

// Run must fail immediately when a listener cannot be bound (e.g. the fixed
// gRPC port was claimed as an ephemeral port by an outbound connection).
// Before the synchronous bind, the error sat unobserved in the errgroup until
// shutdown while the gateway reported itself active without a gRPC server.
func TestRunFailsFastWhenGatewayGRPCPortTaken(t *testing.T) {
	// Occupy a port to simulate the collision. The blocker must bind the
	// wildcard address to conflict with the gateway's ":port" bind on all
	// platforms.
	blocker, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer func() { _ = blocker.Close() }()
	blockedPort := blocker.Addr().(*net.TCPAddr).Port

	svc := NewConnectGatewayService(
		// Port 0 lets the gateway api bind any free port; only the gRPC
		// bind should fail.
		WithGatewayPublicPort(0),
		WithGRPCConfig(connectConfig.NewGRPCConfig(
			context.Background(),
			"127.0.0.1", blockedPort,
			"127.0.0.1", 0,
		)),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	select {
	case err := <-done:
		require.Error(t, err)
		require.Contains(t, err.Error(), "gateway grpc")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after failing to bind the grpc listener")
	}
}
