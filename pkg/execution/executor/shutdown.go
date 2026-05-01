package executor

import (
	"context"
	"time"
)

// StopTimeout returns the maximum time the executor should wait for
// in-flight queue items to finish during graceful shutdown.  The Helm
// chart sets terminationGracePeriodSeconds=120 with a 15s preStop
// hook, leaving ~105s.  We use 90s to leave headroom for the global
// waitgroup drain and other cleanup.
func (s *svc) StopTimeout() time.Duration { return 90 * time.Second }

func (s *svc) Stop(ctx context.Context) error {
	s.exec.CloseLifecycleListeners(ctx)

	// Wait for all in-flight queue runs to finish, but respect the
	// context deadline so that the service framework's stop timeout
	// is honoured instead of blocking indefinitely.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
