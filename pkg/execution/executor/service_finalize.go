package executor

import (
	"context"
	"fmt"

	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
)

// handleFinalize is the durable backstop for run finalization. It is dispatched
// when a KindFinalize queue item — enqueued at the start of Finalize() — is
// dequeued by any pod. In the normal case the inline Finalize() already
// completed and state was deleted, so this is a no-op. When the pod that called
// Finalize() was killed mid-flight, state still exists and Cancel() drives the
// run to a terminal state (cancelled) so it does not remain stuck.
func (s *svc) handleFinalize(ctx context.Context, item queue.Item) error {
	payload, ok := item.Payload.(queue.PayloadFinalize)
	if !ok {
		return fmt.Errorf("unable to get finalize payload from queue item: %T", item.Payload)
	}

	id := sv2.ID{
		RunID:      payload.RunID,
		FunctionID: payload.FunctionID,
		Tenant: sv2.Tenant{
			AccountID: payload.AccountID,
			EnvID:     payload.EnvID,
			AppID:     payload.AppID,
		},
	}

	return s.exec.Cancel(ctx, id, execution.CancelRequest{
		ForceLifecycleHook: true,
	})
}
