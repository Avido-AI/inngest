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
// dequeued by any pod after a 30-second delay.
//
// Normal case: the inline Finalize() already completed and state was deleted,
// so Cancel() finds no metadata and returns nil (no-op).
//
// Crash-recovery case: state still exists because the pod was killed mid-
// finalize. Cancel() drives the run to a terminal state via Finalize(Cancel:true)
// which cleans up state and publishes the function.cancelled event. However,
// SkipLifecycleHooks is set so that Cancel's unconditional OnFunctionCancelled
// call is suppressed — the synchronous lifecycle hooks (Layer 1) may have
// already written function_finishes as "completed" before the crash, and
// re-firing as "cancelled" would produce a conflicting record.
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

	s.log.Info("finalize backstop: triggered",
		"run_id", id.RunID.String(),
		"function_id", id.FunctionID.String(),
	)

	err := s.exec.Cancel(ctx, id, execution.CancelRequest{
		SkipLifecycleHooks: true,
	})
	if err != nil {
		s.log.Error("finalize backstop: cancel failed, will retry",
			"run_id", id.RunID.String(),
			"function_id", id.FunctionID.String(),
			"error", err,
		)
	}
	return err
}
