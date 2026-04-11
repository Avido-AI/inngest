package executor

import (
	"context"
	"time"

	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/logger"
)

// runStaleRunRecovery runs a background goroutine that periodically scans for
// stale RUNNING runs (runs with no outstanding queue items that have been active
// longer than StaleRunThreshold). When found, these runs are cancelled via the
// executor, which triggers Finalize() to release concurrency locks and fire
// function.finished events.
//
// This handles orphaned runs caused by lost events during rolling deployments,
// where in-flight events in the in-memory pubsub are lost when pods terminate.
func (s *svc) runStaleRunRecovery(ctx context.Context) {
	l := s.log.With("component", "stale-run-recovery")

	// Get the queue shard for ConfigLease coordination and stale run scanning.
	qp, ok := s.queue.(queue.QueueProcessor)
	if !ok {
		l.Warn("queue does not implement QueueProcessor, stale run recovery disabled")
		return
	}

	shard := qp.Shard()
	if shard == nil {
		l.Warn("no primary queue shard available, stale run recovery disabled")
		return
	}

	scavenger, ok := shard.(queue.StaleRunScavenger)
	if !ok {
		l.Warn("queue shard does not support stale run scavenging, stale run recovery disabled")
		return
	}

	// Use ConfigLease for distributed coordination - only one pod should run this.
	leaseKey := "stale-run-recovery"
	leaseDuration := queue.ConfigLeaseDuration

	leaseID, err := shard.ConfigLease(ctx, leaseKey, leaseDuration)
	if err != nil && err != queue.ErrConfigAlreadyLeased {
		l.Error("error claiming stale run recovery lease", "error", err)
		return
	}

	leaseTick := time.NewTicker(leaseDuration / 3)
	scavengeTick := time.NewTicker(consts.StaleRunScavengerInterval)

	defer leaseTick.Stop()
	defer scavengeTick.Stop()

	isLeaseHolder := leaseID != nil

	for {
		select {
		case <-ctx.Done():
			return
		case <-scavengeTick.C:
			if !isLeaseHolder {
				continue
			}
			s.scavengeStaleRuns(ctx, l, scavenger)
		case <-leaseTick.C:
			newLeaseID, err := shard.ConfigLease(ctx, leaseKey, leaseDuration, leaseID)
			if err == queue.ErrConfigAlreadyLeased {
				isLeaseHolder = false
				leaseID = nil
				continue
			}
			if err != nil {
				l.Error("error renewing stale run recovery lease", "error", err)
				isLeaseHolder = false
				leaseID = nil
				continue
			}
			leaseID = newLeaseID
			isLeaseHolder = true
		}
	}
}

func (s *svc) scavengeStaleRuns(ctx context.Context, l logger.Logger, scavenger queue.StaleRunScavenger) {
	staleRuns, err := scavenger.ScavengeStaleRuns(ctx, consts.StaleRunThreshold)
	if err != nil {
		l.Error("error scanning for stale runs", "error", err)
		return
	}

	if len(staleRuns) == 0 {
		return
	}

	l.Info("found stale runs to recover", "count", len(staleRuns))

	for _, run := range staleRuns {
		runLogger := l.With(
			"run_id", run.RunID.String(),
			"function_id", run.FunctionID.String(),
			"account_id", run.AccountID.String(),
		)

		id := sv2.ID{
			RunID:      run.RunID,
			FunctionID: run.FunctionID,
			Tenant: sv2.Tenant{
				AccountID: run.AccountID,
				EnvID:     run.WorkspaceID,
				AppID:     run.AppID,
			},
		}

		runLogger.Warn("cancelling stale run")

		if err := s.exec.Cancel(ctx, id, execution.CancelRequest{}); err != nil {
			runLogger.Error("error cancelling stale run", "error", err)
			continue
		}

		// Remove from active runs index after successful cancellation.
		if err := scavenger.RemoveActiveRun(ctx, run); err != nil {
			runLogger.Error("error removing stale run from active runs index", "error", err)
		}

		runLogger.Info("successfully cancelled stale run")
	}
}
