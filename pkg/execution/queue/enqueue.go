package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
)

// batchEnqueueShard is an optional interface for shards that support batch enqueue
// via Redis pipeline.
type batchEnqueueShard interface {
	EnqueueItemBatch(ctx context.Context, items []QueueItem, ats []time.Time, opts EnqueueOpts) []error
}

const (
	pkgName = "queue.processor"
)

// Enqueue adds an item to the queue to be processed at the given time.
// TODO: Lift this function and the queue interface to a higher level, so that it's disconnected from the
// concrete Redis implementation.
func (q *queueProcessor) Enqueue(ctx context.Context, item Item, at time.Time, opts EnqueueOpts) error {
	l := logger.StdlibLogger(ctx)

	// propagate
	if item.Metadata == nil {
		item.Metadata = map[string]any{}
	}

	id := ""
	if item.JobID != nil {
		id = *item.JobID
	}

	if item.QueueName == nil {
		// Check if we have a kind mapping.
		if name, ok := q.queueKindMapping[item.Kind]; ok {
			item.QueueName = &name
		}
	}

	qi := QueueItem{
		ID:          id,
		AtMS:        at.UnixMilli(),
		WorkspaceID: item.WorkspaceID,
		FunctionID:  item.Identifier.WorkflowID,
		Data:        item,
		QueueName:   item.QueueName,
		WallTimeMS:  at.UnixMilli(),
	}

	l = l.With(
		"item", qi,
		"account_id", item.Identifier.AccountID,
		"env_id", item.WorkspaceID,
		"app_id", item.Identifier.AppID,
		"fn_id", item.Identifier.WorkflowID,
	)

	if item.QueueName == nil && qi.FunctionID == uuid.Nil {
		err := fmt.Errorf("queue name or function ID must be set")
		l.ReportError(err, "attempted to enqueue QueueItem without function ID or queueName override")
		return err
	}

	// Pass optional idempotency period to queue item
	if opts.IdempotencyPeriod != nil {
		qi.IdempotencyPeriod = opts.IdempotencyPeriod
	}

	// Use the queue item's score, ensuring we process older function runs first
	// (eg. before at)
	next := time.UnixMilli(qi.Score(q.Clock().Now()))

	if factor := qi.Data.GetPriorityFactor(); factor != 0 {
		// Ensure we mutate the AtMS time by the given priority factor.
		qi.AtMS -= factor
	}

	shard, err := q.selectShard(ctx, opts.ForceQueueShardName, qi)
	if err != nil {
		return err
	}

	metrics.IncrQueueItemStatusCounter(ctx, metrics.CounterOpt{
		PkgName: pkgName,
		Tags: map[string]any{
			"status":      "enqueued",
			"kind":        item.Kind,
			"queue_shard": shard.Name(),
		},
	})

	switch shard.Kind() {
	case enums.QueueShardKindRedis:
		_, err := shard.EnqueueItem(ctx, qi, next, opts)
		if err != nil {
			return err
		}

		// XXX: If we've enqueued a user queue item (sleep, retry, step, etc.) and it's in the future,
		// we want to ensure that we schedule a rebalance job which takes the queue item and places it
		// at the correct score based off of the item's run ID when it becomes available.
		//
		// Without this, step.sleep or retries for a very old workflow may still lag behind steps from
		// later workflows when scheduled in the future.  This can, worst case, cause never-ending runs.
		if !q.enableJobPromotion || !qi.RequiresPromotionJob(q.Clock().Now()) {
			// scheule a rebalance job automatically.
			return nil
		}

		// This is to prevent infinite recursion in case RequiresPromotion is accidentally refactored
		// to include the below job kind.
		if qi.Data.Kind == KindJobPromote {
			return nil
		}

		// This is the fudge job.  What a name!
		//
		// If we're processing a user function and the sleep duration is in the future,
		// enqueue a sleep scavenge system queue item that will Requeue the original sleep queue item.
		// We do this to fudge the original queue item at the exact time, the run was scheduled for to ensure
		// sleeps for existing function runs are picked up earlier than items for later function runs.
		promoteAt := time.UnixMilli(qi.AtMS).Add(consts.FutureAtLimit * -1)
		promoteJobID := fmt.Sprintf("promote-%s", qi.ID)
		promoteQueueName := fmt.Sprintf("job-promote:%s", qi.FunctionID)
		err = q.Enqueue(ctx, Item{
			JobID:          &promoteJobID,
			WorkspaceID:    qi.Data.WorkspaceID,
			QueueName:      &promoteQueueName,
			Kind:           KindJobPromote,
			Identifier:     qi.Data.Identifier,
			PriorityFactor: qi.Data.PriorityFactor,
			Attempt:        0,
			Payload: PayloadJobPromote{
				PromoteJobID: qi.ID,
				ScheduledAt:  qi.AtMS,
			},
		}, promoteAt, EnqueueOpts{})
		if err != nil && err != ErrQueueItemExists {
			// This is best effort, and shouldn't fail the OG enqueue.
			l.ReportError(err, "error scheduling promotion job")
		}
		return nil
	default:
		return fmt.Errorf("unknown shard kind: %s", string(shard.Kind()))
	}
}

// EnqueueBatch enqueues multiple items in a single Redis pipeline roundtrip.
// Returns a per-item error slice (nil = success). This satisfies the BatchEnqueuer
// optional interface.
func (q *queueProcessor) EnqueueBatch(ctx context.Context, items []Item, ats []time.Time, opts EnqueueOpts) []error {
	if len(items) == 0 {
		return nil
	}

	qis, effectiveAts, prepErr := q.prepareQueueItems(items, ats, opts)
	if prepErr != nil {
		return prepErr
	}

	shard, errs := q.selectBatchShard(ctx, opts, qis[0], len(items))
	if errs != nil {
		return errs
	}

	bs, ok := shard.(batchEnqueueShard)
	if !ok {
		return q.enqueueFallback(ctx, items, ats, opts)
	}

	errs = bs.EnqueueItemBatch(ctx, qis, effectiveAts, opts)
	q.emitBatchMetrics(ctx, items, errs, shard)
	return errs
}

// prepareQueueItems converts Items to QueueItems without mutating the input slice.
func (q *queueProcessor) prepareQueueItems(items []Item, ats []time.Time, opts EnqueueOpts) ([]QueueItem, []time.Time, []error) {
	qis := make([]QueueItem, len(items))
	effectiveAts := make([]time.Time, len(items))

	for idx := range items {
		workingItem := items[idx]
		if workingItem.Metadata == nil {
			workingItem.Metadata = map[string]any{}
		}

		id := ""
		if workingItem.JobID != nil {
			id = *workingItem.JobID
		}

		if workingItem.QueueName == nil {
			if name, ok := q.queueKindMapping[workingItem.Kind]; ok {
				workingItem.QueueName = &name
			}
		}

		qi := QueueItem{
			ID:          id,
			AtMS:        ats[idx].UnixMilli(),
			WorkspaceID: workingItem.WorkspaceID,
			FunctionID:  workingItem.Identifier.WorkflowID,
			Data:        workingItem,
			QueueName:   workingItem.QueueName,
			WallTimeMS:  ats[idx].UnixMilli(),
		}

		if workingItem.QueueName == nil && qi.FunctionID == uuid.Nil {
			errs := make([]error, len(items))
			errs[idx] = fmt.Errorf("queue name or function ID must be set")
			return nil, nil, errs
		}

		if opts.IdempotencyPeriod != nil {
			qi.IdempotencyPeriod = opts.IdempotencyPeriod
		}

		effectiveAts[idx] = time.UnixMilli(qi.Score(q.Clock().Now()))

		if factor := qi.Data.GetPriorityFactor(); factor != 0 {
			qi.AtMS -= factor
		}

		qis[idx] = qi
	}

	return qis, effectiveAts, nil
}

// selectBatchShard selects a Redis shard for the batch and validates it.
func (q *queueProcessor) selectBatchShard(ctx context.Context, opts EnqueueOpts, firstItem QueueItem, count int) (QueueShard, []error) {
	shard, err := q.selectShard(ctx, opts.ForceQueueShardName, firstItem)
	if err != nil {
		errs := make([]error, count)
		for i := range errs {
			errs[i] = err
		}
		return nil, errs
	}

	if shard.Kind() != enums.QueueShardKindRedis {
		errs := make([]error, count)
		for i := range errs {
			errs[i] = fmt.Errorf("batch enqueue only supported on Redis shards")
		}
		return nil, errs
	}

	return shard, nil
}

// enqueueFallback enqueues items sequentially when batch is not supported.
func (q *queueProcessor) enqueueFallback(ctx context.Context, items []Item, ats []time.Time, opts EnqueueOpts) []error {
	errs := make([]error, len(items))
	for idx := range items {
		errs[idx] = q.Enqueue(ctx, items[idx], ats[idx], opts)
	}
	return errs
}

// emitBatchMetrics emits per-item enqueue metrics after a batch operation.
func (q *queueProcessor) emitBatchMetrics(ctx context.Context, items []Item, errs []error, shard QueueShard) {
	for idx := range items {
		status := "enqueued"
		if errs[idx] != nil {
			if errs[idx] == ErrQueueItemExists {
				status = "exists"
			} else {
				status = "error"
			}
		}
		metrics.IncrQueueItemStatusCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags: map[string]any{
				"status":      status,
				"kind":        items[idx].Kind,
				"queue_shard": shard.Name(),
			},
		})
	}
}
