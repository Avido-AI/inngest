package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
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
func (q *queueProducer) Enqueue(ctx context.Context, item Item, at time.Time, opts EnqueueOpts) error {
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
		// Check if we have a kind => queuename mapping.
		item.QueueName = q.defaultQueueNameForItemKind(item.Kind)
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

	ctx, span := q.conditionalTracer.NewSpan(ctx, "queue.Enqueue.select_shard", TraceScopeFromQueueItem(qi, opts.ForceQueueShardName))
	shard, err := q.selectShard(ctx, opts.ForceQueueShardName, qi)
	span.End()
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

	_, err = shard.EnqueueItem(ctx, qi, next, opts)
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
	if err != nil && !errors.Is(err, ErrQueueItemExists) {
		l.ReportError(err, "error scheduling promotion job")
	}
	return nil
}

// buildQueueItem converts an Item to a QueueItem, validates it, and computes its effective enqueue time.
func (q *queueProcessor) buildQueueItem(item Item, at time.Time, opts EnqueueOpts) (QueueItem, time.Time, error) {
	if item.Metadata == nil {
		item.Metadata = map[string]any{}
	}

	id := ""
	if item.JobID != nil {
		id = *item.JobID
	}

	if item.QueueName == nil {
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

	if qi.Data.QueueName == nil && qi.FunctionID == uuid.Nil {
		return QueueItem{}, time.Time{}, fmt.Errorf("queue name or function ID must be set")
	}

	if opts.IdempotencyPeriod != nil {
		qi.IdempotencyPeriod = opts.IdempotencyPeriod
	}

	effectiveAt := time.UnixMilli(qi.Score(q.Clock().Now()))

	if factor := qi.Data.GetPriorityFactor(); factor != 0 {
		qi.AtMS -= factor
	}

	return qi, effectiveAt, nil
}

// maybeEnqueuePromotionJob schedules a promotion/rebalance job for future queue items.
func (q *queueProcessor) maybeEnqueuePromotionJob(ctx context.Context, l logger.Logger, qi QueueItem) {
	if !q.enableJobPromotion || !qi.RequiresPromotionJob(q.Clock().Now()) {
		return
	}
	if qi.Data.Kind == KindJobPromote {
		return
	}

	promoteAt := time.UnixMilli(qi.AtMS).Add(consts.FutureAtLimit * -1)
	promoteJobID := fmt.Sprintf("promote-%s", qi.ID)
	promoteQueueName := fmt.Sprintf("job-promote:%s", qi.FunctionID)
	err := q.Enqueue(ctx, Item{
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
	if err != nil && !errors.Is(err, ErrQueueItemExists) {
		// This is best effort, and shouldn't fail the OG enqueue.
		l.ReportError(err, "error scheduling promotion job")
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
	q.maybeEnqueueBatchPromotionJobs(ctx, qis, errs)
	return errs
}

// maybeEnqueueBatchPromotionJobs schedules promotion jobs for successfully batch-enqueued
// items that require them, matching the single-item Enqueue path behavior.
func (q *queueProcessor) maybeEnqueueBatchPromotionJobs(ctx context.Context, qis []QueueItem, errs []error) {
	l := logger.StdlibLogger(ctx)
	for idx := range qis {
		if errs[idx] != nil {
			continue
		}
		q.maybeEnqueuePromotionJob(ctx, l, qis[idx])
	}
}

// prepareQueueItems converts Items to QueueItems using the shared buildQueueItem helper.
func (q *queueProcessor) prepareQueueItems(items []Item, ats []time.Time, opts EnqueueOpts) ([]QueueItem, []time.Time, []error) {
	qis := make([]QueueItem, len(items))
	effectiveAts := make([]time.Time, len(items))

	for idx := range items {
		qi, effectiveAt, err := q.buildQueueItem(items[idx], ats[idx], opts)
		if err != nil {
			errs := make([]error, len(items))
			errs[idx] = err
			return nil, nil, errs
		}

		qis[idx] = qi
		effectiveAts[idx] = effectiveAt
	}

	return qis, effectiveAts, nil
}

// selectBatchShard selects a shard for the batch. Non-batch shards are handled
// by the type assertion in EnqueueBatch, which falls back to enqueueFallback.
func (q *queueProcessor) selectBatchShard(ctx context.Context, opts EnqueueOpts, firstItem QueueItem, count int) (QueueShard, []error) {
	l := logger.StdlibLogger(ctx)

	var shard QueueShard
	var err error
	if opts.ForceQueueShardName != "" {
		shard, err = q.shards.ByName(opts.ForceQueueShardName)
		if err != nil {
			err = fmt.Errorf("tried to force invalid queue shard %q", opts.ForceQueueShardName)
		}
	} else {
		var queueItemKind *string
		if _, ok := q.queueKindMapping[firstItem.Data.Kind]; ok {
			queueItemKind = &firstItem.Data.Kind
		}
		shard, err = q.shards.Resolve(ctx, firstItem.Data.Identifier.AccountID, queueItemKind)
		if err != nil {
			l.Error("error selecting shard", "error", err, "item", firstItem)
			err = fmt.Errorf("could not select shard: %w", err)
		}
	}

	if err != nil {
		errs := make([]error, count)
		for i := range errs {
			errs[i] = err
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
			if errors.Is(errs[idx], ErrQueueItemExists) {
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
