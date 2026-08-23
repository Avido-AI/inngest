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

const (
	pkgName = "queue.processor"
)

type batchEnqueueShard interface {
	EnqueueItemBatch(ctx context.Context, items []QueueItem, ats []time.Time, opts EnqueueOpts) []error
}

// buildQueueItem converts an Item to a QueueItem, validates it, and computes its effective enqueue time.
func (q *queueProducer) buildQueueItem(item Item, at time.Time, opts EnqueueOpts) (QueueItem, time.Time, error) {
	if item.Metadata == nil {
		item.Metadata = map[string]any{}
	}

	id := ""
	if item.JobID != nil {
		id = *item.JobID
	}

	if item.QueueName == nil {
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

	if item.QueueName == nil && qi.FunctionID == uuid.Nil {
		return QueueItem{}, time.Time{}, fmt.Errorf("queue name or function ID must be set")
	}

	if opts.IdempotencyPeriod != nil {
		qi.IdempotencyPeriod = opts.IdempotencyPeriod
	}

	next := time.UnixMilli(qi.Score(q.Clock().Now()))
	if factor := qi.Data.GetPriorityFactor(); factor != 0 {
		qi.AtMS -= factor
	}

	return qi, next, nil
}

// Enqueue adds an item to the queue to be processed at the given time.
// TODO: Lift this function and the queue interface to a higher level, so that it's disconnected from the
// concrete Redis implementation.
func (q *queueProducer) Enqueue(ctx context.Context, item Item, at time.Time, opts EnqueueOpts) error {
	l := logger.StdlibLogger(ctx)

	qi, next, err := q.buildQueueItem(item, at, opts)

	l = l.With(
		"item", qi,
		"account_id", item.Identifier.AccountID,
		"env_id", item.WorkspaceID,
		"app_id", item.Identifier.AppID,
		"fn_id", item.Identifier.WorkflowID,
	)

	if err != nil {
		l.ReportError(err, "attempted to enqueue QueueItem without function ID or queueName override")
		return err
	}

	// Use the queue item's score, ensuring we process older function runs first
	// (eg. before at)
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

	q.maybeEnqueuePromotionJob(ctx, l, qi)
	return nil
}

func (q *queueProducer) maybeEnqueuePromotionJob(ctx context.Context, l logger.Logger, qi QueueItem) {
	// XXX: If we've enqueued a user queue item (sleep, retry, step, etc.) and it's in the future,
	// we want to ensure that we schedule a rebalance job which takes the queue item and places it
	// at the correct score based off of the item's run ID when it becomes available.
	//
	// Without this, step.sleep or retries for a very old workflow may still lag behind steps from
	// later workflows when scheduled in the future.  This can, worst case, cause never-ending runs.
	if !q.enableJobPromotion || !qi.RequiresPromotionJob(q.Clock().Now()) {
		return
	}
	if qi.Data.Kind == KindJobPromote {
		return
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
		l.ReportError(err, "error scheduling promotion job")
	}
}

func (q *queueProducer) EnqueueBatch(ctx context.Context, items []Item, ats []time.Time, opts EnqueueOpts) []error {
	if len(items) == 0 {
		return nil
	}

	if len(items) != len(ats) {
		err := fmt.Errorf("queue batch items and times must have the same length")
		errs := make([]error, len(items))
		for idx := range errs {
			errs[idx] = err
		}
		return errs
	}

	qis := make([]QueueItem, len(items))
	effectiveAts := make([]time.Time, len(items))
	for idx := range items {
		qi, effectiveAt, err := q.buildQueueItem(items[idx], ats[idx], opts)
		if err != nil {
			errs := make([]error, len(items))
			errs[idx] = err
			return errs
		}
		qis[idx] = qi
		effectiveAts[idx] = effectiveAt
	}

	ctx, span := q.conditionalTracer.NewSpan(
		ctx,
		"queue.Enqueue.select_shard",
		TraceScopeFromQueueItem(qis[0], opts.ForceQueueShardName),
	)
	shard, err := q.selectShard(ctx, opts.ForceQueueShardName, qis[0])
	span.End()
	if err != nil {
		errs := make([]error, len(items))
		for idx := range errs {
			errs[idx] = err
		}
		return errs
	}

	batchShard, ok := shard.(batchEnqueueShard)
	if !ok {
		errs := make([]error, len(items))
		for idx := range items {
			errs[idx] = q.Enqueue(ctx, items[idx], ats[idx], opts)
		}
		return errs
	}

	errs := batchShard.EnqueueItemBatch(ctx, qis, effectiveAts, opts)
	if len(errs) != len(items) {
		normalized := make([]error, len(items))
		copy(normalized, errs)
		errs = normalized
	}

	q.emitBatchMetrics(ctx, items, errs, shard)
	q.maybeEnqueueBatchPromotionJobs(ctx, qis, errs)
	return errs
}

func (q *queueProducer) maybeEnqueueBatchPromotionJobs(ctx context.Context, qis []QueueItem, errs []error) {
	l := logger.StdlibLogger(ctx)
	for idx := range qis {
		if idx >= len(errs) || errs[idx] != nil {
			continue
		}
		q.maybeEnqueuePromotionJob(ctx, l, qis[idx])
	}
}

func (q *queueProducer) emitBatchMetrics(ctx context.Context, items []Item, errs []error, shard QueueShard) {
	for idx := range items {
		status := "enqueued"
		if idx < len(errs) && errs[idx] != nil {
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
