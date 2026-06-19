package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/inngest/inngest/pkg/constraintapi"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/apiresult"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/headers"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/service"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/inngest/inngest/pkg/util"
	"github.com/inngest/inngestgo"
	"github.com/oklog/ulid/v2"
)

// cancelationGracePeriod is the amount of time we add when marking a cancelled function as finished.  This
// allows any in-flight steps to complete and report their status, which prevents orphaned steps and ensures
// that the function's final status is correct.
const cancelationGracePeriod = 10 * time.Second

// invokeCompleteQueueName is the dedicated system queue for KindInvokeComplete
// items. Keeping these off the child run's per-function partition prevents
// finalizeRemoveJobs from dequeueing them when the child run is cleaned up.
var (
	invokeCompleteQueueName = "invoke-complete"

	// finalizeQueueName is the dedicated system queue for KindFinalize
	// backstop items. Using a separate queue prevents finalizeRemoveJobs
	// from dequeueing these items when cleaning up the run's per-function
	// partition.
	finalizeQueueName = "finalize"
)

// Finalize performs run finalization, which involves sending the function
// finished/failed event and deleting state.
func (e *executor) Finalize(ctx context.Context, opts execution.FinalizeOpts) error {
	ctx = context.WithoutCancel(ctx)
	l := logger.StdlibLogger(ctx)

	l.Info("finalize: starting",
		"run_id", opts.Metadata.ID.RunID,
		"function_id", opts.Metadata.ID.FunctionID,
		"status", opts.Status(),
		"is_cancel", opts.Optional.Cancel,
		"reason", opts.Optional.Reason,
	)

	// Enqueue a durable backstop so that if this pod is killed mid-finalize,
	// any other pod can dequeue and complete the cleanup via Cancel().
	// Skip on the Cancel path to avoid re-entrant backstop enqueues when
	// handleFinalize -> Cancel() -> Finalize() fires.
	if !opts.Optional.Cancel {
		e.enqueueFinalizeBackstop(ctx, opts)
	}

	var endTimeOffset time.Duration
	status := opts.Status()
	if status == enums.StepStatusCancelled {
		endTimeOffset = cancelationGracePeriod
	}

	err := e.tracerProvider.UpdateSpan(ctx, &tracing.UpdateSpanOptions{
		EndTime:       e.now(),
		EndTimeOffset: endTimeOffset,
		Debug:         &tracing.SpanDebugData{Location: "executor.finalize"},
		Metadata:      &opts.Metadata,
		TargetSpan:    tracing.RunSpanRefFromMetadata(&opts.Metadata),
		Status:        opts.Status(),
		Attributes:    finalizeSpanAttributes(opts),
	})
	if err != nil {
		// TODO This should be a warning/error once these spans are critical.
		l.Debug(
			"error updating run span end time",
			"error", err,
			"run_id", opts.Metadata.ID.RunID,
			"target_span", tracing.RunSpanRefFromMetadata(&opts.Metadata),
		)
	}

	// If there are no input events, fetch them.
	if len(opts.Optional.InputEvents) == 0 {
		opts.Optional.InputEvents, err = e.smv2.LoadEvents(ctx, opts.Metadata.ID)
		if err != nil && !errors.Is(err, state.ErrEventNotFound) {
			// Transient error (not "already deleted"). Return the error so
			// the queue handler retries finalization. If we continued with
			// empty events, finalizeEvents would skip creating the
			// function.finished event and the KindInvokeComplete backstop,
			// then Delete would remove the events permanently — stranding
			// the parent run forever.
			//
			// This early return skips the semaphore-release block below;
			// the semaphore stays held until the retry succeeds, which is
			// acceptable because persistent Redis failures that exhaust
			// all retries are rare and the alternative (stranded parent)
			// is worse.
			l.Error(
				"error loading run events to finalize, will retry",
				"error", err,
				"run_id", opts.Metadata.ID.RunID,
			)
			return fmt.Errorf("error loading run events to finalize: %w", err)
		}
	}

	// Release any manual-release semaphores held by this run.  Manual-release semaphores
	// (e.g. function concurrency) are acquired when the start job is dequeued but are NOT
	// released when individual step leases complete — they persist for the lifetime of the
	// run.  We must release them here, before state deletion, so that the semaphore info
	// from run metadata is still available.  The run ID is used as the idempotency key to
	// guarantee safe retries.
	if e.semaphoreManager == nil && len(opts.Metadata.Config.Semaphores) > 0 {
		l.Error(
			"semaphore manager is nil but run holds semaphores, leading to deadlock",
			"run_id", opts.Metadata.ID.RunID,
			"semaphores", len(opts.Metadata.Config.Semaphores),
		)
	}

	if e.semaphoreManager != nil && len(opts.Metadata.Config.Semaphores) > 0 {
		for _, sem := range opts.Metadata.Config.Semaphores {
			if sem.Release != constraintapi.SemaphoreReleaseManual {
				continue
			}
			// Retry semaphore release — a failure here means the semaphore is permanently
			// held, which deadlocks all future runs waiting on capacity.
			_, releaseErr := util.WithRetry(ctx, "release-semaphore", func(ctx context.Context) (struct{}, error) {
				return struct{}{}, e.semaphoreManager.ReleaseSemaphore(
					ctx,
					opts.Metadata.ID.Tenant.AccountID,
					sem.ID,
					sem.UsageValue,
					opts.Metadata.ID.RunID.String(),
					sem.Weight,
				)
			}, util.NewRetryConf())
			if releaseErr != nil {
				l.Error(
					"error releasing semaphore on finalize after retries",
					"error", releaseErr,
					"run_id", opts.Metadata.ID.RunID,
					"semaphore", sem.ID,
				)
			}
		}
	}

	// Load defers BEFORE Delete since they live in state and won't survive the
	// deletion. Retry on transient failures so the events get a chance to
	// publish even when Redis is briefly unavailable. Defer-related failures
	// are best-effort: log and continue with no defer events rather than
	// blocking Finalize. The downstream cleanup (Delete, finalizeRemoveJobs,
	// finalizeEvents for function.X) must still run regardless.
	loadDefersStart := e.now()
	defers, deferErr := util.WithRetry(ctx, "state.LoadDefers",
		func(ctx context.Context) (map[string]sv2.Defer, error) {
			return e.smv2.LoadDefers(ctx, opts.Metadata.ID)
		},
		util.NewRetryConf(),
	)
	metrics.HistogramDefersLoadDuration(ctx, e.now().Sub(loadDefersStart), metrics.HistogramOpt{
		PkgName: pkgName,
	})
	if deferErr != nil {
		l.Error(
			"error loading defers to finalize; continuing without defer events",
			"error", deferErr,
			"run_id", opts.Metadata.ID.RunID,
		)
	}
	metrics.HistogramDefersPerRun(ctx, int64(len(defers)), metrics.HistogramOpt{
		PkgName: pkgName,
	})

	// Build defer events from the loaded map BEFORE Delete (resolves fnSlug
	// using the in-memory function loader, not state). The actual publish
	// happens in finalizeEvents so all finalize-time events go through a
	// single finishHandler call.
	deferEvents, err := e.buildDeferEvents(ctx, opts, defers)
	if err != nil {
		l.Error(
			"error building deferred schedule events; continuing without defer events",
			"error", err,
			"run_id", opts.Metadata.ID.RunID,
		)
	}

	finalizationClaim := e.claimFinalization(ctx, opts.Metadata)

	// finalizeEvents creates function finished events and durably enqueues
	// the parent-resume notification for any invoke pause. This runs BEFORE
	// Delete so that, if the pod is rotated between the two, the worst case
	// is that the child state remains (and finalize is retried) rather than
	// the child being marked complete while the parent never resumes. Defer
	// events are published as part of the same finishHandler call.
	//
	// The claim only gates event publishing; state deletion and job removal
	// always run regardless of claim status.
	if finalizationClaim.Claimed() {
		feErr := e.finalizeEvents(ctx, opts, deferEvents)
		if feErr != nil {
			// Do NOT delete state when finalizeEvents failed. Keeping state
			// ensures the 30-second finalize backstop (KindFinalize) can
			// retry the full finalization — including the KindInvokeComplete
			// enqueue that resumes the parent run. Without this guard, the
			// backstop finds no metadata and silently no-ops, permanently
			// stranding the parent.
			l.Error(
				"finalizeEvents failed, preserving state for backstop retry",
				"error", feErr,
				"run_id", opts.Metadata.ID.RunID,
			)
			if releaseErr := finalizationClaim.Release(ctx); releaseErr != nil {
				logger.StdlibLogger(ctx).Warn(
					"error releasing finalization claim after failed publish",
					"error", releaseErr,
					"run_id", opts.Metadata.ID.RunID,
				)
				return errors.Join(feErr, releaseErr)
			}
			return feErr
		}
	}

	// Delete the function state in every case.
	err = e.smv2.Delete(ctx, opts.Metadata.ID)
	if err != nil {
		l.Error(
			"error deleting state in finalize",
			"error", err,
			"run_id", opts.Metadata.ID.RunID,
		)
	} else {
		l.Info("finalize: completed, state deleted",
			"run_id", opts.Metadata.ID.RunID,
			"function_id", opts.Metadata.ID.FunctionID,
		)
	}

	metrics.IncrRunFinalizedCounter(ctx, metrics.CounterOpt{
		PkgName: pkgName,
		Tags: map[string]any{
			"reason": opts.Optional.Reason,
		},
	})

	e.finalizeRemoveJobs(ctx, opts)

	return nil
}

func (e *executor) claimFinalization(ctx context.Context, md sv2.Metadata) sv2.FinalizationClaim {
	if e.finishHandler == nil {
		return sv2.NewFinalizationClaim(false, nil)
	}

	claim, _, err := sv2.TryClaimFinalization(ctx, e.smv2, md)
	if err != nil {
		logger.StdlibLogger(ctx).Warn(
			"error claiming finalization; continuing without dedupe",
			"error", err,
			"run_id", md.ID.RunID,
		)
		return sv2.NewFinalizationClaim(true, nil)
	}

	if !claim.Claimed() {
		logger.StdlibLogger(ctx).Debug(
			"skipping duplicate finalize effects",
			"run_id", md.ID.RunID,
		)
	}

	return claim
}

// buildDeferEvents constructs the inngest/deferred.schedule events for every
// AfterRun defer in `defers`. It does no publishing — the events are returned
// for the caller (Finalize) to fold into the single finishHandler call inside
// finalizeEvents.
//
// Per-defer validation failures (Validate, status filter, malformed Input)
// log and skip the bad record. They are not fatal to the batch.
func (e *executor) buildDeferEvents(
	ctx context.Context,
	opts execution.FinalizeOpts,
	defers map[string]sv2.Defer,
) ([]event.Event, error) {
	if len(defers) == 0 {
		return nil, nil
	}

	fnSlug := opts.Optional.FnSlug
	if fnSlug == "" {
		fnSlug = opts.Metadata.Config.FunctionSlug()
	}
	if fnSlug == "" {
		return nil, fmt.Errorf("function slug missing from run metadata for deferred events")
	}

	now := e.now()
	var events []event.Event

	for _, d := range defers {
		if err := d.Validate(); err != nil {
			logger.StdlibLogger(ctx).Error(
				"invalid defer",
				"error", err,
				"run_id", opts.Metadata.ID.RunID,
			)
			metrics.IncrDefersFinalizedCounter(ctx, "invalid", metrics.CounterOpt{PkgName: pkgName})
			continue
		}

		// TODO: what about an immediate execution mode?
		if d.ScheduleStatus != enums.DeferStatusAfterRun {
			metrics.IncrDefersFinalizedCounter(ctx, d.ScheduleStatus.String(), metrics.CounterOpt{PkgName: pkgName})
			continue
		}

		eventID, err := event.DeferEventID(opts.Metadata.ID.RunID, d.HashedID)
		if err != nil {
			logger.StdlibLogger(ctx).Error(
				"failed to create defer event ID",
				"error", err,
				"hashed_id", d.HashedID,
				"run_id", opts.Metadata.ID.RunID,
			)
			metrics.IncrDefersFinalizedCounter(ctx, "invalid", metrics.CounterOpt{PkgName: pkgName})
			continue
		}

		data := map[string]any{}
		if len(d.Input) > 0 {
			if err := json.Unmarshal(d.Input, &data); err != nil {
				logger.StdlibLogger(ctx).Error(
					"deferred input is not a JSON object",
					"error", err,
					"run_id", opts.Metadata.ID.RunID,
				)
				metrics.IncrDefersFinalizedCounter(ctx, "invalid", metrics.CounterOpt{PkgName: pkgName})
				continue
			}
			if data == nil {
				// Reachable if the input is `null`. We need to set it to an
				// empty map to avoid panicking later
				data = make(map[string]any)
			}
		}

		deferredMeta := event.DeferredScheduleMetadata{
			FnSlug:          d.FnSlug,
			ParentAppID:     opts.Metadata.ID.Tenant.AppID,
			ParentDeferSpan: tracing.DeferSpanRef(opts.Metadata.ID.RunID, d.HashedID),
			ParentFnID:      opts.Metadata.ID.FunctionID,
			ParentFnSlug:    fnSlug,
			ParentRunID:     opts.Metadata.ID.RunID,
		}
		if err := deferredMeta.Validate(); err != nil {
			logger.StdlibLogger(ctx).Error(
				"invalid deferred event metadata",
				"error", err,
				"run_id", opts.Metadata.ID.RunID,
			)
			metrics.IncrDefersFinalizedCounter(ctx, "invalid", metrics.CounterOpt{PkgName: pkgName})
			continue
		}
		data[consts.InngestEventDataPrefix] = deferredMeta

		events = append(events, event.Event{
			ID:        eventID.String(),
			Name:      consts.FnDeferScheduleName,
			Timestamp: now.UnixMilli(),
			Data:      data,
		})
		metrics.IncrDefersFinalizedCounter(ctx, "after_run", metrics.CounterOpt{PkgName: pkgName})
	}

	return events, nil
}

// enqueueSystemItem enqueues a durable system queue item with retry and
// idempotency handling. Items that already exist (ErrQueueItemExists) are
// silently deduplicated. This is the shared path for all backstop items
// (KindFinalize, KindInvokeComplete, etc.) that must survive pod rotation.
func (e *executor) enqueueSystemItem(ctx context.Context, label string, item queue.Item, at time.Time) error {
	if e.queue == nil {
		return fmt.Errorf("executor queue is nil")
	}

	_, err := util.WithRetry(ctx, label,
		func(ctx context.Context) (struct{}, error) {
			err := e.queue.Enqueue(ctx, item, at, queue.EnqueueOpts{})
			if errors.Is(err, queue.ErrQueueItemExists) {
				return struct{}{}, nil
			}
			return struct{}{}, err
		},
		util.NewRetryConf(),
	)
	return err
}

// enqueueFinalizeBackstop enqueues a durable KindFinalize queue item so that if
// the current pod is killed mid-finalize (SIGKILL, OOM), another pod can pick up
// the item and complete the cleanup via Cancel(). The JobID is derived from the
// RunID, so repeated Finalize() calls for the same run are deduplicated by the
// queue's idempotency window. The item lives on a dedicated system queue that
// finalizeRemoveJobs does not touch.
// finalizeBackstopDelay is how long the backstop item waits before becoming
// eligible for processing. This gives the inline Finalize() time to complete
// in almost all cases, preventing a concurrent second Finalize() call.
const finalizeBackstopDelay = 30 * time.Second

func (e *executor) enqueueFinalizeBackstop(ctx context.Context, opts execution.FinalizeOpts) {
	id := opts.Metadata.ID
	jobID := fmt.Sprintf("finalize-%s", id.RunID.String())
	item := queue.Item{
		JobID:     &jobID,
		Kind:      queue.KindFinalize,
		QueueName: &finalizeQueueName,
		Identifier: state.Identifier{
			AccountID:   id.Tenant.AccountID,
			WorkspaceID: id.Tenant.EnvID,
		},
		Payload: queue.PayloadFinalize{
			RunID:      id.RunID,
			FunctionID: id.FunctionID,
			AccountID:  id.Tenant.AccountID,
			EnvID:      id.Tenant.EnvID,
			AppID:      id.Tenant.AppID,
		},
	}

	at := e.now().Add(finalizeBackstopDelay)
	if err := e.enqueueSystemItem(ctx, "queue.EnqueueFinalizeBackstop", item, at); err != nil {
		logger.StdlibLogger(ctx).Warn("failed to enqueue finalize backstop",
			"error", err,
			"run_id", id.RunID.String(),
		)
	}
}

// finalizeRemoveJobs removes any other jobs for a finalized run, as the function is
// marked as finished and no other jobs need to execute.
func (e *executor) finalizeRemoveJobs(ctx context.Context, opts execution.FinalizeOpts) {
	l := logger.StdlibLogger(ctx)

	shard, err := e.shards.Resolve(ctx, opts.Metadata.ID.Tenant.AccountID, nil)
	if err != nil {
		return
	}

	// A concurrent executor may still be enqueuing items for this run (e.g.,
	// a KindSleep item being scheduled while the run is being cancelled). Use
	// a bounded loop: keep sweeping while items are found, up to a maximum
	// number of attempts, to avoid orphaning items enqueued during a sweep.
	const maxSweeps = 3
	for i := 0; i < maxSweeps; i++ {
		removed := e.doRemoveRunJobs(ctx, l, shard, opts)
		if removed == 0 {
			break
		}
		if i < maxSweeps-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// doRemoveRunJobs performs a single pass of removing all queue items for the given run.
// Returns the number of items successfully dequeued.
func (e *executor) doRemoveRunJobs(ctx context.Context, l logger.Logger, shard queue.ShardOperations, opts execution.FinalizeOpts) int {
	// We may be cancelling an in-progress run.  If that's the case, we want to delete any
	// outstanding jobs from the queue, if possible.
	//
	// XXX: Remove this typecast and normalize queue interface to a single package
	// Find all items for the current function run.
	jobs, err := shard.RunJobs(
		ctx,
		queue.Scope{
			AccountID:  opts.Metadata.ID.Tenant.AccountID,
			EnvID:      opts.Metadata.ID.Tenant.EnvID,
			FunctionID: opts.Metadata.ID.FunctionID,
		},
		opts.Metadata.ID.RunID,
		1000,
		0,
	)
	if err != nil {
		l.Error(
			"error fetching run jobs",
			"error", err,
			"run_id", opts.Metadata.ID.RunID,
		)
		return 0
	}

	removed := 0
	for _, j := range jobs {
		qi, _ := j.Raw.(*queue.QueueItem)
		if qi == nil {
			continue
		}

		jobID := queue.JobIDFromContext(ctx)
		if jobID != "" && qi.ID == jobID {
			// Do not dequeue the current job that we're working on.
			continue
		}

		err := shard.Dequeue(ctx, *qi)
		if err != nil && !errors.Is(err, queue.ErrQueueItemNotFound) {
			l.Error(
				"error dequeueing run job",
				"error", err,
				"run_id", opts.Metadata.ID.RunID.String(),
			)
		}
		if err == nil {
			removed++
		}
	}
	return removed
}

func (e *executor) finalizeEvents(ctx context.Context, opts execution.FinalizeOpts, extraEvents []event.Event) error {
	if e.finishHandler == nil {
		// the finishHandler handles sending finalization events.
		return nil
	}

	var (
		// Track whether this run was an invoke.
		isInvoke bool
		fnSlug   = opts.Optional.FnSlug
		evts     = opts.Optional.InputEvents
	)

	// Find the function slug.
	if fnSlug == "" {
		fn, err := e.fl.LoadFunction(ctx, opts.Metadata.ID.Tenant.EnvID, opts.Metadata.ID.FunctionID)
		if err != nil {
			return err
		}
		fnSlug = fn.Function.Slug
	}

	// Parse events for the fail handler before deleting state.
	inputEvents := make([]event.Event, len(evts))
	for n, e := range evts {
		evt, err := event.NewEvent(e)
		if err != nil {
			return err
		}
		inputEvents[n] = *evt
	}

	// Prepare events that we must send
	now := e.now()
	base := &functionFinishedData{
		FunctionID: fnSlug,
		RunID:      opts.Metadata.ID.RunID,
		Events:     inputEvents,
	}
	base.setResponse(opts.Response)

	// We'll send many events - some for each items in the batch.  This ensures that invoke works
	// for batched functions.
	freshEvents := []event.Event{}
	for n, runEvt := range inputEvents {
		if runEvt.Name == event.FnFailedName || runEvt.Name == event.FnFinishedName {
			// Don't recursively trigger internal finish handlers.
			continue
		}

		invokeID := correlationID(runEvt)
		if invokeID == nil && n > 0 {
			// We only send function finish events for either the first event in a batch or for
			// all events with a correlation ID.
			continue
		}

		isInvoke = true

		// Copy the base data to set the event.
		copied := *base
		copied.Event = runEvt.Map()
		if invokeID != nil {
			copied.InvokeCorrelationID = *invokeID
		}
		data := copied.Map()

		// Add a status field.
		data[consts.InngestEventDataPrefix] = map[string]any{
			"status": opts.Status(),
		}

		// Deterministic event ID so that backstop retries of finalizeEvents
		// publish the same ID and downstream consumers can deduplicate.
		finishedSeed := []byte(opts.Metadata.ID.RunID.String() + "-finished-" + runEvt.ID)
		finishedID, idErr := util.DeterministicULID(ulid.Time(opts.Metadata.ID.RunID.Time()), finishedSeed)
		if idErr != nil {
			return idErr
		}

		// Add an `inngest/function.finished` event.  Lifecycle events carry
		// the sessions of the event they report on, so that runs triggered by
		// them (eg. onFailure handlers) stay in the same sessions.
		freshEvents = append(freshEvents, event.Event{
			ID:        finishedID.String(),
			Name:      event.FnFinishedName,
			Timestamp: now.UnixMilli(),
			Data:      data,
			Meta:      runEvt.Meta,
		})

		switch opts.Status() {
		case enums.StepStatusCancelled:
			freshEvents = append(freshEvents, event.Event{
				ID:        opts.Metadata.ID.RunID.String(), // using the RunID as the ID prevents duped runs for parallel steps
				Name:      event.FnCancelledName,
				Timestamp: now.UnixMilli(),
				Data:      data,
				Meta:      runEvt.Meta,
			})
		case enums.StepStatusFailed:
			// Legacy - send inngest/function.failed, except for when the function has been cancelled.
			freshEvents = append(freshEvents, event.Event{
				ID:        opts.Metadata.ID.RunID.String(), // using the RunID as the ID prevents duped runs for parallel steps
				Name:      event.FnFailedName,
				Timestamp: now.UnixMilli(),
				Data:      data,
				Meta:      runEvt.Meta,
			})
		}
	}

	// Two-track invoke completion delivery:
	//
	//   1. Fast path: an in-process goroutine that calls HandleInvokeFinish
	//      directly. Low latency, but tied to this pod's lifecycle — can lose
	//      the notification if the pod is rotated mid-call even with retries.
	//
	//   2. Durable path: a KindInvokeComplete queue item written to Redis
	//      before state Delete. Any pod can dequeue and resume the parent,
	//      so a rotated pod cannot strand the parent.
	//
	// HandleInvokeFinish is idempotent: once the pause is consumed, a duplicate
	// call returns ErrPauseNotFound which both paths swallow.
	var enqueueErr error
	if isInvoke {
		invokeCount := 0
		for _, evt := range freshEvents {
			if evt.CorrelationID() != "" {
				invokeCount++
			}
		}
		if invokeCount > 0 {
			logger.From(ctx).Info("invoke completion: delivering parent resume",
				"child_run_id", opts.Metadata.ID.RunID.String(),
				"invoke_event_count", invokeCount,
			)
		}
		enqueueErr = e.enqueueInvokeCompletes(ctx, opts, freshEvents)
		if enqueueErr != nil {
			logger.From(ctx).Error("invoke completion: durable enqueue failed",
				"error", enqueueErr,
				"child_run_id", opts.Metadata.ID.RunID.String(),
			)
		}

		for _, evt := range freshEvents {
			tracked := event.BaseTrackedEvent{
				ID:          ulid.MustParse(evt.ID),
				Event:       evt,
				AccountID:   opts.Metadata.ID.Tenant.AccountID,
				WorkspaceID: opts.Metadata.ID.Tenant.EnvID,
			}
			service.Go(func() {
				bgCtx := context.WithoutCancel(ctx)
				_, err := util.WithRetry(bgCtx, "fast-resume-invoke", func(ctx context.Context) (struct{}, error) {
					return struct{}{}, e.HandleInvokeFinish(ctx, tracked)
				}, util.NewRetryConf(util.WithRetryConfRetryableErrors(func(err error) bool {
					// Stop retrying if the pause is already gone — either the
					// durable KindInvokeComplete path won the race, or there
					// never was a pause. In that case the work is done.
					return !errors.Is(err, ErrNoCorrelationID) &&
						!errors.Is(err, state.ErrPauseNotFound) &&
						!errors.Is(err, state.ErrInvokePauseNotFound)
				})))
				switch {
				case err == nil:
					logger.From(ctx).Info("invoke completion: fast path delivered",
						"event_id", evt.ID,
						"child_run_id", opts.Metadata.ID.RunID,
						"correlation_id", evt.CorrelationID(),
					)
				case errors.Is(err, ErrNoCorrelationID) ||
					errors.Is(err, state.ErrPauseNotFound) ||
					errors.Is(err, state.ErrInvokePauseNotFound):
					logger.From(ctx).Debug("invoke completion: fast path skipped (already handled or no correlation)",
						"event_id", evt.ID,
						"child_run_id", opts.Metadata.ID.RunID,
						"reason", err.Error(),
					)
				default:
					logger.From(ctx).Error("invoke completion: fast path failed after retries",
						"error", err,
						"event_id", evt.ID,
						"child_run_id", opts.Metadata.ID.RunID,
						"correlation_id", evt.CorrelationID(),
					)
				}
			})
		}
	}

	// Append extra events (e.g. inngest/deferred.schedule) AFTER the invoke
	// goroutine loop so they aren't dispatched to HandleInvokeFinish.
	freshEvents = append(freshEvents, extraEvents...)

	publishErr := e.finishHandler(ctx, opts.Metadata.ID, freshEvents)
	return errors.Join(enqueueErr, publishErr)
}

// enqueueInvokeCompletes enqueues a durable KindInvokeComplete queue item for
// each FnFinished event that carries an InvokeCorrelationID. The item is
// processed by any executor pod, which calls HandleInvokeFinish to resume the
// parent run's pause. The JobID is keyed off the correlation ID so duplicate
// finalize calls dedupe via the queue's idempotency window.
func (e *executor) enqueueInvokeCompletes(ctx context.Context, opts execution.FinalizeOpts, events []event.Event) error {
	wsID := opts.Metadata.ID.Tenant.EnvID
	accID := opts.Metadata.ID.Tenant.AccountID

	var errs error
	for _, evt := range events {
		corrID := evt.CorrelationID()
		if corrID == "" {
			continue
		}

		tracked := event.BaseTrackedEvent{
			ID:          ulid.MustParse(evt.ID),
			Event:       evt,
			AccountID:   accID,
			WorkspaceID: wsID,
		}

		jobID := fmt.Sprintf("invoke-complete-%s", corrID)
		item := queue.Item{
			JobID:       &jobID,
			WorkspaceID: wsID,
			Kind:        queue.KindInvokeComplete,
			QueueName:   &invokeCompleteQueueName,
			// Identifier carries AccountID/WorkspaceID so multi-shard
			// deployments route this item via shards.Resolve to the
			// correct shard (see queueProcessor.selectShard).
			Identifier: state.Identifier{
				AccountID:   accID,
				WorkspaceID: wsID,
			},
			Payload: queue.PayloadInvokeComplete{
				TrackedEvent: tracked,
			},
		}

		if err := e.enqueueSystemItem(ctx, "queue.EnqueueInvokeComplete", item, e.now()); err != nil {
			errs = errors.Join(errs, fmt.Errorf("correlation_id=%s: %w", corrID, err))
		} else {
			logger.From(ctx).Debug("invoke complete: enqueued durable backstop",
				"correlation_id", corrID,
				"child_run_id", opts.Metadata.ID.RunID.String(),
			)
		}
	}
	return errs
}

func finalizeSpanAttributes(f execution.FinalizeOpts) *meta.SerializableAttrs {
	// We're explicitly not setting any output span reference here and passing
	// `nil` instead. We do this because we need to be setting the function
	// output twice - once for the execution itself and once for the run span -
	// in order to appropriately filter this in Cloud and other data stores.

	switch f.Response.Type {
	case execution.FinalizeResponseAPI:
		return apiAttributes(f.Response.APIResponse)
	case execution.FinalizeResponseRunComplete:
		return runCompleteAttrs(f.Response.RunComplete)
	case execution.FinalizeResponseDriver:
		return tracing.DriverResponseAttrs(&f.Response.DriverResponse, nil)
	}

	panic("unknown finalize response type")
}

func apiAttributes(res apiresult.APIResult) *meta.SerializableAttrs {
	h := http.Header{}
	for k, v := range res.Headers {
		h.Set(k, v)
	}

	compactHeaders := headers.Compact(headers.Redact(h))

	rawAttrs := meta.NewAttrSet()
	meta.AddAttr(rawAttrs, meta.Attrs.IsFunctionOutput, inngestgo.Ptr(true))
	meta.AddAttr(rawAttrs, meta.Attrs.ResponseHeaders, &compactHeaders)
	meta.AddAttr(rawAttrs, meta.Attrs.ResponseStatusCode, &res.StatusCode)
	meta.AddAttr(rawAttrs, meta.Attrs.ResponseOutputSize, inngestgo.Ptr(len(res.Body)))
	// XXX: We always wrap trace output with {"data":T} or {"error":T} for consistency with steps.
	meta.AddAttr(rawAttrs, meta.Attrs.StepOutput, inngestgo.Ptr(util.DataWrap([]byte(res.Body))))

	return rawAttrs
}

func runCompleteAttrs(gen state.GeneratorOpcode) *meta.SerializableAttrs {
	rawAttrs := meta.NewAttrSet()

	meta.AddAttr(rawAttrs, meta.Attrs.IsFunctionOutput, inngestgo.Ptr(true))
	meta.AddAttr(rawAttrs, meta.Attrs.ResponseStatusCode, inngestgo.Ptr(200)) // Must be to have this code.  It's an async fn.
	meta.AddAttr(rawAttrs, meta.Attrs.ResponseOutputSize, inngestgo.Ptr(len(gen.Data)))
	// XXX: We always wrap trace output with {"data":T} or {"error":T} for consistency with steps.
	meta.AddAttr(rawAttrs, meta.Attrs.StepOutput, inngestgo.Ptr(util.DataWrap(gen.Data)))

	rawAttrs = rawAttrs.Merge(tracing.GeneratorAttrs(&gen))

	return rawAttrs
}
