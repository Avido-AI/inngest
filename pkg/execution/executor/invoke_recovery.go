package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution/pauses"
	"github.com/inngest/inngest/pkg/execution/state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/service"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
)

// InvokeRecovery resumes parent runs stranded on a step.invoke whose completion
// was never delivered (e.g. the child's inngest/function.finished event was lost
// during an OOM / read-only-DB incident; resume is fire-once with no retry). The
// parent keeps a valid invoke pause (~1y TTL) and a non-empty pending set, so its
// state is never finalized and accumulates in Redis until OOM.
//
// This is a convergent reconciler, NOT another notification-delivery patch: it
// periodically inspects open invoke pauses, checks the child run's ACTUAL status
// in the CQRS store, and re-drives the resume. It never cancels runs.
//
// Decision (keyed on the child's real status, never on pause age):
//   - child terminal       -> Tier 1: resume the parent from the child's
//     persisted result (no re-execution).
//   - child has no run      -> Tier 2: re-run the child (re-publish the original
//     invocation event); the fresh child resumes the still-valid parent pause.
//   - child still running   -> skip (a legitimately long-running child, any
//     duration, is left alone).
//   - parent state gone /
//     pause past expiry      -> clean up the orphaned pause.

// recoveryAction is the decision for a single stuck invoke pause.
type recoveryAction int

const (
	recoverySkip    recoveryAction = iota // not stuck, too young, or give-up
	recoveryResume                        // Tier 1: resume from persisted result
	recoveryRerun                         // Tier 2: re-run the child
	recoveryCleanup                       // orphaned/expired pause: delete it
)

// childStatus is the minimal child-run state the decision needs.
type childStatus int

const (
	childUnknown  childStatus = iota // couldn't determine — treat as not-actionable
	childRunning                     // in flight (any duration) — leave alone
	childTerminal                    // completed/failed/cancelled — resumable
	childMissing                     // no run found — re-run candidate
)

// recoveryInput is the pure decision input (unit-tested in isolation).
type recoveryInput struct {
	now              time.Time
	pauseExpires     time.Time
	parentRunAge     time.Duration
	parentExists     bool
	child            childStatus
	rerunAttempts    int
	minAge           time.Duration
	maxRerunAttempts int
}

// decideRecovery is a pure function: given a stuck invoke pause's context, decide
// what to do. Safety comes from the child status, not pause age — a child that is
// still Running is never acted on, no matter how long it has run.
func decideRecovery(in recoveryInput) recoveryAction {
	// Pause expired beyond the deletion grace period: the wait can no longer be
	// consumed, so the pause is stranded garbage — clean it up.
	if !in.pauseExpires.IsZero() && in.pauseExpires.Add(consts.PauseExpiredDeletionGracePeriod).Before(in.now) {
		return recoveryCleanup
	}
	// Parent run state is gone: the pause is orphaned (resuming would write into
	// a deleted run). Clean up the dangling pause + correlation entry.
	if !in.parentExists {
		return recoveryCleanup
	}
	// Too young: don't race the normal completion fast path. This is a churn
	// filter only — not the safety mechanism.
	if in.parentRunAge < in.minAge {
		return recoverySkip
	}
	switch in.child {
	case childTerminal:
		return recoveryResume
	case childMissing:
		if in.rerunAttempts >= in.maxRerunAttempts {
			// Give up re-running; leave resident for ops rather than cancel.
			return recoverySkip
		}
		return recoveryRerun
	default:
		// childRunning or childUnknown: leave it alone.
		return recoverySkip
	}
}

// --- dependencies (small interfaces so the service is testable) ---

type invokeResumer interface {
	HandleInvokeFinish(ctx context.Context, evt event.TrackedEvent) error
}

type pauseReaderDeleter interface {
	PausesSince(ctx context.Context, index pauses.Index, since time.Time) (state.PauseIterator, error)
	Delete(ctx context.Context, index pauses.Index, pause state.Pause, opts ...state.DeletePauseOpt) error
}

type runExister interface {
	Exists(ctx context.Context, id statev2.ID) (bool, error)
}

type recoveryData interface {
	GetRunsByEventID(ctx context.Context, eventID ulid.ULID, opts cqrs.GetRunsByEventIDOpts) ([]cqrs.Run, error)
	GetEventByInternalID(ctx context.Context, internalID ulid.ULID) (*cqrs.Event, error)
}

// eventPublisher re-ingests an event (used to re-run a child in Tier 2).
type eventPublisher func(ctx context.Context, e event.Event) error

// InvokeRecoveryOpts configures the recovery service.
type InvokeRecoveryOpts struct {
	Log       logger.Logger
	Executor  invokeResumer
	Pauses    pauseReaderDeleter
	Runs      runExister
	Data      recoveryData
	Publish   eventPublisher
	Redis     rueidis.Client // leader lease (unsharded)
	AccountID uuid.UUID
	EnvID     uuid.UUID

	Interval         time.Duration
	MinAge           time.Duration
	RerunsPerTick    int
	MaxRerunAttempts int
	LeaseDuration    time.Duration
}

type invokeRecoveryService struct {
	opts     InvokeRecoveryOpts
	holderID string
	// rerunAttempts caps Tier-2 re-runs per correlation ID across ticks.
	rerunAttempts map[string]int
}

// NewInvokeRecoveryService builds the reconciler as a service.Service. It is
// safe to run on every pod: a Redis leader lease ensures only one instance does
// work per interval.
func NewInvokeRecoveryService(opts InvokeRecoveryOpts) service.Service {
	if opts.Interval <= 0 {
		opts.Interval = consts.InvokeRecoveryInterval
	}
	if opts.MinAge <= 0 {
		opts.MinAge = consts.InvokeRecoveryMinAge
	}
	if opts.RerunsPerTick <= 0 {
		opts.RerunsPerTick = consts.InvokeRecoveryRerunsPerTick
	}
	if opts.MaxRerunAttempts <= 0 {
		opts.MaxRerunAttempts = consts.InvokeRecoveryMaxRerunAttempts
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = consts.InvokeRecoveryLeaseDuration
	}
	return &invokeRecoveryService{
		opts:          opts,
		holderID:      ulid.Make().String(),
		rerunAttempts: map[string]int{},
	}
}

func (s *invokeRecoveryService) Name() string { return "invoke-recovery" }

func (s *invokeRecoveryService) Pre(ctx context.Context) error {
	if s.opts.Executor == nil || s.opts.Pauses == nil || s.opts.Data == nil || s.opts.Redis == nil {
		return fmt.Errorf("invoke-recovery: missing required dependencies")
	}
	return nil
}

func (s *invokeRecoveryService) Run(ctx context.Context) error {
	l := s.opts.Log.With("service", "invoke-recovery")
	t := time.NewTicker(s.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if !s.acquireLease(ctx) {
			// Another pod holds the lease; skip this tick.
			continue
		}
		resumed, rerun, cleaned, skipped, err := s.reconcile(ctx)
		if err != nil {
			l.Error("invoke-recovery tick failed", "error", err)
			continue
		}
		if resumed+rerun+cleaned > 0 {
			l.Info("invoke-recovery tick",
				"resumed", resumed, "rerun", rerun, "cleaned", cleaned, "skipped", skipped)
		}
	}
}

func (s *invokeRecoveryService) Stop(ctx context.Context) error { return nil }

// acquireLease grabs/refreshes the singleton leader lease via SET NX EX. Only the
// holder reconciles, so pods don't double-process (critical for Tier-2 re-runs).
func (s *invokeRecoveryService) acquireLease(ctx context.Context) bool {
	// Scope the leader lease per environment so a future multi-env deployment
	// doesn't let one env's leader block recovery for the others.
	key := fmt.Sprintf("{estate}:invoke-recovery:leader:%s", s.opts.EnvID)
	leaseSecs := int64(s.opts.LeaseDuration.Seconds())

	// Fresh acquire wins immediately.
	if err := s.opts.Redis.Do(ctx, s.opts.Redis.B().Set().Key(key).Value(s.holderID).Nx().ExSeconds(leaseSecs).Build()).Error(); err == nil {
		return true
	}

	// Not acquired: only proceed if we already hold it (refresh across ticks).
	got, err := s.opts.Redis.Do(ctx, s.opts.Redis.B().Get().Key(key).Build()).ToString()
	if err != nil || got != s.holderID {
		return false
	}

	// We own it: refresh the TTL. A failed refresh isn't fatal (we'll re-try to
	// acquire next tick), but surface it for observability.
	if rerr := s.opts.Redis.Do(ctx, s.opts.Redis.B().Expire().Key(key).Seconds(leaseSecs).Build()).Error(); rerr != nil {
		s.opts.Log.Warn("invoke-recovery: failed to refresh leader lease", "error", rerr)
	}
	return true
}

// reconcile scans open invoke pauses and acts on each.
func (s *invokeRecoveryService) reconcile(ctx context.Context) (resumed, rerun, cleaned, skipped int, err error) {
	index := pauses.Index{WorkspaceID: s.opts.EnvID, EventName: event.FnFinishedName}
	iter, err := s.opts.Pauses.PausesSince(ctx, index, time.Time{})
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("listing invoke pauses: %w", err)
	}

	rerunsThisTick := 0
	seen := make(map[string]struct{})
	for iter.Next(ctx) {
		p := iter.Val(ctx)
		if p == nil || p.InvokeCorrelationID == nil || p.TriggeringEventID == nil {
			continue
		}
		seen[*p.InvokeCorrelationID] = struct{}{}

		action, childRun := s.classify(ctx, p)
		switch action {
		case recoveryResume:
			if e := s.resume(ctx, p, childRun); e != nil {
				s.opts.Log.Warn("invoke-recovery resume failed", "error", e, "parent_run_id", p.Identifier.RunID.String())
				skipped++
				continue
			}
			resumed++
		case recoveryRerun:
			if rerunsThisTick >= s.opts.RerunsPerTick {
				skipped++
				continue
			}
			if e := s.rerunChild(ctx, p); e != nil {
				s.opts.Log.Warn("invoke-recovery re-run failed", "error", e, "parent_run_id", p.Identifier.RunID.String())
				skipped++
				continue
			}
			s.rerunAttempts[*p.InvokeCorrelationID]++
			rerunsThisTick++
			rerun++
		case recoveryCleanup:
			if e := s.opts.Pauses.Delete(ctx, index, *p); e != nil {
				s.opts.Log.Warn("invoke-recovery cleanup failed", "error", e, "pause_id", p.ID.String())
				skipped++
				continue
			}
			cleaned++
		default:
			skipped++
		}
	}

	// Redis iterators signal normal completion with context.Canceled; any other
	// error means the scan ended early. Surface it (and skip pruning, since the
	// seen-set is partial) rather than masking a partial scan as a clean tick.
	if ierr := iter.Error(); ierr != nil && ierr != context.Canceled {
		return resumed, rerun, cleaned, skipped, fmt.Errorf("pause iteration: %w", ierr)
	}

	// Prune attempt counters for pauses no longer present (resumed, cleaned up,
	// or gone), so the map can't grow without bound over the service lifetime.
	for corrID := range s.rerunAttempts {
		if _, ok := seen[corrID]; !ok {
			delete(s.rerunAttempts, corrID)
		}
	}
	return resumed, rerun, cleaned, skipped, nil
}

// classify runs the status oracle (child run lookup + parent existence) and
// returns the decided action plus the terminal child run (for Tier 1).
func (s *invokeRecoveryService) classify(ctx context.Context, p *state.Pause) (recoveryAction, *cqrs.Run) {
	triggerID, perr := ulid.Parse(*p.TriggeringEventID)
	if perr != nil {
		return recoverySkip, nil
	}

	envID := s.opts.EnvID
	runs, rerr := s.opts.Data.GetRunsByEventID(ctx, triggerID, cqrs.GetRunsByEventIDOpts{
		AccountID:   s.opts.AccountID,
		WorkspaceID: &envID,
	})
	child := childMissing
	var terminal *cqrs.Run
	if rerr != nil {
		child = childUnknown
	} else if len(runs) > 0 {
		// The invocation event triggers exactly one child run.
		r := runs[0]
		if isTerminalStatus(r.Status) {
			child = childTerminal
			terminal = &r
		} else {
			child = childRunning
		}
	}

	parentExists := s.parentExists(ctx, p)

	in := recoveryInput{
		now:              time.Now(),
		pauseExpires:     p.Expires.Time(),
		parentRunAge:     time.Since(ulid.Time(p.Identifier.RunID.Time())),
		parentExists:     parentExists,
		child:            child,
		rerunAttempts:    s.rerunAttempts[*p.InvokeCorrelationID],
		minAge:           s.opts.MinAge,
		maxRerunAttempts: s.opts.MaxRerunAttempts,
	}
	return decideRecovery(in), terminal
}

func (s *invokeRecoveryService) parentExists(ctx context.Context, p *state.Pause) bool {
	if s.opts.Runs == nil {
		return true // can't check — assume present and let resume handle it
	}
	id := statev2.ID{
		RunID:      p.Identifier.RunID,
		FunctionID: p.Identifier.FunctionID,
		Tenant: statev2.Tenant{
			AccountID: s.opts.AccountID,
			EnvID:     s.opts.EnvID,
		},
	}
	ok, err := s.opts.Runs.Exists(ctx, id)
	if err != nil {
		return true // on error, don't treat as orphaned
	}
	return ok
}

// resume drives Tier 1: reconstruct the child's inngest/function.finished event
// from its persisted result and feed it to the idempotent HandleInvokeFinish.
func (s *invokeRecoveryService) resume(ctx context.Context, p *state.Pause, child *cqrs.Run) error {
	if child == nil {
		return fmt.Errorf("no terminal child run to resume from")
	}

	var output any
	if child.Output != nil && *child.Output != "" {
		// Best-effort: preserve the raw value if it isn't valid JSON.
		if jerr := json.Unmarshal([]byte(*child.Output), &output); jerr != nil {
			output = *child.Output
		}
	}

	data := map[string]any{
		consts.InvokeCorrelationId: *p.InvokeCorrelationID,
		"run_id":                   child.ID.String(),
	}
	if child.Status == enums.RunStatusCompleted {
		data["result"] = output
	} else {
		data["error"] = output
	}

	evt := event.Event{
		Name:      event.FnFinishedName,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}
	tracked := event.NewBaseTrackedEvent(evt, nil)
	return s.opts.Executor.HandleInvokeFinish(ctx, tracked)
}

// rerunChild drives Tier 2: re-publish the original invocation event so a fresh
// child runs and, on completion, resumes the still-valid parent pause.
func (s *invokeRecoveryService) rerunChild(ctx context.Context, p *state.Pause) error {
	if s.opts.Publish == nil {
		return fmt.Errorf("no event publisher configured")
	}
	triggerID, err := ulid.Parse(*p.TriggeringEventID)
	if err != nil {
		return fmt.Errorf("parsing triggering event id: %w", err)
	}
	ce, err := s.opts.Data.GetEventByInternalID(ctx, triggerID)
	if err != nil {
		return fmt.Errorf("loading original invocation event %s: %w", triggerID, err)
	}
	if ce == nil {
		return fmt.Errorf("original invocation event %s not found", triggerID)
	}
	evt := ce.GetEvent()
	// Re-publishing the same event (same correlation ID + deterministic child
	// scheduling) is idempotent: a duplicate child schedule dedupes, and once a
	// child is Running subsequent ticks see "Running -> skip".
	return s.opts.Publish(ctx, evt)
}

func isTerminalStatus(s enums.RunStatus) bool {
	switch s {
	case enums.RunStatusCompleted, enums.RunStatusFailed, enums.RunStatusCancelled, enums.RunStatusOverflowed:
		return true
	default:
		return false
	}
}
