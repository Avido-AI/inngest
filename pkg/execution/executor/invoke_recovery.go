package executor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
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

// eventPublisher re-ingests an event (used to re-run a child in Tier 2). The
// seed makes the re-ingested event's internal ID deterministic.
type eventPublisher func(ctx context.Context, e event.Event, seed *event.SeededID) error

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
	// lastRerun spaces Tier-2 re-runs per correlation ID (cooldown).
	lastRerun map[string]time.Time
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
		lastRerun:     map[string]time.Time{},
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

// leaderLeaseScript atomically acquires-or-refreshes the singleton leader lease:
// acquire if unset, refresh the TTL if we already hold it, otherwise deny. Doing
// this in one Lua eval avoids the GET->EXPIRE race that two pods could otherwise
// hit when refreshing concurrently.
var leaderLeaseScript = rueidis.NewLuaScript(`
local cur = redis.call("GET", KEYS[1])
if not cur then
	redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
	return 1
elseif cur == ARGV[1] then
	redis.call("EXPIRE", KEYS[1], ARGV[2])
	return 1
end
return 0
`)

// acquireLease returns whether this pod holds the singleton leader lease. Only
// the holder reconciles, so pods don't double-process (critical for Tier-2
// re-runs). The lease key is scoped per environment.
func (s *invokeRecoveryService) acquireLease(ctx context.Context) bool {
	key := fmt.Sprintf("{estate}:invoke-recovery:leader:%s", s.opts.EnvID)
	ttl := strconv.Itoa(int(s.opts.LeaseDuration.Seconds()))
	held, err := leaderLeaseScript.Exec(ctx, s.opts.Redis, []string{key}, []string{s.holderID, ttl}).AsInt64()
	if err != nil {
		s.opts.Log.Warn("invoke-recovery: leader lease check failed", "error", err)
		return false
	}
	return held == 1
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
		// Fix-forward only: act on invoke pauses that carry the internal
		// triggering-event ID (the indexed link to the child run). Legacy
		// pauses without it are skipped — the oracle can't safely classify them.
		if p == nil || p.InvokeCorrelationID == nil || p.TriggeringEventInternalID == nil {
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
			corr := *p.InvokeCorrelationID
			// Cooldown: a re-published child gets a fresh triggering-event ID,
			// so the oracle can't see it under the original ID until it
			// finishes. Space attempts so we don't spawn duplicate children.
			if last, ok := s.lastRerun[corr]; ok && time.Since(last) < consts.InvokeRecoveryRerunCooldown {
				skipped++
				continue
			}
			if rerunsThisTick >= s.opts.RerunsPerTick {
				skipped++
				continue
			}
			// Record the attempt + stamp the cooldown BEFORE attempting, so a
			// FAILED re-run (e.g. the invocation event was purged) still counts
			// toward the cap and respects the cooldown. Otherwise it would retry
			// every tick forever and never hand the unrecoverable case to ops.
			s.rerunAttempts[corr]++
			s.lastRerun[corr] = time.Now()
			rerunsThisTick++
			if e := s.rerunChild(ctx, p); e != nil {
				s.opts.Log.Warn("invoke-recovery re-run failed", "error", e, "parent_run_id", p.Identifier.RunID.String())
				skipped++
				continue
			}
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
	for corrID := range s.lastRerun {
		if _, ok := seen[corrID]; !ok {
			delete(s.lastRerun, corrID)
		}
	}
	return resumed, rerun, cleaned, skipped, nil
}

// classify runs the status oracle (child run lookup + parent existence) and
// returns the decided action plus the terminal child run (for Tier 1).
func (s *invokeRecoveryService) classify(ctx context.Context, p *state.Pause) (recoveryAction, *cqrs.Run) {
	// Use the INTERNAL triggering-event ID: child runs are keyed by it
	// (function_runs.event_id) and events are indexed by internal_id.
	triggerID, perr := ulid.Parse(*p.TriggeringEventInternalID)
	if perr != nil {
		return recoverySkip, nil
	}

	child, terminal := s.childStatusFor(ctx, triggerID)
	if child == childMissing {
		// No run for the original invoke. A Tier-2 re-run (if any) publishes a
		// child under a deterministic triggering-event ID derived from the
		// correlation, so look there too — otherwise the re-run child would be
		// invisible and we'd re-run again every cooldown, spawning duplicates.
		if rid, err := reRunSeed(*p.InvokeCorrelationID, p.Identifier.RunID).ToULID(); err == nil {
			if c, t := s.childStatusFor(ctx, rid); c != childMissing {
				child, terminal = c, t
			}
		}
	}

	in := recoveryInput{
		now:              time.Now(),
		pauseExpires:     p.Expires.Time(),
		parentRunAge:     time.Since(ulid.Time(p.Identifier.RunID.Time())),
		parentExists:     s.parentExists(ctx, p),
		child:            child,
		rerunAttempts:    s.rerunAttempts[*p.InvokeCorrelationID],
		minAge:           s.opts.MinAge,
		maxRerunAttempts: s.opts.MaxRerunAttempts,
	}
	return decideRecovery(in), terminal
}

// childStatusFor looks up the child run by a triggering-event internal ID and
// maps it to a childStatus (+ the terminal run for Tier-1 resume).
func (s *invokeRecoveryService) childStatusFor(ctx context.Context, eventID ulid.ULID) (childStatus, *cqrs.Run) {
	envID := s.opts.EnvID
	runs, err := s.opts.Data.GetRunsByEventID(ctx, eventID, cqrs.GetRunsByEventIDOpts{
		AccountID:   s.opts.AccountID,
		WorkspaceID: &envID,
	})
	if err != nil {
		return childUnknown, nil
	}
	if len(runs) == 0 {
		return childMissing, nil
	}
	// The invocation event triggers exactly one child run.
	r := runs[0]
	if isTerminalStatus(r.Status) {
		return childTerminal, &r
	}
	return childRunning, nil
}

// reRunSeed derives a deterministic event seed for a Tier-2 re-run so the
// re-published child always lands on the same triggering-event internal ID.
// That keeps re-ingestion idempotent and lets classify see the re-run child
// (Running -> skip / Terminal -> resume) instead of spawning a new one each tick.
func reRunSeed(correlationID string, parentRunID ulid.ULID) *event.SeededID {
	h := sha256.Sum256([]byte("invoke-recovery-rerun:" + correlationID))
	return &event.SeededID{
		Entropy: h[:10],
		Millis:  int64(parentRunID.Time()),
	}
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
	// Carry the pause's tenant on the tracked event. HandleInvokeFinish resolves
	// the pause via GetWorkspaceID(), so an unset workspace would look in the
	// wrong (zero) workspace and miss the pause — and it's required for correct
	// routing in a multi-tenant/multi-env deployment.
	tracked := event.BaseTrackedEvent{
		ID:          ulid.Make(),
		AccountID:   p.Identifier.AccountID,
		WorkspaceID: p.WorkspaceID,
		Event:       evt,
	}
	return s.opts.Executor.HandleInvokeFinish(ctx, tracked)
}

// rerunChild drives Tier 2: re-publish the original invocation event so a fresh
// child runs and, on completion, resumes the still-valid parent pause.
func (s *invokeRecoveryService) rerunChild(ctx context.Context, p *state.Pause) error {
	if s.opts.Publish == nil {
		return fmt.Errorf("no event publisher configured")
	}
	triggerID, err := ulid.Parse(*p.TriggeringEventInternalID)
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
	// Publish with a deterministic seed so the re-run child always lands on the
	// same triggering-event internal ID. classify checks that ID, so once the
	// re-run child exists it's seen ("Running -> skip" / "Terminal -> resume")
	// and we never spawn a second one.
	seed := reRunSeed(*p.InvokeCorrelationID, p.Identifier.RunID)
	return s.opts.Publish(ctx, evt, seed)
}

// isTerminalStatus reports whether a child run has ended. It delegates to the
// canonical enums.RunStatusEnded so the recovery oracle's notion of "terminal"
// can't drift from the codebase (e.g. it must include Skipped — a skipped child
// would otherwise look Running forever and strand the parent).
func isTerminalStatus(s enums.RunStatus) bool {
	return enums.RunStatusEnded(s)
}
