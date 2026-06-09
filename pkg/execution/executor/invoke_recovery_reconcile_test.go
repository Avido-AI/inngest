package executor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution/pauses"
	"github.com/inngest/inngest/pkg/execution/state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/oklog/ulid/v2"
)

// --- fakes ---

type fakePauseIter struct {
	pauses []*state.Pause
	idx    int
}

func (f *fakePauseIter) Count() int { return len(f.pauses) }
func (f *fakePauseIter) Next(ctx context.Context) bool {
	f.idx++
	return f.idx <= len(f.pauses)
}
func (f *fakePauseIter) Error() error { return context.Canceled } // normal completion
func (f *fakePauseIter) Val(context.Context) *state.Pause {
	if f.idx >= 1 && f.idx <= len(f.pauses) {
		return f.pauses[f.idx-1]
	}
	return nil
}
func (f *fakePauseIter) Index() int64 { return int64(f.idx) }

type fakePauses struct {
	iter    state.PauseIterator
	deleted int
}

func (f *fakePauses) PausesSince(ctx context.Context, _ pauses.Index, _ time.Time) (state.PauseIterator, error) {
	return f.iter, nil
}
func (f *fakePauses) Delete(ctx context.Context, _ pauses.Index, _ state.Pause, _ ...state.DeletePauseOpt) error {
	f.deleted++
	return nil
}

type fakeResumer struct {
	calls   int
	lastEvt event.TrackedEvent
}

func (f *fakeResumer) HandleInvokeFinish(ctx context.Context, e event.TrackedEvent) error {
	f.calls++
	f.lastEvt = e
	return nil
}

type fakeRuns struct{ exists bool }

func (f *fakeRuns) Exists(ctx context.Context, _ statev2.ID) (bool, error) { return f.exists, nil }

type fakeData struct {
	runs []cqrs.Run
	evt  *cqrs.Event
}

func (f *fakeData) GetRunsByEventID(ctx context.Context, _ ulid.ULID, _ cqrs.GetRunsByEventIDOpts) ([]cqrs.Run, error) {
	return f.runs, nil
}
func (f *fakeData) GetEventByInternalID(ctx context.Context, _ ulid.ULID) (*cqrs.Event, error) {
	return f.evt, nil
}

// --- helpers ---

var (
	testWsID   = uuid.New()
	testAcctID = uuid.New()
)

func oldRunID() ulid.ULID {
	// Very old timestamp => well past the min-age churn filter.
	return ulid.MustParse("01000000000000000000000000")
}

func invokePause(withInternalID bool) *state.Pause {
	corr := "01KTM8G2NASW1A19FFBKHD0H9R.abc"
	internal := ulid.Make().String()
	p := &state.Pause{
		ID:                  uuid.New(),
		WorkspaceID:         testWsID,
		Identifier:          state.PauseIdentifier{RunID: oldRunID(), FunctionID: uuid.New(), AccountID: testAcctID},
		InvokeCorrelationID: &corr,
		Expires:             state.Time(time.Now().Add(24 * time.Hour)),
	}
	if withInternalID {
		p.TriggeringEventInternalID = &internal
	}
	return p
}

func newTestService(fp *fakePauses, fr *fakeRuns, fd *fakeData, resumer *fakeResumer) *invokeRecoveryService {
	return &invokeRecoveryService{
		opts: InvokeRecoveryOpts{
			Log:              logger.StdlibLogger(context.Background()),
			Executor:         resumer,
			Pauses:           fp,
			Runs:             fr,
			Data:             fd,
			AccountID:        uuid.New(),
			EnvID:            uuid.New(),
			MinAge:           2 * time.Minute,
			RerunsPerTick:    20,
			MaxRerunAttempts: 3,
		},
		rerunAttempts: map[string]int{},
		lastRerun:     map[string]time.Time{},
	}
}

// --- tests ---

func TestReconcileTerminalChildResumes(t *testing.T) {
	out := `"done"`
	fd := &fakeData{runs: []cqrs.Run{{ID: ulid.Make(), Status: enums.RunStatusCompleted, Output: &out}}}
	fp := &fakePauses{iter: &fakePauseIter{pauses: []*state.Pause{invokePause(true)}}}
	fr := &fakeRuns{exists: true}
	resumer := &fakeResumer{}
	s := newTestService(fp, fr, fd, resumer)

	resumed, rerun, cleaned, _, err := s.reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if resumed != 1 || resumer.calls != 1 {
		t.Fatalf("expected 1 resume (HandleInvokeFinish called), got resumed=%d calls=%d", resumed, resumer.calls)
	}
	if rerun != 0 || cleaned != 0 {
		t.Fatalf("unexpected rerun=%d cleaned=%d", rerun, cleaned)
	}
	// The resumed event must carry the pause's tenant, or HandleInvokeFinish
	// would look up the pause in the wrong (zero) workspace and miss it.
	if got := resumer.lastEvt.GetWorkspaceID(); got != testWsID {
		t.Fatalf("resumed event workspace = %v, want %v", got, testWsID)
	}
	if got := resumer.lastEvt.GetAccountID(); got != testAcctID {
		t.Fatalf("resumed event account = %v, want %v", got, testAcctID)
	}
}

func TestReconcileSkipsLegacyPauseWithoutInternalID(t *testing.T) {
	// Pause without TriggeringEventInternalID must be skipped (fix-forward only),
	// even though a terminal child "exists" — guards against the original bug
	// where every child looked missing.
	out := `"done"`
	fd := &fakeData{runs: []cqrs.Run{{ID: ulid.Make(), Status: enums.RunStatusCompleted, Output: &out}}}
	fp := &fakePauses{iter: &fakePauseIter{pauses: []*state.Pause{invokePause(false)}}}
	s := newTestService(fp, &fakeRuns{exists: true}, fd, &fakeResumer{})

	resumed, rerun, cleaned, _, err := s.reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if resumed != 0 || rerun != 0 || cleaned != 0 {
		t.Fatalf("legacy pause should be skipped, got resumed=%d rerun=%d cleaned=%d", resumed, rerun, cleaned)
	}
}

func TestReconcileRerunFailureStillCountsAttempt(t *testing.T) {
	// No child run => Tier-2 re-run. Publish is nil in the test service, so
	// rerunChild fails. The attempt must still be counted and the cooldown
	// stamped, or the cap/cooldown would be bypassed and it would retry forever.
	fd := &fakeData{runs: nil}
	p := invokePause(true)
	fp := &fakePauses{iter: &fakePauseIter{pauses: []*state.Pause{p}}}
	s := newTestService(fp, &fakeRuns{exists: true}, fd, &fakeResumer{})

	_, rerun, _, _, err := s.reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if rerun != 0 {
		t.Fatalf("failed re-run should not count as a successful rerun, got %d", rerun)
	}
	corr := *p.InvokeCorrelationID
	if s.rerunAttempts[corr] != 1 {
		t.Fatalf("failed re-run must still count as an attempt; got %d", s.rerunAttempts[corr])
	}
	if _, ok := s.lastRerun[corr]; !ok {
		t.Fatalf("failed re-run must stamp the cooldown")
	}
}

func TestReconcileSkipsRunningChild(t *testing.T) {
	fd := &fakeData{runs: []cqrs.Run{{ID: ulid.Make(), Status: enums.RunStatusRunning}}}
	fp := &fakePauses{iter: &fakePauseIter{pauses: []*state.Pause{invokePause(true)}}}
	resumer := &fakeResumer{}
	s := newTestService(fp, &fakeRuns{exists: true}, fd, resumer)

	resumed, rerun, _, _, err := s.reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if resumed != 0 || rerun != 0 || resumer.calls != 0 {
		t.Fatalf("running child must be skipped, got resumed=%d rerun=%d calls=%d", resumed, rerun, resumer.calls)
	}
}
