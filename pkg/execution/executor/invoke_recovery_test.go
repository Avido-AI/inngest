package executor

import (
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
)

func TestDecideRecovery(t *testing.T) {
	now := time.Now()
	grace := consts.PauseExpiredDeletionGracePeriod
	minAge := 2 * time.Minute

	// A pause that is well past the young-skip window and not expired.
	old := recoveryInput{
		now:              now,
		pauseExpires:     now.Add(365 * 24 * time.Hour), // far future (typical invoke)
		parentRunAge:     time.Hour,
		parentExists:     true,
		minAge:           minAge,
		maxRerunAttempts: 3,
	}

	cases := []struct {
		name string
		in   recoveryInput
		want recoveryAction
	}{
		{
			name: "long-running child is never acted on (hours)",
			in:   withChildAge(old, childRunning, 0, 12*time.Hour),
			want: recoverySkip,
		},
		{
			name: "terminal child resumes",
			in:   withChild(old, childTerminal, 0),
			want: recoveryResume,
		},
		{
			name: "missing child re-runs when under attempt cap",
			in:   withChild(old, childMissing, 1),
			want: recoveryRerun,
		},
		{
			name: "missing child gives up (skip) at attempt cap",
			in:   withChild(old, childMissing, 3),
			want: recoverySkip,
		},
		{
			name: "unknown child status skips (safe default)",
			in:   withChild(old, childUnknown, 0),
			want: recoverySkip,
		},
		{
			name: "young pause skips even if child terminal",
			in:   withChildAge(old, childTerminal, 0, 30*time.Second),
			want: recoverySkip,
		},
		{
			name: "orphaned pause (parent gone) cleans up even if child terminal",
			in: func() recoveryInput {
				i := withChild(old, childTerminal, 0)
				i.parentExists = false
				return i
			}(),
			want: recoveryCleanup,
		},
		{
			name: "expired-past-grace pause cleans up even if child running",
			in: func() recoveryInput {
				i := withChild(old, childRunning, 0)
				i.pauseExpires = now.Add(-grace - time.Hour)
				return i
			}(),
			want: recoveryCleanup,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideRecovery(tc.in); got != tc.want {
				t.Fatalf("decideRecovery = %v, want %v", got, tc.want)
			}
		})
	}
}

func withChild(base recoveryInput, c childStatus, attempts int) recoveryInput {
	base.child = c
	base.rerunAttempts = attempts
	return base
}

func withChildAge(base recoveryInput, c childStatus, attempts int, age time.Duration) recoveryInput {
	base = withChild(base, c, attempts)
	base.parentRunAge = age
	return base
}

// TestDecideRecoveryLongRunningAcrossTicks asserts the core safety property: a
// child that stays Running is skipped on every tick, for any duration, and only
// resumes once the child becomes terminal.
func TestDecideRecoveryLongRunningAcrossTicks(t *testing.T) {
	now := time.Now()
	base := recoveryInput{
		now:              now,
		pauseExpires:     now.Add(365 * 24 * time.Hour),
		parentExists:     true,
		minAge:           2 * time.Minute,
		maxRerunAttempts: 3,
		child:            childRunning,
	}
	for _, age := range []time.Duration{5 * time.Minute, time.Hour, 6 * time.Hour, 24 * time.Hour} {
		in := base
		in.parentRunAge = age
		if got := decideRecovery(in); got != recoverySkip {
			t.Fatalf("running child at age %s: got %v, want skip", age, got)
		}
	}
	// Child completes -> resume.
	done := base
	done.parentRunAge = 24 * time.Hour
	done.child = childTerminal
	if got := decideRecovery(done); got != recoveryResume {
		t.Fatalf("terminal child: got %v, want resume", got)
	}
}

func TestIsTerminalStatus(t *testing.T) {
	for _, s := range []enums.RunStatus{
		enums.RunStatusCompleted, enums.RunStatusFailed, enums.RunStatusCancelled, enums.RunStatusOverflowed, enums.RunStatusSkipped,
	} {
		if !isTerminalStatus(s) {
			t.Errorf("status %v should be terminal", s)
		}
	}
	for _, s := range []enums.RunStatus{enums.RunStatusRunning, enums.RunStatusScheduled} {
		if isTerminalStatus(s) {
			t.Errorf("status %v should not be terminal", s)
		}
	}
}
