package run

import (
	"context"
	"crypto/rand"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	rpbv2 "github.com/inngest/inngest/proto/gen/run/v2"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// newTestRunTree builds a runTree with a single span group, bypassing
// NewRunTree so tests can exercise group processing directly.
func newTestRunTree(groupID string, group []*cqrs.Span) *runTree {
	tb := &runTree{
		runID:     ulid.MustNew(ulid.Now(), rand.Reader),
		spans:     map[string]*cqrs.Span{},
		groups:    map[string][]*cqrs.Span{groupID: group},
		processed: map[string]bool{},
	}
	for _, s := range group {
		tb.spans[s.SpanID] = s
	}
	return tb
}

// Regression test: a retried wait group can contain an in-flight peer whose
// span has no step opcode yet. constructSpan leaves StepOp nil for such
// spans, and the single-child group-collapse check used to dereference it.
func TestProcessWaitForEventGroupNilChildStepOp(t *testing.T) {
	ctx := context.Background()
	groupID := "group-1"
	parentID := "parent"
	now := time.Now()

	waitExec := &cqrs.Span{
		SpanID:       "span-wait-exec",
		ParentSpanID: &parentID,
		ScopeName:    consts.OtelScopeExecution,
		Timestamp:    now,
		SpanAttributes: map[string]string{
			consts.OtelSysStepGroupID: groupID,
			consts.OtelSysStepOpcode:  enums.OpcodeWaitForEvent.String(),
		},
	}
	// An in-flight attempt with no opcode recorded yet.
	pending := &cqrs.Span{
		SpanID:       "span-pending",
		ParentSpanID: &parentID,
		ScopeName:    consts.OtelScopeStep,
		Timestamp:    now.Add(time.Second),
		SpanAttributes: map[string]string{
			consts.OtelSysStepGroupID: groupID,
		},
	}

	tb := newTestRunTree(groupID, []*cqrs.Span{waitExec, pending})

	mod := &rpbv2.RunSpan{}
	err := tb.processWaitForEventGroup(ctx, waitExec, mod)
	require.NoError(t, err)
	require.Len(t, mod.Children, 1)
	require.Nil(t, mod.Children[0].StepOp)
}

// Same regression as TestProcessWaitForEventGroupNilChildStepOp for the
// waitForSignal group-collapse path.
func TestProcessWaitForSignalGroupNilChildStepOp(t *testing.T) {
	ctx := context.Background()
	groupID := "group-1"
	parentID := "parent"
	now := time.Now()

	waitExec := &cqrs.Span{
		SpanID:       "span-wait-exec",
		ParentSpanID: &parentID,
		ScopeName:    consts.OtelScopeExecution,
		Timestamp:    now,
		SpanAttributes: map[string]string{
			consts.OtelSysStepGroupID: groupID,
			consts.OtelSysStepOpcode:  enums.OpcodeWaitForSignal.String(),
		},
	}
	pending := &cqrs.Span{
		SpanID:       "span-pending",
		ParentSpanID: &parentID,
		ScopeName:    consts.OtelScopeStep,
		Timestamp:    now.Add(time.Second),
		SpanAttributes: map[string]string{
			consts.OtelSysStepGroupID: groupID,
		},
	}

	tb := newTestRunTree(groupID, []*cqrs.Span{waitExec, pending})

	mod := &rpbv2.RunSpan{}
	err := tb.processWaitForSignalGroup(ctx, waitExec, mod)
	require.NoError(t, err)
	require.Len(t, mod.Children, 1)
	require.Nil(t, mod.Children[0].StepOp)
}

func TestNewRunTreePrefersTerminalFunctionStatus(t *testing.T) {
	ctx := context.Background()

	acctID := uuid.New()
	wsID := uuid.New()
	appID := uuid.New()
	fnID := uuid.New()
	runID := ulid.MustNew(ulid.Now(), nil)
	traceID := "trace-id"
	fnSpanID := "fn-span-id"

	t0 := time.UnixMilli(1_700_000_000_000)

	fnAttrs := func(status enums.RunStatus) map[string]string {
		return map[string]string{
			consts.OtelSysAccountID:          acctID.String(),
			consts.OtelSysWorkspaceID:        wsID.String(),
			consts.OtelSysAppID:              appID.String(),
			consts.OtelSysFunctionID:         fnID.String(),
			consts.OtelAttrSDKRunID:          runID.String(),
			consts.OtelSysFunctionStatusCode: strconv.FormatInt(status.ToCode(), 10),
		}
	}

	mkFnSpan := func(ts time.Time, dur time.Duration, status enums.RunStatus) *cqrs.Span {
		return &cqrs.Span{
			Timestamp:      ts,
			TraceID:        traceID,
			SpanID:         fnSpanID,
			SpanName:       "fn",
			ScopeName:      consts.OtelScopeFunction,
			Duration:       dur,
			RunID:          &runID,
			SpanAttributes: fnAttrs(status),
		}
	}

	trigger := &cqrs.Span{
		Timestamp:      t0,
		TraceID:        traceID,
		SpanID:         "trigger-span",
		SpanName:       consts.OtelSpanTrigger,
		ScopeName:      consts.OtelScopeTrigger,
		RunID:          &runID,
		SpanAttributes: fnAttrs(enums.RunStatusScheduled),
	}

	running := mkFnSpan(t0, 2*time.Millisecond, enums.RunStatusRunning)
	// The terminal row can survive read-time dedup with a later millisecond.
	failed := mkFnSpan(t0.Add(5*time.Millisecond), 1000*time.Millisecond, enums.RunStatusFailed)

	newTree := func(t *testing.T, spans []*cqrs.Span) *runTree {
		t.Helper()
		tree, err := NewRunTree(RunTreeOpts{
			AccountID:   acctID,
			WorkspaceID: wsID,
			AppID:       appID,
			FunctionID:  fnID,
			RunID:       runID,
			Spans:       spans,
		})
		require.NoError(t, err)
		return tree
	}

	// Row order must not affect root selection.
	t.Run("failed row first", func(t *testing.T) {
		tree := newTree(t, []*cqrs.Span{trigger, failed, running})
		require.Equal(t, enums.RunStatusFailed, tree.root.FunctionStatus())

		root, err := tree.ToRunSpan(ctx)
		require.NoError(t, err)
		require.Equal(t, rpbv2.SpanStatus_FAILED, root.GetStatus())
	})

	t.Run("running row first", func(t *testing.T) {
		tree := newTree(t, []*cqrs.Span{trigger, running, failed})
		require.Equal(t, enums.RunStatusFailed, tree.root.FunctionStatus())

		root, err := tree.ToRunSpan(ctx)
		require.NoError(t, err)
		require.Equal(t, rpbv2.SpanStatus_FAILED, root.GetStatus())
	})

	// Children attach through the selected root's span map entry.
	t.Run("children attach to the chosen root", func(t *testing.T) {
		childSpanID := "child-span"
		child := &cqrs.Span{
			Timestamp:    t0.Add(time.Millisecond),
			TraceID:      traceID,
			SpanID:       childSpanID,
			ParentSpanID: &fnSpanID,
			SpanName:     "step",
			ScopeName:    consts.OtelScopeStep,
			RunID:        &runID,
			SpanAttributes: map[string]string{
				consts.OtelSysAccountID:   acctID.String(),
				consts.OtelSysWorkspaceID: wsID.String(),
				consts.OtelSysAppID:       appID.String(),
				consts.OtelSysFunctionID:  fnID.String(),
				consts.OtelAttrSDKRunID:   runID.String(),
			},
		}

		tree := newTree(t, []*cqrs.Span{trigger, failed, running, child})
		require.Equal(t, enums.RunStatusFailed, tree.root.FunctionStatus())

		var found bool
		for _, c := range tree.root.Children {
			if c.SpanID == childSpanID {
				found = true
			}
		}
		require.True(t, found, "child span should attach to the terminal-status root")
	})
}
