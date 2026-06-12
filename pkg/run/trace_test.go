package run

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

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
