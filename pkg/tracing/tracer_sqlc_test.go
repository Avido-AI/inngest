package tracing

import (
	"context"
	"testing"

	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestExtractSpanFieldsAttrDuplication pins down that known column-backed
// attributes are NOT duplicated into the generic attrs JSON: the span fragment
// queries select the dedicated columns per fragment and the read side overlays
// them back into the typed extraction (overlayFragmentColumnAttrs). Keys
// without a dedicated column must stay in attrs.
func TestExtractSpanFieldsAttrDuplication(t *testing.T) {
	accountID := "11111111-1111-1111-1111-111111111111"
	envID := "22222222-2222-2222-2222-222222222222"
	appID := "33333333-3333-3333-3333-333333333333"
	functionID := "44444444-4444-4444-4444-444444444444"
	runID := "01HXXXXXXXXXXXXXXXXXXXXXXX"

	stub := tracetest.SpanStub{
		Name: meta.SpanNameRun,
		Attributes: []attribute.KeyValue{
			attribute.String(meta.Attrs.AccountID.Key(), accountID),
			attribute.String(meta.Attrs.EnvID.Key(), envID),
			attribute.String(meta.Attrs.RunID.Key(), runID),
			attribute.String(meta.Attrs.AppID.Key(), appID),
			attribute.String(meta.Attrs.FunctionID.Key(), functionID),
			attribute.String(meta.Attrs.DynamicTraceID.Key(), "dyn-trace"),
			attribute.String(meta.Attrs.DynamicSpanID.Key(), "dyn-span"),
			attribute.String(meta.Attrs.DynamicStatus.Key(), "Completed"),
			attribute.String(meta.Attrs.DebugRunID.Key(), "debug-run"),
			attribute.String(meta.Attrs.DebugSessionID.Key(), "debug-session"),
			attribute.StringSlice(meta.Attrs.EventIDs.Key(), []string{"evt-1"}),
			attribute.String("sdk.language", "go"),
		},
	}

	sf := extractSpanFields(context.Background(), stub.Snapshot())

	// Every known attribute lands in its dedicated field.
	assert.Equal(t, accountID, sf.accountID)
	assert.Equal(t, envID, sf.envID)
	assert.Equal(t, runID, sf.runID)
	assert.Equal(t, appID, sf.appID)
	assert.Equal(t, functionID, sf.functionID)
	assert.Equal(t, "dyn-trace", sf.traceID)
	assert.Equal(t, "dyn-span", sf.dynamicSpanID)
	assert.Equal(t, "Completed", sf.status)
	assert.Equal(t, "debug-run", sf.debugRunID)
	assert.Equal(t, "debug-session", sf.debugSessionID)
	assert.JSONEq(t, `["evt-1"]`, string(sf.eventIdsByt))

	// Column-backed keys are not duplicated into the attrs JSON.
	for _, key := range []string{
		meta.Attrs.AccountID.Key(),
		meta.Attrs.EnvID.Key(),
		meta.Attrs.RunID.Key(),
		meta.Attrs.DynamicTraceID.Key(),
		meta.Attrs.DynamicSpanID.Key(),
		meta.Attrs.AppID.Key(),
		meta.Attrs.FunctionID.Key(),
		meta.Attrs.DynamicStatus.Key(),
		meta.Attrs.DebugRunID.Key(),
		meta.Attrs.DebugSessionID.Key(),
		meta.Attrs.EventIDs.Key(),
	} {
		assert.NotContains(t, sf.attrs, key)
	}

	// Keys without a dedicated column are stored in attrs as-is.
	assert.Contains(t, sf.attrs, "sdk.language")
}

func TestAnyToBytes(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		assert.Nil(t, anyToBytes(nil))
	})

	t.Run("empty string returns nil", func(t *testing.T) {
		assert.Nil(t, anyToBytes(""))
	})

	t.Run("empty byte slice returns nil", func(t *testing.T) {
		assert.Nil(t, anyToBytes([]byte{}))
	})

	t.Run("string passed through without double encoding", func(t *testing.T) {
		// This is the critical regression test: a JSON string like
		// `{"data":{"num":12}}` must NOT be wrapped in extra quotes.
		input := `{"data":{"num":12}}`
		got := anyToBytes(input)
		assert.Equal(t, []byte(input), got)
		// Verify it doesn't start with a quote (double-encoding symptom)
		assert.NotEqual(t, byte('"'), got[0], "output should not be double-encoded")
	})

	t.Run("byte slice passed through as-is", func(t *testing.T) {
		input := []byte(`{"key":"value"}`)
		got := anyToBytes(input)
		assert.Equal(t, input, got)
	})

	t.Run("other types are JSON marshaled", func(t *testing.T) {
		got := anyToBytes(map[string]int{"x": 1})
		assert.Equal(t, []byte(`{"x":1}`), got)
	})
}
