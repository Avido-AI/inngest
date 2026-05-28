package executor

import (
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestFunctionFinishedDataMap(t *testing.T) {
	runID := ulid.MustNew(ulid.Now(), nil)

	t.Run("correlation ID present produces string type", func(t *testing.T) {
		data := functionFinishedData{
			FunctionID:          "fn-1",
			RunID:               runID,
			InvokeCorrelationID: "run-1.step-1",
		}
		m := data.Map()

		raw, ok := m["correlation_id"]
		require.True(t, ok, "correlation_id key must exist")

		// The downstream CorrelationID() method uses .(string) assertion.
		corrID, ok := raw.(string)
		require.True(t, ok, "correlation_id must be a string, got %T", raw)
		require.Equal(t, "run-1.step-1", corrID)
	})

	t.Run("empty correlation ID is omitted", func(t *testing.T) {
		data := functionFinishedData{
			FunctionID: "fn-1",
			RunID:      runID,
		}
		m := data.Map()

		_, ok := m["correlation_id"]
		require.False(t, ok, "empty correlation_id should be omitted")
	})

	t.Run("run_id produces string type", func(t *testing.T) {
		data := functionFinishedData{
			FunctionID: "fn-1",
			RunID:      runID,
		}
		m := data.Map()

		raw, ok := m["run_id"]
		require.True(t, ok, "run_id key must exist")

		// GetResumeData() uses evt.Data["run_id"].(string) to extract
		// the child run ID. This must succeed — not return ulid.ULID.
		strRunID, ok := raw.(string)
		require.True(t, ok, "run_id must be a string, got %T", raw)
		require.Equal(t, runID.String(), strRunID)
	})
}
