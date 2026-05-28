package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionFinishedDataMapCorrelationID(t *testing.T) {
	t.Run("correlation ID present produces string type", func(t *testing.T) {
		data := functionFinishedData{
			FunctionID:          "fn-1",
			InvokeCorrelationID: "run-1.step-1",
		}
		m := data.Map()

		raw, ok := m["correlation_id"]
		require.True(t, ok, "correlation_id key must exist")

		// The downstream CorrelationID() method uses .(string) assertion.
		// This must succeed — not return *string.
		corrID, ok := raw.(string)
		require.True(t, ok, "correlation_id must be a string, got %T", raw)
		require.Equal(t, "run-1.step-1", corrID)
	})

	t.Run("empty correlation ID is omitted", func(t *testing.T) {
		data := functionFinishedData{
			FunctionID: "fn-1",
		}
		m := data.Map()

		_, ok := m["correlation_id"]
		require.False(t, ok, "empty correlation_id should be omitted")
	})
}
