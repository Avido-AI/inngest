package manager

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/cqrs"
	dbsqlite "github.com/inngest/inngest/pkg/db/sqlite"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/history"
	"github.com/inngest/inngest/pkg/history_reader"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// TestReaderUnfinishedRunStatus asserts that runs without a finish row report
// Scheduled (GraphQL "QUEUED") until a FunctionStarted history entry exists,
// matching the semantics of the removed in-memory history mirror.
func TestReaderUnfinishedRunStatus(t *testing.T) {
	ctx := context.Background()

	db, err := dbsqlite.Open(ctx, dbsqlite.Options{Persist: false, ForTest: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	adapter := dbsqlite.New(db)
	cm := New(adapter)
	reader := NewHistoryReader(adapter)

	var (
		runID   = ulid.Make()
		eventID = ulid.Make()
		fnID    = uuid.New()
		wsID    = uuid.New()
		acctID  = uuid.New()
	)

	require.NoError(t, cm.InsertFunctionRun(ctx, cqrs.FunctionRun{
		RunID:        runID,
		RunStartedAt: time.Now(),
		FunctionID:   fnID,
		EventID:      eventID,
		WorkspaceID:  wsID,
	}))
	require.NoError(t, cm.InsertHistory(ctx, history.History{
		ID:         ulid.Make(),
		CreatedAt:  time.Now(),
		RunID:      runID,
		EventID:    eventID,
		FunctionID: fnID,
		AccountID:  acctID,
		Type:       enums.HistoryTypeFunctionScheduled.String(),
	}))

	run, err := reader.GetRun(ctx, runID, history_reader.GetRunOpts{})
	require.NoError(t, err)
	require.Equal(t, enums.RunStatusScheduled, run.Status)

	byEvent, err := reader.GetRunsByEventID(ctx, eventID, history_reader.GetRunsByEventIDOpts{})
	require.NoError(t, err)
	require.Len(t, byEvent, 1)
	require.Equal(t, enums.RunStatusScheduled, byEvent[0].Status)

	// Once the run starts, it must report Running.
	require.NoError(t, cm.InsertHistory(ctx, history.History{
		ID:         ulid.Make(),
		CreatedAt:  time.Now(),
		RunID:      runID,
		EventID:    eventID,
		FunctionID: fnID,
		AccountID:  acctID,
		Type:       enums.HistoryTypeFunctionStarted.String(),
	}))

	run, err = reader.GetRun(ctx, runID, history_reader.GetRunOpts{})
	require.NoError(t, err)
	require.Equal(t, enums.RunStatusRunning, run.Status)
}
