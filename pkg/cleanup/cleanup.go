package cleanup

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// tableTarget defines a table + column pair for time-based deletion.
type tableTarget struct {
	table  string
	column string
	// isBigintMs indicates the column stores unix milliseconds (bigint)
	// rather than a timestamp type.
	isBigintMs bool
}

// archivedTables are cleaned in Phase 1 (soft-deleted rows).
var archivedTables = []string{"functions", "apps"}

// timestampTables are cleaned in Phase 2 (time-based retention).
var timestampTables = []tableTarget{
	{table: "events", column: "received_at"},
	{table: "event_batches", column: "started_at"},
	{table: "function_finishes", column: "created_at"},
	{table: "function_runs", column: "run_started_at"},
	{table: "history", column: "created_at"},
	{table: "spans", column: "start_time"},
	{table: "trace_runs", column: "queued_at", isBigintMs: true},
	{table: "traces", column: `"timestamp"`},
}

// Logger is the minimal logging interface required by the cleanup service.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Service implements service.Service and runs periodic database cleanup.
type Service struct {
	cfg    Config
	db     *sql.DB
	logger Logger
}

// NewService creates a cleanup service. The caller must supply an open *sql.DB
// that targets the inngest schema (search_path already set or schema-qualified
// queries are used). Passing a nil db when Postgres is not configured is safe;
// the service will no-op.
func NewService(cfg Config, db *sql.DB, logger Logger) *Service {
	return &Service{cfg: cfg, db: db, logger: logger}
}

func (s *Service) Name() string { return "cleanup" }

func (s *Service) Pre(_ context.Context) error { return nil }

func (s *Service) Run(ctx context.Context) error {
	if !s.cfg.Enabled || s.db == nil {
		return nil
	}

	if s.cfg.Interval <= 0 {
		s.cfg.Interval = DefaultInterval
	}
	if s.cfg.RetentionDays <= 0 {
		s.cfg.RetentionDays = DefaultRetentionDays
	}

	// Run immediately on startup to catch up after restarts.
	s.runCleanup(ctx)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.runCleanup(ctx)
		}
	}
}

func (s *Service) Stop(_ context.Context) error { return nil }

// runCleanup performs one full pass over all tables.
func (s *Service) runCleanup(ctx context.Context) {
	start := time.Now()
	cutoff := time.Now().Add(-time.Duration(s.cfg.RetentionDays) * 24 * time.Hour)
	cutoffMs := cutoff.UnixMilli()

	s.logger.Info("cleanup: starting pass",
		"retention_days", s.cfg.RetentionDays,
		"cutoff", cutoff.Format(time.RFC3339),
		"batch_size", s.cfg.BatchSize,
	)

	var totalDeleted int64

	// Phase 1: archived functions & apps.
	for _, table := range archivedTables {
		deleted := s.batchDelete(ctx, table,
			"archived_at IS NOT NULL AND archived_at < $1",
			cutoff,
		)
		totalDeleted += deleted
	}

	// Phase 2: time-based retention on event/trace tables.
	for _, t := range timestampTables {
		var deleted int64
		if t.isBigintMs {
			deleted = s.batchDelete(ctx, t.table,
				fmt.Sprintf("%s < $1", t.column),
				cutoffMs,
			)
		} else {
			deleted = s.batchDelete(ctx, t.table,
				fmt.Sprintf("%s < $1", t.column),
				cutoff,
			)
		}
		totalDeleted += deleted
	}

	s.logger.Info("cleanup: pass complete",
		"total_deleted", totalDeleted,
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

// batchDelete loops DELETE ... WHERE ctid IN (SELECT ctid ... LIMIT N) until
// zero rows are affected or the context is cancelled.
//
// Safety: table and whereClause MUST be compile-time constants from the package-
// level archivedTables / timestampTables slices. They are interpolated via
// fmt.Sprintf (not parameterised) because Postgres does not allow parameterised
// identifiers. Never pass user-controlled strings.
func (s *Service) batchDelete(ctx context.Context, table, whereClause string, arg any) int64 {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE ctid IN (SELECT ctid FROM %s WHERE %s LIMIT %d)`,
		table, table, whereClause, s.cfg.BatchSize,
	)

	var totalDeleted int64
	for {
		if ctx.Err() != nil {
			return totalDeleted
		}

		result, err := s.db.ExecContext(ctx, query, arg)
		if err != nil {
			s.logger.Error("cleanup: batch delete failed",
				"table", table,
				"error", err,
			)
			return totalDeleted
		}

		n, _ := result.RowsAffected()
		if n == 0 {
			break
		}
		totalDeleted += n

		// If we deleted a full batch there are likely more rows; continue.
		if n < int64(s.cfg.BatchSize) {
			break
		}
	}

	if totalDeleted > 0 {
		s.logger.Info("cleanup: deleted rows",
			"table", table,
			"count", totalDeleted,
		)
		// Run VACUUM ANALYZE when we cleared at least one full batch.
		if totalDeleted >= int64(s.cfg.BatchSize) {
			s.vacuum(ctx, table)
		}
	}

	return totalDeleted
}

func (s *Service) vacuum(ctx context.Context, table string) {
	// VACUUM cannot run inside a transaction, so use the raw connection.
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("VACUUM ANALYZE %s", table))
	if err != nil {
		s.logger.Warn("cleanup: vacuum failed (non-fatal)",
			"table", table,
			"error", err,
		)
	}
}
