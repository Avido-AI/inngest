package manager

import (
	"context"

	sq "github.com/doug-martin/goqu/v9"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/run"
)

// GetTraceRunsCount returns the number of runs matching opt.
func (w wrapper) GetTraceRunsCount(ctx context.Context, opt cqrs.GetTraceRunOpt) (int, error) {
	// explicitly set it to zero so it would not attempt to paginate
	opt.Items = 0
	// Clear any pagination cursor so the count reflects the full result set, not
	// just the runs after the current page position — otherwise totalCount would
	// shrink with every page turn.
	opt.Cursor = ""

	// The span path expresses every filter in SQL, so it can count the grouped
	// subquery directly instead of materializing every matching run.
	if opt.Preview {
		return w.getSpanRunsCount(ctx, opt)
	}

	// The non-preview trace_runs path post-filters event-ID and output CEL
	// expressions in Go (see GetTraceRuns), so a SQL COUNT(*) is only exact when
	// neither is present. When they are, fall back to counting the materialized
	// rows so the count stays correct.
	expHandler, err := run.NewExpressionHandler(ctx,
		run.WithExpressionHandlerBlob(opt.Filter.CEL, "\n"),
	)
	if err != nil {
		return 0, err
	}
	if !expHandler.HasEventFilters() && !expHandler.HasOutputFilters() {
		return w.getTraceRunsCountSQL(ctx, opt)
	}

	res, err := w.GetTraceRuns(ctx, opt)
	if err != nil {
		return 0, err
	}
	return len(res), nil
}

// getTraceRunsCountSQL counts trace_runs rows matching opt with a SQL COUNT(*).
// Only safe when the caller has confirmed there are no Go-side post-filters
// (event-ID / output CEL); see GetTraceRunsCount.
func (w wrapper) getTraceRunsCountSQL(ctx context.Context, opt cqrs.GetTraceRunOpt) (int, error) {
	l := logger.StdlibLogger(ctx)
	builder := newRunsQueryBuilder(ctx, opt)
	sqlQuery, args, err := sq.Dialect(w.dialect()).
		From("trace_runs").
		Select(sq.COUNT(sq.Star())).
		Where(builder.filter...).
		ToSQL()
	if err != nil {
		return 0, err
	}

	l.Debug("getTraceRunsCountSQL query", "sql", sqlQuery, "args", args)

	var count int
	if err := w.adapter.Conn().QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
		l.Debug("getTraceRunsCountSQL query error", "error", err)
		return 0, err
	}
	return count, nil
}
