package manager

import (
	"context"

	sq "github.com/doug-martin/goqu/v9"
	sqexp "github.com/doug-martin/goqu/v9/exp"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/db/driverhelp"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/run"
	"github.com/inngest/inngest/pkg/tracing/meta"
)

// spanRunGroupByCols are the columns that identify a single run when folding the
// executor.run + EXTEND spans back together. Shared by the list and count
// queries so they group identically.
func spanRunGroupByCols() []interface{} {
	return []interface{}{
		"spans.run_id",
		"spans.dynamic_span_id",
		"spans.account_id",
		"spans.env_id",
		"spans.app_id",
		"spans.function_id",
		"spans.trace_id",
	}
}

// GetSpanRuns retrieves a list of span-based runs using the same filtering
// logic as GetTraceRuns but working against the spans table with executor.run +
// EXTEND span grouping.
func (w wrapper) GetSpanRuns(ctx context.Context, opt cqrs.GetTraceRunOpt) ([]*cqrs.TraceRun, error) {
	l := logger.StdlibLogger(ctx)
	h := w.helpers()

	base, builder, err := w.buildSpanRunsBaseQuery(ctx, opt)
	if err != nil {
		return nil, err
	}

	q := base.Select(spanRunSelectCols(h)...).Order(spanRunOrderExprs(opt.Order)...)
	if opt.Items > 0 {
		q = q.Limit(opt.Items + 1) // fetch one more item than requested to determine hasNextPage
	}

	sqlQuery, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	l.Debug("GetSpanRuns query", "sql", sqlQuery, "args", args)

	rows, err := w.adapter.Conn().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Debug("GetSpanRuns query error", "error", err)
		return nil, err
	}
	defer rows.Close()

	return w.convertSpanRunRows(ctx, rows, builder.cursorLayout, h, opt.Items)
}

// spanRunSelectCols are the aggregated columns the run list projects per group.
func spanRunSelectCols(h driverhelp.DialectHelpers) []interface{} {
	return []interface{}{
		"spans.run_id",
		"spans.dynamic_span_id",
		"spans.account_id",
		"spans.env_id",
		"spans.app_id",
		"spans.function_id",
		"spans.trace_id",
		sq.L("MIN(spans.start_time)").As("start_time"),
		sq.L("MAX(spans.end_time)").As("end_time"),
		// subselect for argmax(status, end_time)
		// not the most efficient but it'll do for now
		sq.L(`(SELECT s2.status FROM spans s2
			WHERE s2.run_id = spans.run_id AND s2.dynamic_span_id = spans.dynamic_span_id
			ORDER BY s2.end_time DESC LIMIT 1)`).As("status"),
		// Only executor.run rows carry is_deferred (EXTEND rows leave it NULL),
		// so a subselect anchored on that row gives an unambiguous boolean.
		sq.L(`(SELECT s3.is_deferred FROM spans s3
			WHERE s3.run_id = spans.run_id AND s3.name = ?
			ORDER BY s3.start_time ASC LIMIT 1)`, meta.SpanNameRun).As("is_deferred"),
		h.EventIDsExpr(), // DB-specific due to storage differences
	}
}

// spanRunOrderExprs maps the requested ordering onto the aggregated columns,
// always appending run_id as a stable final sort key.
func spanRunOrderExprs(order []cqrs.GetTraceRunOrder) []sqexp.OrderedExpression {
	var orderExprs []sqexp.OrderedExpression
	for _, o := range order {
		var aggExpr sqexp.LiteralExpression
		switch o.Field {
		case enums.TraceRunTimeEndedAt:
			aggExpr = sq.L("MAX(spans.end_time)")
		default: // queued_at / started_at and unknown fields sort by start_time
			aggExpr = sq.L("MIN(spans.start_time)")
		}
		if o.Direction == enums.TraceRunOrderAsc {
			orderExprs = append(orderExprs, aggExpr.Asc())
		} else {
			orderExprs = append(orderExprs, aggExpr.Desc())
		}
	}
	if len(orderExprs) == 0 {
		orderExprs = append(orderExprs, sq.L("MIN(spans.start_time)").Desc())
	}
	// always add run_id at the end for stable sorting
	return append(orderExprs, sq.C("run_id").Asc())
}

// buildSpanRunsBaseQuery builds the shared FROM/JOIN/WHERE/GROUP BY for the
// span-based run list. GetSpanRuns layers SELECT/ORDER BY/LIMIT on top for the
// list; getSpanRunsCount wraps it in a COUNT(*). Keeping the filtering and
// grouping in one place stops the list and count queries from drifting apart.
func (w wrapper) buildSpanRunsBaseQuery(ctx context.Context, opt cqrs.GetTraceRunOpt) (*sq.SelectDataset, *runsQueryBuilder, error) {
	h := w.helpers()
	builder := newSpanRunsQueryBuilder(ctx, opt)

	celFilters, useJoin, err := spanRunCELFilters(ctx, opt, h)
	if err != nil {
		return nil, nil, err
	}

	q := sq.Dialect(h.GoquDialect()).From("spans")
	if useJoin {
		// database specific join syntax needed because event_ids is an array of ids to the events table,
		// so we need to unpack that and perform the join before the spans are grouped back together by run_id
		q = h.BuildEventJoin(q)
	}

	allFilters := append(builder.filter, celFilters...)
	if deferred := spanRunDeferredFilter(opt, h); deferred != nil {
		allFilters = append(allFilters, deferred)
	}

	q = q.Where(spanRunWindowPredicate(opt, h)).
		Where(allFilters...).
		GroupBy(spanRunGroupByCols()...)

	return q, builder, nil
}

// spanRunCELFilters parses opt's CEL blob into SQL filter expressions and
// reports whether the event-table join is required to evaluate them.
func spanRunCELFilters(ctx context.Context, opt cqrs.GetTraceRunOpt, h driverhelp.DialectHelpers) ([]sq.Expression, bool, error) {
	if opt.Filter.CEL == "" {
		return nil, false, nil
	}

	expHandler, err := run.NewExpressionHandler(ctx,
		run.WithExpressionHandlerBlob(opt.Filter.CEL, "\n"),
		run.WithExpressionSQLConverter(h.CELConverter()),
	)
	if err != nil {
		return nil, false, err
	}
	if !expHandler.HasFilters() {
		return nil, false, nil
	}

	celFilters, err := expHandler.ToSQLFilters(ctx)
	if err != nil {
		return nil, false, err
	}
	return celFilters, needsEventJoin(opt.Filter.CEL), nil
}

// spanRunDeferredFilter returns the run_id membership filter for the isDeferred
// facet, or nil when the facet is unset.
func spanRunDeferredFilter(opt cqrs.GetTraceRunOpt, h driverhelp.DialectHelpers) sq.Expression {
	if opt.Filter.IsDeferred == nil {
		return nil
	}

	// is_deferred uses TRUE/NULL encoding, so a non-deferred filter must check
	// IS NULL. Anchor on the executor.run row because EXTEND rows don't carry
	// is_deferred.
	var deferPred sq.Expression
	if *opt.Filter.IsDeferred {
		deferPred = sq.C("is_deferred").IsTrue()
	} else {
		deferPred = sq.C("is_deferred").IsNull()
	}
	sub := sq.Dialect(h.GoquDialect()).
		From("spans").
		Select(sq.C("run_id")).
		Where(sq.C("name").Eq(meta.SpanNameRun), deferPred)
	return sq.I("spans.run_id").In(sub)
}

// spanRunWindowPredicate restricts the query to runs whose executor.run root
// span falls inside the requested time window.
//
// The inner subquery is bounded by Until (an executor.run always precedes any
// EXTEND sharing its dynamic_span_id). For start_time-based windows we also
// apply the From floor so the inner scan stops growing O(total executor.run
// history) as the system ages — at the cost of excluding runs whose root
// started before From but had EXTEND activity inside the window. For
// end_time-based windows we skip the floor: a run that ended in-window can
// legitimately have started arbitrarily earlier.
func spanRunWindowPredicate(opt cqrs.GetTraceRunOpt, h driverhelp.DialectHelpers) sq.Expression {
	innerPreds := []sq.Expression{
		sq.C("name").Eq(meta.SpanNameRun),
		sq.C("start_time").Lt(opt.Filter.Until.UTC()),
	}
	if opt.Filter.TimeField != enums.TraceRunTimeEndedAt {
		innerPreds = append(innerPreds, sq.C("start_time").Gte(opt.Filter.From.UTC()))
	}
	return sq.L("spans.dynamic_span_id").In(
		sq.Dialect(h.GoquDialect()).Select("dynamic_span_id").From("spans").Where(innerPreds...),
	)
}

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

// getSpanRunsCount counts the distinct span-based runs matching opt. Every
// filter for the span path is expressed in SQL (unlike the trace_runs path,
// which post-filters event/output CEL in Go), so counting the grouped subquery
// is exact — and it rides the same start_time/name indexes as the list query
// instead of materializing every matching row.
func (w wrapper) getSpanRunsCount(ctx context.Context, opt cqrs.GetTraceRunOpt) (int, error) {
	l := logger.StdlibLogger(ctx)
	h := w.helpers()

	base, _, err := w.buildSpanRunsBaseQuery(ctx, opt)
	if err != nil {
		return 0, err
	}

	// base carries the GROUP BY, so each row is one run; wrap it and count rows.
	countQ := sq.Dialect(h.GoquDialect()).
		From(base.Select(sq.L("1")).As("runs")).
		Select(sq.COUNT(sq.Star()))

	sqlQuery, args, err := countQ.ToSQL()
	if err != nil {
		return 0, err
	}

	l.Debug("getSpanRunsCount query", "sql", sqlQuery, "args", args)

	var count int
	if err := w.adapter.Conn().QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
		l.Debug("getSpanRunsCount query error", "error", err)
		return 0, err
	}
	return count, nil
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
