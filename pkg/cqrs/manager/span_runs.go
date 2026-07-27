package manager

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/doug-martin/goqu/v9"
	sqexp "github.com/doug-martin/goqu/v9/exp"
	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/db/driverhelp"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/run"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/oklog/ulid/v2"
)

// GetRuns loads runs from spans.  querying by start time can use executor.run
// spans (which have start embedded).  everything else needs an aggregation...
func (w wrapper) GetRuns(ctx context.Context, opt cqrs.GetTraceRunOpt) ([]*cqrs.TraceRun, error) {
	if err := validateSpanRunFilters(opt); err != nil {
		return nil, err
	}
	if canPushDownRootPage(opt) {
		return w.getSpanRunsPushdown(ctx, opt)
	}
	return w.getSpanRunsFullAggregate(ctx, opt)
}

func validateSpanRunFilters(opt cqrs.GetTraceRunOpt) error {
	if len(opt.Filter.FunctionSlug) > 0 && len(opt.Filter.AppName) == 0 {
		return fmt.Errorf("app name is required when filtering by function slug")
	}
	return nil
}

// Required for ended_at ordering and CEL filters, which depend on data outside
// the root span.

// Required for ended_at ordering and CEL filters, which depend on data outside
// the root span.
func (w wrapper) getSpanRunsFullAggregate(ctx context.Context, opt cqrs.GetTraceRunOpt) ([]*cqrs.TraceRun, error) {
	l := logger.StdlibLogger(ctx)
	h := w.helpers()

	builder := newSpanRunsQueryBuilder(ctx, opt)

	celFilters, useJoin, err := spanRunCELFilters(ctx, opt, h)
	if err != nil {
		return nil, err
	}

	selectCols := []interface{}{
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
			ORDER BY s2.end_time DESC NULLS LAST, s2.span_id DESC LIMIT 1)`).As("status"),
		// Only executor.run rows carry is_deferred (EXTEND rows leave it NULL),
		// so a subselect anchored on that row gives an unambiguous boolean.
		sq.L(`(SELECT s3.is_deferred FROM spans s3
			WHERE s3.run_id = spans.run_id AND s3.name = ?
			ORDER BY s3.start_time ASC LIMIT 1)`, meta.SpanNameRun).As("is_deferred"),
		sq.L(`(SELECT CAST(s4.attributes AS TEXT) FROM spans s4
			WHERE s4.run_id = spans.run_id AND s4.name = ?
			ORDER BY s4.start_time ASC LIMIT 1)`, meta.SpanNameRun).As("attributes"),
		h.EventIDsExpr(), // DB-specific due to storage differences
		"run_apps.name",
		h.RunFunctionSlugExpr().As("function_slug"),
		h.RunFunctionNameExpr().As("function_name"),
	}

	groupByCols := []interface{}{
		"spans.run_id",
		"spans.dynamic_span_id",
		"spans.account_id",
		"spans.env_id",
		"spans.app_id",
		"spans.function_id",
		"spans.trace_id",
		"run_apps.name",
		"run_functions.slug",
		"run_functions.name",
		"run_functions.config",
	}

	// Build ORDER BY for aggregated columns
	var orderExprs []sqexp.OrderedExpression
	for _, o := range opt.Order {
		var aggExpr sqexp.LiteralExpression
		switch o.Field {
		case enums.TraceRunTimeQueuedAt, enums.TraceRunTimeStartedAt:
			aggExpr = sq.L("MIN(spans.start_time)")
		case enums.TraceRunTimeEndedAt:
			aggExpr = sq.L("MAX(spans.end_time)")
		default:
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
	orderExprs = append(orderExprs, sq.C("run_id").Asc())

	q := spanRunsWithMetadata(sq.Dialect(h.GoquDialect()).From("spans"))
	if useJoin {
		// database specific join syntax needed because event_ids is an array of ids to the events table,
		// so we need to unpack that and perform the join before the spans are grouped back together by run_id
		q = h.BuildEventJoin(q)
	}

	allFilters := append(builder.filter, celFilters...)
	allFilters = append(allFilters, spanRunFinalStatusPredicates(opt)...)
	q = q.Select(selectCols...).
		Where(sq.L("spans.dynamic_span_id").In(spanRunRootQuery(opt, h).Select("spans.dynamic_span_id").Where(spanRunRootPredicates(opt, h)...))).
		Where(allFilters...).
		GroupBy(groupByCols...).
		Having(spanRunAggregatePredicates(opt, builder.cursor)...).
		Order(orderExprs...)

	if opt.Items > 0 {
		q = q.Limit(opt.Items + 1) // fetch one more item than requested to determine hasNextPage
	}

	sqlQuery, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	l.Debug("GetRuns query", "sql", sqlQuery, "args", args)

	rows, err := w.adapter.Conn().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Debug("GetRuns query error", "error", err)
		return nil, err
	}
	defer rows.Close()

	runs, err := w.convertSpanRunRows(ctx, rows, builder.cursorLayout, h, opt.Items)
	if err != nil {
		return nil, err
	}
	if err := w.loadSpanRunPageOutput(ctx, runs, opt.IncludeOutput); err != nil {
		return nil, err
	}
	return runs, nil
}

func (w wrapper) getSpanRunsCount(ctx context.Context, opt cqrs.GetTraceRunOpt) (int, error) {
	// Total counts should ignore pagination state.
	opt.Cursor = ""
	opt.Items = 0
	if err := validateSpanRunFilters(opt); err != nil {
		return 0, err
	}

	if canPushDownRootPage(opt) {
		return w.countSpanRunRoots(ctx, opt)
	}
	return w.getSpanRunsCountFullAggregate(ctx, opt)
}

// One executor.run root is one run, so counts can avoid grouping spans.

// One executor.run root is one run, so counts can avoid grouping spans.
func (w wrapper) countSpanRunRoots(ctx context.Context, opt cqrs.GetTraceRunOpt) (int, error) {
	l := logger.StdlibLogger(ctx)
	h := w.helpers()

	preds := spanRunRootPredicates(opt, h)
	preds = append(preds, spanRunFinalStatusPredicates(opt)...)

	q := spanRunRootQuery(opt, h).
		Select(sq.COUNT("*")).
		Where(preds...)
	sqlQuery, args, err := q.ToSQL()
	if err != nil {
		return 0, err
	}

	l.Debug("GetRuns count root query", "sql", sqlQuery, "args", args)

	var count int
	if err := w.adapter.Conn().QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
		l.Debug("GetRuns count root query error", "error", err)
		return 0, err
	}

	return count, nil
}

// getSpanRunsCountFullAggregate is a complex query that has to aggregate spans based
// off of filters to get accurate counts

// getSpanRunsCountFullAggregate is a complex query that has to aggregate spans based
// off of filters to get accurate counts
func (w wrapper) getSpanRunsCountFullAggregate(ctx context.Context, opt cqrs.GetTraceRunOpt) (int, error) {
	l := logger.StdlibLogger(ctx)
	h := w.helpers()

	builder := newSpanRunsQueryBuilder(ctx, opt)
	celFilters, useJoin, err := spanRunCELFilters(ctx, opt, h)
	if err != nil {
		return 0, err
	}

	q := sq.Dialect(h.GoquDialect()).From("spans")
	if useJoin {
		q = h.BuildEventJoin(q)
	}

	groupedFilters := append(builder.filter, celFilters...)
	groupedFilters = append(groupedFilters, spanRunFinalStatusPredicates(opt)...)

	grouped := q.Select(sq.L("1")).
		Where(sq.L("spans.dynamic_span_id").In(
			spanRunRootQuery(opt, h).Select("spans.dynamic_span_id").Where(spanRunRootPredicates(opt, h)...),
		)).
		Where(groupedFilters...).
		GroupBy(spanRunGroupByCols()...).
		Having(spanRunAggregatePredicates(opt, nil)...)

	sqlQuery, args, err := sq.Dialect(h.GoquDialect()).
		From(grouped.As("span_runs")).
		Select(sq.COUNT("*").As("count")).
		ToSQL()
	if err != nil {
		return 0, err
	}

	l.Debug("GetRuns count query", "sql", sqlQuery, "args", args)

	var count int
	if err := w.adapter.Conn().QueryRowContext(ctx, sqlQuery, args...).Scan(&count); err != nil {
		l.Debug("GetRuns count query error", "error", err)
		return 0, err
	}

	return count, nil
}

// getSpanRunsPushdown pages executor spans first so we ignore work outside of the page size

// getSpanRunsPushdown pages executor spans first so we ignore work outside of the page size
func (w wrapper) getSpanRunsPushdown(ctx context.Context, opt cqrs.GetTraceRunOpt) ([]*cqrs.TraceRun, error) {
	l := logger.StdlibLogger(ctx)
	h := w.helpers()

	builder := newSpanRunsQueryBuilder(ctx, opt)

	rootPreds := spanRunRootPredicates(opt, h)
	rootPreds = append(rootPreds, spanRunFinalStatusPredicates(opt)...)

	rootQuery := sq.Dialect(h.GoquDialect()).
		From("spans").
		Select(
			"spans.run_id",
			"spans.dynamic_span_id",
			"spans.account_id",
			"spans.env_id",
			"spans.app_id",
			"spans.function_id",
			"spans.trace_id",
			"spans.start_time",
			"spans.end_time",
			"spans.status",
			"spans.is_deferred",
			sq.L("CAST(attributes AS TEXT)").As("attributes"),
			h.RootEventIDsExpr(),
			"run_apps.name",
			h.RunFunctionSlugExpr().As("function_slug"),
			h.RunFunctionNameExpr().As("function_name"),
		)
	rootQuery = spanRunsWithMetadata(rootQuery).
		Where(rootPreds...).
		Where(builder.cursorPred...).
		Order(spanRunRootOrder(opt)...)
	if opt.Items > 0 {
		rootQuery = rootQuery.Limit(opt.Items + 1)
	}

	sqlQuery, args, err := rootQuery.ToSQL()
	if err != nil {
		return nil, err
	}

	l.Debug("GetRuns root-page query", "sql", sqlQuery, "args", args)

	rows, err := w.adapter.Conn().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Debug("GetRuns root-page query error", "error", err)
		return nil, err
	}

	pageRows := []spanRunRow{}
	for rows.Next() {
		row, err := scanSpanRunRow(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		pageRows = append(pageRows, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	if len(pageRows) == 0 {
		return []*cqrs.TraceRun{}, nil
	}

	if err := w.loadSpanRunPageDetails(ctx, h, pageRows); err != nil {
		return nil, err
	}

	res := make([]*cqrs.TraceRun, 0, len(pageRows))
	var count uint
	for i := range pageRows {
		traceRun, ok := convertSpanRunRow(ctx, pageRows[i], builder.cursorLayout, h)
		if !ok {
			continue
		}
		res = append(res, traceRun)
		count++
		if opt.Items > 0 && count >= opt.Items {
			break
		}
	}
	if err := w.loadSpanRunPageOutput(ctx, res, opt.IncludeOutput); err != nil {
		return nil, err
	}

	return res, nil
}

func (w wrapper) loadSpanRunPageOutput(
	ctx context.Context,
	runs []*cqrs.TraceRun,
	includeOutput bool,
) error {
	if len(runs) == 0 || !includeOutput {
		return nil
	}

	runIDs := make([]string, len(runs))
	byID := make(map[string]*cqrs.TraceRun, len(runs))
	for i, run := range runs {
		runIDs[i] = run.RunID
		byID[run.RunID] = run
	}

	q := sq.Dialect(w.dialect()).
		From("spans").
		Select("spans.run_id", w.helpers().RunOutputExpr()).
		Where(
			sq.C("name").Eq(meta.SpanNameRun),
			sq.C("debug_run_id").IsNull(),
			sq.C("run_id").In(runIDs),
		).
		Order(sq.C("start_time").Asc())

	sqlQuery, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	rows, err := w.adapter.Conn().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var runID string
		var output *string
		err = rows.Scan(&runID, &output)
		if err != nil {
			return err
		}

		run := byID[runID]
		if run == nil {
			continue
		}
		if output != nil && *output != "" {
			run.Output = []byte(*output)
		}
	}
	return rows.Err()
}

// loadSpanRunPageDetails fills in end_time and status for the selected roots.
func (w wrapper) loadSpanRunPageDetails(
	ctx context.Context,
	h driverhelp.DialectHelpers,
	pageRows []spanRunRow,
) error {
	l := logger.StdlibLogger(ctx)

	// Root dynamic_span_id is unique to a run and shared by its EXTEND spans.
	runIDs := make([]string, 0, len(pageRows))
	dynamicSpanIDs := make([]string, 0, len(pageRows))
	for _, p := range pageRows {
		runIDs = append(runIDs, p.RunID)
		dynamicSpanIDs = append(dynamicSpanIDs, p.DynamicSpanID)
	}

	q := sq.Dialect(h.GoquDialect()).
		From("spans").
		Select(
			"spans.run_id",
			"spans.dynamic_span_id",
			sq.L("MAX(spans.end_time)").As("end_time"),
			// Final status ignores the page window, matching the aggregate path.
			sq.L(`(SELECT s2.status FROM spans s2
				WHERE s2.run_id = spans.run_id AND s2.dynamic_span_id = spans.dynamic_span_id
				ORDER BY s2.end_time DESC NULLS LAST, s2.span_id DESC LIMIT 1)`).As("status"),
		).
		Where(
			sq.C("run_id").In(runIDs),
			sq.C("dynamic_span_id").In(dynamicSpanIDs),
		).
		Where(
			sq.C("debug_run_id").IsNull(),
			sq.Or(
				sq.C("status").IsNull(),
				sq.C("status").Neq(enums.RunStatusSkipped.String()),
			),
		).
		GroupBy("spans.run_id", "spans.dynamic_span_id")

	sqlQuery, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	l.Debug("GetRuns enrich query", "sql", sqlQuery, "args", args)

	rows, err := w.adapter.Conn().QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		l.Debug("GetRuns enrich query error", "error", err)
		return err
	}
	defer rows.Close()

	type enriched struct {
		endTime *string
		status  *string
	}
	byKey := make(map[string]enriched, len(pageRows))
	for rows.Next() {
		var runID, dynID string
		var endTime, status *string
		if err := rows.Scan(&runID, &dynID, &endTime, &status); err != nil {
			return err
		}
		byKey[runID+"\x00"+dynID] = enriched{endTime: endTime, status: status}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for i := range pageRows {
		if e, ok := byKey[pageRows[i].RunID+"\x00"+pageRows[i].DynamicSpanID]; ok {
			pageRows[i].EndTime = e.endTime
			pageRows[i].Status = e.status
		}
	}

	return nil
}

func spanRunCELFilters(
	ctx context.Context,
	opt cqrs.GetTraceRunOpt,
	h driverhelp.DialectHelpers,
) ([]sq.Expression, bool, error) {
	var celFilters []sq.Expression
	var useJoin bool

	if opt.Filter.CEL == "" {
		return celFilters, useJoin, nil
	}

	expHandler, err := run.NewExpressionHandler(ctx,
		run.WithExpressionHandlerBlob(opt.Filter.CEL, "\n"),
		run.WithExpressionSQLConverter(h.CELConverter()),
	)
	if err != nil {
		return nil, false, err
	}
	if !expHandler.HasFilters() {
		return celFilters, useJoin, nil
	}

	celFilters, err = expHandler.ToSQLFilters(ctx)
	if err != nil {
		return nil, false, err
	}
	useJoin = needsEventJoin(opt.Filter.CEL)

	return celFilters, useJoin, nil
}

func spanRunRootPredicates(opt cqrs.GetTraceRunOpt, h driverhelp.DialectHelpers) []sq.Expression {
	// the root subquery is bounded by Until (an executor.run always exists,
	// before any EXTEND that adds finalization, etc.)
	//
	// for start_time-based windows we also apply the From rnage so the
	// inner scan stops growing O(total executor.run history) as runs grow...
	// at the cost of excluding runs whose root started before From but
	// was EXTENDed inside the window.  that's fine, given start time shit.
	//
	// Convert times to UTC to match spans storage format in SQLite
	// We currently store SQLite timestamps as Go's time.Time string: "2025-07-13 19:32:24.939517 +0000 UTC m=+..."
	// SQLite compares these as strings, so filter times must also serialize with "+0000 UTC" suffix to correctly use
	// lexicographic comparisons.
	// The UTC conversion was not strictly necessary for Postgres because the timestamp columns are timestamptz, so
	// type and timezone conversion were handled for us
	preds := []sq.Expression{
		sq.I("spans.name").Eq(meta.SpanNameRun),
		sq.I("spans.debug_run_id").IsNull(),
		sq.Or(
			sq.I("spans.status").IsNull(),
			sq.I("spans.status").Neq(enums.RunStatusSkipped.String()),
		),
		sq.I("spans.start_time").Lte(opt.Filter.Until.UTC()),
	}
	if opt.Filter.AccountID != uuid.Nil {
		preds = append(preds, sq.I("spans.account_id").Eq(opt.Filter.AccountID))
	}
	if opt.Filter.WorkspaceID != uuid.Nil {
		preds = append(preds, sq.I("spans.env_id").Eq(opt.Filter.WorkspaceID))
	}
	if len(opt.Filter.AppID) > 0 {
		preds = append(preds, sq.I("spans.app_id").In(opt.Filter.AppID))
	}
	if len(opt.Filter.FunctionID) > 0 {
		preds = append(preds, sq.I("spans.function_id").In(opt.Filter.FunctionID))
	}
	if len(opt.Filter.AppName) > 0 {
		preds = append(preds, sq.I("run_apps.name").In(opt.Filter.AppName))
	}
	if len(opt.Filter.FunctionSlug) > 0 {
		preds = append(preds, h.RunFunctionSlugExpr().In(opt.Filter.FunctionSlug))
	}
	if len(opt.Filter.EventID) > 0 {
		eventIDs := make([]string, len(opt.Filter.EventID))
		for i, id := range opt.Filter.EventID {
			eventIDs[i] = id.String()
		}
		preds = append(preds, h.EventIDsContain(eventIDs))
	}
	if opt.Filter.IsDeferred != nil {
		if *opt.Filter.IsDeferred {
			preds = append(preds, sq.I("spans.is_deferred").IsTrue())
		} else {
			preds = append(preds, sq.I("spans.is_deferred").IsNull())
		}
	}
	if opt.Filter.TimeField != enums.TraceRunTimeEndedAt {
		preds = append(preds, sq.I("spans.start_time").Gte(opt.Filter.From.UTC()))
	}

	return preds
}

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

func spanRunsWithMetadata(q *sq.SelectDataset) *sq.SelectDataset {
	return q.
		LeftJoin(
			sq.T("apps").As("run_apps"),
			sq.On(sq.I("run_apps.id").Eq(sq.I("spans.app_id"))),
		).
		LeftJoin(
			sq.T("functions").As("run_functions"),
			sq.On(sq.I("run_functions.id").Eq(sq.I("spans.function_id"))),
		)
}

func spanRunRootQuery(opt cqrs.GetTraceRunOpt, h driverhelp.DialectHelpers) *sq.SelectDataset {
	q := sq.Dialect(h.GoquDialect()).From("spans")
	if len(opt.Filter.AppName) > 0 {
		return spanRunsWithMetadata(q)
	}
	return q
}

func spanRunAggregatePredicates(opt cqrs.GetTraceRunOpt, cursor *cqrs.TracePageCursor) []sq.Expression {
	fieldName := "start_time"
	aggregate := sq.L("MIN(spans.start_time)")
	preds := []sq.Expression{}
	if opt.Filter.TimeField == enums.TraceRunTimeEndedAt {
		fieldName = "end_time"
		aggregate = sq.L("MAX(spans.end_time)")
		//
		// UTC preserves the SQLite timestamp ordering described in spanRunRootPredicates.
		preds = append(preds,
			aggregate.Gte(opt.Filter.From.UTC()),
			aggregate.Lte(opt.Filter.Until.UTC()),
		)
	}
	if cursor == nil {
		return preds
	}
	fieldCursor := cursor.Find(fieldName)
	if fieldCursor == nil {
		return preds
	}

	cursorTime := time.UnixMicro(fieldCursor.Value).UTC()
	direction := enums.TraceRunOrderDesc
	for _, order := range opt.Order {
		if (fieldName == "end_time" && order.Field == enums.TraceRunTimeEndedAt) ||
			(fieldName == "start_time" && order.Field != enums.TraceRunTimeEndedAt) {
			direction = order.Direction
			break
		}
	}
	var after sq.Expression
	if direction == enums.TraceRunOrderAsc {
		after = aggregate.Gt(cursorTime)
	} else {
		after = aggregate.Lt(cursorTime)
	}
	if cursor.ID != "" {
		after = sq.Or(
			after,
			sq.And(aggregate.Eq(cursorTime), sq.C("run_id").Gt(cursor.ID)),
		)
	}
	return append(preds, after)
}

// ended_at and CEL depend on data outside the root span.

// ended_at and CEL depend on data outside the root span.
func canPushDownRootPage(opt cqrs.GetTraceRunOpt) bool {
	if opt.Filter.CEL != "" {
		return false
	}
	if opt.Filter.TimeField == enums.TraceRunTimeEndedAt {
		return false
	}
	for _, o := range opt.Order {
		switch o.Field {
		case enums.TraceRunTimeQueuedAt, enums.TraceRunTimeStartedAt:
			// ignore and fall through
		default:
			return false
		}
	}
	return true
}

// Status filters use final run status, not any matching span row.

// Status filters use final run status, not any matching span row.
func spanRunFinalStatusPredicates(opt cqrs.GetTraceRunOpt) []sq.Expression {
	if len(opt.Filter.Status) == 0 {
		return nil
	}
	statusStrings := make([]string, 0, len(opt.Filter.Status)*2)
	includeMissing := false
	for _, s := range opt.Filter.Status {
		switch s {
		case enums.RunStatusScheduled:
			statusStrings = append(statusStrings, enums.StepStatusScheduled.String(), enums.StepStatusQueued.String())
		case enums.RunStatusRunning:
			statusStrings = append(statusStrings,
				enums.StepStatusUnknown.String(),
				enums.StepStatusRunning.String(),
				enums.StepStatusWaiting.String(),
				enums.StepStatusSleeping.String(),
				enums.StepStatusInvoking.String(),
			)
			includeMissing = true
		case enums.RunStatusCompleted:
			statusStrings = append(statusStrings, enums.StepStatusCompleted.String())
		case enums.RunStatusFailed, enums.RunStatusOverflowed:
			statusStrings = append(statusStrings, enums.StepStatusFailed.String(), enums.StepStatusErrored.String())
		case enums.RunStatusCancelled:
			statusStrings = append(statusStrings, enums.StepStatusCancelled.String(), enums.StepStatusTimedOut.String())
		case enums.RunStatusSkipped:
			statusStrings = append(statusStrings, enums.StepStatusSkipped.String())
		}
	}
	statusExpr := sq.L(`(SELECT s2.status FROM spans s2
			WHERE s2.run_id = spans.run_id AND s2.dynamic_span_id = spans.dynamic_span_id
			ORDER BY s2.end_time DESC NULLS LAST, s2.span_id DESC LIMIT 1)`)
	predicate := sq.Expression(statusExpr.In(statusStrings))
	if includeMissing {
		predicate = sq.Or(statusExpr.IsNull(), predicate)
	}
	return []sq.Expression{predicate}
}

// queued_at and started_at both map to root start_time.

// queued_at and started_at both map to root start_time.
func spanRunRootOrder(opt cqrs.GetTraceRunOpt) []sqexp.OrderedExpression {
	order := make([]sqexp.OrderedExpression, 0, len(opt.Order)+1)
	for _, o := range opt.Order {
		if o.Direction == enums.TraceRunOrderAsc {
			order = append(order, sq.C("start_time").Asc())
		} else {
			order = append(order, sq.C("start_time").Desc())
		}
	}
	if len(order) == 0 {
		order = append(order, sq.C("start_time").Desc())
	}
	order = append(order, sq.C("run_id").Asc())
	return order
}

type spanRunRow struct {
	RunID         string
	DynamicSpanID string
	AccountID     string
	EnvID         string
	AppID         string
	FunctionID    string
	TraceID       string
	StartTime     string
	EndTime       *string
	Status        *string
	IsDeferred    sql.NullBool
	Attributes    *string
	EventIDs      *string
	AppName       *string
	FunctionSlug  *string
	FunctionName  *string
}

func scanSpanRunRow(rows *sql.Rows) (spanRunRow, error) {
	var row spanRunRow
	err := rows.Scan(
		&row.RunID,
		&row.DynamicSpanID,
		&row.AccountID,
		&row.EnvID,
		&row.AppID,
		&row.FunctionID,
		&row.TraceID,
		&row.StartTime,
		&row.EndTime,
		&row.Status,
		&row.IsDeferred,
		&row.Attributes,
		&row.EventIDs,
		&row.AppName,
		&row.FunctionSlug,
		&row.FunctionName,
	)
	return row, err
}

func convertSpanRunRow(
	ctx context.Context,
	row spanRunRow,
	cursorLayout *cqrs.TracePageCursor,
	h driverhelp.DialectHelpers,
) (*cqrs.TraceRun, bool) {
	l := logger.StdlibLogger(ctx)

	startTime, err := h.ParseTime(row.StartTime)
	if err != nil {
		l.Debug("invalid start_time", "start_time", row.StartTime, "error", err)
		return nil, false
	}
	var endTime *time.Time
	if row.EndTime != nil && *row.EndTime != "" {
		if t, err := h.ParseTime(*row.EndTime); err == nil {
			endTime = &t
		}
	}

	accountUUID, err := uuid.Parse(row.AccountID)
	if err != nil {
		l.Debug("invalid account ID", "account_id", row.AccountID, "error", err)
		return nil, false
	}
	workspaceUUID, err := uuid.Parse(row.EnvID)
	if err != nil {
		l.Debug("invalid workspace ID", "env_id", row.EnvID, "error", err)
		return nil, false
	}
	appUUID, err := uuid.Parse(row.AppID)
	if err != nil {
		l.Debug("invalid app ID", "app_id", row.AppID, "error", err)
		return nil, false
	}
	functionUUID, err := uuid.Parse(row.FunctionID)
	if err != nil {
		l.Debug("invalid function ID", "function_id", row.FunctionID, "error", err)
		return nil, false
	}

	status := enums.RunStatusRunning
	if row.Status != nil && *row.Status != "" {
		if stepStatus, err := enums.StepStatusString(*row.Status); err == nil && stepStatus != enums.StepStatusUnknown {
			status = enums.StepStatusToRunStatus(stepStatus)
		}
	}

	triggerIDs := h.ParseEventIDs(row.EventIDs)

	var duration time.Duration
	if endTime != nil {
		duration = endTime.Sub(startTime)
	}

	var cursor string
	if cursorLayout != nil {
		c := &cqrs.TracePageCursor{
			ID:      row.RunID,
			Cursors: map[string]cqrs.TraceCursor{},
		}
		for field := range cursorLayout.Cursors {
			switch field {
			case "start_time":
				c.Cursors[field] = cqrs.TraceCursor{Field: field, Value: startTime.UnixMicro()}
			case "end_time":
				if endTime != nil {
					c.Cursors[field] = cqrs.TraceCursor{Field: field, Value: endTime.UnixMicro()}
				}
			}
		}
		if encoded, err := c.Encode(); err == nil {
			cursor = encoded
		}
	}

	traceRun := &cqrs.TraceRun{
		AccountID:   accountUUID,
		WorkspaceID: workspaceUUID,
		AppID:       appUUID,
		FunctionID:  functionUUID,
		TraceID:     row.TraceID,
		RunID:       row.RunID,
		QueuedAt:    startTime,
		StartedAt:   startTime,
		Duration:    duration,
		Status:      status,
		IsDeferred:  row.IsDeferred.Valid && row.IsDeferred.Bool,
		Cursor:      cursor,
		TriggerIDs:  triggerIDs,
	}
	if row.AppName != nil {
		traceRun.AppName = *row.AppName
	}
	if row.FunctionSlug != nil {
		traceRun.FunctionSlug = *row.FunctionSlug
	}
	if row.FunctionName != nil {
		traceRun.FunctionName = *row.FunctionName
	}
	attrs := decodeSpanAttributes(row.Attributes)
	if batchID, ok := stringAttribute(attrs, meta.Attrs.BatchID.Key()); ok {
		if parsed, err := ulid.Parse(batchID); err == nil {
			traceRun.BatchID = &parsed
			traceRun.IsBatch = true
		}
	}
	if cron, ok := stringAttribute(attrs, meta.Attrs.CronSchedule.Key()); ok {
		traceRun.CronSchedule = &cron
	}

	if endTime != nil {
		traceRun.EndedAt = *endTime
	}

	return traceRun, true
}

// convertSpanRunRows converts database rows to TraceRun structs

// convertSpanRunRows converts database rows to TraceRun structs
func (w wrapper) convertSpanRunRows(
	ctx context.Context,
	rows *sql.Rows,
	cursorLayout *cqrs.TracePageCursor,
	h driverhelp.DialectHelpers,
	itemLimit uint,
) ([]*cqrs.TraceRun, error) {
	res := []*cqrs.TraceRun{}
	var count uint

	for rows.Next() {
		row, err := scanSpanRunRow(rows)
		if err != nil {
			return nil, err
		}

		traceRun, ok := convertSpanRunRow(ctx, row, cursorLayout, h)
		if !ok {
			continue
		}

		res = append(res, traceRun)
		count++

		// We have filled a page's worth of requests, so break
		if itemLimit > 0 && count >= itemLimit {
			break
		}
	}

	return res, nil
}

// newSpanRunsQueryBuilder creates a query builder for span-based runs Similar
// to newRunsQueryBuilder but adapted for spans table structure

// newSpanRunsQueryBuilder creates a query builder for span-based runs Similar
// to newRunsQueryBuilder but adapted for spans table structure
func newSpanRunsQueryBuilder(ctx context.Context, opt cqrs.GetTraceRunOpt) *runsQueryBuilder {
	l := logger.StdlibLogger(ctx)

	// filters
	filter := []sq.Expression{}
	//
	// debug runs are a special kind of run that should not be included in the main runs list
	filter = append(filter, sq.I("spans.debug_run_id").IsNull())
	if opt.Filter.AccountID != uuid.Nil {
		filter = append(filter, sq.I("spans.account_id").Eq(opt.Filter.AccountID))
	}
	if opt.Filter.WorkspaceID != uuid.Nil {
		filter = append(filter, sq.I("spans.env_id").Eq(opt.Filter.WorkspaceID))
	}
	if len(opt.Filter.AppID) > 0 {
		filter = append(filter, sq.I("spans.app_id").In(opt.Filter.AppID))
	}
	if len(opt.Filter.FunctionID) > 0 {
		filter = append(filter, sq.I("spans.function_id").In(opt.Filter.FunctionID))
	}
	// Status filters use final run status; this only excludes skipped span rows.
	// Skipped runs should only be visible in event-scoped queries, not the runs list.
	// status is nullable in spans, so we must also accept NULL.
	filter = append(filter, sq.Or(
		sq.I("spans.status").IsNull(),
		sq.I("spans.status").Neq(enums.RunStatusSkipped.String()),
	))

	// cursor
	resCursorLayout := &cqrs.TracePageCursor{
		Cursors: map[string]cqrs.TraceCursor{},
	}

	// decode request cursor if there's one
	var reqCursor *cqrs.TracePageCursor
	if len(opt.Cursor) > 0 {
		reqCursor = &cqrs.TracePageCursor{Cursors: map[string]cqrs.TraceCursor{}}
		if err := reqCursor.Decode(opt.Cursor); err != nil {
			l.Debug("cursor decode failed", "error", err)
			reqCursor = nil
		}
	}

	// orders
	order := []sqexp.OrderedExpression{}
	for _, o := range opt.Order {
		// Map enum field names to column names
		var field string
		switch o.Field {
		case enums.TraceRunTimeQueuedAt, enums.TraceRunTimeStartedAt:
			field = "start_time"
		case enums.TraceRunTimeEndedAt:
			field = "end_time"
		default:
			field = "start_time"
		}

		resCursorLayout.Add(field)

		switch o.Direction {
		case enums.TraceRunOrderAsc:
			order = append(order, sq.C(field).Asc())
		case enums.TraceRunOrderDesc:
			order = append(order, sq.C(field).Desc())
		}
	}

	// Always add run_id as final sort field for stable pagination
	order = append(order, sq.C("run_id").Asc())
	resCursorLayout.Add("run_id")

	// cursor-based pagination filter
	var cursorPred []sq.Expression
	if reqCursor != nil {
		cursorFilters := []sq.Expression{}
		for i, o := range opt.Order {
			// Map field names same as above
			var field string
			switch o.Field {
			case enums.TraceRunTimeQueuedAt, enums.TraceRunTimeStartedAt:
				field = "start_time"
			case enums.TraceRunTimeEndedAt:
				field = "end_time"
			default:
				field = "start_time"
			}

			if cursor := reqCursor.Find(field); cursor != nil {
				// Build cursor condition for this field
				// Convert int64 microseconds to time.Time in UTC for spans table comparison
				cursorTime := time.UnixMicro(cursor.Value).UTC()
				var baseCondition sq.Expression
				if o.Direction == enums.TraceRunOrderAsc {
					baseCondition = sq.C(field).Gt(cursorTime)
				} else {
					baseCondition = sq.C(field).Lt(cursorTime)
				}

				// Build compound condition for tie-breaking
				equalityConditions := []sq.Expression{sq.C(field).Eq(cursorTime)}

				// Add conditions for all subsequent fields in sort order
				for j := i + 1; j < len(opt.Order); j++ {
					var nextField string
					switch opt.Order[j].Field {
					case enums.TraceRunTimeQueuedAt, enums.TraceRunTimeStartedAt:
						nextField = "start_time"
					case enums.TraceRunTimeEndedAt:
						nextField = "end_time"
					default:
						nextField = "start_time"
					}

					if nextCursor := reqCursor.Find(nextField); nextCursor != nil {
						nextCursorTime := time.UnixMicro(nextCursor.Value).UTC()
						if opt.Order[j].Direction == enums.TraceRunOrderAsc {
							equalityConditions = append(equalityConditions, sq.C(nextField).Gt(nextCursorTime))
						} else {
							equalityConditions = append(equalityConditions, sq.C(nextField).Lt(nextCursorTime))
						}
					}
				}

				// Add run_id tie-breaker
				if reqCursor.ID != "" {
					equalityConditions = append(equalityConditions, sq.C("run_id").Gt(reqCursor.ID))
				}

				// Combine: (field > cursor_value) OR (field = cursor_value AND next_conditions)
				tieBreakingCondition := sq.And(equalityConditions...)
				cursorFilters = append(cursorFilters, sq.Or(baseCondition, tieBreakingCondition))
			}
		}

		if len(cursorFilters) > 0 {
			cursorPred = []sq.Expression{sq.Or(cursorFilters...)}
		}
	}

	return &runsQueryBuilder{
		filter:       filter,
		order:        order,
		cursor:       reqCursor,
		cursorLayout: resCursorLayout,
		cursorPred:   cursorPred,
	}
}

// needsEventJoin checks if CEL expression references event.* fields

func decodeSpanAttributes(raw *string) map[string]any {
	if raw == nil || *raw == "" {
		return nil
	}

	var decoded any
	if err := json.Unmarshal([]byte(*raw), &decoded); err != nil {
		return nil
	}
	if encoded, ok := decoded.(string); ok {
		if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
			return nil
		}
	}
	attrs, _ := decoded.(map[string]any)
	return attrs
}

func stringAttribute(attrs map[string]any, key string) (string, bool) {
	value, ok := attrs[key].(string)
	return value, ok && value != ""
}

// loadSpanRunPageDetails fills in end_time and status for the selected roots.

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
