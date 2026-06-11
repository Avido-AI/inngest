package tracing

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	dbpkg "github.com/inngest/inngest/pkg/db"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	// sqlcFlushInterval bounds how long a finished span can sit in memory
	// before it is persisted (and thus visible to readers).
	sqlcFlushInterval = 50 * time.Millisecond
	// sqlcFlushBatchSize matches the bulk InsertSpans chunk size so a full
	// buffer flushes as a single INSERT statement.
	sqlcFlushBatchSize = 500
)

func NewSqlcTracerProvider(q dbpkg.Querier) TracerProvider {
	// Spans are exported synchronously per span.End(); the batching wrapper
	// coalesces them so the database sees bulk INSERTs every flush interval
	// instead of one single-row INSERT (and one commit) per span. The
	// batchTimeout argument is unused at the OTel layer (getTracer builds a
	// SimpleSpanProcessor), so pass 0: the interval is owned entirely by the
	// batching exporter.
	exp := newBatchingExporter(&dbExporter{q: q}, sqlcFlushInterval, sqlcFlushBatchSize)
	return NewOtelTracerProvider(exp, 0)
}

type dbExporter struct {
	q dbpkg.Querier
}

func (e *dbExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}

	params := make([]dbpkg.InsertSpanParams, 0, len(spans))
	for _, span := range spans {
		p, ok := e.parseSpan(ctx, span)
		if !ok {
			continue
		}
		params = append(params, p)
	}

	if len(params) == 0 {
		return nil
	}

	if err := e.q.InsertSpans(ctx, params); err != nil {
		logger.StdlibLogger(ctx).Error("failed to bulk insert spans into database",
			"count", len(params),
			"error", err,
		)
	}
	return nil
}

// spanFields holds the extracted metadata from a span's attributes.
type spanFields struct {
	traceID        string
	spanID         string
	parentID       string
	envID          string
	accountID      string
	appID          string
	dynamicSpanID  string
	functionID     string
	output         any
	input          any
	runID          string
	debugSessionID string
	debugRunID     string
	status         string
	eventIdsByt    []byte
	isDeferred     bool
	attrs          map[string]any
}

// extractSpanFields iterates over a span's attributes and extracts known
// metadata fields into a spanFields struct. Generic attributes are collected
// into the attrs map.
func extractSpanFields(ctx context.Context, span sdktrace.ReadOnlySpan) spanFields {
	sf := spanFields{
		traceID:  span.SpanContext().TraceID().String(),
		spanID:   span.SpanContext().SpanID().String(),
		parentID: span.Parent().SpanID().String(),
		attrs:    make(map[string]any),
	}
	for _, attr := range span.Attributes() {
		if store := assignSpanAttr(ctx, &sf, attr, span.Name()); store {
			sf.attrs[string(attr.Key)] = attr.Value.AsInterface()
		}
	}
	return sf
}

// assignSpanAttr extracts a known attribute into the spanFields struct and
// returns whether the attribute should also be stored in the generic attrs map.
//
// Keys that return false are persisted only in their dedicated spans column;
// the span fragment queries (GetSpansByRunID and friends) select those columns
// per fragment and overlayFragmentColumnAttrs feeds them back into the typed
// extraction on the read side. Rows written before keys stopped being
// duplicated still carry them in the attributes JSON, which readers tolerate
// (the column overlay wins where both exist, with the identical value).
func assignSpanAttr(ctx context.Context, sf *spanFields, attr attribute.KeyValue, spanName string) bool {
	key := string(attr.Key)
	switch key {
	case meta.Attrs.StepOutput.Key():
		sf.output = attr.Value.AsInterface()
		return false
	case meta.Attrs.EventsInput.Key(), meta.Attrs.StepInput.Key():
		sf.input = attr.Value.AsInterface()
		return false
	case meta.Attrs.EventIDs.Key():
		if byt, err := json.Marshal(attr.Value.AsStringSlice()); err != nil {
			logger.StdlibLogger(ctx).Error("failed to marshal event IDs",
				"span_id", sf.spanID, "trace_id", sf.traceID,
				"name", spanName, "error", err,
			)
		} else {
			sf.eventIdsByt = byt
		}
		return false
	case meta.Attrs.AccountID.Key():
		sf.accountID = attr.Value.AsString()
		return false
	case meta.Attrs.EnvID.Key():
		sf.envID = attr.Value.AsString()
		return false
	case meta.Attrs.RunID.Key():
		sf.runID = attr.Value.AsString()
		return false
	case meta.Attrs.AppID.Key():
		sf.appID = attr.Value.AsString()
		return false
	case meta.Attrs.FunctionID.Key():
		sf.functionID = attr.Value.AsString()
		return false
	case meta.Attrs.DynamicTraceID.Key():
		// Becomes the trace_id column for this row.
		sf.traceID = attr.Value.AsString()
		return false
	case meta.Attrs.DynamicSpanID.Key():
		// Readers key the span tree off the dynamic_span_id column.
		sf.dynamicSpanID = attr.Value.AsString()
		return false
	case meta.Attrs.DebugSessionID.Key():
		sf.debugSessionID = attr.Value.AsString()
		return false
	case meta.Attrs.DebugRunID.Key():
		sf.debugRunID = attr.Value.AsString()
		return false
	case meta.Attrs.DynamicStatus.Key():
		sf.status = attr.Value.AsString()
		return false
	case meta.Attrs.DeferParentRunIDs.Key():
		sf.isDeferred = true
		return true
	default:
		// Includes UserlandSpanID and DropSpan, which have no dedicated
		// column and are only available via the attributes JSON.
		return true
	}
}

// marshalSpanJSON marshals the attributes map and links slice, returning the
// serialised bytes. Returns an error on marshal failure.
func marshalSpanJSON(ctx context.Context, sf spanFields, span sdktrace.ReadOnlySpan) (attrsByt, linksByt []byte, err error) {
	attrsByt, err = json.Marshal(sf.attrs)
	if err != nil {
		logger.StdlibLogger(ctx).Error("failed to marshal span attributes",
			"span_id", sf.spanID, "trace_id", sf.traceID,
			"name", span.Name(), "error", err,
		)
		return nil, nil, err
	}

	linksByt, err = json.Marshal(span.Links())
	if err != nil {
		logger.StdlibLogger(ctx).Error("failed to marshal span links",
			"span_id", sf.spanID, "trace_id", sf.traceID,
			"name", span.Name(), "error", err,
		)
		return nil, nil, err
	}
	return attrsByt, linksByt, nil
}

// buildInsertSpanParams constructs the DB insert params from the extracted
// fields and serialised JSON payloads.
func buildInsertSpanParams(sf spanFields, span sdktrace.ReadOnlySpan, attrsByt, linksByt []byte) dbpkg.InsertSpanParams {
	return dbpkg.InsertSpanParams{
		SpanID:       sf.spanID,
		TraceID:      sf.traceID,
		ParentSpanID: sql.NullString{String: sf.parentID, Valid: sf.parentID != ""},
		Name:         span.Name(),
		StartTime:    span.StartTime().Round(0),
		EndTime:      span.EndTime().Round(0),
		RunID:        sf.runID,
		AppID:        sf.appID,
		FunctionID:   sf.functionID,
		Attributes:   attrsByt,
		Links:        linksByt,
		DynamicSpanID: sql.NullString{
			String: sf.dynamicSpanID,
			Valid:  sf.dynamicSpanID != "",
		},
		AccountID: sf.accountID,
		EnvID:     sf.envID,
		Output:    anyToBytes(sf.output),
		Input:     anyToBytes(sf.input),
		DebugSessionID: sql.NullString{
			String: sf.debugSessionID,
			Valid:  sf.debugSessionID != "",
		},
		DebugRunID: sql.NullString{
			String: sf.debugRunID,
			Valid:  sf.debugRunID != "",
		},
		Status: sql.NullString{
			String: sf.status,
			Valid:  sf.status != "",
		},
		EventIds:   sf.eventIdsByt,
		IsDeferred: sql.NullBool{Bool: sf.isDeferred, Valid: sf.isDeferred},
	}
}

func (e *dbExporter) parseSpan(ctx context.Context, span sdktrace.ReadOnlySpan) (dbpkg.InsertSpanParams, bool) {
	sf := extractSpanFields(ctx, span)

	if sf.runID == "" {
		logger.StdlibLogger(ctx).Error("span missing run ID",
			"span_id", sf.spanID, "trace_id", sf.traceID,
			"name", span.Name(),
		)
		return dbpkg.InsertSpanParams{}, false
	}

	attrsByt, linksByt, err := marshalSpanJSON(ctx, sf, span)
	if err != nil {
		return dbpkg.InsertSpanParams{}, false
	}

	return buildInsertSpanParams(sf, span, attrsByt, linksByt), true
}

func (e *dbExporter) Shutdown(context.Context) error { return nil }

// anyToBytes converts a value to []byte for storage in a JSON column.
// Strings and byte slices are used directly to avoid double-encoding;
// other types are JSON-marshaled.
func anyToBytes(v any) []byte {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return nil
		}
		return []byte(val)
	case []byte:
		if len(val) == 0 {
			return nil
		}
		return val
	default:
		byt, _ := json.Marshal(val)
		return byt
	}
}
