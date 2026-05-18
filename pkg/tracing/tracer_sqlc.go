package tracing

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	dbpkg "github.com/inngest/inngest/pkg/db"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing/meta"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	cleanAttrs = false
)

func NewSqlcTracerProvider(q dbpkg.Querier) TracerProvider {
	return NewOtelTracerProvider(&dbExporter{q: q}, 5*time.Second)
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

func (e *dbExporter) parseSpan(ctx context.Context, span sdktrace.ReadOnlySpan) (dbpkg.InsertSpanParams, bool) {
	traceID := span.SpanContext().TraceID().String()
	spanID := span.SpanContext().SpanID().String()
	parentID := span.Parent().SpanID().String()
	isExtensionSpan := span.Name() == meta.SpanNameDynamicExtension
	var envID string
	var accountID string
	var appID string
	var dynamicSpanID string
	var functionID string
	var output any
	var input any
	var runID string
	var debugSessionID string
	var debugRunID string
	var status string
	var eventIdsByt []byte

	attrs := make(map[string]any)
	for _, attr := range span.Attributes() {
		if string(attr.Key) == meta.Attrs.StepOutput.Key() {
			output = attr.Value.AsInterface()
			continue
		}

		if string(attr.Key) == meta.Attrs.EventsInput.Key() || string(attr.Key) == meta.Attrs.StepInput.Key() {
			input = attr.Value.AsInterface()
			continue
		}

		if string(attr.Key) == meta.Attrs.EventIDs.Key() {
			var err error
			if eventIdsByt, err = json.Marshal(attr.Value.AsStringSlice()); err != nil {
				logger.StdlibLogger(ctx).Error("failed to marshal event IDs",
					"span_id", spanID,
					"trace_id", traceID,
					"parent_id", parentID,
					"name", span.Name(),
					"start_time", span.StartTime(),
					"end_time", span.EndTime(),
					"app_id", appID,
					"function_id", functionID,
				)
			}
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.AccountID.Key() {
			accountID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.EnvID.Key() {
			envID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.RunID.Key() {
			runID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.AppID.Key() {
			appID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.FunctionID.Key() {
			functionID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.DynamicTraceID.Key() {
			traceID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.DynamicSpanID.Key() {
			dynamicSpanID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if isExtensionSpan && string(attr.Key) == meta.Attrs.DropSpan.Key() {
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.DebugSessionID.Key() {
			debugSessionID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.DebugRunID.Key() {
			debugRunID = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.DynamicStatus.Key() {
			status = attr.Value.AsString()
			if cleanAttrs {
				continue
			}
		}

		if string(attr.Key) == meta.Attrs.UserlandSpanID.Key() {
			if cleanAttrs {
				continue
			}
		}

		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}

	if runID == "" {
		logger.StdlibLogger(ctx).Error("span missing run ID",
			"span_id", spanID,
			"trace_id", traceID,
			"parent_id", parentID,
			"name", span.Name(),
			"start_time", span.StartTime(),
			"end_time", span.EndTime(),
			"app_id", appID,
			"function_id", functionID,
		)
		return dbpkg.InsertSpanParams{}, false
	}

	attrsByt, err := json.Marshal(attrs)
	if err != nil {
		logger.StdlibLogger(ctx).Error("failed to marshal span attributes",
			"span_id", spanID,
			"trace_id", traceID,
			"parent_id", parentID,
			"name", span.Name(),
			"start_time", span.StartTime(),
			"end_time", span.EndTime(),
			"app_id", appID,
			"function_id", functionID,
			"error", err,
		)
		return dbpkg.InsertSpanParams{}, false
	}

	linksByt, err := json.Marshal(span.Links())
	if err != nil {
		logger.StdlibLogger(ctx).Error("failed to marshal span links",
			"span_id", spanID,
			"trace_id", traceID,
			"parent_id", parentID,
			"name", span.Name(),
			"start_time", span.StartTime(),
			"end_time", span.EndTime(),
			"app_id", appID,
			"function_id", functionID,
			"error", err,
		)
		return dbpkg.InsertSpanParams{}, false
	}

	outputByt := anyToBytes(output)
	inputByt := anyToBytes(input)

	return dbpkg.InsertSpanParams{
		SpanID:       spanID,
		TraceID:      traceID,
		ParentSpanID: sql.NullString{String: parentID, Valid: parentID != ""},
		Name:         span.Name(),
		StartTime:    span.StartTime().Round(0),
		EndTime:      span.EndTime().Round(0),
		RunID:        runID,
		AppID:        appID,
		FunctionID:   functionID,
		Attributes:   attrsByt,
		Links:        linksByt,
		DynamicSpanID: sql.NullString{
			String: dynamicSpanID,
			Valid:  dynamicSpanID != "",
		},
		AccountID: accountID,
		EnvID:     envID,
		Output:    outputByt,
		Input:     inputByt,
		DebugSessionID: sql.NullString{
			String: debugSessionID,
			Valid:  debugSessionID != "",
		},
		DebugRunID: sql.NullString{
			String: debugRunID,
			Valid:  debugRunID != "",
		},
		Status: sql.NullString{
			String: status,
			Valid:  status != "",
		},
		EventIds: eventIdsByt,
	}, true
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
