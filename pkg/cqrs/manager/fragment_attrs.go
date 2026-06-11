package manager

import (
	"encoding/json"
	"fmt"

	"github.com/inngest/inngest/pkg/tracing/meta"
)

// extractFragmentAttrs extracts the "attributes" field from a span fragment
// as map[string]any. In SQLite the column is TEXT so the value arrives as a
// JSON string; in PostgreSQL it is jsonb so json_build_object embeds it as an
// already-decoded object.
func extractFragmentAttrs(fragment map[string]any) (map[string]any, error) {
	switch v := fragment["attributes"].(type) {
	case string:
		m := map[string]any{}
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			return nil, fmt.Errorf("unmarshal fragment attributes string: %w", err)
		}
		return m, nil
	case map[string]any:
		return v, nil
	case nil:
		return nil, fmt.Errorf("fragment attributes is nil")
	default:
		return nil, fmt.Errorf("unexpected fragment attributes type: %T", v)
	}
}

// overlayFragmentColumnAttrs copies a fragment's dedicated column values into
// its attribute map under the canonical attribute keys, so the typed
// extraction (and the per-fragment last-wins merge) sees them regardless of
// whether the writer also duplicated them into the attributes JSON. Columns
// win over any attrs-JSON copy carried by old rows; both hold the same value
// since they were written from the same span attribute.
func overlayFragmentColumnAttrs(fragment, attrs map[string]any) {
	for col, key := range map[string]string{
		"status":           meta.Attrs.DynamicStatus.Key(),
		"app_id":           meta.Attrs.AppID.Key(),
		"function_id":      meta.Attrs.FunctionID.Key(),
		"debug_run_id":     meta.Attrs.DebugRunID.Key(),
		"debug_session_id": meta.Attrs.DebugSessionID.Key(),
	} {
		if v, ok := fragment[col].(string); ok && v != "" {
			attrs[key] = v
		}
	}

	// event_ids holds a JSON array; sqlite's json_object embeds the TEXT
	// column as a JSON string while postgres' json_build_object embeds the
	// jsonb as an already-decoded array.
	switch v := fragment["event_ids"].(type) {
	case string:
		var ids []any
		if err := json.Unmarshal([]byte(v), &ids); err == nil && len(ids) > 0 {
			attrs[meta.Attrs.EventIDs.Key()] = ids
		}
	case []any:
		if len(v) > 0 {
			attrs[meta.Attrs.EventIDs.Key()] = v
		}
	}
}
