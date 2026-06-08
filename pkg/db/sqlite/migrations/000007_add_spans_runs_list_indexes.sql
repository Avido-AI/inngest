-- +goose Up

-- Mirror of postgres migrations 000002 and 000008: index the spans columns the
-- run list (GetSpanRuns, used when the runs query is sent with preview=true)
-- filters on. Without these the SQLite dev server falls back to full table scans
-- of spans on every runs query, which grows unbounded with run history and makes
-- the runs list take minutes once enough spans accumulate.

-- Inner subquery:
--   SELECT dynamic_span_id FROM spans
--   WHERE name = 'executor.run' AND start_time >= :from AND start_time < :until
-- All three columns let SQLite seek by name, range-scan start_time, and read
-- dynamic_span_id straight from the index instead of scanning every historical
-- executor.run row.
CREATE INDEX IF NOT EXISTS idx_spans_name_start_time_dynamic_span_id ON spans(name, start_time, dynamic_span_id);

-- Outer query: the start_time window scan/sort over the spans table.
CREATE INDEX IF NOT EXISTS idx_spans_start_time ON spans(start_time);

-- +goose Down

DROP INDEX IF EXISTS idx_spans_name_start_time_dynamic_span_id;
DROP INDEX IF EXISTS idx_spans_start_time;
