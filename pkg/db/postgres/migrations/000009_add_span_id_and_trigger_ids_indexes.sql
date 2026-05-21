-- +goose NO TRANSACTION
-- +goose Up

-- GetSpanOutput queries spans by span_id alone, but the PK is (trace_id,
-- span_id) which cannot serve a span_id-only lookup efficiently.
-- 35 calls, 10 s total, 286 ms mean in dev pg_stat_statements.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_spans_span_id
  ON spans (span_id);

-- +goose Down

DROP INDEX CONCURRENTLY IF EXISTS idx_spans_span_id;
