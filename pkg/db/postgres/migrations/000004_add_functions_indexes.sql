-- +goose NO TRANSACTION
-- +goose Up

-- Build the unique index concurrently (no exclusive lock)
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS functions_pkey ON functions(id);
-- Promote to PK (instant; index already exists)
ALTER TABLE functions ADD CONSTRAINT functions_pkey PRIMARY KEY USING INDEX functions_pkey;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_functions_app_id ON functions(app_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_functions_active ON functions(app_id) WHERE archived_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_functions_slug ON functions(slug) WHERE archived_at IS NULL;

-- +goose Down
ALTER TABLE functions DROP CONSTRAINT functions_pkey;
DROP INDEX CONCURRENTLY IF EXISTS idx_functions_app_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_functions_active;
DROP INDEX CONCURRENTLY IF EXISTS idx_functions_slug;
