-- +goose Up
ALTER TABLE functions ADD PRIMARY KEY (id);
CREATE INDEX idx_functions_app_id ON functions(app_id);
CREATE INDEX idx_functions_active ON functions(archived_at) WHERE archived_at IS NULL;
CREATE INDEX idx_functions_slug ON functions(slug) WHERE archived_at IS NULL;

-- +goose Down
ALTER TABLE functions DROP CONSTRAINT functions_pkey;
DROP INDEX IF EXISTS idx_functions_app_id;
DROP INDEX IF EXISTS idx_functions_active;
DROP INDEX IF EXISTS idx_functions_slug;
