package sqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenReset verifies that Open with Reset drops all existing tables and
// re-runs migrations, leaving a clean database.
func TestOpenReset(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// First open: migrate a clean DB, then add a probe table + row that is not
	// part of any migration.
	conn, err := Open(ctx, Options{ForTest: true, Persist: true, Directory: dir})
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TABLE reset_probe (x INTEGER)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `INSERT INTO reset_probe (x) VALUES (1)`)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// Re-open the same file with Reset: the probe table must be gone and
	// migrations must have been re-applied from scratch.
	conn2, err := Open(ctx, Options{ForTest: true, Persist: true, Directory: dir, Reset: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn2.Close()) })

	var probe int
	require.NoError(t, conn2.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='reset_probe'`).Scan(&probe))
	require.Equal(t, 0, probe, "reset should have dropped the probe table")

	var migrations int
	require.NoError(t, conn2.QueryRowContext(ctx,
		`SELECT count(*) FROM goose_db_version`).Scan(&migrations))
	require.Greater(t, migrations, 0, "migrations should have been re-applied after reset")
}
