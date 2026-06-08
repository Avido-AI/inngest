package sqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenReset verifies that Open with Reset drops and recreates Inngest's own
// tables while leaving any other (non-Inngest) tables in the database untouched.
func TestOpenReset(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// First open: migrate a clean DB.
	conn, err := Open(ctx, Options{ForTest: true, Persist: true, Directory: dir})
	require.NoError(t, err)

	// A foreign table that Inngest's migrations never create. It must survive a
	// reset.
	_, err = conn.ExecContext(ctx, `CREATE TABLE foreign_probe (x INTEGER)`)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `INSERT INTO foreign_probe (x) VALUES (1)`)
	require.NoError(t, err)

	// Mark an Inngest-owned table so we can prove it was dropped + recreated by
	// the reset (the sentinel column should be gone afterwards).
	_, err = conn.ExecContext(ctx, `ALTER TABLE apps ADD COLUMN reset_sentinel INTEGER`)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// Re-open the same file with Reset.
	conn2, err := Open(ctx, Options{ForTest: true, Persist: true, Directory: dir, Reset: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn2.Close()) })

	// The foreign table and its data are preserved.
	var foreignRows int
	require.NoError(t, conn2.QueryRowContext(ctx,
		`SELECT count(*) FROM foreign_probe`).Scan(&foreignRows))
	require.Equal(t, 1, foreignRows, "reset must not touch non-Inngest tables")

	// The Inngest table was dropped and recreated by migrations: it exists again
	// but without the sentinel column.
	var sentinel int
	require.NoError(t, conn2.QueryRowContext(ctx,
		`SELECT count(*) FROM pragma_table_info('apps') WHERE name='reset_sentinel'`).Scan(&sentinel))
	require.Equal(t, 0, sentinel, "reset should have dropped and recreated the apps table")

	// Migrations were re-applied from scratch.
	var migrations int
	require.NoError(t, conn2.QueryRowContext(ctx,
		`SELECT count(*) FROM goose_db_version`).Scan(&migrations))
	require.Greater(t, migrations, 0, "migrations should have been re-applied after reset")
}
