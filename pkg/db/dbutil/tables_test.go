package dbutil

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestMigrationTableNames(t *testing.T) {
	fsys := fstest.MapFS{
		"m/001.sql": {Data: []byte("CREATE TABLE IF NOT EXISTS apps (id int);\nCREATE TABLE functions (id int);")},
		"m/002.sql": {Data: []byte("CREATE  TABLE\n\t\"events\" (id int);\nCREATE TABLE IF NOT EXISTS apps (x int);")},
		// Commented-out CREATE TABLE statements must be ignored.
		"m/003.sql": {Data: []byte("-- CREATE TABLE commented_line (id int);\n/* CREATE TABLE commented_block (id int); */\nCREATE TABLE traces (id int);")},
		// Non-.sql files are skipped entirely, even if they contain DDL-looking text.
		"m/readme":    {Data: []byte("CREATE TABLE not_a_migration (id int);")},
		"m/notes.txt": {Data: []byte("CREATE TABLE also_not (id int);")},
	}

	names, err := MigrationTableNames(fsys, "m")
	require.NoError(t, err)

	require.Contains(t, names, "goose_db_version")
	require.Contains(t, names, "apps")
	require.Contains(t, names, "functions")
	require.Contains(t, names, "events")
	require.Contains(t, names, "traces")

	require.NotContains(t, names, "commented_line", "line-commented CREATE TABLE must be ignored")
	require.NotContains(t, names, "commented_block", "block-commented CREATE TABLE must be ignored")
	require.NotContains(t, names, "not_a_migration", "non-.sql files must be skipped")
	require.NotContains(t, names, "also_not", "non-.sql files must be skipped")

	// Duplicates across files collapse to a single entry.
	count := 0
	for _, n := range names {
		if n == "apps" {
			count++
		}
	}
	require.Equal(t, 1, count, "duplicate CREATE TABLE should be deduplicated")
}
