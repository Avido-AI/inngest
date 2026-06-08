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
		"m/readme":  {Data: []byte("not sql")},
	}

	names, err := MigrationTableNames(fsys, "m")
	require.NoError(t, err)

	require.Contains(t, names, "goose_db_version")
	require.Contains(t, names, "apps")
	require.Contains(t, names, "functions")
	require.Contains(t, names, "events")

	// Duplicates across files collapse to a single entry.
	count := 0
	for _, n := range names {
		if n == "apps" {
			count++
		}
	}
	require.Equal(t, 1, count, "duplicate CREATE TABLE should be deduplicated")
}
