// Package dbutil holds small database helpers shared across the SQLite and
// Postgres adapters.
package dbutil

import (
	"io/fs"
	"regexp"
)

var createTableRe = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([a-zA-Z_][a-zA-Z0-9_]*)"?`)

// MigrationTableNames returns the names of every table created by the SQL
// migration files in dir of fsys, plus goose's version table. It is used to
// reset only the tables an application owns, leaving any other tables in a
// shared database untouched.
//
// Names are returned in discovery order with duplicates removed. goose's
// version table is always included so a reset re-runs every migration from
// scratch.
func MigrationTableNames(fsys fs.FS, dir string) ([]string, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	names := []string{}
	add := func(n string) {
		if _, ok := seen[n]; ok {
			return
		}
		seen[n] = struct{}{}
		names = append(names, n)
	}

	add("goose_db_version")
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := fs.ReadFile(fsys, dir+"/"+e.Name())
		if err != nil {
			return nil, err
		}
		for _, m := range createTableRe.FindAllSubmatch(b, -1) {
			add(string(m[1]))
		}
	}
	return names, nil
}
