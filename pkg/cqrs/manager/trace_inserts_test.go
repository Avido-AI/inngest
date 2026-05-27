package manager

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsDeadlock(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "non-pg error",
			err:  errors.New("some other error"),
			want: false,
		},
		{
			name: "pg deadlock error",
			err:  &pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
			want: true,
		},
		{
			name: "pg unique violation (not deadlock)",
			err:  &pgconn.PgError{Code: "23505", Message: "unique_violation"},
			want: false,
		},
		{
			name: "wrapped pg deadlock error",
			err:  errors.Join(errors.New("wrapper"), &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDeadlock(tt.err)
			if got != tt.want {
				t.Errorf("isDeadlock() = %v, want %v", got, tt.want)
			}
		})
	}
}
