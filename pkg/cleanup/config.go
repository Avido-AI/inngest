package cleanup

import "time"

const (
	DefaultInterval      = 10 * time.Minute
	DefaultRetentionDays = 7
	DefaultBatchSize     = 5000
)

// Config holds all tuning knobs for the background cleanup loop.
type Config struct {
	// Enabled toggles the cleanup loop. When false the service exits
	// immediately after Pre() without performing any work.
	Enabled bool

	// Interval between successive cleanup passes.
	Interval time.Duration

	// RetentionDays controls how old data must be before deletion.
	RetentionDays int

	// BatchSize limits the number of rows deleted per DELETE statement
	// to keep transactions short and WAL volume bounded.
	BatchSize int
}

// DefaultConfig returns production defaults: enabled, 10 min interval, 7 day
// retention, 5000-row batches.
func DefaultConfig() Config {
	return Config{
		Enabled:       true,
		Interval:      DefaultInterval,
		RetentionDays: DefaultRetentionDays,
		BatchSize:     DefaultBatchSize,
	}
}
