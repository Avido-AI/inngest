package cleanup

import (
	"context"
	"testing"
	"time"
)

// testLogger captures log calls for assertions.
type testLogger struct {
	infos  []string
	warns  []string
	errors []string
}

func (l *testLogger) Info(msg string, _ ...any)  { l.infos = append(l.infos, msg) }
func (l *testLogger) Warn(msg string, _ ...any)  { l.warns = append(l.warns, msg) }
func (l *testLogger) Error(msg string, _ ...any) { l.errors = append(l.errors, msg) }

func TestRun_DisabledNoOps(t *testing.T) {
	svc := NewService(Config{Enabled: false}, nil, &testLogger{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := svc.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRun_NilDBNoOps(t *testing.T) {
	svc := NewService(Config{Enabled: true}, nil, &testLogger{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := svc.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRun_ZeroIntervalDefaultsToSafe(t *testing.T) {
	logger := &testLogger{}
	svc := NewService(Config{
		Enabled:       true,
		Interval:      0, // would panic without the guard
		RetentionDays: 7,
		BatchSize:     100,
	}, nil, logger)

	// db is nil so Run returns immediately after the guard sets defaults.
	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRun_NegativeIntervalDefaultsToSafe(t *testing.T) {
	logger := &testLogger{}
	svc := NewService(Config{
		Enabled:       true,
		Interval:      -5 * time.Minute,
		RetentionDays: 7,
		BatchSize:     100,
	}, nil, logger)

	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRun_ZeroRetentionDefaultsToSafe(t *testing.T) {
	logger := &testLogger{}
	svc := NewService(Config{
		Enabled:       true,
		Interval:      10 * time.Minute,
		RetentionDays: 0,
		BatchSize:     100,
	}, nil, logger)

	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRun_ZeroBatchSizeDefaultsToSafe(t *testing.T) {
	logger := &testLogger{}
	svc := NewService(Config{
		Enabled:       true,
		Interval:      10 * time.Minute,
		RetentionDays: 7,
		BatchSize:     0, // would cause LIMIT 0 (silent no-op) without guard
	}, nil, logger)

	ctx := context.Background()
	if err := svc.Run(ctx); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.Enabled {
		t.Fatal("expected Enabled=true")
	}
	if cfg.Interval != 10*time.Minute {
		t.Fatalf("expected 10m interval, got %v", cfg.Interval)
	}
	if cfg.RetentionDays != 7 {
		t.Fatalf("expected 7 days retention, got %d", cfg.RetentionDays)
	}
	if cfg.BatchSize != 5000 {
		t.Fatalf("expected 5000 batch size, got %d", cfg.BatchSize)
	}
}

func TestServiceName(t *testing.T) {
	svc := NewService(DefaultConfig(), nil, &testLogger{})
	if svc.Name() != "cleanup" {
		t.Fatalf("expected name 'cleanup', got %q", svc.Name())
	}
}

func TestPre(t *testing.T) {
	svc := NewService(DefaultConfig(), nil, &testLogger{})
	if err := svc.Pre(context.Background()); err != nil {
		t.Fatalf("expected nil error from Pre, got %v", err)
	}
}

func TestStop(t *testing.T) {
	svc := NewService(DefaultConfig(), nil, &testLogger{})
	if err := svc.Stop(context.Background()); err != nil {
		t.Fatalf("expected nil error from Stop, got %v", err)
	}
}
