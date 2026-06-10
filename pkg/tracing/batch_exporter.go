package tracing

import (
	"context"
	"errors"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// batchingExporter buffers spans in memory and forwards them to the wrapped
// exporter when the flush interval elapses or the buffer reaches maxBatch,
// whichever comes first.
//
// It exists because getTracer wires exporters through a SimpleSpanProcessor,
// which exports synchronously on every span.End(). Wrapping the db exporter
// here means End() enqueues instead of paying a database round trip per span,
// and the wrapped exporter receives batches sized for its bulk INSERT path.
type batchingExporter struct {
	inner    sdktrace.SpanExporter
	maxBatch int

	mu  sync.Mutex
	buf []sdktrace.ReadOnlySpan

	stopOnce sync.Once
	done     chan struct{}
	stopped  chan struct{}
}

func newBatchingExporter(inner sdktrace.SpanExporter, interval time.Duration, maxBatch int) *batchingExporter {
	e := &batchingExporter{
		inner:    inner,
		maxBatch: maxBatch,
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	go e.run(interval)
	return e
}

// ExportSpans buffers spans, flushing inline only when the buffer reaches
// maxBatch; the inline flush doubles as backpressure on producers. Retaining
// the spans past the call is safe: they are immutable snapshots taken at
// span.End().
func (e *batchingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	var full []sdktrace.ReadOnlySpan
	e.mu.Lock()
	e.buf = append(e.buf, spans...)
	if len(e.buf) >= e.maxBatch {
		full = e.buf
		e.buf = nil
	}
	e.mu.Unlock()
	if full == nil {
		return nil
	}
	return e.inner.ExportSpans(ctx, full)
}

func (e *batchingExporter) run(interval time.Duration) {
	defer close(e.stopped)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// The db exporter logs failures and drops the batch, matching
			// the previous per-span log-and-continue behavior; there is no
			// caller to propagate the error to here.
			_ = e.flush(context.Background())
		case <-e.done:
			return
		}
	}
}

func (e *batchingExporter) flush(ctx context.Context) error {
	e.mu.Lock()
	buf := e.buf
	e.buf = nil
	e.mu.Unlock()
	if len(buf) == 0 {
		return nil
	}
	return e.inner.ExportSpans(ctx, buf)
}

// Shutdown stops the flush loop, drains the buffer, and shuts down the
// wrapped exporter. Per the SpanExporter contract it gives up (leaving the
// flush loop to finish in the background) if ctx expires while an in-flight
// flush delays the loop's exit.
func (e *batchingExporter) Shutdown(ctx context.Context) error {
	e.stopOnce.Do(func() { close(e.done) })
	select {
	case <-e.stopped:
	case <-ctx.Done():
		return ctx.Err()
	}
	return errors.Join(e.flush(ctx), e.inner.Shutdown(ctx))
}
