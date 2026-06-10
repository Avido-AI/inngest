package tracing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// recordingExporter captures the batch boundaries of ExportSpans calls, which
// tracetest.InMemoryExporter does not expose.
type recordingExporter struct {
	mu       sync.Mutex
	batches  [][]sdktrace.ReadOnlySpan
	shutdown bool
}

func (r *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, spans)
	return nil
}

func (r *recordingExporter) Shutdown(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdown = true
	return nil
}

func (r *recordingExporter) snapshot() [][]sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]sdktrace.ReadOnlySpan{}, r.batches...)
}

func stubSpans(n int) []sdktrace.ReadOnlySpan {
	out := make([]sdktrace.ReadOnlySpan, n)
	for i := range out {
		out[i] = tracetest.SpanStub{Name: fmt.Sprintf("span-%d", i)}.Snapshot()
	}
	return out
}

func TestBatchingExporterBuffersUntilFlush(t *testing.T) {
	inner := &recordingExporter{}
	e := newBatchingExporter(inner, time.Hour, 500)
	defer func() { require.NoError(t, e.Shutdown(context.Background())) }()

	for _, s := range stubSpans(3) {
		require.NoError(t, e.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{s}))
	}
	require.Empty(t, inner.snapshot(), "spans below maxBatch must not be exported before a flush")

	require.NoError(t, e.flush(context.Background()))
	batches := inner.snapshot()
	require.Len(t, batches, 1, "buffered spans must flush as a single batch")
	require.Len(t, batches[0], 3)

	require.NoError(t, e.flush(context.Background()))
	require.Len(t, inner.snapshot(), 1, "an empty buffer must not trigger an export")
}

func TestBatchingExporterFlushesInlineAtMaxBatch(t *testing.T) {
	inner := &recordingExporter{}
	e := newBatchingExporter(inner, time.Hour, 2)
	defer func() { require.NoError(t, e.Shutdown(context.Background())) }()

	spans := stubSpans(3)
	require.NoError(t, e.ExportSpans(context.Background(), spans[:1]))
	require.Empty(t, inner.snapshot())

	require.NoError(t, e.ExportSpans(context.Background(), spans[1:]))
	batches := inner.snapshot()
	require.Len(t, batches, 1, "reaching maxBatch must flush inline")
	require.Len(t, batches[0], 3)
}

func TestBatchingExporterIntervalFlush(t *testing.T) {
	inner := &recordingExporter{}
	e := newBatchingExporter(inner, 10*time.Millisecond, 500)
	defer func() { require.NoError(t, e.Shutdown(context.Background())) }()

	require.NoError(t, e.ExportSpans(context.Background(), stubSpans(1)))
	require.Eventually(t, func() bool {
		return len(inner.snapshot()) > 0
	}, time.Second, 5*time.Millisecond, "background loop must flush on the interval")
}

func TestBatchingExporterShutdownDrains(t *testing.T) {
	inner := &recordingExporter{}
	e := newBatchingExporter(inner, time.Hour, 500)

	require.NoError(t, e.ExportSpans(context.Background(), stubSpans(2)))
	require.NoError(t, e.Shutdown(context.Background()))

	batches := inner.snapshot()
	require.Len(t, batches, 1, "Shutdown must drain the buffer")
	require.Len(t, batches[0], 2)
	require.True(t, inner.shutdown, "Shutdown must propagate to the wrapped exporter")

	// A second Shutdown must not panic or block.
	require.NoError(t, e.Shutdown(context.Background()))
}
