// Package redisdiag provides an opt-in, read-only Redis memory profiler.
//
// It exists for diagnosing Redis OOM / memory-growth incidents on deployments
// where operators cannot connect to Redis directly (e.g. managed Redis with no
// shell/CLI access). Instead of `redis-cli --bigkeys`, it periodically samples
// the keyspace from inside the process and logs a per-key-prefix memory
// attribution to the normal log pipeline.
//
// It is deliberately conservative:
//   - SCAN only (never KEYS); the per-cycle scan is capped to a sample size so
//     cost is bounded regardless of how large the keyspace is.
//   - MEMORY USAGE / TTL lookups are pipelined in chunks.
//   - Only read commands are issued; it never mutates state.
//   - It iterates client.Nodes(), so it is correct for both standalone and
//     cluster clients (each node is reported separately).
//
// Enable it via the dev server wiring (INNGEST_REDIS_DIAG=1). It is off by
// default and should stay off in steady state.
package redisdiag

import (
	"context"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/inngest/inngest/pkg/logger"
	"github.com/redis/rueidis"
)

// Env vars controlling the opt-in profiler. See StartFromEnv.
const (
	envEnabled  = "INNGEST_REDIS_DIAG"
	envInterval = "INNGEST_REDIS_DIAG_INTERVAL"
	envSample   = "INNGEST_REDIS_DIAG_SAMPLE"
)

const (
	defaultInterval    = 60 * time.Second
	defaultSampleLimit = 1000
	defaultTopN        = 20
	scanCount          = 512 // COUNT hint per SCAN call
	lookupChunk        = 50  // keys per pipelined MEMORY USAGE/TTL batch
	maxPrefixSegments  = 5   // cap normalized prefix depth
)

// NamedClient pairs a rueidis client with a human label (e.g. "sharded",
// "unsharded", "connect") so the report identifies which keyspace it sampled.
type NamedClient struct {
	Name   string
	Client rueidis.Client
}

// Reporter periodically samples one or more Redis clients and logs a per-prefix
// memory breakdown.
type Reporter struct {
	clients     []NamedClient
	log         logger.Logger
	interval    time.Duration
	sampleLimit int
	topN        int
}

// Config tunes the reporter. Zero values fall back to sensible defaults.
type Config struct {
	Interval    time.Duration
	SampleLimit int
	TopN        int
}

// New builds a Reporter. Clients with a nil underlying client are skipped.
func New(log logger.Logger, cfg Config, clients ...NamedClient) *Reporter {
	r := &Reporter{
		log:         log.With("svc", "redisdiag"),
		interval:    cfg.Interval,
		sampleLimit: cfg.SampleLimit,
		topN:        cfg.TopN,
	}
	if r.interval <= 0 {
		r.interval = defaultInterval
	}
	if r.sampleLimit <= 0 {
		r.sampleLimit = defaultSampleLimit
	}
	if r.topN <= 0 {
		r.topN = defaultTopN
	}
	for _, c := range clients {
		if c.Client != nil {
			r.clients = append(r.clients, c)
		}
	}
	return r
}

// ReportOnce runs a single synchronous report across all clients. Unlike Start
// it blocks the caller and returns when done, so it is the right call to make
// during startup: it guarantees a Redis snapshot reaches the logs BEFORE any
// later initialization that might crash (e.g. a Redis write that fails under
// maxmemory and aborts boot). A background goroutine with a delayed first tick
// would be killed by such a crash before it ever logs.
func (r *Reporter) ReportOnce(ctx context.Context) {
	if len(r.clients) == 0 {
		return
	}
	r.reportAll(ctx)
}

// StartFromEnv starts the profiler iff INNGEST_REDIS_DIAG is truthy, reading
// tunables (INNGEST_REDIS_DIAG_INTERVAL, INNGEST_REDIS_DIAG_SAMPLE) from the
// environment. It emits one synchronous snapshot (so the breakdown reaches the
// logs even if a later startup step crashes under maxmemory) and then samples
// on an interval in the background until ctx is cancelled. No-op when disabled.
func StartFromEnv(ctx context.Context, log logger.Logger, clients ...NamedClient) {
	if enabled, _ := strconv.ParseBool(os.Getenv(envEnabled)); !enabled {
		return
	}
	cfg := Config{}
	if d, err := time.ParseDuration(os.Getenv(envInterval)); err == nil {
		cfg.Interval = d
	}
	if n, err := strconv.Atoi(os.Getenv(envSample)); err == nil {
		cfg.SampleLimit = n
	}
	r := New(log, cfg, clients...)
	r.ReportOnce(ctx)
	go r.Start(ctx)
}

// Start runs the report loop until ctx is cancelled. It blocks, so callers
// typically run it in its own goroutine. It does not emit an immediate report —
// call ReportOnce first if you want a snapshot at t=0 (recommended at startup).
func (r *Reporter) Start(ctx context.Context) {
	if len(r.clients) == 0 {
		return
	}
	r.log.Info("redis diagnostics enabled",
		"interval", r.interval.String(),
		"sample_limit", r.sampleLimit,
		"clients", len(r.clients),
	)

	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.reportAll(ctx)
		}
	}
}

func (r *Reporter) reportAll(ctx context.Context) {
	for _, nc := range r.clients {
		for addr, node := range nc.Client.Nodes() {
			r.reportNode(ctx, nc.Name, addr, node)
		}
	}
}

type prefixStat struct {
	prefix       string
	sampledKeys  int64
	sampledBytes int64
	keysWithTTL  int64
}

func (r *Reporter) reportNode(ctx context.Context, name, addr string, node rueidis.Client) {
	log := r.log.With("client", name, "node", addr)

	mem := r.memInfo(ctx, node)
	dbsize := r.dbsize(ctx, node)
	stats, sampled, complete := r.sampleKeyspace(ctx, node)

	r.logSummary(log, mem, dbsize, sampled, complete)
	r.logPrefixBreakdown(log, stats, mem.usedMemory)
}

// logSummary emits the node-level memory summary. It is always logged, even
// when the per-prefix sample is empty (fresh / tiny keyspace).
func (r *Reporter) logSummary(log logger.Logger, mem memStats, dbsize, sampled int64, complete bool) {
	usedPct := 0.0
	if mem.maxmemory > 0 {
		usedPct = float64(mem.usedMemory) / float64(mem.maxmemory) * 100
	}
	log.Info("redis memory report",
		"used_memory_mb", bytesToMB(mem.usedMemory),
		"maxmemory_mb", bytesToMB(mem.maxmemory),
		"used_pct", round2(usedPct),
		"rss_mb", bytesToMB(mem.usedMemoryRSS),
		"frag_ratio", mem.fragRatio,
		"dbsize", dbsize,
		"sampled_keys", sampled,
		"sample_complete", complete,
		"maxmemory_policy", mem.policy,
	)
}

// logPrefixBreakdown sorts prefixes by sampled bytes desc and logs the top N
// with an estimated share of used_memory. The estimate attributes used_memory
// proportionally to each prefix's share of sampled bytes — approximate, but
// enough to identify the dominant consumer.
func (r *Reporter) logPrefixBreakdown(log logger.Logger, stats []prefixStat, usedMemory int64) {
	var totalSampledBytes int64
	for _, s := range stats {
		totalSampledBytes += s.sampledBytes
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].sampledBytes > stats[j].sampledBytes
	})
	limit := r.topN
	if len(stats) < limit {
		limit = len(stats)
	}
	for i := 0; i < limit; i++ {
		s := stats[i]
		estMB := 0.0
		if totalSampledBytes > 0 {
			estMB = bytesToMB(int64(float64(usedMemory) * float64(s.sampledBytes) / float64(totalSampledBytes)))
		}
		ttlPct := 0.0
		if s.sampledKeys > 0 {
			ttlPct = float64(s.keysWithTTL) / float64(s.sampledKeys) * 100
		}
		log.Info("redis prefix breakdown",
			"prefix", s.prefix,
			"sampled_keys", s.sampledKeys,
			"sampled_bytes", s.sampledBytes,
			"avg_bytes", safeDiv(s.sampledBytes, s.sampledKeys),
			"est_used_mb", estMB,
			"ttl_pct", round2(ttlPct),
		)
	}
}

// sampleKeyspace SCANs up to sampleLimit keys and aggregates MEMORY USAGE + TTL
// per normalized prefix. It returns the per-prefix stats, the number of keys
// sampled, and whether the full keyspace was scanned (cursor returned to 0).
func (r *Reporter) sampleKeyspace(ctx context.Context, node rueidis.Client) ([]prefixStat, int64, bool) {
	agg := map[string]*prefixStat{}
	var sampled int64
	cursor := uint64(0)
	complete := false

	keys := make([]string, 0, scanCount)
	for sampled < int64(r.sampleLimit) {
		entry, err := node.Do(ctx, node.B().Scan().Cursor(cursor).Count(scanCount).Build()).AsScanEntry()
		if err != nil {
			r.log.Warn("redis diag scan failed", "error", err)
			break
		}
		keys = append(keys[:0], entry.Elements...)
		cursor = entry.Cursor

		r.lookupAndAggregate(ctx, node, keys, agg, &sampled)

		if cursor == 0 {
			complete = true
			break
		}
	}

	out := make([]prefixStat, 0, len(agg))
	for _, s := range agg {
		out = append(out, *s)
	}
	return out, sampled, complete
}

// lookupAndAggregate pipelines MEMORY USAGE + TTL for the given keys and folds
// the results into agg, bucketed by normalized prefix.
func (r *Reporter) lookupAndAggregate(ctx context.Context, node rueidis.Client, keys []string, agg map[string]*prefixStat, sampled *int64) {
	for start := 0; start < len(keys); start += lookupChunk {
		end := start + lookupChunk
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]

		cmds := make([]rueidis.Completed, 0, len(batch)*2)
		for _, k := range batch {
			cmds = append(cmds, node.B().Arbitrary("MEMORY", "USAGE", k).Build())
			cmds = append(cmds, node.B().Ttl().Key(k).Build())
		}
		results := node.DoMulti(ctx, cmds...)

		for i, k := range batch {
			memRes := results[i*2]
			ttlRes := results[i*2+1]

			usage, err := memRes.AsInt64()
			if err != nil {
				// Key may have been deleted between SCAN and lookup, or MEMORY
				// USAGE is unsupported. Skip it.
				continue
			}
			ttl, _ := ttlRes.AsInt64() // -1 no expiry, -2 missing, >=0 has TTL

			prefix := normalizeKey(k)
			s := agg[prefix]
			if s == nil {
				s = &prefixStat{prefix: prefix}
				agg[prefix] = s
			}
			s.sampledKeys++
			s.sampledBytes += usage
			if ttl >= 0 {
				s.keysWithTTL++
			}
			*sampled++
		}
	}
}

type memStats struct {
	usedMemory    int64
	usedMemoryRSS int64
	maxmemory     int64
	fragRatio     string
	policy        string
}

func (r *Reporter) memInfo(ctx context.Context, node rueidis.Client) memStats {
	var m memStats
	s, err := node.Do(ctx, node.B().Arbitrary("INFO", "memory").Build()).ToString()
	if err != nil {
		r.log.Warn("redis diag INFO memory failed", "error", err)
		return m
	}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch k {
		case "used_memory":
			m.usedMemory, _ = strconv.ParseInt(v, 10, 64)
		case "used_memory_rss":
			m.usedMemoryRSS, _ = strconv.ParseInt(v, 10, 64)
		case "maxmemory":
			m.maxmemory, _ = strconv.ParseInt(v, 10, 64)
		case "mem_fragmentation_ratio":
			m.fragRatio = v
		case "maxmemory_policy":
			m.policy = v
		}
	}
	return m
}

func (r *Reporter) dbsize(ctx context.Context, node rueidis.Client) int64 {
	n, err := node.Do(ctx, node.B().Dbsize().Build()).AsInt64()
	if err != nil {
		return -1
	}
	return n
}

// normalizeKey collapses a concrete Redis key into a prefix suitable for
// grouping, e.g. "{estate}:metadata:01J..." -> "{estate}:metadata:*". It keeps
// leading segments until it hits an ID-like token (ULID/UUID/hex/numeric) or
// the segment cap, then appends "*".
func normalizeKey(key string) string {
	parts := strings.Split(key, ":")
	out := make([]string, 0, len(parts))
	for i, p := range parts {
		if i >= maxPrefixSegments {
			out = append(out, "*")
			return strings.Join(out, ":")
		}
		if isIDLike(p) {
			out = append(out, "*")
			return strings.Join(out, ":")
		}
		out = append(out, p)
	}
	return strings.Join(out, ":")
}

// isIDLike reports whether a key segment looks like a generated identifier we
// should collapse: ULID (26-char Crockford base32), UUID, long hex, or a long
// run of digits (timestamps / counters).
func isIDLike(s string) bool {
	switch {
	case len(s) == 26 && isCrockfordBase32(s):
		return true
	case len(s) == 36 && isUUID(s):
		return true
	case len(s) >= 16 && isHex(s):
		return true
	case len(s) >= 10 && isDigits(s):
		return true
	}
	return false
}

func isCrockfordBase32(s string) bool {
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z' && c != 'I' && c != 'L' && c != 'O' && c != 'U':
		default:
			return false
		}
	}
	return true
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !isHexRune(c) {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	for _, c := range s {
		if !isHexRune(c) {
			return false
		}
	}
	return true
}

func isHexRune(c rune) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func bytesToMB(b int64) float64 {
	return round2(float64(b) / (1024 * 1024))
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func safeDiv(a, b int64) int64 {
	if b == 0 {
		return 0
	}
	return a / b
}
