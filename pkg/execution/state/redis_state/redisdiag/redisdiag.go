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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/inngest/inngest/pkg/logger"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
)

const (
	defaultInterval    = 60 * time.Second
	defaultSampleLimit = 1000
	defaultTopN        = 20
	scanCount          = 512 // COUNT hint per SCAN call
	lookupChunk        = 50  // keys per pipelined MEMORY USAGE/TTL batch
	maxPrefixSegments  = 5   // cap normalized prefix depth
	maxClassifyRuns    = 256 // cap distinct runs classified per node per cycle
	sampleRunIDs       = 10  // run IDs to surface per category for spot-checking
)

// Terminal run statuses (enums.RunStatus). A run in any of these has finished;
// its state SHOULD have been deleted by finalize. If such a run is still
// resident in Redis, that is a leak (finalize did not delete it). Kept as
// literals to avoid importing the enums package into this diagnostic tool.
//
//	0 Running  1 Completed  2 Failed  3 Cancelled  4 Overflowed  5 Scheduled  6 Unknown
var terminalRunStatus = map[int]bool{1: true, 2: true, 3: true, 4: true}

// NamedClient pairs a rueidis client with a human label (e.g. "sharded",
// "unsharded", "connect") so the report identifies which keyspace it sampled.
type NamedClient struct {
	Name   string
	Client rueidis.Client
}

// FunctionNamer resolves a function's internal UUID (as a string) to a
// human-readable name. Function names are not stored in Redis, so this is
// backed by the CQRS store (Postgres). Returns ok=false when unresolvable.
type FunctionNamer func(ctx context.Context, fnID string) (name string, ok bool)

// Reporter periodically samples one or more Redis clients and logs a per-prefix
// memory breakdown.
type Reporter struct {
	clients     []NamedClient
	log         logger.Logger
	interval    time.Duration
	sampleLimit int
	topN        int

	resolveFn FunctionNamer
	nameCache map[string]string // fnID -> resolved name (or "" if unresolvable)
}

// Config tunes the reporter. Zero values fall back to sensible defaults.
type Config struct {
	Interval    time.Duration
	SampleLimit int
	TopN        int
	// ResolveFunction optionally maps a function UUID to its name via Postgres.
	// When nil, the breakdown logs UUIDs only.
	ResolveFunction FunctionNamer
}

// New builds a Reporter. Clients with a nil underlying client are skipped.
func New(log logger.Logger, cfg Config, clients ...NamedClient) *Reporter {
	r := &Reporter{
		log:         log.With("svc", "redisdiag"),
		interval:    cfg.Interval,
		sampleLimit: cfg.SampleLimit,
		topN:        cfg.TopN,
		resolveFn:   cfg.ResolveFunction,
		nameCache:   map[string]string{},
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

// Run starts the profiler when enabled. It emits one synchronous snapshot (so
// the breakdown reaches the logs even if a later startup step crashes under
// maxmemory) and then samples on an interval in the background until ctx is
// cancelled. No-op when disabled. The caller supplies the resolved config
// (typically from a CLI flag / env var); zero values in cfg fall back to the
// package defaults in New.
func Run(ctx context.Context, log logger.Logger, enabled bool, cfg Config, clients ...NamedClient) {
	if !enabled {
		return
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
	sample := r.sampleKeyspace(ctx, node)

	// Single denominator for all est_used_mb estimates: total bytes across the
	// whole sample. Both the prefix and function breakdowns must divide by this
	// same base so their estimates are on the same scale and comparable.
	totalBytes := sumSampledBytes(sample.stats)

	r.logSummary(log, mem, dbsize, sample.sampled, sample.complete)
	r.logPrefixBreakdown(log, sample.stats, mem.usedMemory, totalBytes)
	r.logFunctionBreakdown(ctx, log, sample.byFunction, mem.usedMemory, totalBytes)
	r.classifyAndLog(ctx, log, node, sample.runs)
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

// topByBytes sorts stats by sampled bytes desc and invokes emit for the top N,
// passing each entry and its estimated share of used_memory (attributed
// proportionally to its share of totalSampledBytes — the whole-sample total).
// Shared by the prefix and function breakdowns so the sort/top-N/estimation is
// computed identically; only the emitted fields differ.
func (r *Reporter) topByBytes(stats []prefixStat, usedMemory, totalSampledBytes int64, emit func(s prefixStat, estMB float64)) {
	sort.Slice(stats, func(i, j int) bool { return stats[i].sampledBytes > stats[j].sampledBytes })
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
		emit(s, estMB)
	}
}

// logPrefixBreakdown logs the top prefixes by sampled bytes, each with its
// estimated share of used_memory and TTL coverage.
func (r *Reporter) logPrefixBreakdown(log logger.Logger, stats []prefixStat, usedMemory, totalSampledBytes int64) {
	r.topByBytes(stats, usedMemory, totalSampledBytes, func(s prefixStat, estMB float64) {
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
	})
}

// runRef holds the keys needed to classify a run. metaKey is always set;
// pendingKey is set only when the run was seen via an actions key (it carries
// the function UUID needed to build it), and points at the run's pending-ops
// set — a non-empty pending set is the definitive "stuck" signal.
type runRef struct {
	metaKey    string
	pendingKey string
}

// keyspaceSample holds the aggregated result of one keyspace scan.
type keyspaceSample struct {
	stats      []prefixStat         // per normalized-prefix memory
	byFunction []prefixStat         // per function UUID (prefix field = fnID)
	runs       map[ulid.ULID]runRef // runID -> run refs, for classification
	sampled    int64
	complete   bool
}

// sampleKeyspace SCANs up to sampleLimit keys and aggregates MEMORY USAGE + TTL
// per normalized prefix and per function UUID, and collects a deduped set of
// run metadata keys (capped at maxClassifyRuns) for leak-vs-working-set
// classification. complete reports whether the full keyspace was scanned.
func (r *Reporter) sampleKeyspace(ctx context.Context, node rueidis.Client) keyspaceSample {
	agg := map[string]*prefixStat{}
	byFn := map[string]*prefixStat{}
	runs := map[ulid.ULID]runRef{}
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

		r.lookupAndAggregate(ctx, node, keys, agg, byFn, runs, &sampled)

		if cursor == 0 {
			complete = true
			break
		}
	}

	return keyspaceSample{
		stats:      flattenStats(agg),
		byFunction: flattenStats(byFn),
		runs:       runs,
		sampled:    sampled,
		complete:   complete,
	}
}

func flattenStats(m map[string]*prefixStat) []prefixStat {
	out := make([]prefixStat, 0, len(m))
	for _, s := range m {
		out = append(out, *s)
	}
	return out
}

// sumSampledBytes totals sampled bytes across all prefix buckets, i.e. the whole
// sample — the shared denominator for every est_used_mb estimate.
func sumSampledBytes(stats []prefixStat) int64 {
	var t int64
	for _, s := range stats {
		t += s.sampledBytes
	}
	return t
}

// lookupAndAggregate pipelines MEMORY USAGE + TTL for the given keys and folds
// the results into agg (by normalized prefix) and byFn (by function UUID). It
// also records run metadata keys (deduped into runs, capped) for run-state keys.
func (r *Reporter) lookupAndAggregate(ctx context.Context, node rueidis.Client, keys []string, agg, byFn map[string]*prefixStat, runs map[ulid.ULID]runRef, sampled *int64) {
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
			r.recordKey(k, results[i*2], results[i*2+1], agg, byFn, runs, sampled)
		}
	}
}

// recordKey folds one sampled key's MEMORY USAGE + TTL into the per-prefix and
// per-function aggregates and, for run-state keys, the run set. Guard clauses
// keep the happy path flat.
func (r *Reporter) recordKey(k string, memRes, ttlRes rueidis.RedisResult, agg, byFn map[string]*prefixStat, runs map[ulid.ULID]runRef, sampled *int64) {
	usage, err := memRes.AsInt64()
	if err != nil {
		// Key was deleted between SCAN and lookup, or MEMORY USAGE is
		// unsupported. Skip it.
		return
	}
	// -1 no expiry, -2 missing, >=0 has TTL. Default to -1 on error so a failed
	// lookup is not miscounted as "has TTL", which would inflate ttl_pct.
	ttl := int64(-1)
	if v, err := ttlRes.AsInt64(); err == nil {
		ttl = v
	}

	s := upsertStat(agg, normalizeKey(k))
	s.sampledKeys++
	s.sampledBytes += usage
	if ttl >= 0 {
		s.keysWithTTL++
	}
	*sampled++

	// Attribute bytes to the function UUID, when the key carries one (e.g.
	// run-state keys: ...:actions:<fnID>:<runID>).
	if fnID, ok := functionID(k); ok {
		fs := upsertStat(byFn, fnID)
		fs.sampledKeys++
		fs.sampledBytes += usage
	}

	r.recordRunKey(k, runs)
}

// recordRunKey records a run's keys for later classification, deduped by run ID
// and capped at maxClassifyRuns. From an actions key it also derives the
// pending-ops key (same hash-tag/fnID, ":actions:" -> ":pending:"), which is
// what lets classification detect stuck runs. No-op for non-run keys.
func (r *Reporter) recordRunKey(k string, runs map[ulid.ULID]runRef) {
	id, metaKey, ok := runMetadataKey(k)
	if !ok {
		return
	}
	ref, seen := runs[id]
	if !seen {
		if len(runs) >= maxClassifyRuns {
			return
		}
		ref = runRef{metaKey: metaKey}
	}
	if strings.Contains(k, ":actions:") {
		ref.pendingKey = strings.Replace(k, ":actions:", ":pending:", 1)
	}
	runs[id] = ref
}

func upsertStat(m map[string]*prefixStat, key string) *prefixStat {
	s := m[key]
	if s == nil {
		s = &prefixStat{prefix: key}
		m[key] = s
	}
	return s
}

// runClassification aggregates the status + age of sampled runs.
type runClassification struct {
	classified       int
	terminalResident int // finished runs still resident — the leak signal
	statusUnknown    int
	withAge          int
	olderThan1h      int
	sumAgeSec        float64
	maxAgeSec        float64
	byStatus         map[int]int
	withSteps        int
	sumSteps         int
	maxSteps         int

	oldestID       string   // run ID with the greatest age (a problematic job)
	terminalSample []string // sample of terminal-but-resident run IDs (leaked jobs)

	pendingChecked  int      // runs whose pending-ops set we could read
	pendingResident int      // runs with a non-empty pending set — STUCK (primary leak signal)
	pendingSample   []string // sample of stuck run IDs (unresolved pending ops)
}

// classifyAndLog reads the status of each sampled run and decodes its age from
// the run ULID, then logs an aggregate. This distinguishes a leak (runs that
// have FINISHED but whose state is still resident — terminal status, and/or
// old age) from a legitimate working set (runs still Running/Scheduled, young).
func (r *Reporter) classifyAndLog(ctx context.Context, log logger.Logger, node rueidis.Client, runs map[ulid.ULID]runRef) {
	if len(runs) == 0 {
		return
	}

	c := r.classifyRuns(ctx, node, runs)

	avgAge := 0.0
	if c.withAge > 0 {
		avgAge = c.sumAgeSec / float64(c.withAge)
	}
	avgSteps := 0.0
	if c.withSteps > 0 {
		avgSteps = float64(c.sumSteps) / float64(c.withSteps)
	}

	log.Info("redis run-state classification",
		"classified_runs", c.classified,
		"running", c.byStatus[0],
		"scheduled", c.byStatus[5],
		"completed", c.byStatus[1],
		"failed", c.byStatus[2],
		"cancelled", c.byStatus[3],
		"overflowed", c.byStatus[4],
		"status_unknown", c.statusUnknown,
		// terminal_resident > 0 means finished runs are still in Redis — a leak.
		"terminal_resident", c.terminalResident,
		"avg_age_sec", round2(avgAge),
		"max_age_sec", round2(c.maxAgeSec),
		"older_than_1h", c.olderThan1h,
		// step counts: high avg/max => many steps; low => few fat step outputs.
		"avg_steps", round2(avgSteps),
		"max_steps", c.maxSteps,
		// PRIMARY leak signal: runs with a non-empty pending-ops set are stuck
		// mid-step (e.g. an unresolved step.invoke) and will never finalize.
		// pending_resident near pending_checked => the resident set is a leak.
		"pending_checked", c.pendingChecked,
		"pending_resident", c.pendingResident,
		// Concrete jobs to inspect.
		"oldest_run_id", c.oldestID,
		"sample_stuck_run_ids", strings.Join(c.pendingSample, ","),
		"sample_terminal_run_ids", strings.Join(c.terminalSample, ","),
	)
}

// classifyRuns folds each sampled run into a runClassification via two pipelined
// passes: (1) HMGET status+step_count on the metadata (plus age from the ULID),
// and (2) SCARD on the pending-ops set for runs that have one. A non-empty
// pending set means the run is stuck mid-step (e.g. an unresolved step.invoke)
// and will never finalize — the most reliable leak signal, since the metadata
// status field can stay "Scheduled" for the whole run.
func (r *Reporter) classifyRuns(ctx context.Context, node rueidis.Client, runs map[ulid.ULID]runRef) runClassification {
	ids := make([]ulid.ULID, 0, len(runs))
	metaKeys := make([]string, 0, len(runs))
	pendIDs := make([]ulid.ULID, 0, len(runs))
	pendKeys := make([]string, 0, len(runs))
	for id, ref := range runs {
		ids = append(ids, id)
		metaKeys = append(metaKeys, ref.metaKey)
		if ref.pendingKey != "" {
			pendIDs = append(pendIDs, id)
			pendKeys = append(pendKeys, ref.pendingKey)
		}
	}

	c := runClassification{byStatus: map[int]int{}}
	now := time.Now()

	// Pass 1: status + step_count + age.
	for start := 0; start < len(ids); start += lookupChunk {
		end := min(start+lookupChunk, len(ids))
		cmds := make([]rueidis.Completed, 0, end-start)
		for _, mk := range metaKeys[start:end] {
			cmds = append(cmds, node.B().Hmget().Key(mk).Field("status", "step_count").Build())
		}
		for i, res := range node.DoMulti(ctx, cmds...) {
			id := ids[start+i]
			c.recordAge(now, id)
			c.recordMeta(id, res)
		}
	}

	// Pass 2: pending-ops set cardinality (the stuck signal).
	for start := 0; start < len(pendKeys); start += lookupChunk {
		end := min(start+lookupChunk, len(pendKeys))
		cmds := make([]rueidis.Completed, 0, end-start)
		for _, pk := range pendKeys[start:end] {
			cmds = append(cmds, node.B().Scard().Key(pk).Build())
		}
		for i, res := range node.DoMulti(ctx, cmds...) {
			c.recordPending(pendIDs[start+i], res)
		}
	}
	return c
}

// recordPending folds a run's pending-ops set cardinality into the
// classification. A non-empty set means the run is stuck mid-step.
func (c *runClassification) recordPending(id ulid.ULID, res rueidis.RedisResult) {
	n, err := res.AsInt64()
	if err != nil {
		return
	}
	c.pendingChecked++
	if n > 0 {
		c.pendingResident++
		if len(c.pendingSample) < sampleRunIDs {
			c.pendingSample = append(c.pendingSample, id.String())
		}
	}
}

// recordAge folds a run's age (from its ULID timestamp) into the classification.
func (c *runClassification) recordAge(now time.Time, id ulid.ULID) {
	age := now.Sub(ulid.Time(id.Time())).Seconds()
	if age < 0 {
		return
	}
	c.withAge++
	c.sumAgeSec += age
	if age > c.maxAgeSec {
		c.maxAgeSec = age
		c.oldestID = id.String()
	}
	if age > 3600 {
		c.olderThan1h++
	}
}

// recordMeta folds a run's metadata HMGET(status, step_count) result into the
// classification, sampling terminal-but-resident run IDs (leaked jobs).
func (c *runClassification) recordMeta(id ulid.ULID, res rueidis.RedisResult) {
	c.classified++
	arr, err := res.ToArray()
	if err != nil || len(arr) < 1 {
		// Metadata gone (race) or unreadable; can't classify.
		c.statusUnknown++
		return
	}

	statusStr, err := arr[0].ToString()
	if err != nil {
		c.statusUnknown++
		return
	}
	code, err := strconv.Atoi(strings.TrimSuffix(statusStr, ".0"))
	if err != nil {
		c.statusUnknown++
		return
	}
	c.byStatus[code]++
	if terminalRunStatus[code] {
		c.terminalResident++
		if len(c.terminalSample) < sampleRunIDs {
			c.terminalSample = append(c.terminalSample, id.String())
		}
	}

	if len(arr) >= 2 {
		if sc, err := arr[1].ToString(); err == nil {
			if n, err := strconv.Atoi(strings.TrimSuffix(sc, ".0")); err == nil {
				c.withSteps++
				c.sumSteps += n
				if n > c.maxSteps {
					c.maxSteps = n
				}
			}
		}
	}
}

// runMetadataKey returns the run ULID and the run's metadata key for a run-state
// key (one containing :actions:/:metadata:/:stack:), reusing the key's own
// hash-tag so it is correct for both sharded ("{estate:<runID>}") and unsharded
// ("{estate}") layouts. Returns ok=false for non-run keys.
func runMetadataKey(key string) (ulid.ULID, string, bool) {
	if !isRunStateKey(key) {
		return ulid.ULID{}, "", false
	}
	tag, ok := hashTag(key)
	if !ok {
		return ulid.ULID{}, "", false
	}
	id, ok := lastULID(key)
	if !ok {
		return ulid.ULID{}, "", false
	}
	return id, tag + ":metadata:" + id.String(), true
}

// functionID returns the function UUID embedded in a run-state key, e.g. the
// "<fnID>" in "{estate:<runID>}:actions:<fnID>:<runID>". It is the only UUID
// segment in such keys. Returns ok=false for keys without one (metadata, stack,
// queue, etc.). The UUID maps to the function in the Inngest dashboard/registry
// — function names are not stored in Redis.
func functionID(key string) (string, bool) {
	for _, seg := range strings.Split(key, ":") {
		if isUUID(seg) {
			return seg, true
		}
	}
	return "", false
}

// logFunctionBreakdown logs the top functions by sampled bytes, with their
// estimated share of used_memory and, when a resolver is configured, the
// function name looked up from Postgres. It shares topByBytes (and thus the
// same whole-sample denominator) with logPrefixBreakdown, so per-function
// est_used_mb is on the same scale and the two tables are directly comparable.
func (r *Reporter) logFunctionBreakdown(ctx context.Context, log logger.Logger, fns []prefixStat, usedMemory, totalSampledBytes int64) {
	r.topByBytes(fns, usedMemory, totalSampledBytes, func(f prefixStat, estMB float64) {
		log.Info("redis run-state by function",
			"function_id", f.prefix, // UUID; maps to the function in the dashboard
			"function_name", r.functionName(ctx, f.prefix),
			"sampled_keys", f.sampledKeys,
			"sampled_bytes", f.sampledBytes,
			"avg_bytes", safeDiv(f.sampledBytes, f.sampledKeys),
			"est_used_mb", estMB,
		)
	})
}

// functionName resolves a function UUID to its name via the configured Postgres
// resolver, caching results (function names are immutable per ID). Returns ""
// when no resolver is set or the lookup fails.
func (r *Reporter) functionName(ctx context.Context, fnID string) string {
	if r.resolveFn == nil {
		return ""
	}
	if name, ok := r.nameCache[fnID]; ok {
		return name
	}
	name, ok := r.resolveFn(ctx, fnID)
	if !ok {
		name = ""
	}
	r.nameCache[fnID] = name
	return name
}

func isRunStateKey(key string) bool {
	return strings.Contains(key, ":actions:") ||
		strings.Contains(key, ":metadata:") ||
		strings.Contains(key, ":stack:")
}

// hashTag returns the leading "{...}" Redis hash-tag of a key, inclusive of
// braces, or ok=false if absent.
func hashTag(key string) (string, bool) {
	open := strings.IndexByte(key, '{')
	if open != 0 {
		return "", false
	}
	close := strings.IndexByte(key, '}')
	if close < 0 {
		return "", false
	}
	return key[:close+1], true
}

// lastULID returns the last ":"-delimited segment that parses as a ULID, after
// stripping any hash-tag braces. Run-state keys end with ":<runID>".
func lastULID(key string) (ulid.ULID, bool) {
	parts := strings.Split(key, ":")
	for i := len(parts) - 1; i >= 0; i-- {
		seg := strings.TrimRight(strings.TrimLeft(parts[i], "{"), "}")
		if len(seg) != 26 {
			continue
		}
		if id, err := ulid.Parse(strings.ToUpper(seg)); err == nil {
			return id, true
		}
	}
	return ulid.ULID{}, false
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
// grouping, replacing each ID-like component with "*". For example:
//   - "{estate}:metadata:01J..."        -> "{estate}:metadata:*"
//   - "{estate:01J...}:actions:01K..."  -> "{estate:*}:actions:*"
//
// The second form matters because Inngest embeds the run ID inside the Redis
// hash-tag ("{estate:<runID>}"); collapsing it is what lets all of a workload's
// run-state keys aggregate into a single bucket instead of one bucket per run.
// Segments beyond maxPrefixSegments are collapsed to a trailing "*".
func normalizeKey(key string) string {
	parts := strings.Split(key, ":")
	out := make([]string, 0, len(parts))
	for i, p := range parts {
		if i >= maxPrefixSegments {
			out = append(out, "*")
			break
		}
		out = append(out, normalizeSegment(p))
	}
	return strings.Join(out, ":")
}

// normalizeSegment replaces a single ":"-delimited segment with "*" if its core
// is ID-like, preserving any surrounding hash-tag braces. e.g. "01J...}" -> "*}"
// (the closing brace of a "{estate:<id>}" hash-tag), "01J..." -> "*".
func normalizeSegment(seg string) string {
	prefix, suffix, core := "", "", seg
	if strings.HasPrefix(core, "{") {
		prefix, core = "{", core[1:]
	}
	if strings.HasSuffix(core, "}") {
		suffix, core = "}", core[:len(core)-1]
	}
	if isIDLike(core) {
		return prefix + "*" + suffix
	}
	return seg
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
	// ULIDs are case-insensitive; some libraries emit lowercase. Uppercase
	// first so lowercase ULIDs still collapse into their prefix.
	for _, c := range strings.ToUpper(s) {
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
	return math.Round(f*100) / 100
}

func safeDiv(a, b int64) int64 {
	if b == 0 {
		return 0
	}
	return a / b
}
