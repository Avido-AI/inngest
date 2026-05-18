package base_cqrs

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/inngest/inngest/pkg/event_trigger_patterns"
	"github.com/inngest/inngest/pkg/inngest"
)

// functionsCache provides a short-TTL in-memory cache for the parsed
// []inngest.Function slice returned by Functions(). This eliminates
// repeated full table scans of the functions table on every incoming event.
type functionsCache struct {
	mu        sync.Mutex
	functions []inngest.Function
	updatedAt time.Time
	ttl       time.Duration
}

func (c *functionsCache) invalidate() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.functions = nil
	c.updatedAt = time.Time{}
	c.mu.Unlock()
}

// invalidateFnCache clears the functions cache after a successful mutation.
func (w wrapper) invalidateFnCache() {
	w.fnCache.invalidate()
}

// Functions returns all functions as inngest functions, using a short-lived
// in-memory cache to avoid repeated full table scans.
func (w wrapper) Functions(ctx context.Context) ([]inngest.Function, error) {
	if w.fnCache != nil && !w.noFnCache {
		w.fnCache.mu.Lock()
		if !w.fnCache.updatedAt.IsZero() && time.Since(w.fnCache.updatedAt) < w.fnCache.ttl {
			result := slices.Clone(w.fnCache.functions)
			w.fnCache.mu.Unlock()
			return result, nil
		}
		w.fnCache.mu.Unlock()
	}

	all, err := w.GetFunctions(ctx)
	if err != nil {
		return nil, err
	}

	funcs := make([]inngest.Function, len(all))
	for n, i := range all {
		f := inngest.Function{}
		_ = json.Unmarshal([]byte(i.Config), &f)
		funcs[n] = f
	}

	if w.fnCache != nil && !w.noFnCache {
		w.fnCache.mu.Lock()
		w.fnCache.functions = funcs
		w.fnCache.updatedAt = time.Now()
		w.fnCache.mu.Unlock()
	}

	return funcs, nil
}

// FunctionsScheduled returns all scheduled functions available.
func (w wrapper) FunctionsScheduled(ctx context.Context) ([]inngest.Function, error) {
	// TODO: Make less naive by storing triggers and caching.
	fns, err := w.Functions(ctx)
	if err != nil {
		return nil, err
	}
	all := []inngest.Function{}
	for _, fn := range fns {
		for _, t := range fn.Triggers {
			if t.CronTrigger != nil {
				all = append(all, fn)
				break
			}
		}
	}
	return all, nil
}

// FunctionsByTrigger returns functions for the given trigger by event name.
func (w wrapper) FunctionsByTrigger(ctx context.Context, eventName string) ([]inngest.Function, error) {
	// TODO: Make less naive by storing triggers and caching.
	fns, err := w.Functions(ctx)
	if err != nil {
		return nil, err
	}
	
	// Generate matching patterns once for efficient trigger matching
	matchingPatterns := event_trigger_patterns.GenerateMatchingPatterns(eventName)
	
	all := []inngest.Function{}
	for _, fn := range fns {
		for _, t := range fn.Triggers {
			if t.EventTrigger != nil && t.EventTrigger.MatchesAnyPattern(matchingPatterns) {
				all = append(all, fn)
				break
			}
		}
	}
	return all, nil
}

