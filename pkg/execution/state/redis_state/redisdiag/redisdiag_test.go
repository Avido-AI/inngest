package redisdiag

import (
	"strings"
	"testing"
)

func TestNormalizeKey(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want string
	}{
		{"plain queue item", "{queue}:queue:item", "{queue}:queue:item"},
		{"ulid collapsed", "{estate}:metadata:01JQ8ZK9X2YV5C7H3M4N6P8R0T", "{estate}:metadata:*"},
		{"lowercase ulid collapsed", "{estate}:metadata:01jq8zk9x2yv5c7h3m4n6p8r0t", "{estate}:metadata:*"},
		{"uuid collapsed", "{cancel}:9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c", "{cancel}:*"},
		{"hex id collapsed", "{queue}:seen:0123456789abcdef0123", "{queue}:seen:*"},
		{"numeric collapsed", "{debounce}:pointer:1700000000000", "{debounce}:pointer:*"},
		{"no id stays intact", "{pauses}:index:global", "{pauses}:index:global"},
		{
			// Run ID embedded in the hash-tag must collapse so all run-state
			// keys aggregate into one bucket instead of one per run.
			name: "hashtag-embedded id collapsed",
			key:  "{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:actions:01KTKR4CE3AXJZWXWTEFY29N7N",
			want: "{estate:*}:actions:*",
		},
		{"hashtag id no trailing segment", "{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:actions", "{estate:*}:actions"},
		{"empty string", "", ""},
		{"single segment", "{queue}", "{queue}"},
		{
			// More than maxPrefixSegments non-ID segments are truncated with "*".
			name: "exceeds max segments",
			key:  "a:b:c:d:e:f:g:h",
			want: "a:b:c:d:e:*",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeKey(tc.key); got != tc.want {
				t.Fatalf("normalizeKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestIsIDLike(t *testing.T) {
	cases := []struct {
		s    string
		want bool
	}{
		{"01JQ8ZK9X2YV5C7H3M4N6P8R0T", true},           // ULID (26 char base32)
		{"01jq8zk9x2yv5c7h3m4n6p8r0t", true},           // lowercase ULID
		{"9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c", true}, // UUID
		{"0123456789abcdef", true},                     // 16-char hex
		{"deadBEEF00112233", true},                     // mixed-case hex >=16
		{"1700000000000", true},                        // long numeric
		{"metadata", false},                            // word
		{"item", false},                                // short word
		{"queue", false},                               // short word
		{"01JQ8ZK9X2YV5C7H3M4N6P8R0", false},           // 25 chars, not ULID len
		{"123", false},                                 // short numeric
		{"abcdef", false},                              // short hex-ish
	}
	for _, tc := range cases {
		if got := isIDLike(tc.s); got != tc.want {
			t.Errorf("isIDLike(%q) = %v, want %v", tc.s, got, tc.want)
		}
	}
}

func TestIsCrockfordBase32(t *testing.T) {
	// Valid, including lowercase (case-insensitive) — "abc" is valid base32.
	for _, good := range []string{
		"0123456789ABCDEFGHJKMNPQRS",
		"abc",
		"01jq8zk9x2yv5c7h3m4n6p8r0t", // lowercase ULID
	} {
		if !isCrockfordBase32(good) {
			t.Errorf("isCrockfordBase32(%q) = false, want true", good)
		}
	}
	// I, L, O, U are excluded (in either case); non-alphanumerics fail.
	for _, bad := range []string{"I", "L", "O", "U", "i", "l", "o", "u", "!@#"} {
		if isCrockfordBase32(bad) {
			t.Errorf("isCrockfordBase32(%q) = true, want false", bad)
		}
	}
}

func TestIsUUID(t *testing.T) {
	if !isUUID("9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c") {
		t.Error("expected valid UUID to pass")
	}
	for _, bad := range []string{
		"9f8a7b6c1d2e3f4a5b6c7d8e9f0a1b2c",     // no dashes
		"9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2",  // too short
		"zf8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c", // non-hex
		"",
	} {
		if isUUID(bad) {
			t.Errorf("isUUID(%q) = true, want false", bad)
		}
	}
}

func TestIsHexAndDigits(t *testing.T) {
	if !isHex("0a1b2c3D4E5F") {
		t.Error("expected hex string to pass isHex")
	}
	if isHex("0a1g") {
		t.Error("expected non-hex to fail isHex")
	}
	if !isDigits("00123") {
		t.Error("expected digit string to pass isDigits")
	}
	if isDigits("12a3") {
		t.Error("expected non-digit to fail isDigits")
	}
}

func TestRound2(t *testing.T) {
	cases := []struct {
		in   float64
		want float64
	}{
		{0, 0},
		{1.234, 1.23},
		{1.235, 1.24},
		{99.999, 100},
		{-1.235, -1.24}, // negative values must round away from zero correctly
		{-0.001, 0},
	}
	for _, tc := range cases {
		if got := round2(tc.in); got != tc.want {
			t.Errorf("round2(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSafeDiv(t *testing.T) {
	if got := safeDiv(10, 2); got != 5 {
		t.Errorf("safeDiv(10,2) = %d, want 5", got)
	}
	if got := safeDiv(10, 0); got != 0 {
		t.Errorf("safeDiv(10,0) = %d, want 0 (no panic)", got)
	}
	if got := safeDiv(0, 5); got != 0 {
		t.Errorf("safeDiv(0,5) = %d, want 0", got)
	}
}

func TestBytesToMB(t *testing.T) {
	cases := []struct {
		in   int64
		want float64
	}{
		{0, 0},
		{1024 * 1024, 1},
		{1024 * 1024 * 3 / 2, 1.5},
		{512 * 1024, 0.5},
	}
	for _, tc := range cases {
		if got := bytesToMB(tc.in); got != tc.want {
			t.Errorf("bytesToMB(%d) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestHashTag(t *testing.T) {
	if tag, ok := hashTag("{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:actions:x"); !ok || tag != "{estate:01KTKR11Q3W56KB7E7T86JYKQQ}" {
		t.Errorf("hashTag sharded = %q,%v", tag, ok)
	}
	if tag, ok := hashTag("{queue}:queue:item"); !ok || tag != "{queue}" {
		t.Errorf("hashTag simple = %q,%v", tag, ok)
	}
	if _, ok := hashTag("noprefix:queue"); ok {
		t.Error("hashTag should fail when key does not start with '{'")
	}
	if _, ok := hashTag("pre{estate}:x"); ok {
		t.Error("hashTag should require '{' at position 0")
	}
}

func TestLastULID(t *testing.T) {
	got, ok := lastULID("{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:actions:9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c:01KTKR4CE3AXJZWXWTEFY29N7N")
	if !ok || got.String() != "01KTKR4CE3AXJZWXWTEFY29N7N" {
		t.Errorf("lastULID = %q,%v want trailing run ULID", got.String(), ok)
	}
	// ULID inside the hash-tag (unsharded-style trailing absent).
	got, ok = lastULID("{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:stack")
	if !ok || got.String() != "01KTKR11Q3W56KB7E7T86JYKQQ" {
		t.Errorf("lastULID hashtag = %q,%v", got.String(), ok)
	}
	if _, ok := lastULID("{queue}:queue:item"); ok {
		t.Error("lastULID should fail when no ULID present")
	}
}

func TestRunMetadataKey(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want string
		ok   bool
	}{
		{
			name: "sharded actions",
			key:  "{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:actions:9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c:01KTKR11Q3W56KB7E7T86JYKQQ",
			want: "{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:metadata:01KTKR11Q3W56KB7E7T86JYKQQ",
			ok:   true,
		},
		{
			name: "unsharded actions",
			key:  "{estate}:actions:9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c:01KTKR11Q3W56KB7E7T86JYKQQ",
			want: "{estate}:metadata:01KTKR11Q3W56KB7E7T86JYKQQ",
			ok:   true,
		},
		{"non-run key", "{queue}:queue:item", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got, ok := runMetadataKey(tc.key)
			if ok != tc.ok || got != tc.want {
				t.Errorf("runMetadataKey(%q) = %q,%v want %q,%v", tc.key, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestFunctionID(t *testing.T) {
	got, ok := functionID("{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:actions:9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c:01KTKR11Q3W56KB7E7T86JYKQQ")
	if !ok || got != "9f8a7b6c-1d2e-3f4a-5b6c-7d8e9f0a1b2c" {
		t.Errorf("functionID = %q,%v want the fnID UUID", got, ok)
	}
	if _, ok := functionID("{estate:01KTKR11Q3W56KB7E7T86JYKQQ}:stack:01KTKR11Q3W56KB7E7T86JYKQQ"); ok {
		t.Error("functionID should be false for keys without a UUID segment")
	}
	if _, ok := functionID("{queue}:queue:item"); ok {
		t.Error("functionID should be false for non-run keys")
	}
}

func TestIsRunStateKey(t *testing.T) {
	for _, k := range []string{"{estate:x}:actions:y", "{estate:x}:metadata:y", "{estate:x}:stack:y"} {
		if !isRunStateKey(k) {
			t.Errorf("isRunStateKey(%q) = false, want true", k)
		}
	}
	if isRunStateKey("{queue}:queue:item") {
		t.Error("isRunStateKey(queue) = true, want false")
	}
}

func TestTerminalRunStatus(t *testing.T) {
	for _, code := range []int{1, 2, 3, 4} { // completed/failed/cancelled/overflowed
		if !terminalRunStatus[code] {
			t.Errorf("status %d should be terminal", code)
		}
	}
	for _, code := range []int{0, 5, 6} { // running/scheduled/unknown
		if terminalRunStatus[code] {
			t.Errorf("status %d should not be terminal", code)
		}
	}
}

// Guard: prefixes produced by normalizeKey must never exceed the segment cap.
func TestNormalizeKeySegmentCap(t *testing.T) {
	got := normalizeKey("a:b:c:d:e:f:g")
	if n := strings.Count(got, ":") + 1; n > maxPrefixSegments+1 {
		t.Fatalf("normalized prefix %q has %d segments, exceeds cap", got, n)
	}
}
