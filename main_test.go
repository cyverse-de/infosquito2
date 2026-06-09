package main

import (
	"fmt"
	"strings"
	"testing"
)

// --- generatePrefixes ---

func TestGeneratePrefixes(t *testing.T) {
	cases := []struct {
		length        int
		wantCount     int
		wantFirst     string
		wantLast      string
		wantContains  []string
	}{
		{
			length:    1,
			wantCount: 16,
			wantFirst: "0",
			wantLast:  "f",
		},
		{
			length:    2,
			wantCount: 256,
			wantFirst: "00",
			wantLast:  "ff",
			wantContains: []string{"0a", "ab", "f0"},
		},
		{
			length:    3,
			wantCount: 4096,
			wantFirst: "000",
			wantLast:  "fff",
			wantContains: []string{"000", "abc", "fff"},
		},
		{
			length:    4,
			wantCount: 65536,
			wantFirst: "0000",
			wantLast:  "ffff",
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("length=%d", c.length), func(t *testing.T) {
			got := generatePrefixes(c.length)

			if len(got) != c.wantCount {
				t.Errorf("len(generatePrefixes(%d)) = %d, want %d", c.length, len(got), c.wantCount)
			}

			if len(got) == 0 {
				return
			}

			if got[0] != c.wantFirst {
				t.Errorf("first prefix = %q, want %q", got[0], c.wantFirst)
			}
			if got[len(got)-1] != c.wantLast {
				t.Errorf("last prefix = %q, want %q", got[len(got)-1], c.wantLast)
			}

			// All prefixes must have the right length and be lowercase hex.
			for _, p := range got {
				if len(p) != c.length {
					t.Errorf("prefix %q has length %d, want %d", p, len(p), c.length)
				}
				if strings.ToLower(p) != p {
					t.Errorf("prefix %q is not lowercase", p)
				}
			}

			// Spot-check expected values.
			set := make(map[string]bool, len(got))
			for _, p := range got {
				set[p] = true
			}
			for _, want := range c.wantContains {
				if !set[want] {
					t.Errorf("generatePrefixes(%d) missing expected prefix %q", c.length, want)
				}
			}

			// No duplicates.
			if len(set) != len(got) {
				t.Errorf("generatePrefixes(%d) contains duplicates (%d unique out of %d)", c.length, len(set), len(got))
			}
		})
	}
}

// --- splitPrefix ---

func TestSplitPrefix(t *testing.T) {
	cases := []struct {
		name         string
		prefix       string
		wantCount    int
		wantStarts   string // all results must start with this
		wantContains []string
	}{
		{
			name:         "empty-prefix",
			prefix:       "",
			wantCount:    16,
			wantStarts:   "",
			wantContains: []string{"0", "9", "a", "f"},
		},
		{
			name:         "single-char-prefix",
			prefix:       "a",
			wantCount:    16,
			wantStarts:   "a",
			wantContains: []string{"a0", "a9", "aa", "af"},
		},
		{
			name:         "multi-char-prefix",
			prefix:       "ab3",
			wantCount:    16,
			wantStarts:   "ab3",
			wantContains: []string{"ab30", "ab3a", "ab3f"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := splitPrefix(c.prefix)

			if len(got) != c.wantCount {
				t.Errorf("len(splitPrefix(%q)) = %d, want %d", c.prefix, len(got), c.wantCount)
			}

			for _, p := range got {
				if !strings.HasPrefix(p, c.wantStarts) {
					t.Errorf("splitPrefix(%q) result %q does not start with %q", c.prefix, p, c.wantStarts)
				}
				if len(p) != len(c.prefix)+1 {
					t.Errorf("splitPrefix(%q) result %q has length %d, want %d", c.prefix, p, len(p), len(c.prefix)+1)
				}
			}

			set := make(map[string]bool, len(got))
			for _, p := range got {
				set[p] = true
			}

			for _, want := range c.wantContains {
				if !set[want] {
					t.Errorf("splitPrefix(%q) missing expected child %q", c.prefix, want)
				}
			}

			// No duplicates.
			if len(set) != len(got) {
				t.Errorf("splitPrefix(%q) contains duplicates", c.prefix)
			}
		})
	}
}
