package main

import "testing"

func TestClassifyIndex(t *testing.T) {
	active := map[string]bool{"m_search_v2": true, "m_search_v3": true}

	cases := []struct {
		name        string
		index       string
		aliasTarget string
		want        cleanupAction
	}{
		{"active version is kept", "m_search_v2", "m_search_v2", keepActive},
		{"de-configured version is removed", "m_search_v1", "m_search_v2", removeIndex},
		{"de-configured alias target is refused", "m_search_v1", "m_search_v1", refuseAliasTarget},
		{"no alias at all still removes", "m_search_v1", "", removeIndex},
		// An active index that is also the alias target must classify as
		// active, not as a refusal — it is simply not a cleanup candidate.
		{"active alias target is kept", "m_search_v3", "m_search_v3", keepActive},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyIndex(tc.index, active, tc.aliasTarget); got != tc.want {
				t.Fatalf("classifyIndex(%q) = %v, want %v", tc.index, got, tc.want)
			}
		})
	}
}

func TestClassifyIndex_DroppedTypeHasNoActiveVersions(t *testing.T) {
	// A type removed from config entirely (the sweep leaves its documents to
	// cleanup) has an empty active set: every remaining index is a candidate,
	// but a still-standing alias target is refused.
	if got := classifyIndex("ghost_search_v1", map[string]bool{}, ""); got != removeIndex {
		t.Fatalf("got %v, want removeIndex", got)
	}
	if got := classifyIndex("ghost_search_v1", map[string]bool{}, "ghost_search_v1"); got != refuseAliasTarget {
		t.Fatalf("got %v, want refuseAliasTarget", got)
	}
}
