package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core"
)

func TestRenderReport_AllReady(t *testing.T) {
	var sb strings.Builder
	ready := renderReport(&sb, []core.ResourceReadiness{
		{
			Resource:     "m",
			Alias:        "m_search",
			CurrentIndex: "m_search_v1",
			TargetIndex:  "m_search_v2",
			Move:         core.AliasForward,
			Ready:        true,
			Checks: []core.ReadinessCheck{
				{Name: core.CheckAliasState, OK: true, Detail: "forward cutover: m_search_v1 -> m_search_v2"},
				{Name: core.CheckTargetIndex, OK: true, Detail: "target index m_search_v2 exists"},
			},
		},
	})

	require.True(t, ready)
	out := sb.String()
	require.Contains(t, out, "m_search_v1 -> m_search_v2")
	require.Contains(t, out, "READY")
	require.Contains(t, out, core.CheckTargetIndex)
	require.Contains(t, out, "target index m_search_v2 exists")
}

func TestRenderReport_FailedCheckMakesNotReady(t *testing.T) {
	var sb strings.Builder
	ready := renderReport(&sb, []core.ResourceReadiness{
		{
			Resource:    "m",
			Alias:       "m_search",
			TargetIndex: "m_search_v2",
			Move:        core.AliasCreate,
			Ready:       true,
		},
		{
			Resource:     "n",
			Alias:        "n_search",
			CurrentIndex: "n_search_v1",
			TargetIndex:  "n_search_v2",
			Move:         core.AliasForward,
			Ready:        false,
			Checks: []core.ReadinessCheck{
				{Name: core.CheckDocParity, OK: false, Detail: "n_search_v1 has 10 docs, n_search_v2 has 4"},
			},
		},
	})

	require.False(t, ready)
	out := sb.String()
	require.Contains(t, out, "NOT READY")
	require.Contains(t, out, "n_search_v1 has 10 docs, n_search_v2 has 4")
	// The verdict must be visually distinct per check.
	require.Contains(t, out, "FAIL")
}
