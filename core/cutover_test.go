package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeCutoverES is an in-memory CutoverBackend.
type fakeCutoverES struct {
	aliases    map[string]string // alias -> current index
	aliasErr   map[string]error  // alias -> injected GetAlias error
	indices    map[string]bool   // index -> exists
	existsErr  map[string]error  // index -> injected IndexExists error
	counts     map[string]int64  // index -> doc count
	countErr   map[string]error  // index -> injected CountDocs error
	countCalls []string          // indices CountDocs was asked about
}

func newFakeCutoverES() *fakeCutoverES {
	return &fakeCutoverES{
		aliases:   map[string]string{},
		aliasErr:  map[string]error{},
		indices:   map[string]bool{},
		existsErr: map[string]error{},
		counts:    map[string]int64{},
		countErr:  map[string]error{},
	}
}

func (f *fakeCutoverES) GetAlias(_ context.Context, aliasName string) (string, error) {
	if err := f.aliasErr[aliasName]; err != nil {
		return "", err
	}
	return f.aliases[aliasName], nil
}

func (f *fakeCutoverES) IndexExists(_ context.Context, indexName string) (bool, error) {
	if err := f.existsErr[indexName]; err != nil {
		return false, err
	}
	return f.indices[indexName], nil
}

func (f *fakeCutoverES) CountDocs(_ context.Context, indexName string) (int64, error) {
	f.countCalls = append(f.countCalls, indexName)
	if err := f.countErr[indexName]; err != nil {
		return 0, err
	}
	return f.counts[indexName], nil
}

// fakeStaleCounter is an in-memory StaleCounter recording what it was asked.
type fakeStaleCounter struct {
	counts    map[string]int // resource type -> stale count
	oldest    map[string]time.Time
	err       error
	gotTypes  []string
	gotBefore time.Time
}

func newFakeStaleCounter() *fakeStaleCounter {
	return &fakeStaleCounter{counts: map[string]int{}, oldest: map[string]time.Time{}}
}

func (f *fakeStaleCounter) CountStale(_ context.Context, resourceType string, before time.Time) (int, time.Time, error) {
	f.gotTypes = append(f.gotTypes, resourceType)
	f.gotBefore = before
	if f.err != nil {
		return 0, time.Time{}, f.err
	}
	return f.counts[resourceType], f.oldest[resourceType], nil
}

// readyForwardES sets up resource "m" one gate short of a v1->v2 cutover:
// alias on v1, both indices exist, equal doc counts.
func readyForwardES() *fakeCutoverES {
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v1"
	es.indices["m_search_v1"] = true
	es.indices["m_search_v2"] = true
	es.counts["m_search_v1"] = 1200
	es.counts["m_search_v2"] = 1200
	return es
}

func findCheck(t *testing.T, r ResourceReadiness, name string) ReadinessCheck {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("readiness for %q has no check %q (got %+v)", r.Resource, name, r.Checks)
	return ReadinessCheck{}
}

func TestCheckCutoverReadiness_ForwardAllGatesPass(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.Equal(t, "m", r.Resource)
	require.Equal(t, "m_search_v1", r.CurrentIndex)
	require.Equal(t, "m_search_v2", r.TargetIndex)
	require.Equal(t, AliasForward, r.Move)
	require.True(t, r.Ready)
	for _, c := range r.Checks {
		require.True(t, c.OK, "check %s: %s", c.Name, c.Detail)
	}
}

func TestCheckCutoverReadiness_TargetIndexMissingShortCircuits(t *testing.T) {
	es := readyForwardES()
	delete(es.indices, "m_search_v2")
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	require.False(t, findCheck(t, r, CheckTargetIndex).OK)
	require.Contains(t, findCheck(t, r, CheckTargetIndex).Detail, "gen-mapping")
	require.Empty(t, es.countCalls, "no point counting docs of a missing index")
	require.Empty(t, st.gotTypes, "gates after a missing target are noise")
}

func TestCheckCutoverReadiness_DocCountParity(t *testing.T) {
	cases := []struct {
		name      string
		target    int64
		tolerance int64
		wantOK    bool
	}{
		{"exact parity", 1200, 0, true},
		{"diverged beyond tolerance", 1100, 0, false},
		{"within tolerance", 1197, 5, true},
		{"just beyond tolerance", 1194, 5, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			es := readyForwardES()
			es.counts["m_search_v2"] = tc.target
			st := newFakeStaleCounter()

			got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}),
				ReadinessOptions{CountTolerance: tc.tolerance})
			require.Len(t, got, 1)

			parity := findCheck(t, got[0], CheckDocParity)
			require.Equal(t, tc.wantOK, parity.OK, parity.Detail)
			require.Equal(t, tc.wantOK, got[0].Ready)
			// The operator must see both counts to judge the gap.
			require.Contains(t, parity.Detail, "1200")
		})
	}
}

func TestCheckCutoverReadiness_StaleBacklogFailsGate(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()
	st.counts["m"] = 3
	st.oldest["m"] = time.Now().Add(-42 * time.Minute)

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	stale := findCheck(t, r, CheckStaleBacklog)
	require.False(t, stale.OK)
	require.Contains(t, stale.Detail, "3")
}

func TestCheckCutoverReadiness_StaleCutoffUsesMaxStaleAge(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()

	CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}),
		ReadinessOptions{MaxStaleAge: time.Hour})

	require.Equal(t, []string{"m"}, st.gotTypes)
	require.WithinDuration(t, time.Now().Add(-time.Hour), st.gotBefore, 10*time.Second)
}

func TestCheckCutoverReadiness_MaxStaleAgeDefaults(t *testing.T) {
	es := readyForwardES()
	st := newFakeStaleCounter()

	CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})

	require.WithinDuration(t, time.Now().Add(-DefaultMaxStaleAge), st.gotBefore, 10*time.Second)
}

func TestCheckCutoverReadiness_BackwardRunsSameGates(t *testing.T) {
	// A rollback is a cutover in reverse (ADR 0009); it deserves the same gates.
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v3"
	es.indices["m_search_v2"] = true
	es.counts["m_search_v3"] = 500
	es.counts["m_search_v2"] = 500
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)
	require.Equal(t, AliasBackward, got[0].Move)
	require.True(t, got[0].Ready)
	require.ElementsMatch(t, []string{"m_search_v3", "m_search_v2"}, es.countCalls)
}

func TestCheckCutoverReadiness_CreateSkipsParity(t *testing.T) {
	// No alias yet: there is no current read index to compare against, so
	// parity cannot gate — but the target must exist and the backlog be clean.
	es := newFakeCutoverES()
	es.indices["m_search_v2"] = true
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.Equal(t, AliasCreate, r.Move)
	require.True(t, r.Ready)
	require.True(t, findCheck(t, r, CheckDocParity).OK)
	require.Empty(t, es.countCalls, "nothing to compare when the alias does not exist")
	require.Equal(t, []string{"m"}, st.gotTypes, "the stale gate still applies")
}

func TestCheckCutoverReadiness_InSyncSkipsParity(t *testing.T) {
	// Already cut over: the tool doubles as a post-cutover soak check, but
	// comparing an index's count with itself proves nothing.
	es := newFakeCutoverES()
	es.aliases["m_search"] = "m_search_v2"
	es.indices["m_search_v2"] = true
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.Equal(t, AliasInSync, r.Move)
	require.True(t, r.Ready)
	require.Empty(t, es.countCalls)
}

func TestCheckCutoverReadiness_ForeignAliasIsNotGateable(t *testing.T) {
	es := newFakeCutoverES()
	es.aliases["m_search"] = "hand_built_index"
	es.indices["m_search_v2"] = true
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
	require.Len(t, got, 1)

	r := got[0]
	require.False(t, r.Ready)
	require.Equal(t, AliasForeign, r.Move)
	alias := findCheck(t, r, CheckAliasState)
	require.False(t, alias.OK)
	require.Contains(t, alias.Detail, "hand_built_index")
	require.Empty(t, es.countCalls)
	require.Empty(t, st.gotTypes)
}

func TestCheckCutoverReadiness_FetchErrorsFailTheirGate(t *testing.T) {
	sentinel := errors.New("es exploded")

	cases := []struct {
		name      string
		breakES   func(*fakeCutoverES)
		breakST   func(*fakeStaleCounter)
		failCheck string
	}{
		{"alias fetch", func(f *fakeCutoverES) { f.aliasErr["m_search"] = sentinel }, nil, CheckAliasState},
		{"index existence", func(f *fakeCutoverES) { f.existsErr["m_search_v2"] = sentinel }, nil, CheckTargetIndex},
		{"doc count", func(f *fakeCutoverES) { f.countErr["m_search_v2"] = sentinel }, nil, CheckDocParity},
		{"stale count", nil, func(f *fakeStaleCounter) { f.err = sentinel }, CheckStaleBacklog},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			es := readyForwardES()
			st := newFakeStaleCounter()
			if tc.breakES != nil {
				tc.breakES(es)
			}
			if tc.breakST != nil {
				tc.breakST(st)
			}

			got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"m": 2}), ReadinessOptions{})
			require.Len(t, got, 1)
			require.False(t, got[0].Ready)
			failed := findCheck(t, got[0], tc.failCheck)
			require.False(t, failed.OK)
			require.Contains(t, failed.Detail, "es exploded")
		})
	}
}

func TestCheckCutoverReadiness_OneResourceFailingDoesNotStopOthers(t *testing.T) {
	es := readyForwardES()
	es.aliasErr["a_search"] = errors.New("es exploded")
	st := newFakeStaleCounter()

	got := CheckCutoverReadiness(context.Background(), es, st, aliasConfigs(map[string]int{"a": 2, "m": 2}), ReadinessOptions{})
	require.Len(t, got, 2)

	byResource := map[string]ResourceReadiness{}
	for _, r := range got {
		byResource[r.Resource] = r
	}
	require.False(t, byResource["a"].Ready)
	require.True(t, byResource["m"].Ready, "the healthy resource must still be assessed")
}
