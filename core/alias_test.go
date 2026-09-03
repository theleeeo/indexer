package core

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/laika/core/resource"
)

func TestPlanAliasMove(t *testing.T) {
	cases := []struct {
		name        string
		current     string
		readVersion int
		want        AliasMove
	}{
		{"in sync", "m_search_v2", 2, AliasInSync},
		{"alias missing", "", 2, AliasCreate},
		{"forward cutover", "m_search_v1", 2, AliasForward},
		{"backward rollback", "m_search_v3", 2, AliasBackward},
		{"foreign target", "hand_built_index", 2, AliasForeign},
		{"other resource's index", "x_search_v1", 2, AliasForeign},
		{"unparseable version", "m_search_vX", 2, AliasForeign},
		{"non-positive version", "m_search_v0", 2, AliasForeign},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, PlanAliasMove("m", tc.current, tc.readVersion))
		})
	}
}

func TestAliasMove_TextForm(t *testing.T) {
	// The readiness report is consumed by scripts (-json); moves must
	// serialize as words, not iota values.
	require.Equal(t, "forward", AliasForward.String())

	got, err := json.Marshal(map[string]AliasMove{"move": AliasBackward})
	require.NoError(t, err)
	require.JSONEq(t, `{"move":"backward"}`, string(got))
}

// fakeAliasBackend is an in-memory AliasBackend recording CreateAlias calls.
type fakeAliasBackend struct {
	targets    map[string]string // alias -> current index
	getErr     map[string]error  // alias -> injected GetAlias error
	createErr  map[string]error  // alias -> injected CreateAlias error
	createdArg map[string]string // alias -> index passed to CreateAlias
}

func newFakeAliasBackend() *fakeAliasBackend {
	return &fakeAliasBackend{
		targets:    map[string]string{},
		getErr:     map[string]error{},
		createErr:  map[string]error{},
		createdArg: map[string]string{},
	}
}

func (f *fakeAliasBackend) GetAlias(_ context.Context, aliasName string) (string, error) {
	if err := f.getErr[aliasName]; err != nil {
		return "", err
	}
	return f.targets[aliasName], nil
}

func (f *fakeAliasBackend) CreateAlias(_ context.Context, aliasName, indexName string) error {
	if err := f.createErr[aliasName]; err != nil {
		return err
	}
	f.createdArg[aliasName] = indexName
	f.targets[aliasName] = indexName
	return nil
}

func aliasConfigs(readVersions map[string]int) resource.Configs {
	var cfgs resource.Configs
	for name, rv := range readVersions {
		cfgs = append(cfgs, &resource.Config{
			Resource: name,
			Versions: []resource.VersionConfig{
				{Version: 1}, {Version: 2}, {Version: 3},
			},
			ReadVersion: rv,
		})
	}
	return cfgs
}

func TestConvergeReadAliases_InSyncIsNoop(t *testing.T) {
	es := newFakeAliasBackend()
	es.targets["m_search"] = "m_search_v2"

	require.NoError(t, ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"m": 2})))
	require.Empty(t, es.createdArg, "an in-sync alias must not be rewritten")
}

func TestConvergeReadAliases_CreatesMissingAlias(t *testing.T) {
	es := newFakeAliasBackend()

	require.NoError(t, ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"m": 2})))
	require.Equal(t, "m_search_v2", es.createdArg["m_search"])
}

func TestConvergeReadAliases_MovesForward(t *testing.T) {
	es := newFakeAliasBackend()
	es.targets["m_search"] = "m_search_v1"

	require.NoError(t, ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"m": 2})))
	require.Equal(t, "m_search_v2", es.createdArg["m_search"])
}

func TestConvergeReadAliases_MovesBackwardOnRollback(t *testing.T) {
	// The config is the owner in both directions: rolling readVersion back in
	// config must converge the alias back without any manual ES step.
	es := newFakeAliasBackend()
	es.targets["m_search"] = "m_search_v3"

	require.NoError(t, ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"m": 2})))
	require.Equal(t, "m_search_v2", es.createdArg["m_search"])
}

func TestConvergeReadAliases_SkipsForeignTarget(t *testing.T) {
	// An alias pointing at an index the naming scheme doesn't own was built by
	// hand; convergence must not destroy it, and must not fail startup over it.
	es := newFakeAliasBackend()
	es.targets["m_search"] = "hand_built_index"

	require.NoError(t, ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"m": 2})))
	require.Empty(t, es.createdArg)
}

func TestConvergeReadAliases_AggregatesErrorsAcrossResources(t *testing.T) {
	// One resource failing must not stop the others from converging, and the
	// aggregate error must name the failing resource.
	sentinel := errors.New("es exploded")
	es := newFakeAliasBackend()
	es.getErr["a_search"] = sentinel
	es.targets["b_search"] = "b_search_v1"

	err := ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"a": 2, "b": 2}))
	require.ErrorIs(t, err, sentinel)
	require.ErrorContains(t, err, "a")
	require.Equal(t, "b_search_v2", es.createdArg["b_search"], "the healthy resource must still converge")
}

func TestConvergeReadAliases_CreateErrorPropagates(t *testing.T) {
	// A missing target index surfaces here as a CreateAlias failure: the
	// deployment is broken (gen-mapping never ran) and startup must say so.
	sentinel := errors.New("index_not_found_exception")
	es := newFakeAliasBackend()
	es.createErr["m_search"] = sentinel

	err := ConvergeReadAliases(context.Background(), es, aliasConfigs(map[string]int{"m": 2}))
	require.ErrorIs(t, err, sentinel)
}
