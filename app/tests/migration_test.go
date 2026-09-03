package tests

import (
	"encoding/json"

	"github.com/theleeeo/laika/backend/elasticsearch"
	"github.com/theleeeo/laika/core"
	"github.com/theleeeo/laika/core/resource"
)

// MigrationResourceConfig is the ADR 0004 shape mid-migration: resource "m"
// with the serving v1 and a freshly added v2 that projects an extra field.
// ReadVersion stays at 1 — the read alias serves v1 while v2 backfills.
var MigrationResourceConfig = resource.Configs{
	{
		Resource: "m",
		Versions: []resource.VersionConfig{
			{Version: 1, Fields: []resource.FieldConfig{
				{Name: "field1", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
			}},
			{Version: 2, Fields: []resource.FieldConfig{
				{Name: "field1", Query: resource.QueryConfig{Search: resource.SearchTierPrimary}},
				{Name: "field2"},
			}},
		},
		ReadVersion: 1,
	},
}

// docFields fetches a document's projected fields directly from a concrete
// version index, bypassing the read alias.
func (t *TestSuite) docFields(index, id string) (map[string]any, bool) {
	res, err := t.esClient.Get(index, id)
	t.Require().NoError(err)
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return nil, false
	}
	t.Require().False(res.IsError(), "get %s/%s: %s", index, id, res.Status())

	var body struct {
		Source map[string]any `json:"_source"`
	}
	t.Require().NoError(json.NewDecoder(res.Body).Decode(&body))
	fields, _ := body.Source["fields"].(map[string]any)
	return fields, true
}

// Test_Migration_MultiVersionLifecycle walks the ADR 0004 rolling-migration
// lifecycle against real infrastructure: live builds fan out to every Schema
// Version, a targeted rebuild backfills only the new version's index, a full
// rebuild resets every version, the read alias converges onto the config's
// readVersion in both directions (ADR 0009), and a delete clears every
// version.
func (t *TestSuite) Test_Migration_MultiVersionLifecycle() {
	for _, c := range MigrationResourceConfig {
		c.ApplyDefaults()
	}
	t.Require().NoError(MigrationResourceConfig.Validate())
	t.setResourceConfig(MigrationResourceConfig)

	const v1Index, v2Index = "m_search_v1", "m_search_v2"
	ids := []string{"1", "2", "3"}

	// --- Live ingest writes every active Schema Version (ADR 0004). ---
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "orig" + id, "field2": "extra" + id,
		})
		t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
			ResourceType: "m", ResourceID: id, Kind: core.ChangeCreated,
		}))
	}
	t.worker.Drain(t.T().Context())

	for _, id := range ids {
		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok, "v1 must hold %s after a live build", id)
		t.Require().Equal("orig"+id, f1["field1"])
		t.Require().NotContains(f1, "field2", "v1's projection must not leak v2 fields")

		f2, ok := t.docFields(v2Index, id)
		t.Require().True(ok, "v2 must hold %s after a live build — every build writes every active Schema Version", id)
		t.Require().Equal("extra"+id, f2["field2"])
	}

	// --- A targeted rebuild backfills only the selected version's index. ---
	for _, id := range ids {
		t.fakeProvider.SetResource("m", id, map[string]any{
			"id": id, "field1": "new" + id, "field2": "fresh" + id,
		})
	}
	t.Require().NoError(t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "m", Versions: []int{2}},
	}))
	t.worker.Drain(t.T().Context())

	for _, id := range ids {
		f2, ok := t.docFields(v2Index, id)
		t.Require().True(ok)
		t.Require().Equal("fresh"+id, f2["field2"], "targeted rebuild must refresh v2")

		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok)
		t.Require().Equal("orig"+id, f1["field1"], "targeted rebuild must not rewrite the serving v1 index")
	}

	// --- A full rebuild resets every version. ---
	t.Require().NoError(t.idx.RebuildNow(t.T().Context(), []core.ResourceSelector{
		{ResourceType: "m"},
	}))
	t.worker.Drain(t.T().Context())

	for _, id := range ids {
		f1, ok := t.docFields(v1Index, id)
		t.Require().True(ok)
		t.Require().Equal("new"+id, f1["field1"], "full rebuild must reset v1 too")
	}

	// --- The read alias still serves ReadVersion 1. ---
	resp, err := t.idx.Search(t.T().Context(), core.SearchRequest{Resource: "m", Query: "new1"})
	t.Require().NoError(err)
	t.Require().Len(resp.Hits, 1)
	t.Require().Equal("1", resp.Hits[0].ID)

	// --- Cutover is a readVersion change: the alias converges onto the config
	// (ADR 0009), forward, idempotently, and back again on a rollback. ---
	ctx := t.T().Context()
	esBackend := elasticsearch.New(t.esClient, true)
	aliasTarget := func() string {
		target, err := esBackend.GetAlias(ctx, core.AliasName("m"))
		t.Require().NoError(err)
		return target
	}

	cut := *MigrationResourceConfig[0]
	cut.ReadVersion = 2
	cutCfg := resource.Configs{&cut}

	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, cutCfg))
	t.Require().Equal(v2Index, aliasTarget(), "forward convergence must move the alias to the new readVersion")

	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, cutCfg))
	t.Require().Equal(v2Index, aliasTarget(), "re-running convergence must be a no-op")

	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, MigrationResourceConfig))
	t.Require().Equal(v1Index, aliasTarget(), "a readVersion rollback must converge the alias back")

	// A missing alias is recreated from config — the bootstrap-completing case.
	delRes, err := t.esClient.Indices.DeleteAlias([]string{v1Index}, []string{core.AliasName("m")})
	t.Require().NoError(err)
	delRes.Body.Close()
	t.Require().NoError(core.ConvergeReadAliases(ctx, esBackend, MigrationResourceConfig))
	t.Require().Equal(v1Index, aliasTarget(), "convergence must recreate a missing alias")

	// A readVersion whose index was never bootstrapped must fail loudly and
	// leave the alias where it was.
	broken := *MigrationResourceConfig[0]
	broken.ReadVersion = 3
	t.Require().Error(core.ConvergeReadAliases(ctx, esBackend, resource.Configs{&broken}))
	t.Require().Equal(v1Index, aliasTarget(), "a failed convergence must not move the alias")

	// --- A delete clears every version's document. ---
	t.fakeProvider.DeleteResource("m", "1")
	t.Require().NoError(t.idx.RegisterChange(t.T().Context(), core.Notification{
		ResourceType: "m", ResourceID: "1", Kind: core.ChangeDeleted,
	}))
	t.worker.Drain(t.T().Context())

	if _, ok := t.docFields(v1Index, "1"); ok {
		t.T().Fatal("delete must remove the v1 document")
	}
	if _, ok := t.docFields(v2Index, "1"); ok {
		t.T().Fatal("delete must remove the v2 document")
	}
	t.Require().False(t.resourceTracked("m", "1"), "the tombstone must be hard-deleted after ES cleanup")
}
