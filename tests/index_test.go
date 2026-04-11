package tests

import (
	"github.com/theleeeo/indexer/gen/search/v1"
	"github.com/theleeeo/indexer/source"
)

func (t *TestSuite) Test_Resource_CRUD_OneIndex() {
	t.setResourceConfig(DefaultResourceConfig)

	// Populate source with two "a" resources.
	t.fakeProvider.SetResource("a", "1", map[string]any{
		"id":     "1",
		"field1": "value1",
	})
	t.fakeProvider.SetResource("a", "2", map[string]any{
		"id":     "2",
		"field1": "value2",
	})

	t.Run("create resources", func() {
		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         source.ChangeCreated,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		err = t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a",
			ResourceID:   "2",
			Kind:         source.ChangeCreated,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())
	})

	t.Run("no query or filters", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 2)
		t.Require().Equal("1", resp.Hits[0].Id)
		t.Require().Equal("2", resp.Hits[1].Id)
	})

	t.Run("with query, string value", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{
			Resource: "a",
			Query:    "value1",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
	})

	t.Run("with query, no matches", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{
			Resource: "a",
			Query:    "false",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})

	t.Run("update existing resource", func() {
		// Update the source data.
		t.fakeProvider.SetResource("a", "1", map[string]any{
			"id":     "1",
			"field1": "updated_value",
		})

		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         source.ChangeUpdated,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{
			Resource: "a",
			Query:    "updated_value",
		})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
	})

	t.Run("delete resource", func() {
		t.fakeProvider.DeleteResource("a", "1")

		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a",
			ResourceID:   "1",
			Kind:         source.ChangeDeleted,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("2", resp.Hits[0].Id)
	})

	t.Run("delete non-existing resource", func() {
		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a",
			ResourceID:   "non_existing_id",
			Kind:         source.ChangeDeleted,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())
	})
}

func (t *TestSuite) Test_Resource_CRUD_MultipleIndices() {
	t.setResourceConfig(DefaultResourceConfig)

	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetResource("b", "2", map[string]any{"id": "2"})

	t.Run("create resources in different indices", func() {
		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a", ResourceID: "1", Kind: source.ChangeCreated,
		})
		t.Require().NoError(err)

		err = t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "b", ResourceID: "2", Kind: source.ChangeCreated,
		})
		t.Require().NoError(err)

		t.worker.Drain(t.T().Context())
	})

	t.Run("search in index a", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
	})

	t.Run("search in index b", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "b"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("2", resp.Hits[0].Id)
	})

	t.Run("search in non existing index", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "c"})
		t.Require().EqualError(err, "unknown resource")
		t.Require().Nil(resp)
	})

	t.Run("delete resources", func() {
		t.fakeProvider.DeleteResource("a", "1")
		t.fakeProvider.DeleteResource("b", "2")

		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a", ResourceID: "1", Kind: source.ChangeDeleted,
		})
		t.Require().NoError(err)

		err = t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "b", ResourceID: "2", Kind: source.ChangeDeleted,
		})
		t.Require().NoError(err)

		t.worker.Drain(t.T().Context())
	})

	t.Run("verify deletions", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)

		resp, err = t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "b"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 0)
	})
}

func (t *TestSuite) Test_Create_WithRelation() {
	t.setResourceConfig(DefaultResourceConfig)

	// Source has resource "a/1" with a related "b/1".
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "field1": "b_val"},
	})

	t.Run("create root resource that has a related resource", func() {
		err := t.idx.RegisterChange(t.T().Context(), source.Notification{
			ResourceType: "a", ResourceID: "1", Kind: source.ChangeCreated,
		})
		t.Require().NoError(err)
		t.worker.Drain(t.T().Context())

		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)

		// The "b" field should be a list with one entry.
		relations := resp.Hits[0].Source.Fields["b"].GetListValue().GetValues()
		t.Require().Len(relations, 1)
		t.Require().Equal("1", relations[0].GetStructValue().Fields["id"].GetStringValue())
	})
}

// Test that when a related child resource is created after the parent,
// and we rebuild the parent, the parent document has the relation populated.
func (t *TestSuite) Test_Create_ParentRelation_Already_Exists() {
	t.setResourceConfig(DefaultResourceConfig)

	// Initially a/1 exists with a relation to b/1 already known by the source.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "field1": "b_val"},
	})

	// Create and build a/1.
	err := t.idx.RegisterChange(t.T().Context(), source.Notification{
		ResourceType: "a", ResourceID: "1", Kind: source.ChangeCreated,
	})
	t.Require().NoError(err)

	// Now create b/1 — since a/1 already has a relation to b/1 in PG after
	// its rebuild, AffectedRoots("b","1") should find a/1 and re-rebuild it.
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1"})

	err = t.idx.RegisterChange(t.T().Context(), source.Notification{
		ResourceType: "b", ResourceID: "1", Kind: source.ChangeCreated,
	})
	t.Require().NoError(err)

	t.worker.Drain(t.T().Context())

	t.Run("verify relation on parent", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "a"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)
		relations := resp.Hits[0].Source.Fields["b"].GetListValue().GetValues()
		t.Require().Len(relations, 1)
		t.Require().Equal("1", relations[0].GetStructValue().Fields["id"].GetStringValue())
	})
}

// Test that when a root "c" has relations to "a" and "b", and the source
// returns the full graph, it gets indexed correctly.
func (t *TestSuite) Test_RelatedRelations_FullGraph() {
	t.setResourceConfig(RelatedResourceConfig)

	// b/1 exists as a root resource.
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "f1": "bval"})

	// a/1 exists as a root resource and has relation to b/1.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "aval"})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "bval"},
	})

	// c/1 exists and has relations to both a/1 and b/1.
	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cval"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "aval"},
	})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "bval"},
	})

	// Create all resources.
	for _, n := range []source.Notification{
		{ResourceType: "b", ResourceID: "1", Kind: source.ChangeCreated},
		{ResourceType: "a", ResourceID: "1", Kind: source.ChangeCreated},
		{ResourceType: "c", ResourceID: "1", Kind: source.ChangeCreated},
	} {
		err := t.idx.RegisterChange(t.T().Context(), n)
		t.Require().NoError(err)
	}
	t.worker.Drain(t.T().Context())

	t.Run("verify c has both a and b relations", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "c"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)
		t.Require().Equal("1", resp.Hits[0].Id)

		aRels := resp.Hits[0].Source.Fields["a"].GetListValue().GetValues()
		t.Require().Len(aRels, 1)
		t.Require().Equal("1", aRels[0].GetStructValue().Fields["id"].GetStringValue())

		bRels := resp.Hits[0].Source.Fields["b"].GetListValue().GetValues()
		t.Require().Len(bRels, 1)
		t.Require().Equal("1", bRels[0].GetStructValue().Fields["id"].GetStringValue())
	})
}

// Test that updating a child resource triggers a rebuild of parent root documents.
func (t *TestSuite) Test_ChildUpdate_Rebuilds_Parent() {
	t.setResourceConfig(RelatedResourceConfig)

	// Set up the graph: c/1 → a/1, c/1 → b/1.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "aval"})
	t.fakeProvider.SetResource("b", "1", map[string]any{"id": "1", "f1": "bval"})
	t.fakeProvider.SetResource("c", "1", map[string]any{"id": "1", "f1": "cval"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "aval"},
	})
	t.fakeProvider.SetRelated("b", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "bval"},
	})

	// Build c/1 to establish the relation graph in PG.
	err := t.idx.RegisterChange(t.T().Context(), source.Notification{
		ResourceType: "c", ResourceID: "1", Kind: source.ChangeCreated,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	// Now update a/1 at the source.
	t.fakeProvider.SetResource("a", "1", map[string]any{"id": "1", "f1": "aval_updated"})
	t.fakeProvider.SetRelated("a", []string{"1"}, []map[string]any{
		{"id": "1", "f1": "aval_updated"},
	})

	err = t.idx.RegisterChange(t.T().Context(), source.Notification{
		ResourceType: "a", ResourceID: "1", Kind: source.ChangeUpdated,
	})
	t.Require().NoError(err)
	t.worker.Drain(t.T().Context())

	t.Run("verify c sees updated a data", func() {
		resp, err := t.idx.Search(t.T().Context(), &search.SearchRequest{Resource: "c"})
		t.Require().NoError(err)
		t.Require().Len(resp.Hits, 1)

		aRels := resp.Hits[0].Source.Fields["a"].GetListValue().GetValues()
		t.Require().Len(aRels, 1)
		t.Require().Equal("aval_updated", aRels[0].GetStructValue().Fields["f1"].GetStringValue())
	})
}
