package dsl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/source"
)

// mockProvider is a minimal source.Provider for testing plan execution.
type mockProvider struct {
	resources map[string]map[string]any   // "type|id" -> data
	related   map[string][]map[string]any // "type|keyval" -> []data
	listed    map[string][]source.ListedResource
	// pageSize controls how many items are returned per ListResources page.
	pageSize int
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		resources: make(map[string]map[string]any),
		related:   make(map[string][]map[string]any),
		listed:    make(map[string][]source.ListedResource),
		pageSize:  100,
	}
}

func (m *mockProvider) FetchResource(_ context.Context, resourceType, resourceID string) (map[string]any, error) {
	data, ok := m.resources[resourceType+"|"+resourceID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *mockProvider) FetchRelated(_ context.Context, params source.FetchRelatedParams) (source.FetchRelatedResult, error) {
	key := params.ResourceType + "|" + params.Key.Value
	data, ok := m.related[key]
	if !ok {
		return source.FetchRelatedResult{}, nil
	}
	return source.FetchRelatedResult{Related: data}, nil
}

func (m *mockProvider) ListResources(_ context.Context, params source.ListResourcesParams) (source.ListResourcesResult, error) {
	all, ok := m.listed[params.ResourceType]
	if !ok {
		return source.ListResourcesResult{}, nil
	}

	pageSize := m.pageSize
	if params.PageSize > 0 && int(params.PageSize) < pageSize {
		pageSize = int(params.PageSize)
	}

	start := 0
	if params.PageToken != "" {
		for i, r := range all {
			if r.ID == params.PageToken {
				start = i
				break
			}
		}
	}

	end := start + pageSize
	if end > len(all) {
		end = len(all)
	}

	var npt string
	if end < len(all) {
		npt = all[end].ID
	}

	return source.ListResourcesResult{
		Resources:     all[start:end],
		NextPageToken: npt,
	}, nil
}

func TestBuildPlanForVersion_FetchSingle(t *testing.T) {
	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget"}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceID:   "1",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	require.Equal(t, "product", docs[0].Root.Type)
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "Widget", docs[0].Doc["fields"].(map[string]any)["title"])
}

func TestBuildPlanForVersion_FetchSingle_NotFound(t *testing.T) {
	prov := newMockProvider()

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceID:   "999",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	require.Nil(t, docs[0].Doc, "doc should be nil for missing resource")
}

func TestBuildPlanForVersion_FetchAll_SinglePage(t *testing.T) {
	prov := newMockProvider()
	prov.listed["product"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "title": "Widget"}},
		{ID: "2", Data: map[string]any{"id": "2", "title": "Gadget"}},
	}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceID:   "", // empty = list all
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 2)
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "Widget", docs[0].Doc["fields"].(map[string]any)["title"])
	require.Equal(t, "2", docs[1].Root.Id)
	require.Equal(t, "Gadget", docs[1].Doc["fields"].(map[string]any)["title"])
}

func TestBuildPlanForVersion_FetchAll_MultiplePages(t *testing.T) {
	prov := newMockProvider()
	prov.pageSize = 2
	prov.listed["product"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "title": "A"}},
		{ID: "2", Data: map[string]any{"id": "2", "title": "B"}},
		{ID: "3", Data: map[string]any{"id": "3", "title": "C"}},
	}

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceID:   "",
	})

	var docs []projection.BuildDoc
	var pages int
	for r := range ch {
		require.NoError(t, r.Err)
		pages++
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 3)
	require.Equal(t, 2, pages, "should have 2 pages")
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "2", docs[1].Root.Id)
	require.Equal(t, "3", docs[2].Root.Id)
}

func TestBuildPlanForVersion_FetchAll_Empty(t *testing.T) {
	prov := newMockProvider()
	// No resources listed for this type.

	fields := []resource.FieldConfig{{Name: "title"}}
	vc := &resource.VersionConfig{Fields: fields}
	plan := buildPlanForVersion(prov, "product", vc)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product",
		ResourceID:   "",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 0)
}

func TestBuildPlanForVersion_FetchAll_WithRelation(t *testing.T) {
	prov := newMockProvider()
	prov.listed["order"] = []source.ListedResource{
		{ID: "1", Data: map[string]any{"id": "1", "number": "ORD-1"}},
	}
	prov.related["customer|1"] = []map[string]any{
		{"id": "c1", "name": "Alice"},
	}

	fields := []resource.FieldConfig{{Name: "number"}}
	vc := &resource.VersionConfig{
		Fields: fields,
		Relations: []resource.RelationConfig{
			{
				Resource: "customer",
				Key:      resource.KeyConfig{Source: "order", Field: "id"},
				Fields:   []resource.FieldConfig{{Name: "name"}},
			},
		},
	}

	plan := buildPlanForVersion(prov, "order", vc)

	ch := plan.Execute(context.Background(), projection.BuildRequest{
		ResourceType: "order",
		ResourceID:   "",
	})

	var docs []projection.BuildDoc
	for r := range ch {
		require.NoError(t, r.Err)
		docs = append(docs, r.Items...)
	}

	require.Len(t, docs, 1)
	require.Equal(t, "1", docs[0].Root.Id)
	require.Equal(t, "ORD-1", docs[0].Doc["fields"].(map[string]any)["number"])

	// Should have the customer relation populated.
	customers, ok := docs[0].Doc["customer"].([]map[string]any)
	require.True(t, ok, "customer field should be present")
	require.Len(t, customers, 1)
	require.Equal(t, "Alice", customers[0]["name"])
	require.Equal(t, "c1", customers[0]["id"])

	// Should have tracked the relation.
	require.Len(t, docs[0].Relations, 1)
	require.Equal(t, "customer", docs[0].Relations[0].Type)
	require.Equal(t, "c1", docs[0].Relations[0].Id)
}

func TestBuildPlansFromConfig_VersionedPlans(t *testing.T) {
	prov := newMockProvider()
	prov.resources["product|1"] = map[string]any{"id": "1", "title": "Widget", "price": "9.99"}

	cfgs := resource.Configs{
		{
			Resource: "product",
			VersionDefs: map[int]*resource.VersionConfig{
				1: {Fields: []resource.FieldConfig{{Name: "title"}}},
				2: {Fields: []resource.FieldConfig{{Name: "title"}, {Name: "price"}}},
			},
			ReadVersion: 1,
		},
	}

	plans := BuildPlansFromConfig(prov, cfgs)

	require.Contains(t, plans, "product")
	require.Contains(t, plans["product"], 1)
	require.Contains(t, plans["product"], 2)

	// Version 1: only title.
	ch1 := plans["product"][1].Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product", ResourceID: "1",
	})
	var docs1 []projection.BuildDoc
	for r := range ch1 {
		require.NoError(t, r.Err)
		docs1 = append(docs1, r.Items...)
	}
	require.Len(t, docs1, 1)
	fields1 := docs1[0].Doc["fields"].(map[string]any)
	require.Equal(t, "Widget", fields1["title"])
	require.NotContains(t, fields1, "price")

	// Version 2: title + price.
	ch2 := plans["product"][2].Execute(context.Background(), projection.BuildRequest{
		ResourceType: "product", ResourceID: "1",
	})
	var docs2 []projection.BuildDoc
	for r := range ch2 {
		require.NoError(t, r.Err)
		docs2 = append(docs2, r.Items...)
	}
	require.Len(t, docs2, 1)
	fields2 := docs2[0].Doc["fields"].(map[string]any)
	require.Equal(t, "Widget", fields2["title"])
	require.Equal(t, "9.99", fields2["price"])
}
