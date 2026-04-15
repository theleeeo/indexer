package dsl

import (
	"context"
	"fmt"

	"github.com/theleeeo/indexer/projection"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/source"
)

// relationFetcher implements aggregation.SubFetcher[BuildDoc].
type relationFetcher struct {
	provider source.Provider
	rel      resource.RelationConfig
}

func (f *relationFetcher) Fetch(parent projection.BuildDoc) (any, error) {
	if parent.Doc == nil {
		return (*fetchedRelation)(nil), nil
	}

	sourceData, ok := parent.Resolved[f.rel.Key.Source]
	if !ok || len(sourceData) == 0 {
		return &fetchedRelation{}, nil
	}

	var keys []source.ResourceKey
	for _, field := range f.rel.Key.Fields {
		if val, ok := sourceData[0][field]; ok {
			if valStr, ok := val.(string); ok {
				keys = append(keys, source.ResourceKey{Field: field, Value: valStr})
			}
		}
	}
	if len(keys) == 0 {
		return &fetchedRelation{}, nil
	}

	relatedResp, err := f.provider.FetchRelated(context.Background(), source.FetchRelatedParams{
		RootResource: source.RootResource{
			Type: parent.Root.Type,
			Id:   parent.Root.Id,
		},
		ResourceType: f.rel.Resource,
		Keys:         keys,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch related %s for %s/%s: %w", f.rel.Resource, parent.Root.Type, parent.Root.Id, err)
	}

	return &fetchedRelation{
		ResourceType: f.rel.Resource,
		Related:      relatedResp.Related,
	}, nil
}

// fetchedRelation holds the raw data returned by a relation fetch.
type fetchedRelation struct {
	ResourceType string
	Related      []map[string]any
}
