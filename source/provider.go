package source

import (
	"context"
)

// TODO: Move this to the core
type Provider interface {
	FetchResource(ctx context.Context, resourceType, resourceId string) (map[string]any, error)
	// FetchRelated fetches resources of resourceType associated with key.
	// sourceResource and sourceField identify where the key was extracted from.
	FetchRelated(ctx context.Context, params FetchRelatedParams) (FetchRelatedResult, error)
}

// ResourceKey holds a single extracted field name and its value.
type ResourceKey struct {
	Field string
	Value string
}

type FetchRelatedParams struct {
	// The resource for which to fetch related resources.
	RootResource RootResource
	// The resource type to fetch.
	ResourceType string
	// Keys are the extracted field-value pairs used to identify the related resources.
	Keys []ResourceKey
}

type RootResource struct {
	Type string
	Id   string
}

type FetchRelatedResult struct {
	// The related resources that were found for the given key.
	Related []map[string]any
}
