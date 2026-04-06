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

type FetchRelatedParams struct {
	// The root resource for which the related resources are being fetched.
	RootResource string
	// The resource type to fetch
	ResourceType string
	// The key value to look up related resources by.
	Key string
	// The name of the field from which the key was extracted.
	SourceField string
}

type FetchRelatedResult struct {
	// The related resources that were found for the given key.
	Related []map[string]any
}
