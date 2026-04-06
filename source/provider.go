package source

import (
	"context"
)

// TODO: Move this to the core
type Provider interface {
	FetchResource(ctx context.Context, resourceType, resourceId string) (map[string]any, error)
	FetchRelated(ctx context.Context, resourceType, key string) ([]map[string]any, error)
}
