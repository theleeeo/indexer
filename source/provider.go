package source

import (
	"context"
)

type Provider interface {
	FetchResource(ctx context.Context, resourceType, resourceId string) (map[string]any, error)
	FetchRelated(ctx context.Context, resourceType, resourceId, relationType string) ([]map[string]any, error)
}
