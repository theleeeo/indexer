package store

import (
	"context"
	"indexer/gen/index/v1"
)

type Resource struct {
	Type string
	Id   string
}

type Store interface {
	AddRelations(ctx context.Context, resource Resource, relations []*index.Relation) error
	RemoveRelation(ctx context.Context, resource Resource, relResource Resource) error
	SetRelation(ctx context.Context, resource Resource, relResource Resource) error
	GetParentResources(ctx context.Context, childResource Resource) ([]Resource, error)
	RemoveResource(ctx context.Context, resource Resource) error
}
