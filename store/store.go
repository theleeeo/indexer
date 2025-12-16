package store

import (
	"context"
)

type Resource struct {
	Type string
	Id   string
}

type Relation struct {
	Parent   Resource
	Children Resource
}

type Store interface {
	AddRelations(ctx context.Context, relations []Relation) error
	RemoveRelation(ctx context.Context, relation Relation) error
	SetRelation(ctx context.Context, relation Relation) error
	GetParentResources(ctx context.Context, childResource Resource) ([]Resource, error)
	RemoveResource(ctx context.Context, resource Resource) error
}
