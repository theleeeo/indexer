package store

import (
	"context"
	"indexer/model"
)

type Relation struct {
	Parent model.Resource
	Child  model.Resource
}

type Store interface {
	AddRelations(ctx context.Context, relations []Relation) error
	RemoveRelation(ctx context.Context, relation Relation) error
	SetRelation(ctx context.Context, relation Relation) error
	GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error)
	GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error)
	RemoveResource(ctx context.Context, resource model.Resource) error
}
