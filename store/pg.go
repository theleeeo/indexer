package store

import (
	"context"
	"indexer/gen/index/v1"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ Store = (*PostgresStore)(nil)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) AddRelations(ctx context.Context, resource Resource, relations []*index.Relation) error {
	_, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"relations"},
		[]string{"resource", "resource_id", "related_resource", "related_resource_id"},
		pgx.CopyFromSlice(len(relations), func(i int) ([]any, error) {
			return []any{resource.Type, resource.Id, relations[i].Resource, relations[i].ResourceId}, nil
		}),
	)
	return err
}

func (s *PostgresStore) RemoveRelation(ctx context.Context, resource, relResource Resource) error {
	_, err := s.pool.Exec(
		ctx,
		`DELETE FROM relations WHERE related_resource=$1 AND related_resource_id=$2 AND resource=$3 AND resource_id=$4`,
		resource.Type, resource.Id, relResource.Type, relResource.Id,
	)
	return err
}

func (s *PostgresStore) SetRelation(ctx context.Context, parentResource, relatedResource Resource) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`DELETE FROM relations WHERE related_resource=$1 AND related_resource_id=$2`,
		relatedResource.Type, relatedResource.Id,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO relations (related_resource, related_resource_id, resource, resource_id) VALUES ($1, $2, $3, $4)`,
		relatedResource.Type, relatedResource.Id, parentResource.Type, parentResource.Id,
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *PostgresStore) GetParentResources(ctx context.Context, childResource Resource) ([]Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT resource, resource_id FROM relations WHERE related_resource=$1 AND related_resource_id=$2`,
		childResource.Type, childResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parents []Resource
	for rows.Next() {
		var parentResource, parentResourceId string
		if err := rows.Scan(&parentResource, &parentResourceId); err != nil {
			return nil, err
		}
		parents = append(parents, Resource{Type: parentResource, Id: parentResourceId})
	}
	return parents, nil
}
