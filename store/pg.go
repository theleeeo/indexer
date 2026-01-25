package store

import (
	"context"
	"indexer/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) AddRelations(ctx context.Context, relations []Relation) error {
	return s.addRelationsBatch(ctx, s.pool, relations)
}

func (s *PostgresStore) addRelationsBatch(ctx context.Context, sender batchSender, relations []Relation) error {
	if len(relations) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, relation := range relations {
		batch.Queue(
			`INSERT INTO relations (resource, resource_id, related_resource, related_resource_id) 
			 VALUES ($1, $2, $3, $4) 
			 ON CONFLICT DO NOTHING`,
			relation.Parent.Type, relation.Parent.Id, relation.Child.Type, relation.Child.Id,
		)
	}

	br := sender.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) RemoveRelation(ctx context.Context, relation Relation) error {
	return s.removeRelationsBatch(ctx, s.pool, []Relation{relation})
}

func (s *PostgresStore) removeRelationsBatch(ctx context.Context, sender batchSender, relations []Relation) error {
	if len(relations) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, relation := range relations {
		batch.Queue(
			`DELETE FROM relations WHERE related_resource=$1 AND related_resource_id=$2 AND resource=$3 AND resource_id=$4`,
			relation.Child.Type, relation.Child.Id, relation.Parent.Type, relation.Parent.Id,
		)
	}

	br := sender.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return nil
}

func (s *PostgresStore) SetRelations(ctx context.Context, resource model.Resource, relatedResources []model.Resource) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := s.removeResource(ctx, tx, resource); err != nil {
		return err
	}

	if len(relatedResources) == 0 {
		return nil
	}

	relations := make([]Relation, 0, len(relatedResources))
	for _, rr := range relatedResources {
		relations = append(relations, Relation{
			Parent: resource,
			Child:  rr,
		})
	}

	if err := s.addRelationsBatch(ctx, tx, relations); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *PostgresStore) GetParentResources(ctx context.Context, childResource model.Resource) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT resource, resource_id FROM relations WHERE related_resource=$1 AND related_resource_id=$2`,
		childResource.Type, childResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parents []model.Resource
	for rows.Next() {
		var parentResource, parentResourceId string
		if err := rows.Scan(&parentResource, &parentResourceId); err != nil {
			return nil, err
		}
		parents = append(parents, model.Resource{Type: parentResource, Id: parentResourceId})
	}
	return parents, nil
}

func (s *PostgresStore) GetChildResources(ctx context.Context, parentResource model.Resource) ([]model.Resource, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT related_resource, related_resource_id FROM relations WHERE resource=$1 AND resource_id=$2`,
		parentResource.Type, parentResource.Id,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var children []model.Resource
	for rows.Next() {
		var childResource, childResourceId string
		if err := rows.Scan(&childResource, &childResourceId); err != nil {
			return nil, err
		}
		children = append(children, model.Resource{Type: childResource, Id: childResourceId})
	}
	return children, nil
}

func (s *PostgresStore) RemoveResource(ctx context.Context, resource model.Resource) error {
	return s.removeResource(ctx, s.pool, resource)
}

func (s *PostgresStore) removeResource(ctx context.Context, sender executor, resource model.Resource) error {
	_, err := sender.Exec(
		ctx,
		`DELETE FROM relations WHERE resource=$1 AND resource_id=$2`,
		resource.Type, resource.Id,
	)
	return err
}
