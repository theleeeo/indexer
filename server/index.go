package server

import (
	"context"
	"fmt"
	"time"

	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	AIndex = "a_search"
	BIndex = "b_search"
	CIndex = "c_search"
)

type IndexerServer struct {
	index.UnimplementedIndexServiceServer

	st *store.Store
	es *es.Client

	// de-dup window (in-memory)
	dedupTTL time.Duration
}

func NewIndexer(st *store.Store, esClient *es.Client) *IndexerServer {
	return &IndexerServer{
		st:       st,
		es:       esClient,
		dedupTTL: 5 * time.Minute,
	}
}

func (s *IndexerServer) Publish(ctx context.Context, req *index.PublishRequest) (*index.PublishResponse, error) {
	if req.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}

	if err := s.applyOne(ctx, req.Event); err != nil {
		return nil, err
	}
	return &index.PublishResponse{}, nil
}

func (s *IndexerServer) PublishBatch(ctx context.Context, req *index.PublishBatchRequest) (*index.PublishBatchResponse, error) {
	if len(req.Events) == 0 {
		return &index.PublishBatchResponse{}, nil
	}

	for _, ev := range req.Events {
		if ev == nil {
			continue
		}
		if err := s.applyOne(ctx, ev); err != nil {
			return nil, err
		}
	}

	return &index.PublishBatchResponse{}, nil
}

func (s *IndexerServer) applyOne(ctx context.Context, ev *index.ChangeEvent) error {
	switch p := ev.Payload.(type) {
	case *index.ChangeEvent_CreatePayload:
		return s.handleCreate(ctx, p.CreatePayload)
	case *index.ChangeEvent_UpdatePayload:
		return s.handleUpdate(ctx, p.UpdatePayload)
	default:
		return fmt.Errorf("unknown payload")
	}
}

func (s *IndexerServer) handleCreate(ctx context.Context, p *index.CreatePayload) error {
	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	s.st.StoreRelations(p.Resource, p.ResourceId, p.Relations)

	if err := s.es.UpsertJSON(ctx, p.Resource+"_search", p.ResourceId, map[string]any{
		"fields": p.Data,
	}); err != nil {
		return err
	}

	relatedResources := s.st.GetRelatedResources(p.Resource, p.ResourceId)
	for _, relatedResource := range relatedResources {
		rsType, rsId := store.KeyParts(relatedResource)
		if err := s.es.UpsertFieldElementByID(ctx, rsType+"_search", rsId, p.Resource, p.ResourceId, p.Data); err != nil {
			return err
		}
	}

	return nil
}

func (s *IndexerServer) handleUpdate(ctx context.Context, p *index.UpdatePayload) error {
	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	// var relationsToAdd []*index.Relation
	// var relationsToRemove []*index.Relation
	// var setRelation *index.Relation
	// if len(p.RelationChanges) == 1 && p.RelationChanges[0].ChangeType == index.RelationChange_CHANGE_TYPE_SET {
	// 	setRelation = &index.Relation{
	// 		Resource:   p.RelationChanges[0].Relation.Resource,
	// 		ResourceId: p.RelationChanges[0].Relation.ResourceId,
	// 	}
	// } else {
	// 	for _, rc := range p.RelationChanges {
	// 		switch rc.GetChangeType() {
	// 		case index.RelationChange_CHANGE_TYPE_ADDED:
	// 			relationsToAdd = append(relationsToAdd, &index.Relation{
	// 				Resource:   rc.Relation.Resource,
	// 				ResourceId: rc.Relation.ResourceId,
	// 			})
	// 		case index.RelationChange_CHANGE_TYPE_REMOVED:
	// 			relationsToRemove = append(relationsToRemove, &index.Relation{
	// 				Resource:   rc.Relation.Resource,
	// 				ResourceId: rc.Relation.ResourceId,
	// 			})
	// 		}
	// 	}
	// }

	// s.st.UpdateRelations(p.Resource, p.ResourceId, store.RelationChangesParameter{
	// 	SetRelation:     setRelation,
	// 	AddRelations:    relationsToAdd,
	// 	RemoveRelations: relationsToRemove,
	// })

	// Update the main document
	if err := s.es.UpsertJSON(ctx, p.Resource+"_search", p.ResourceId, map[string]any{
		"fields": p.Data,
	}); err != nil {
		return err
	}

	relatedResources := s.st.GetRelatedResources(p.Resource, p.ResourceId)
	for _, relatedResource := range relatedResources {
		rsType, rsId := store.KeyParts(relatedResource)
		if err := s.es.UpsertFieldElementByID(ctx, rsType+"_search", rsId, p.Resource, p.ResourceId, p.Data); err != nil {
			return err
		}
	}

	return nil
}
