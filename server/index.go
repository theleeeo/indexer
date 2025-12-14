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
	case *index.ChangeEvent_DeletePayload:
		return s.handleDelete(ctx, p.DeletePayload)
	case *index.ChangeEvent_SetRelationPayload:
		panic("not implemented")
	case *index.ChangeEvent_AddRelationPayload:
		return s.handleAddRelation(ctx, p.AddRelationPayload)
	case *index.ChangeEvent_RemoveRelationPayload:
		panic("not implemented")
	default:
		return fmt.Errorf("unknown payload")
	}
}

type idStruct struct {
	Id string `json:"id"`
}

func (s *IndexerServer) handleCreate(ctx context.Context, p *index.CreatePayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	s.st.StoreRelations(p.Resource, p.ResourceId, p.Relations)

	docMap := map[string]any{
		"fields": p.Data,
	}

	resourceMap := map[string][]string{}
	for _, rel := range p.Relations {
		resourceMap[rel.Resource] = append(resourceMap[rel.Resource], rel.ResourceId)
	}

	// TODO: Weather to make it array or single object should be based on the relation kind from the schema
	for resType, resIds := range resourceMap {
		if len(resIds) == 1 {
			docMap[resType] = idStruct{Id: resIds[0]}
			continue
		}

		idStructs := make([]idStruct, 0, len(resIds))
		for _, rid := range resIds {
			idStructs = append(idStructs, idStruct{Id: rid})
		}
		docMap[resType] = idStructs
	}

	if err := s.es.Upsert(ctx, p.Resource+"_search", p.ResourceId, docMap); err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}

	parentResources := s.st.GetParentResources(p.Resource, p.ResourceId)
	for _, relatedResource := range parentResources {
		rsType, rsId := store.KeyParts(relatedResource)
		if err := s.es.UpsertFieldResourceById(ctx, rsType+"_search", rsId, p.Resource, p.ResourceId, p.Data); err != nil {
			return fmt.Errorf("upsert parent resource failed: %w", err)
		}
	}

	return nil
}

func (s *IndexerServer) handleUpdate(ctx context.Context, p *index.UpdatePayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

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

	// if setRelation != nil {

	// }

	// s.st.UpdateRelations(p.Resource, p.ResourceId, store.RelationChangesParameter{
	// 	SetRelation:     setRelation,
	// 	AddRelations:    relationsToAdd,
	// 	RemoveRelations: relationsToRemove,
	// })

	// Update the main document
	if err := s.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, "fields", p.Data); err != nil {
		return err
	}

	// Update parent documents
	relatedResources := s.st.GetParentResources(p.Resource, p.ResourceId)
	for _, relatedResource := range relatedResources {
		rsType, rsId := store.KeyParts(relatedResource)
		if err := s.es.UpsertFieldResourceById(ctx, rsType+"_search", rsId, p.Resource, p.ResourceId, p.Data); err != nil {
			return err
		}
	}

	return nil
}

func (s *IndexerServer) handleDelete(ctx context.Context, p *index.DeletePayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := s.es.Delete(ctx, p.Resource+"_search", p.ResourceId); err != nil {
		return err
	}

	// TODO: Flag for cascade delete?
	parentResources := s.st.GetParentResources(p.Resource, p.ResourceId)
	for _, relatedResource := range parentResources {
		rsType, rsId := store.KeyParts(relatedResource)
		if err := s.es.DeleteFieldResourceById(ctx, rsType+"_search", rsId, p.Resource, p.ResourceId); err != nil {
			return err
		}
	}

	return nil
}

// TODO: Failes if applied on object, not array
func (s *IndexerServer) handleAddRelation(ctx context.Context, p *index.AddRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	s.st.UpdateRelations(p.Resource, p.ResourceId, store.RelationChangesParameter{
		AddRelations: []*index.Relation{
			{
				Resource:   p.RelationToAdd.Resource,
				ResourceId: p.RelationToAdd.ResourceId,
			},
		},
	})

	if err := s.es.AddFieldResource(ctx, p.Resource+"_search", p.ResourceId, p.RelationToAdd.Resource, map[string]any{
		"id": p.RelationToAdd.ResourceId,
	}); err != nil {
		return err
	}

	return nil
}
