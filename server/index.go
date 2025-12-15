package server

import (
	"context"
	"fmt"

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

	st store.Store
	es *es.Client
}

func NewIndexer(st store.Store, esClient *es.Client) *IndexerServer {
	return &IndexerServer{
		st: st,
		es: esClient,
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
		return s.handleSetRelation(ctx, p.SetRelationPayload)
	case *index.ChangeEvent_AddRelationPayload:
		return s.handleAddRelation(ctx, p.AddRelationPayload)
	case *index.ChangeEvent_RemoveRelationPayload:
		return s.handleRemoveRelation(ctx, p.RemoveRelationPayload)
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

	if err := s.st.AddRelations(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId}, p.Relations); err != nil {
		return fmt.Errorf("store relations failed: %w", err)
	}

	docMap := map[string]any{
		"fields": p.Data,
	}

	resourceMap := map[string][]string{}
	for _, rel := range p.Relations {
		resourceMap[rel.Resource] = append(resourceMap[rel.Resource], rel.ResourceId)
	}

	// TODO: Weather to make it array or single object should be based on the relation kind from the schema
	for resType, resIds := range resourceMap {
		// TODO: Until we have proper handling of single vs multiple relations, always use array
		// if len(resIds) == 1 {
		// 	docMap[resType] = idStruct{Id: resIds[0]}
		// 	continue
		// }

		idStructs := make([]idStruct, 0, len(resIds))
		for _, rid := range resIds {
			idStructs = append(idStructs, idStruct{Id: rid})
		}
		docMap[resType] = idStructs
	}

	if err := s.es.Upsert(ctx, p.Resource+"_search", p.ResourceId, docMap); err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}

	parentResources, err := s.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}

	for _, relatedResource := range parentResources {
		if err := s.es.UpsertFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId, p.Data); err != nil {
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
	parentResources, err := s.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}
	for _, relatedResource := range parentResources {
		if err := s.es.UpsertFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId, p.Data); err != nil {
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
	parentResources, err := s.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}
	for _, relatedResource := range parentResources {
		if err := s.es.RemoveFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId); err != nil {
			return fmt.Errorf("remove from parent resource failed: %w", err)
		}
	}

	if err := s.st.RemoveResource(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId}); err != nil {
		return fmt.Errorf("remove relations failed: %w", err)
	}

	return nil
}

// TODO: Failes if applied on object, not array
// TODO: Validate that the relation does not alrady exists. Can be done by store.UpdateRelations
func (s *IndexerServer) handleAddRelation(ctx context.Context, p *index.AddRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := s.st.AddRelations(ctx,
		store.Resource{Type: p.Resource, Id: p.ResourceId},
		[]*index.Relation{
			{
				Resource:   p.RelationToAdd.Resource,
				ResourceId: p.RelationToAdd.ResourceId,
			},
		}); err != nil {
		return fmt.Errorf("store relations failed: %w", err)
	}

	if err := s.es.AddFieldResource(ctx, p.Resource+"_search", p.ResourceId, p.RelationToAdd.Resource, map[string]any{
		"id": p.RelationToAdd.ResourceId,
	}); err != nil {
		return err
	}

	return nil
}

func (s *IndexerServer) handleRemoveRelation(ctx context.Context, p *index.RemoveRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := s.st.RemoveRelation(ctx,
		store.Resource{Type: p.Resource, Id: p.ResourceId},
		store.Resource{Type: p.RelationToRemove.Resource, Id: p.RelationToRemove.ResourceId}); err != nil {
		return fmt.Errorf("remove relation failed: %w", err)
	}

	if err := s.es.RemoveFieldResourceById(ctx, p.Resource+"_search", p.ResourceId, p.RelationToRemove.Resource, p.RelationToRemove.ResourceId); err != nil {
		return err
	}

	return nil
}

func (s *IndexerServer) handleSetRelation(ctx context.Context, p *index.SetRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := s.st.SetRelation(ctx,
		store.Resource{Type: p.Resource, Id: p.ResourceId},
		store.Resource{Type: p.RelationToSet.Resource, Id: p.RelationToSet.ResourceId}); err != nil {
		return fmt.Errorf("set relation failed: %w", err)
	}

	if err := s.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, p.RelationToSet.Resource, idStruct{Id: p.RelationToSet.ResourceId}); err != nil {
		return err
	}

	return nil
}
