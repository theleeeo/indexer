package app

import (
	"context"
	"fmt"
	"indexer/gen/index/v1"
	"indexer/resource"
	"indexer/store"

	"google.golang.org/protobuf/types/known/structpb"
)

type idStruct struct {
	Id string `json:"id"`
}

func buildResourceData(rawData *structpb.Struct, resourceConfig *resource.Config) map[string]any {
	result := make(map[string]interface{})

	for _, fieldConfig := range resourceConfig.Fields {
		fieldValue, exists := rawData.Fields[fieldConfig.Name]
		if !exists {
			continue
		}

		// result[fieldConfig.Name] = fieldValue.AsInterface()
		result[fieldConfig.Name] = fieldValue
	}

	return result
}

func (a *App) Create(ctx context.Context, p *index.CreatePayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	r := a.resolveResourceConfig(p.Resource)
	if r == nil {
		return ErrUnknownResource
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	relations := make([]store.Relation, 0, len(p.Relations))
	for _, crp := range p.Relations {
		if crp.Relation == nil {
			return &InvalidArgumentError{Msg: "relation is missing the related resource"}
		}

		relations = append(relations, store.Relation{
			Parent: store.Resource{
				Type: p.Resource,
				Id:   p.ResourceId,
			},
			Children: store.Resource{
				Type: crp.Relation.Resource,
				Id:   crp.Relation.ResourceId,
			},
		})

		if crp.TwoWay {
			relations = append(relations, store.Relation{
				Parent: store.Resource{
					Type: crp.Relation.Resource,
					Id:   crp.Relation.ResourceId,
				},
				Children: store.Resource{
					Type: p.Resource,
					Id:   p.ResourceId,
				},
			})
		}
	}
	if err := a.st.AddRelations(ctx, relations); err != nil {
		return fmt.Errorf("store relations failed: %w", err)
	}

	docMap := map[string]any{
		"fields": buildResourceData(p.Data, r),
	}

	resourceMap := map[string][]string{}
	for _, rel := range p.Relations {
		resourceMap[rel.Relation.Resource] = append(resourceMap[rel.Relation.Resource], rel.Relation.ResourceId)
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

	if err := a.es.Upsert(ctx, p.Resource+"_search", p.ResourceId, docMap); err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}

	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}

	for _, relatedResource := range parentResources {
		if err := a.es.UpsertFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId, p.Data); err != nil {
			return fmt.Errorf("upsert parent resource failed: %w", err)
		}
	}

	return nil
}

func (a *App) Update(ctx context.Context, p *index.UpdatePayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	r := a.resolveResourceConfig(p.Resource)
	if r == nil {
		return ErrUnknownResource
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	// Update the main document
	if err := a.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, "fields", buildResourceData(p.Data, r)); err != nil {
		return err
	}

	// Update parent documents
	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}
	for _, relatedResource := range parentResources {
		if err := a.es.UpsertFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId, p.Data); err != nil {
			return err
		}
	}

	return nil
}

func (a *App) Delete(ctx context.Context, p *index.DeletePayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	r := a.resolveResourceConfig(p.Resource)
	if r == nil {
		return ErrUnknownResource
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := a.es.Delete(ctx, p.Resource+"_search", p.ResourceId); err != nil {
		return err
	}

	// TODO: Flag for cascade delete?
	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}
	for _, relatedResource := range parentResources {
		if err := a.es.RemoveFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId); err != nil {
			return fmt.Errorf("remove from parent resource failed: %w", err)
		}
	}

	if err := a.st.RemoveResource(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId}); err != nil {
		return fmt.Errorf("remove relations failed: %w", err)
	}

	return nil
}

// TODO: Failes if applied on object, not array
// TODO: Validate that the relation does not alrady exists. Can be done by store.UpdateRelations
func (a *App) AddRelation(ctx context.Context, p *index.AddRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	r := a.resolveResourceConfig(p.Resource)
	if r == nil {
		return ErrUnknownResource
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := a.st.AddRelations(ctx,
		[]store.Relation{
			{
				Parent:   store.Resource{Type: p.Resource, Id: p.ResourceId},
				Children: store.Resource{Type: p.Relation.Resource, Id: p.Relation.ResourceId},
			},
		}); err != nil {
		return fmt.Errorf("store relations failed: %w", err)
	}

	if err := a.es.AddFieldResource(ctx, p.Resource+"_search", p.ResourceId, p.Relation.Resource, map[string]any{
		"id": p.Relation.ResourceId,
	}); err != nil {
		return err
	}

	return nil
}

func (a *App) RemoveRelation(ctx context.Context, p *index.RemoveRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	r := a.resolveResourceConfig(p.Resource)
	if r == nil {
		return ErrUnknownResource
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := a.st.RemoveRelation(ctx,
		store.Relation{
			Parent:   store.Resource{Type: p.Resource, Id: p.ResourceId},
			Children: store.Resource{Type: p.Relation.Resource, Id: p.Relation.ResourceId},
		},
	); err != nil {
		return fmt.Errorf("remove relation failed: %w", err)
	}

	if err := a.es.RemoveFieldResourceById(ctx, p.Resource+"_search", p.ResourceId, p.Relation.Resource, p.Relation.ResourceId); err != nil {
		return err
	}

	return nil
}

func (a *App) SetRelation(ctx context.Context, p *index.SetRelationPayload) error {
	if p.Resource == "" {
		return fmt.Errorf("resource required")
	}

	r := a.resolveResourceConfig(p.Resource)
	if r == nil {
		return ErrUnknownResource
	}

	if p.ResourceId == "" {
		return fmt.Errorf("resource_id required")
	}

	if err := a.st.SetRelation(ctx,
		store.Relation{
			Parent:   store.Resource{Type: p.Resource, Id: p.ResourceId},
			Children: store.Resource{Type: p.Relation.Resource, Id: p.Relation.ResourceId},
		},
	); err != nil {
		return fmt.Errorf("set relation failed: %w", err)
	}

	if err := a.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, p.Relation.Resource, idStruct{Id: p.Relation.ResourceId}); err != nil {
		return err
	}

	return nil
}
