package app

import (
	"context"
	"fmt"
	"indexer/gen/index/v1"
	"indexer/resource"
	"indexer/store"
	"log/slog"

	"google.golang.org/protobuf/types/known/structpb"
)

type idStruct struct {
	Id string `json:"id"`
}

func buildResourceData(rawData *structpb.Struct, fields []resource.FieldConfig) map[string]any {
	result := make(map[string]interface{})

	for _, fieldConfig := range fields {
		fieldValue, exists := rawData.Fields[fieldConfig.Name]
		if !exists {
			continue
		}

		result[fieldConfig.Name] = fieldValue
	}

	return result
}

func buildResourceDataFromMap(rawData map[string]any, fields []resource.FieldConfig) map[string]any {
	result := make(map[string]interface{})

	for _, fieldConfig := range fields {
		fieldValue, exists := rawData[fieldConfig.Name]
		if !exists {
			continue
		}

		result[fieldConfig.Name] = fieldValue
	}

	return result
}

func (a *App) handleCreate(ctx context.Context, p *index.CreatePayload) error {
	r, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
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
		"fields": buildResourceData(p.Data, r.Fields),
	}

	relationMap := map[string][]string{}
	for _, rel := range p.Relations {
		relationMap[rel.Relation.Resource] = append(relationMap[rel.Relation.Resource], rel.Relation.ResourceId)
	}

	// TODO: Weather to make it array or single object should be based on the relation kind from the schema
	for resType, resIds := range relationMap {
		// TODO: Until we have proper handling of single vs multiple relations, always use array
		// if len(resIds) == 1 {
		// 	docMap[resType] = idStruct{Id: resIds[0]}
		// 	continue
		// }

		relationConfig := r.GetRelation(resType)
		if relationConfig == nil {
			slog.Warn("relation does not exist in the schema", "related_resource", resType)
			continue
		}

		subResources := make([]map[string]any, 0, len(resIds))
		for _, rid := range resIds {
			doc, err := a.es.Get(ctx, resType+"_search", resIds[0])
			if err != nil {
				return fmt.Errorf("get related doc failed: %w", err)
			}

			if doc == nil {
				subResources = append(subResources, map[string]any{"id": rid})
			} else {
				doc = buildResourceDataFromMap(doc, relationConfig.Fields)

				// Make sure the ID is always set.
				// TODO: This might be redundant if the ES document always contains the ID field
				doc["id"] = rid
				subResources = append(subResources, doc)
			}
		}
		docMap[resType] = subResources

	}

	if err := a.es.Upsert(ctx, p.Resource+"_search", p.ResourceId, docMap); err != nil {
		return fmt.Errorf("upsert failed: %w", err)
	}

	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}

	for _, relatedResource := range parentResources {
		rrc := a.resolveResourceConfig(relatedResource.Type)
		if rrc == nil {
			slog.Warn("related resource does not exist in the schema", "related_resource", relatedResource.Type)
			continue
		}

		rf := rrc.GetRelation(p.Resource)
		if rf == nil {
			// This can happen if the resource schema is changed and the parent no longer has a relation field for this resource
			slog.Warn("related resource does not have field for resource", "related_resource", relatedResource.Type, "field", p.Resource)
			continue
		}

		if err := a.es.UpsertFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId, buildResourceData(p.Data, rf.Fields)); err != nil {
			return fmt.Errorf("upsert parent resource failed: %w", err)
		}
	}

	return nil
}

func (a *App) handleUpdate(ctx context.Context, p *index.UpdatePayload) error {
	r, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	// Update the main document
	if err := a.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, "fields", buildResourceData(p.Data, r.Fields)); err != nil {
		return err
	}

	// Update parent documents
	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}
	for _, relatedResource := range parentResources {
		rrc := a.resolveResourceConfig(relatedResource.Type)
		if rrc == nil {
			slog.Warn("related resource does not exist in the schema", "related_resource", relatedResource.Type)
			continue
		}

		rf := rrc.GetRelation(p.Resource)
		if rf == nil {
			// This can happen if the resource schema is changed and the parent no longer has a relation field for this resource
			slog.Warn("related resource does not have field for resource", "related_resource", relatedResource.Type, "field", p.Resource)
			continue
		}

		if err := a.es.UpsertFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId, buildResourceData(p.Data, rf.Fields)); err != nil {
			return err
		}
	}

	return nil
}

func (a *App) handleDelete(ctx context.Context, p *index.DeletePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
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
// TODO: Validate relation in schema
func (a *App) handleAddRelation(ctx context.Context, p *index.AddRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
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

func (a *App) handleRemoveRelation(ctx context.Context, p *index.RemoveRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
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

func (a *App) handleSetRelation(ctx context.Context, p *index.SetRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
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
