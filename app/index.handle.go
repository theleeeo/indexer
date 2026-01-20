package app

import (
	"context"
	"errors"
	"fmt"
	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/resource"
	"indexer/store"
	"log/slog"
)

type idStruct struct {
	Id string `json:"id"`
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

type CreatePayload struct {
	Resource   string
	ResourceId string
	Data       map[string]any
	Relations  []CreateRelationPayload
}

type CreateRelationPayload struct {
	RelatedResource   string
	RelatedResourceId string
	Bidirectional     bool
}

func (a *App) handleCreate(ctx context.Context, p CreatePayload) error {
	logger := slog.With("resource", p.Resource, "resource_id", p.ResourceId)

	rCfg, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	relations := make([]store.Relation, 0, len(p.Relations)*2)
	for _, crp := range p.Relations {
		relations = append(relations, store.Relation{
			Parent: store.Resource{
				Type: p.Resource,
				Id:   p.ResourceId,
			},
			Children: store.Resource{
				Type: crp.RelatedResource,
				Id:   crp.RelatedResourceId,
			},
		})

		if crp.Bidirectional {
			relations = append(relations, store.Relation{
				Parent: store.Resource{
					Type: crp.RelatedResource,
					Id:   crp.RelatedResourceId,
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

	// We must load all child resources from the store instead of using the ones passed in the payload,
	// because there might be existing relations from previously that were not included in the create payload.
	// Example: Create resource A with a bidirectional relation to resource B. When you later create resource B the relation to A exists in the store but not in the payload.
	children, err := a.st.GetChildResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get child resources failed: %w", err)
	}

	docMap := map[string]any{
		"fields": p.Data,
	}

	childRelationMap := map[string][]string{}
	for _, rel := range children {
		childRelationMap[rel.Type] = append(childRelationMap[rel.Type], rel.Id)
	}

	for resType, resIds := range childRelationMap {
		relationConfig := rCfg.GetRelation(resType)
		if relationConfig == nil {
			// This can happen if the resource schema is changed and the relation no longer exists
			logger.Warn("relation does not exist in the schema", "related_resource", resType)
			continue
		}

		subResources := make([]map[string]any, 0, len(resIds))
		for _, rid := range resIds {
			doc, err := a.es.Get(ctx, resType+"_search", rid, []string{"fields"})
			if err != nil {
				return fmt.Errorf("get related doc failed: %w", err)
			}

			if doc == nil {
				subResources = append(subResources, map[string]any{"id": rid})
			} else {
				// Only include the fields defined in the schema
				doc = buildResourceDataFromMap(doc["fields"].(map[string]any), relationConfig.Fields)

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
	logger.Info("created resource")

	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}

	for _, parentResource := range parentResources {
		relRCfg := a.resolveResourceConfig(parentResource.Type)
		if relRCfg == nil {
			logger.Warn("parent resource does not exist in the schema", "parent_resource", parentResource.Type)
			continue
		}

		rf := relRCfg.GetRelation(p.Resource)
		if rf == nil {
			logger.Warn("related resource does not have field for resource", "related_resource", parentResource.Type, "field", p.Resource)
			continue
		}

		if err := a.es.UpsertFieldResourceById(ctx, parentResource.Type+"_search", parentResource.Id, p.Resource, p.ResourceId, buildResourceDataFromMap(p.Data, rf.Fields)); err != nil {
			if errors.Is(err, es.ErrNotFound) {
				logger.Warn("parent resource document not found in index", "parent_resource", parentResource.Type, "parent_resource_id", parentResource.Id)
				continue
			}
			return fmt.Errorf("upsert parent resource failed: %w", err)
		}
	}

	return nil
}

func (a *App) handleUpdate(ctx context.Context, p *index.UpdatePayload) error {
	logger := slog.With("resource", p.Resource, "resource_id", p.ResourceId)

	rCfg, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	// Update the main document
	if err := a.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, "fields", buildResourceData(p.Data, rCfg.Fields)); err != nil {
		return err
	}

	// Update parent documents
	parentResources, err := a.st.GetParentResources(ctx, store.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources failed: %w", err)
	}
	for _, parentResource := range parentResources {
		relRCfg := a.resolveResourceConfig(parentResource.Type)
		if relRCfg == nil {
			logger.Warn("parent resource does not exist in the schema", "parent_resource", parentResource.Type)
			continue
		}

		rf := relRCfg.GetRelation(p.Resource)
		if rf == nil {
			// This can happen if the resource schema is changed and the parent no longer has a relation field for this resource
			logger.Warn("parent resource does not have field for resource", "parent_resource", parentResource.Type, "field", p.Resource)
			continue
		}

		if err := a.es.UpsertFieldResourceById(ctx, parentResource.Type+"_search", parentResource.Id, p.Resource, p.ResourceId, buildResourceData(p.Data, rf.Fields)); err != nil {
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
