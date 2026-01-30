package app

import (
	"context"
	"errors"
	"fmt"
	"indexer/es"
	"indexer/gen/index/v1"
	"indexer/model"
	"indexer/resource"
	"indexer/store"
	"log/slog"
	"time"
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
	// ParentRelations are the relations where this resource is the child
	ParentResources []model.Resource
}

func (a *App) handleCreate(ctx context.Context, occuredAt time.Time, p CreatePayload) error {
	logger := slog.With(slog.Group("resource", "type", p.Resource, "id", p.ResourceId))

	rCfg, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		// TODO: Persistent errors, non retryable
		return err
	}

	// We must load all child resources from the store instead of using the ones passed in the payload,
	// because there might be existing relations from previously that were not included in the create payload.
	// Example: Create resource A with a bidirectional relation to resource B. When you later create resource B the relation to A exists in the store but not in the payload.
	children, err := a.st.GetChildResources(ctx, model.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get child resources: %w", err)
	}

	docMap, err := a.buildDocument(ctx, rCfg, p.Data, children)
	if err != nil {
		return fmt.Errorf("build document: %w", err)
	}

	if err := a.es.Upsert(ctx, p.Resource+"_search", p.ResourceId, docMap); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}

	logger.Info("created resource")

	// if err := a.addResourceToParents(ctx, p.Resource, p.ResourceId, p.Data); err != nil {
	// 	return fmt.Errorf("add resource to parents: %w", err)
	// }

	for _, r := range p.ParentResources {
		if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", r.Type, r.Id), "add_relation", occuredAt, AddRelationPayload{
			Parent: model.Resource{
				Type: r.Type,
				Id:   r.Id,
			},
			Child: model.Resource{
				Type: p.Resource,
				Id:   p.ResourceId,
			},
		}, nil); err != nil {
			return fmt.Errorf("enqueue add relation job failed: %w", err)
		}
	}

	return nil
}

func (a *App) buildDocument(ctx context.Context, rCfg *resource.Config, fields map[string]any, children []model.Resource) (map[string]any, error) {
	docMap := map[string]any{
		"fields": fields,
	}

	childRelationMap := map[string][]string{}
	for _, rel := range children {
		childRelationMap[rel.Type] = append(childRelationMap[rel.Type], rel.Id)
	}

	for resType, resIds := range childRelationMap {
		relationConfig := rCfg.GetRelation(resType)
		if relationConfig == nil {
			// This can happen if the resource schema is changed and the relation no longer exists
			slog.Warn("relation does not exist in the schema", "related_resource", resType)
			continue
		}

		subResources := make([]map[string]any, 0, len(resIds))
		for _, rid := range resIds {
			doc, err := a.es.Get(ctx, resType+"_search", rid, []string{"fields"})
			if err != nil {
				return nil, fmt.Errorf("get related doc: %w", err)
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

	return docMap, nil
}

// TODO: No, this should be a job on the parent
func (a *App) addResourceToParents(ctx context.Context, resourceType, resourceId string, data map[string]any) error {
	parentResources, err := a.st.GetParentResources(ctx, model.Resource{Type: resourceType, Id: resourceId})
	if err != nil {
		return fmt.Errorf("get parent resources: %w", err)
	}

	for _, parentResource := range parentResources {
		relRCfg := a.resolveResourceConfig(parentResource.Type)
		if relRCfg == nil {
			// TODO: Investigate better handling/warnings in these cases
			slog.Warn("parent resource does not exist in the schema", "parent_resource", parentResource.Type)
			continue
		}

		rf := relRCfg.GetRelation(resourceType)
		if rf == nil {
			slog.Warn("related resource does not have field for resource", "related_resource", parentResource.Type, "field", resourceType)
			continue
		}

		if err := a.es.UpsertFieldResourceById(ctx, parentResource.Type+"_search", parentResource.Id, resourceType, resourceId, buildResourceDataFromMap(data, rf.Fields)); err != nil {
			if errors.Is(err, es.ErrNotFound) {
				slog.Warn("parent resource document not found in index", "parent_resource", parentResource.Type, "parent_resource_id", parentResource.Id)
				continue
			}
			return fmt.Errorf("upsert parent resource: %w", err)
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
	parentResources, err := a.st.GetParentResources(ctx, model.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources: %w", err)
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
	parentResources, err := a.st.GetParentResources(ctx, model.Resource{Type: p.Resource, Id: p.ResourceId})
	if err != nil {
		return fmt.Errorf("get parent resources: %w", err)
	}
	for _, relatedResource := range parentResources {
		if err := a.es.RemoveFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource, p.ResourceId); err != nil {
			return fmt.Errorf("remove from parent resource: %w", err)
		}
	}

	if err := a.st.RemoveResource(ctx, model.Resource{Type: p.Resource, Id: p.ResourceId}); err != nil {
		return fmt.Errorf("remove relations: %w", err)
	}

	return nil
}

type AddRelationPayload struct {
	Parent model.Resource
	Child  model.Resource
}

// TODO: Failes if applied on object, not array
// TODO: Validate that the relation does not alrady exists. Can be done by store.UpdateRelations
// TODO: Validate relation in schema
func (a *App) handleAddRelation(ctx context.Context, p AddRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Parent.Type, p.Parent.Id)
	if err != nil {
		return err
	}

	if err := a.st.AddRelations(ctx,
		[]store.Relation{store.Relation(p)}); err != nil {
		return fmt.Errorf("store relations: %w", err)
	}

	doc, err := a.es.Get(ctx, p.Child.Type+"_search", p.Child.Id, []string{"fields"})
	if err != nil {
		return fmt.Errorf("get child document: %w", err)
	}

	doc = buildResourceDataFromMap(doc["fields"].(map[string]any), a.resolveResourceConfig(p.Child.Type).Fields)
	doc["id"] = p.Child.Id

	// TODO: This should load the related resource data from the store instead of passing only the ID.
	if err := a.es.AddFieldResource(ctx, p.Parent.Type+"_search", p.Parent.Id, p.Child.Type, doc); err != nil {
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
			Parent: model.Resource{Type: p.Resource, Id: p.ResourceId},
			Child:  model.Resource{Type: p.Relation.Resource, Id: p.Relation.ResourceId},
		},
	); err != nil {
		return fmt.Errorf("remove relation: %w", err)
	}

	if err := a.es.RemoveFieldResourceById(ctx, p.Resource+"_search", p.ResourceId, p.Relation.Resource, p.Relation.ResourceId); err != nil {
		return err
	}

	return nil
}

// TODO: Load related resource data from store instead of only passing the ID.
func (a *App) handleSetRelation(ctx context.Context, p *index.SetRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if err := a.st.SetRelations(ctx, model.Resource{Type: p.Resource, Id: p.ResourceId}, []model.Resource{
		{Type: p.Relation.Resource, Id: p.Relation.ResourceId},
	}); err != nil {
		return fmt.Errorf("set relation: %w", err)
	}

	if err := a.es.UpdateField(ctx, p.Resource+"_search", p.ResourceId, p.Relation.Resource, idStruct{Id: p.Relation.ResourceId}); err != nil {
		return err
	}

	return nil
}
