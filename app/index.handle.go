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

func (a *App) handleCreate(ctx context.Context, p CreatePayload) error {
	logger := slog.With(slog.String("jobType", "create"), slog.Group("resource", "type", p.Resource, "id", p.ResourceId))

	rCfg, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		// TODO: Persistent errors, non retryable
		return err
	}

	// We must load all child resources from the store instead of using the ones passed in the payload,
	// because there might be existing relations from previously that were not included in the create payload.
	// Example: Create resource A with a bidirectional relation to resource B. When you later create resource B the relation to A exists in the store but not in the payload.
	children, err := a.st.GetChildResources(ctx, model.Resource{Type: p.Resource, Id: p.ResourceId}, false)
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
		if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", r.Type, r.Id), "add_relation", AddRelationPayload{
			Relation: store.Relation{
				Parent: model.Resource{
					Type: r.Type,
					Id:   r.Id,
				},
				Child: model.Resource{
					Type: p.Resource,
					Id:   p.ResourceId,
				},
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
	parentResources, err := a.st.GetParentResources(ctx, model.Resource{Type: resourceType, Id: resourceId}, false)
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
	logger := slog.With(slog.String("jobType", "update"), slog.Group("resource", "type", p.Resource.Type, "id", p.Resource.Id))

	rCfg, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	// Update the main document
	if err := a.es.UpdateField(ctx, p.Resource.Type+"_search", p.Resource.Id, "fields", buildResourceData(p.Data, rCfg.Fields)); err != nil {
		return err
	}

	// Update parent documents
	parentResources, err := a.st.GetParentResources(ctx, model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}, false)
	if err != nil {
		return fmt.Errorf("get parent resources: %w", err)
	}
	for _, parentResource := range parentResources {
		relRCfg := a.resolveResourceConfig(parentResource.Type)
		if relRCfg == nil {
			logger.Warn("parent resource does not exist in the schema", "parent_resource", parentResource.Type)
			continue
		}

		rf := relRCfg.GetRelation(p.Resource.Type)
		if rf == nil {
			// This can happen if the resource schema is changed and the parent no longer has a relation field for this resource
			logger.Warn("parent resource does not have field for resource", "parent_resource", parentResource.Type, "field", p.Resource.Type)
			continue
		}

		if err := a.es.UpsertFieldResourceById(ctx, parentResource.Type+"_search", parentResource.Id, p.Resource.Type, p.Resource.Id, buildResourceData(p.Data, rf.Fields)); err != nil {
			return err
		}
	}

	return nil
}

func (a *App) handleDelete(ctx context.Context, p *index.DeletePayload) error {
	_, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	if err := a.es.Delete(ctx, p.Resource.Type+"_search", p.Resource.Id); err != nil {
		return err
	}

	// TODO: Flag for cascade delete?
	parentResources, err := a.st.GetParentResources(ctx, model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}, false)
	if err != nil {
		return fmt.Errorf("get parent resources: %w", err)
	}
	for _, relatedResource := range parentResources {
		if err := a.es.RemoveFieldResourceById(ctx, relatedResource.Type+"_search", relatedResource.Id, p.Resource.Type, p.Resource.Id); err != nil {
			return fmt.Errorf("remove from parent resource: %w", err)
		}
	}

	if err := a.st.RemoveResource(ctx, model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}); err != nil {
		return fmt.Errorf("remove relations: %w", err)
	}

	return nil
}

type AddRelationPayload struct {
	Relation store.Relation
}

// TODO: Validate relation in schema
func (a *App) handleAddRelation(ctx context.Context, p AddRelationPayload) error {
	logger := slog.With(slog.String("jobType", "create"), slog.Group("resource", "type", p.Relation.Parent.Type, "id", p.Relation.Parent.Id), slog.Group("related_resource", "type", p.Relation.Child.Type, "id", p.Relation.Child.Id))

	_, err := a.verifyResourceConfig(p.Relation.Parent.Type, p.Relation.Parent.Id)
	if err != nil {
		return err
	}

	exists, err := a.st.RelationExists(ctx, p.Relation)
	if err != nil {
		return fmt.Errorf("check relation exists: %w", err)
	}

	if !exists {
		logger.Info("the relation was removed before it could be processed, skipping")
		return nil
	}

	doc, err := a.es.Get(ctx, p.Relation.Child.Type+"_search", p.Relation.Child.Id, []string{"fields"})
	if err != nil {
		return fmt.Errorf("get child document: %w", err)
	}

	doc = buildResourceDataFromMap(doc["fields"].(map[string]any), a.resolveResourceConfig(p.Relation.Child.Type).Fields)
	doc["id"] = p.Relation.Child.Id

	if err := a.es.UpsertFieldResourceById(ctx, p.Relation.Parent.Type+"_search", p.Relation.Parent.Id, p.Relation.Child.Type, p.Relation.Child.Id, doc); err != nil {
		return err
	}

	return nil
}

type RemoveRelationPayload struct {
	Relation store.Relation
}

func (a *App) handleRemoveRelation(ctx context.Context, p RemoveRelationPayload) error {
	logger := slog.With(slog.String("jobType", "remove_relation"), slog.Group("resource", "type", p.Relation.Parent.Type, "id", p.Relation.Parent.Id), slog.Group("related_resource", "type", p.Relation.Child.Type, "id", p.Relation.Child.Id))

	_, err := a.verifyResourceConfig(p.Relation.Parent.Type, p.Relation.Parent.Id)
	if err != nil {
		return err
	}

	exists, err := a.st.RelationExists(ctx, p.Relation)
	if err != nil {
		return fmt.Errorf("check relation exists: %w", err)
	}

	if exists {
		logger.Info("the relation was re-added before it could be processed, skipping")
		return nil
	}

	if err := a.es.RemoveFieldResourceById(ctx, p.Relation.Parent.Type+"_search", p.Relation.Parent.Id, p.Relation.Child.Type, p.Relation.Child.Id); err != nil {
		return err
	}

	if err := a.st.PersistRemoveRelation(ctx, p.Relation); err != nil {
		return fmt.Errorf("persist remove relation: %w", err)
	}

	return nil
}
