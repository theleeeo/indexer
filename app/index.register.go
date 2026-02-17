package app

import (
	"context"
	"fmt"
	"indexer/gen/index/v1"
	"indexer/model"
	"indexer/resource"
	"indexer/store"

	"google.golang.org/protobuf/types/known/structpb"
)

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

func (a *App) RegisterCreate(ctx context.Context, p *index.CreatePayload) error {
	rCfg, err := a.verifyResourceConfig(p.Resource.GetType(), p.Resource.GetId())
	if err != nil {
		return err
	}

	relations, parentResources, err := convertCreateRelationParameters(rCfg, model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}, p.Relations)
	if err != nil {
		return fmt.Errorf("converting relations: %w", err)
	}

	// TODO: Do transactionally with the enqueue?
	// TODO: Should really be SetRelations to have Create be upsert-like
	if err := a.st.AddRelations(ctx, relations); err != nil {
		return fmt.Errorf("adding relations failed: %w", err)
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "create", CreatePayload{
		Resource:        p.Resource.Type,
		ResourceId:      p.Resource.Id,
		Data:            buildResourceData(p.Data, rCfg.Fields),
		ParentResources: parentResources,
	}, nil); err != nil {
		return fmt.Errorf("enqueue create job failed: %w", err)
	}

	return nil
}

func convertCreateRelationParameters(rCfg *resource.Config, resource model.Resource, relationsToCreate []*index.Relation) (relations []store.Relation, parentResources []model.Resource, err error) {
	relations = make([]store.Relation, 0, len(relationsToCreate))
	parentResources = make([]model.Resource, 0, len(relationsToCreate))
	for _, rel := range relationsToCreate {
		if rel.GetResource() == nil {
			return nil, nil, &InvalidArgumentError{Msg: "relation is missing the related resource"}
		}

		rCfgRel := rCfg.GetRelation(rel.Resource.Type)
		if rCfgRel == nil {
			return nil, nil, &InvalidArgumentError{Msg: fmt.Sprintf("relation to resource '%s' is not defined in the schema for resource '%s'", rel.Resource, resource.Type)}
		}

		relations = append(relations, store.Relation{
			Parent: model.Resource{
				Type: resource.Type,
				Id:   resource.Id,
			},
			Child: model.Resource{
				Type: rel.Resource.Type,
				Id:   rel.Resource.Id,
			},
		})

		if rCfgRel.Bidirectional {
			parentResource := model.Resource{
				Type: rel.Resource.Type,
				Id:   rel.Resource.Id,
			}

			r := store.Relation{
				Parent: parentResource,
				Child: model.Resource{
					Type: resource.Type,
					Id:   resource.Id,
				},
			}

			relations = append(relations, r)
			parentResources = append(parentResources, parentResource)
		}
	}

	return relations, parentResources, nil
}

func (a *App) RegisterUpdate(ctx context.Context, p *index.UpdatePayload) error {
	_, err := a.verifyResourceConfig(p.Resource.GetType(), p.Resource.GetId())
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "update", p, nil); err != nil {
		return fmt.Errorf("enqueue update job failed: %w", err)
	}

	return nil
}

// TODO: Do the relation deletions here and not in the job-handler
func (a *App) RegisterDelete(ctx context.Context, p *index.DeletePayload) error {
	_, err := a.verifyResourceConfig(p.Resource.GetType(), p.Resource.GetId())
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "delete", p, nil); err != nil {
		return fmt.Errorf("enqueue delete job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterAddRelation(ctx context.Context, p *index.AddRelationPayload) error {
	if p.Relation == nil {
		return &InvalidArgumentError{Msg: "relation is missing the related resource"}
	}

	rCfg, err := a.verifyResourceConfig(p.Resource.GetType(), p.Resource.GetId())
	if err != nil {
		return err
	}

	relCfg := rCfg.GetRelation(p.Relation.Resource.Type)
	if relCfg == nil {
		return &InvalidArgumentError{Msg: fmt.Sprintf("relation to resource '%s' is not defined in the schema for resource '%s'", p.Relation.Resource, p.Resource)}
	}

	if err := a.persistAddRelation(ctx, store.Relation{
		Parent: model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		Child:  model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
	},
	); err != nil {
		return fmt.Errorf("add relation: %w", err)
	}

	if relCfg.Bidirectional {
		if err := a.persistAddRelation(ctx, store.Relation{
			Parent: model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
			Child:  model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		},
		); err != nil {
			return fmt.Errorf("add bidirectional relation: %w", err)
		}
	}

	if err := a.addDependentRelations(ctx, relCfg.UpdateResources, p); err != nil {
		return fmt.Errorf("updating dependant resources: %w", err)
	}

	return nil
}

// TODO: Error handling includes the resource currently being worked on?
func (a *App) addDependentRelations(ctx context.Context, updateResources []string, p *index.AddRelationPayload) error {
	fmt.Printf("Updating dependent relations for resource '%s|%s' and relation to '%s|%s'. Dependent resources to update: %v\n", p.Resource.Type, p.Resource.Id, p.Relation.Resource.Type, p.Relation.Resource.Id, updateResources)
	for _, resToUpdate := range updateResources {
		for _, rel := range a.resources.Get(resToUpdate).Relations { // Schema validation ensures that this resource exists
			if rel.Dependance != p.Resource.Type {
				continue
			}

			// The resource to update is the parent of the current relation.
			if resToUpdate == p.Resource.Type {
				// Whe know the one parent relation to add the dependant resources to. We need to figure out the child resources to add the relation for.
				panic("not implemented yet")

				// relatedResources, err := a.st.GetChildResourcesOfType(ctx, model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id}, resToUpdate, false)
				// if err != nil {
				// 	return fmt.Errorf("getting child resources: %w", err)
				// }

				// for _, rr := range relatedResources {
				// 	if err := a.persistAddRelation(ctx, store.Relation{
				// 		Parent: model.Resource{Type: resToUpdate, Id: p.Resource.Id},
				// 		Child:  model.Resource{Type: rr.Type, Id: rr.Id},
				// 	},
				// 	); err != nil {
				// 		return fmt.Errorf("add relation to dependant resource: %w", err)
				// 	}
				// }
			} else {
				// The resource to update is the child of the current relation. We need to figure out the parent resources to add the relation for.
				relatedResources, err := a.st.GetParentResourcesOfType(ctx, model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}, resToUpdate, false)
				if err != nil {
					return fmt.Errorf("getting parent resources: %w", err)
				}
				fmt.Printf("parents: %v\n", relatedResources)

				for _, rr := range relatedResources {
					if err := a.persistAddRelation(ctx, store.Relation{
						Parent: model.Resource{Type: resToUpdate, Id: rr.Id},
						Child:  model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
					},
					); err != nil {
						return fmt.Errorf("add relation to dependant resource: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func (a *App) persistAddRelation(ctx context.Context, relation store.Relation) error {
	if err := a.st.AddRelations(ctx,
		[]store.Relation{relation},
	); err != nil {
		return fmt.Errorf("add bidirectional relation: %w", err)
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", relation.Parent.Type, relation.Parent.Id), "add_relation", AddRelationPayload{
		Relation: store.Relation{
			Parent: relation.Parent,
			Child:  relation.Child,
		},
	}, nil); err != nil {
		return fmt.Errorf("enqueue add bidirectional relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterRemoveRelation(ctx context.Context, p *index.RemoveRelationPayload) error {
	rCfg, err := a.verifyResourceConfig(p.Resource.GetType(), p.Resource.GetId())
	if err != nil {
		return err
	}

	relCfg := rCfg.GetRelation(p.Relation.Resource.Type)
	if relCfg == nil {
		return &InvalidArgumentError{Msg: fmt.Sprintf("relation to resource '%s' is not defined in the schema for resource '%s'", p.Relation.Resource, p.Resource)}
	}

	if err := a.persistRemoveRelation(ctx, store.Relation{
		Parent: model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		Child:  model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
	},
	); err != nil {
		return fmt.Errorf("remove relation: %w", err)
	}

	if relCfg.Bidirectional {
		if err := a.persistRemoveRelation(ctx, store.Relation{
			Parent: model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
			Child:  model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		},
		); err != nil {
			return fmt.Errorf("remove bidirectional relation: %w", err)
		}
	}

	return nil
}

// TODO: This have to be done in a transaction to avoid corrupted state
func (a *App) persistRemoveRelation(ctx context.Context, relation store.Relation) error {
	if err := a.st.MarkRemoveRelation(ctx,
		relation,
	); err != nil {
		return fmt.Errorf("remove bidirectional relation: %w", err)
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", relation.Parent.Type, relation.Parent.Id), "remove_relation", RemoveRelationPayload{
		Relation: relation,
	}, nil); err != nil {
		return fmt.Errorf("enqueue remove bidirectional relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterSetRelations(ctx context.Context, p *index.SetRelationsPayload) error {
	rCfg, err := a.verifyResourceConfig(p.Resource.GetType(), p.Resource.GetId())
	if err != nil {
		return err
	}

	resource := model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}

	if err := a.removeChildRelations(ctx, resource); err != nil {
		return fmt.Errorf("removing child relations: %w", err)
	}

	if err := a.removeParentRelations(ctx, resource); err != nil {
		return fmt.Errorf("removing parent relations: %w", err)
	}

	relations, _, err := convertCreateRelationParameters(rCfg, model.Resource{Type: p.Resource.Type, Id: p.Resource.Id}, p.Relations)
	if err != nil {
		return fmt.Errorf("converting relations: %w", err)
	}

	for _, relation := range relations {
		if err := a.persistAddRelation(ctx, relation); err != nil {
			return fmt.Errorf("adding relation: %w", err)
		}
	}

	return nil
}

// TODO: This have to be done in a transaction to avoid corrupted state
func (a *App) removeChildRelations(ctx context.Context, resource model.Resource) error {
	existingChildResources, err := a.st.GetChildResources(ctx, resource, true)
	if err != nil {
		return fmt.Errorf("getting existing child resources: %w", err)
	}

	for _, existingChildResource := range existingChildResources {
		if err := a.persistRemoveRelation(ctx, store.Relation{
			Parent: resource,
			Child:  existingChildResource,
		}); err != nil {
			return fmt.Errorf("removing existing relation to child resource '%s|%s': %w", existingChildResource.Type, existingChildResource.Id, err)
		}
	}

	return nil
}

// TODO: This have to be done in a transaction to avoid corrupted state
func (a *App) removeParentRelations(ctx context.Context, resource model.Resource) error {
	existingParentResources, err := a.st.GetParentResources(ctx, resource, true)
	if err != nil {
		return fmt.Errorf("getting existing parent resources: %w", err)
	}

	for _, existingParentResource := range existingParentResources {
		if err := a.persistRemoveRelation(ctx, store.Relation{
			Parent: existingParentResource,
			Child:  resource,
		}); err != nil {
			return fmt.Errorf("removing existing relation from parent resource '%s|%s': %w", existingParentResource.Type, existingParentResource.Id, err)
		}
	}

	return nil
}
