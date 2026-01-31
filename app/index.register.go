package app

import (
	"context"
	"fmt"
	"indexer/gen/index/v1"
	"indexer/model"
	"indexer/resource"
	"indexer/store"
	"time"

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

func (a *App) RegisterCreate(ctx context.Context, occuredAt time.Time, p *index.CreatePayload) error {
	rCfg, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
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

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "create", occuredAt, CreatePayload{
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

func (a *App) RegisterUpdate(ctx context.Context, occuredAt time.Time, p *index.UpdatePayload) error {
	_, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "update", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue update job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterDelete(ctx context.Context, occuredAt time.Time, p *index.DeletePayload) error {
	_, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "delete", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue delete job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterAddRelation(ctx context.Context, occuredAt time.Time, p *index.AddRelationPayload) error {
	if p.Relation == nil {
		return &InvalidArgumentError{Msg: "relation is missing the related resource"}
	}

	rCfg, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	relCrfg := rCfg.GetRelation(p.Relation.Resource.Type)
	if relCrfg == nil {
		return &InvalidArgumentError{Msg: fmt.Sprintf("relation to resource '%s' is not defined in the schema for resource '%s'", p.Relation.Resource, p.Resource)}
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if err := a.persistAddRelation(ctx, occuredAt, store.Relation{
		Parent: model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		Child:  model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
	},
	); err != nil {
		return fmt.Errorf("add relation: %w", err)
	}

	if relCrfg.Bidirectional {
		if err := a.persistAddRelation(ctx, occuredAt, store.Relation{
			Parent: model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
			Child:  model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		},
		); err != nil {
			return fmt.Errorf("add bidirectional relation: %w", err)
		}
	}

	return nil
}

func (a *App) persistAddRelation(ctx context.Context, occuredAt time.Time, relation store.Relation) error {
	if err := a.st.AddRelations(ctx,
		[]store.Relation{relation},
	); err != nil {
		return fmt.Errorf("add bidirectional relation: %w", err)
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", relation.Parent.Type, relation.Parent.Id), "add_relation", occuredAt, AddRelationPayload{
		Relation: store.Relation{
			Parent: relation.Parent,
			Child:  relation.Child,
		},
	}, nil); err != nil {
		return fmt.Errorf("enqueue add bidirectional relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterRemoveRelation(ctx context.Context, occuredAt time.Time, p *index.RemoveRelationPayload) error {
	rCfg, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	if err != nil {
		return err
	}

	relCrfg := rCfg.GetRelation(p.Relation.Resource.Type)
	if relCrfg == nil {
		return &InvalidArgumentError{Msg: fmt.Sprintf("relation to resource '%s' is not defined in the schema for resource '%s'", p.Relation.Resource, p.Resource)}
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if err := a.persistRemoveRelation(ctx, occuredAt, store.Relation{
		Parent: model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		Child:  model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
	},
	); err != nil {
		return fmt.Errorf("remove relation: %w", err)
	}

	if relCrfg.Bidirectional {
		if err := a.persistRemoveRelation(ctx, occuredAt, store.Relation{
			Parent: model.Resource{Type: p.Relation.Resource.Type, Id: p.Relation.Resource.Id},
			Child:  model.Resource{Type: p.Resource.Type, Id: p.Resource.Id},
		},
		); err != nil {
			return fmt.Errorf("remove bidirectional relation: %w", err)
		}
	}

	return nil
}

func (a *App) persistRemoveRelation(ctx context.Context, occuredAt time.Time, relation store.Relation) error {
	if err := a.st.RemoveRelation(ctx,
		relation,
	); err != nil {
		return fmt.Errorf("remove bidirectional relation: %w", err)
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", relation.Parent.Type, relation.Parent.Id), "remove_relation", occuredAt, RemoveRelationPayload{
		Relation: relation,
	}, nil); err != nil {
		return fmt.Errorf("enqueue remove bidirectional relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterSetRelations(ctx context.Context, occuredAt time.Time, p *index.SetRelationsPayload) error {
	// _, err := a.verifyResourceConfig(p.Resource.Type, p.Resource.Id)
	// if err != nil {
	// 	return err
	// }

	// if p.Relation == nil {
	// 	return &InvalidArgumentError{Msg: "relation is missing the related resource"}
	// }

	// if occuredAt.IsZero() {
	// 	occuredAt = time.Now()
	// }

	// if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource.Type, p.Resource.Id), "set_relation", occuredAt, p, nil); err != nil {
	// 	return fmt.Errorf("enqueue set relation job failed: %w", err)
	// }

	return nil
}
