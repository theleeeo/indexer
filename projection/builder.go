package projection

import (
	"context"
	"fmt"

	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/source"
	"github.com/theleeeo/indexer/store"
)

type Builder struct {
	provider  source.Provider
	resources resource.Configs
	st        *store.PostgresStore
}

func NewBuilder(provider source.Provider, resources resource.Configs, st *store.PostgresStore) *Builder {
	return &Builder{
		provider:  provider,
		resources: resources,
		st:        st,
	}
}

func (b *Builder) SetResourceConfig(resources resource.Configs) {
	b.resources = resources
}

func (b *Builder) Build(ctx context.Context, rootType, rootID string) (map[string]any, error) {
	rCfg := b.resources.Get(rootType)
	if rCfg == nil {
		return nil, fmt.Errorf("unknown resource type %q", rootType)
	}

	data, err := b.provider.FetchResource(ctx, rootType, rootID)
	if err != nil {
		return nil, fmt.Errorf("fetch resource %s/%s: %w", rootType, rootID, err)
	}

	if data == nil {
		return nil, nil
	}

	fields := filterFields(data, rCfg.Fields)

	doc := map[string]any{
		"fields": fields,
	}

	// Clear existing relation edges for this root so we can re-add the
	// current set. This keeps the PG relation graph in sync with the source.
	parent := model.Resource{Type: rootType, Id: rootID}
	if err := b.st.RemoveResource(ctx, parent); err != nil {
		return nil, fmt.Errorf("clear relations for %s/%s: %w", rootType, rootID, err)
	}

	for _, rel := range rCfg.Relations {
		related, err := b.provider.FetchRelated(ctx, rootType, rootID, rel.Resource)
		if err != nil {
			return nil, fmt.Errorf("fetch related %s for %s/%s: %w", rel.Resource, rootType, rootID, err)
		}

		subResources := make([]map[string]any, 0, len(related))
		var children []store.Relation
		for _, r := range related {
			filtered := filterFields(r, rel.Fields)
			if id, ok := r["id"]; ok {
				filtered["id"] = id
				if idStr, ok := id.(string); ok {
					children = append(children, store.Relation{
						Parent: parent,
						Child:  model.Resource{Type: rel.Resource, Id: idStr},
					})
				}
			}
			subResources = append(subResources, filtered)
		}

		// Persist discovered relations so AffectedRoots can look them up.
		if err := b.st.AddRelations(ctx, children); err != nil {
			return nil, fmt.Errorf("persist relations %s for %s/%s: %w", rel.Resource, rootType, rootID, err)
		}

		doc[rel.Resource] = subResources
	}

	return doc, nil
}

func (b *Builder) AffectedRoots(ctx context.Context, resourceType, resourceID string) ([]model.Resource, error) {
	var roots []model.Resource

	if rCfg := b.resources.Get(resourceType); rCfg != nil {
		roots = append(roots, model.Resource{Type: resourceType, Id: resourceID})
	}

	for _, rCfg := range b.resources {
		if rCfg.Resource == resourceType {
			continue
		}

		hasRelation := false
		for _, rel := range rCfg.Relations {
			if rel.Resource == resourceType {
				hasRelation = true
				break
			}
		}

		if !hasRelation {
			continue
		}

		parents, err := b.st.GetParentResourcesOfType(ctx, model.Resource{Type: resourceType, Id: resourceID}, rCfg.Resource)
		if err != nil {
			return nil, fmt.Errorf("get parents of type %s for %s/%s: %w", rCfg.Resource, resourceType, resourceID, err)
		}

		roots = append(roots, parents...)
	}

	return roots, nil
}

func filterFields(data map[string]any, fields []resource.FieldConfig) map[string]any {
	result := make(map[string]any, len(fields))
	for _, f := range fields {
		if v, ok := data[f.Name]; ok {
			result[f.Name] = v
		}
	}
	return result
}
