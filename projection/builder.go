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

	ordered, err := resolveOrder(rootType, rCfg.Relations)
	if err != nil {
		return nil, fmt.Errorf("resolve relation order for %s: %w", rootType, err)
	}

	resolved := map[string][]map[string]any{
		rootType: {data},
	}

	for _, rel := range ordered {
		sourceData, ok := resolved[rel.Key.Source]
		if !ok || len(sourceData) == 0 {
			continue
		}

		keyVal, ok := sourceData[0][rel.Key.Field]
		if !ok {
			continue
		}

		keyStr, ok := keyVal.(string)
		if !ok {
			continue
		}

		relatedResp, err := b.provider.FetchRelated(ctx, source.FetchRelatedParams{
			RootResource: rootType,
			ResourceType: rel.Resource,
			Key:          keyStr,
			SourceField:  rel.Key.Field,
		})
		if err != nil {
			return nil, fmt.Errorf("fetch related %s for %s/%s: %w", rel.Resource, rootType, rootID, err)
		}

		resolved[rel.Resource] = relatedResp.Related

		subResources := make([]map[string]any, 0, len(relatedResp.Related))
		var children []store.Relation
		for _, r := range relatedResp.Related {
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

// resolveOrder topologically sorts relations so that dependencies (relations
// whose key source is another relation) are resolved before their dependants.
func resolveOrder(rootType string, relations []resource.RelationConfig) ([]resource.RelationConfig, error) {
	byResource := make(map[string]resource.RelationConfig, len(relations))
	for _, r := range relations {
		byResource[r.Resource] = r
	}

	var ordered []resource.RelationConfig
	visited := make(map[string]bool)
	inStack := make(map[string]bool)

	var visit func(rel resource.RelationConfig) error
	visit = func(rel resource.RelationConfig) error {
		if inStack[rel.Resource] {
			return fmt.Errorf("cycle detected involving %q", rel.Resource)
		}
		if visited[rel.Resource] {
			return nil
		}
		inStack[rel.Resource] = true

		if rel.Key.Source != rootType {
			dep, ok := byResource[rel.Key.Source]
			if !ok {
				return fmt.Errorf("key source %q not found among relations", rel.Key.Source)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}

		inStack[rel.Resource] = false
		visited[rel.Resource] = true
		ordered = append(ordered, rel)
		return nil
	}

	for _, rel := range relations {
		if err := visit(rel); err != nil {
			return nil, err
		}
	}

	return ordered, nil
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
