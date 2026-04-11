package projection

import (
	"context"
	"fmt"

	"github.com/theleeeo/indexer/aggregation"
	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/source"
	"github.com/theleeeo/indexer/store"
)

// BuildRequest is the request parameter for the aggregation plan.
type BuildRequest struct {
	ResourceType string
	ResourceID   string
}

// buildDoc is the intermediate document flowing through the aggregation plan.
// It carries the final doc, the resolved data for chained relations, and root info.
type buildDoc struct {
	Doc      map[string]any
	Resolved map[string][]map[string]any
	Root     model.Resource
}

type Builder struct {
	provider  source.Provider
	resources resource.Configs
	st        *store.PostgresStore

	plans map[string]aggregation.Executor[buildDoc, BuildRequest]
}

func NewBuilder(provider source.Provider, resources resource.Configs, st *store.PostgresStore) *Builder {
	b := &Builder{
		provider:  provider,
		resources: resources,
		st:        st,
	}
	b.plans = b.buildPlans()
	return b
}

func (b *Builder) SetResourceConfig(resources resource.Configs) {
	b.resources = resources
	b.plans = b.buildPlans()
}

// buildPlans constructs an aggregation plan for each resource type in the config.
func (b *Builder) buildPlans() map[string]aggregation.Executor[buildDoc, BuildRequest] {
	plans := make(map[string]aggregation.Executor[buildDoc, BuildRequest], len(b.resources))
	for _, rCfg := range b.resources {
		plans[rCfg.Resource] = b.buildPlanForResource(rCfg)
	}
	return plans
}

// buildPlanForResource creates a RootPlan for the resource and chains SubPlans
// for each relation in topological order.
func (b *Builder) buildPlanForResource(rCfg *resource.Config) aggregation.Executor[buildDoc, BuildRequest] {
	// Root plan: fetches the root resource and initialises the buildDoc.
	rootPlan := aggregation.NewRootPlan(func(params aggregation.FetchParameters[BuildRequest]) (aggregation.FetchResult[buildDoc], error) {
		data, err := b.provider.FetchResource(context.Background(), params.Request.ResourceType, params.Request.ResourceID)
		if err != nil {
			return aggregation.FetchResult[buildDoc]{}, fmt.Errorf("fetch resource %s/%s: %w", params.Request.ResourceType, params.Request.ResourceID, err)
		}

		root := model.Resource{Type: params.Request.ResourceType, Id: params.Request.ResourceID}

		if data == nil {
			return aggregation.FetchResult[buildDoc]{Items: []buildDoc{{
				Doc:      nil,
				Resolved: nil,
				Root:     root,
			}}}, nil
		}

		// Clear existing relation edges so we can re-add the current set.
		if err := b.st.RemoveResource(context.Background(), root); err != nil {
			return aggregation.FetchResult[buildDoc]{}, fmt.Errorf("clear relations for %s/%s: %w", root.Type, root.Id, err)
		}

		fields := filterFields(data, rCfg.Fields)

		return aggregation.FetchResult[buildDoc]{
			Items: []buildDoc{{
				Doc: map[string]any{
					"fields": fields,
				},
				Resolved: map[string][]map[string]any{
					rCfg.Resource: {data},
				},
				Root: root,
			}},
		}, nil
	})

	// Resolve the topological order of relations.
	ordered, err := resolveOrder(rCfg.Resource, rCfg.Relations)
	if err != nil {
		// If the config is invalid the plan will never execute successfully,
		// but we defer the error to execution time rather than panicking at
		// startup so that validation can catch it first.
		return rootPlan
	}

	// Chain a SubPlan for each relation.
	var current aggregation.Executor[buildDoc, BuildRequest] = rootPlan
	for _, rel := range ordered {
		current = b.buildRelationSubPlan(current, rCfg.Resource, rel)
	}

	return current
}

// relationFetcher implements aggregation.SubFetcher[buildDoc].
type relationFetcher struct {
	provider     source.Provider
	st           *store.PostgresStore
	rel          resource.RelationConfig
	rootResource string
}

func (f *relationFetcher) Fetch(parent buildDoc) (any, error) {
	if parent.Doc == nil {
		return (*fetchedRelation)(nil), nil
	}

	sourceData, ok := parent.Resolved[f.rel.Key.Source]
	if !ok || len(sourceData) == 0 {
		return &fetchedRelation{}, nil
	}

	var keys []source.ResourceKey
	for _, field := range f.rel.Key.Fields {
		if val, ok := sourceData[0][field]; ok {
			if valStr, ok := val.(string); ok {
				keys = append(keys, source.ResourceKey{Field: field, Value: valStr})
			}
		}
	}
	if len(keys) == 0 {
		return &fetchedRelation{}, nil
	}

	relatedResp, err := f.provider.FetchRelated(context.Background(), source.FetchRelatedParams{
		RootResource: source.RootResource{
			Type: parent.Root.Type,
			Id:   parent.Root.Id,
		},
		ResourceType: f.rel.Resource,
		Keys:         keys,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch related %s for %s/%s: %w", f.rel.Resource, parent.Root.Type, parent.Root.Id, err)
	}

	// Persist discovered relations so AffectedRoots can look them up.
	var children []store.Relation
	for _, r := range relatedResp.Related {
		if id, ok := r["id"]; ok {
			if idStr, ok := id.(string); ok {
				children = append(children, store.Relation{
					Parent: parent.Root,
					Child:  model.Resource{Type: f.rel.Resource, Id: idStr},
				})
			}
		}
	}
	if err := f.st.AddRelations(context.Background(), children); err != nil {
		return nil, fmt.Errorf("persist relations %s for %s/%s: %w", f.rel.Resource, parent.Root.Type, parent.Root.Id, err)
	}

	return &fetchedRelation{Related: relatedResp.Related}, nil
}

// fetchedRelation holds the raw data returned by a relation fetch.
type fetchedRelation struct {
	Related []map[string]any
}

// buildRelationSubPlan creates a SubPlan for a single relation.
func (b *Builder) buildRelationSubPlan(
	parent aggregation.Executor[buildDoc, BuildRequest],
	rootResource string,
	rel resource.RelationConfig,
) *aggregation.SubPlan[buildDoc, buildDoc, BuildRequest] {
	fetcher := &relationFetcher{
		provider:     b.provider,
		st:           b.st,
		rel:          rel,
		rootResource: rootResource,
	}

	builder := func(parentDoc buildDoc, fetchResult any) buildDoc {
		if parentDoc.Doc == nil {
			return parentDoc
		}

		fr, ok := fetchResult.(*fetchedRelation)
		if !ok || fr == nil {
			return parentDoc
		}

		// Update the resolved map so downstream relations can reference this data.
		parentDoc.Resolved[rel.Resource] = fr.Related

		subResources := make([]map[string]any, 0, len(fr.Related))
		for _, r := range fr.Related {
			filtered := filterFields(r, rel.Fields)
			if id, ok := r["id"]; ok {
				filtered["id"] = id
			}
			subResources = append(subResources, filtered)
		}

		parentDoc.Doc[rel.Resource] = subResources
		return parentDoc
	}

	return aggregation.NewSubPlan(parent, fetcher, builder)
}

func (b *Builder) Build(ctx context.Context, rootType, rootID string) (map[string]any, error) {
	plan, ok := b.plans[rootType]
	if !ok {
		return nil, fmt.Errorf("unknown resource type %q", rootType)
	}

	ch, err := plan.Execute(ctx, BuildRequest{
		ResourceType: rootType,
		ResourceID:   rootID,
	})
	if err != nil {
		return nil, err
	}

	for result := range ch {
		if result.Err != nil {
			return nil, result.Err
		}
		if len(result.Items) > 0 {
			return result.Items[0].Doc, nil
		}
	}

	return nil, nil
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
