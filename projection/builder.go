package projection

import (
	"context"
	"fmt"

	"github.com/theleeeo/indexer/aggregation"
	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/resource"
	"github.com/theleeeo/indexer/store"
)

// BuildRequest is the request parameter for the aggregation plan.
type BuildRequest struct {
	ResourceType string
	ResourceID   string
}

// BuildDoc is the intermediate document flowing through the aggregation plan.
// It carries the final doc, the resolved data for chained relations, and root info.
type BuildDoc struct {
	Root model.Resource

	Doc      map[string]any
	Resolved map[string][]map[string]any

	Relations []model.Resource
}

// Plan is an aggregation executor that produces BuildDoc results.
type Plan = aggregation.Executer[BuildRequest, BuildDoc]

type Builder struct {
	resources resource.Configs
	st        *store.PostgresStore

	plans map[string]Plan
}

func NewBuilder(plans map[string]Plan, resources resource.Configs, st *store.PostgresStore) *Builder {
	return &Builder{
		plans:     plans,
		resources: resources,
		st:        st,
	}
}

func (b *Builder) SetPlans(plans map[string]Plan, resources resource.Configs) {
	b.plans = plans
	b.resources = resources
}

func (b *Builder) Build(ctx context.Context, rootType, rootID string) (map[string]any, error) {
	plan, ok := b.plans[rootType]
	if !ok {
		return nil, fmt.Errorf("unknown resource type %q", rootType)
	}

	if err := b.st.RemoveResource(ctx, model.Resource{Type: rootType, Id: rootID}); err != nil {
		return nil, fmt.Errorf("clear relations for %s/%s: %w", rootType, rootID, err)
	}

	ch := plan.Execute(ctx, BuildRequest{
		ResourceType: rootType,
		ResourceID:   rootID,
	})

	for result := range ch {
		if result.Err != nil {
			return nil, result.Err
		}
		if len(result.Items) > 0 {
			doc := result.Items[0].Doc
			// TODO: Race condition where childs can be changed between fetch and persist.
			if err := b.st.AddChildResources(ctx,
				model.Resource{Type: rootType, Id: rootID},
				result.Items[0].Relations,
			); err != nil {
				return nil, fmt.Errorf("persist relations for %s/%s: %w", rootType, rootID, err)
			}

			return doc, nil
		}
	}

	return nil, nil
}

func (b *Builder) AffectedRoots(ctx context.Context, resourceType, resourceID string) ([]model.Resource, error) {
	var roots []model.Resource

	// TODO: Should not be able to fail?
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
