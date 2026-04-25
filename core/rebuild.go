package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/projection"
)

// ResourceSelector identifies a set of resources and versions to rebuild.
type ResourceSelector struct {
	ResourceType string
	Versions     []int
	ResourceIDs  []string
}

// FullRebuildPayload is the job payload for a "full_rebuild" job.
type FullRebuildPayload struct {
	Selector ResourceSelector
	Metadata map[string]string
}

// Rebuild validates the selectors and enqueues a "full_rebuild" job per selector.
func (idx *Indexer) Rebuild(ctx context.Context, selectors []ResourceSelector) error {
	if len(selectors) == 0 {
		return &InvalidArgumentError{Msg: "at least one selector is required"}
	}

	for _, sel := range selectors {
		cfg := idx.resources.Get(sel.ResourceType)
		if cfg == nil {
			return fmt.Errorf("resource type %q: %w", sel.ResourceType, ErrUnknownResource)
		}
		for _, v := range sel.Versions {
			if cfg.GetVersion(v) == nil {
				return &InvalidArgumentError{Msg: fmt.Sprintf("resource %q has no version %d", sel.ResourceType, v)}
			}
		}
	}

	for _, sel := range selectors {
		if _, err := idx.river.Insert(ctx, FullRebuildArgs{
			Selector: sel,
			// TODO: Should we allow passing metadata for full rebuilds?
			Metadata: nil,
		}, nil); err != nil {
			return fmt.Errorf("enqueue full_rebuild for %s: %w", sel.ResourceType, err)
		}
	}

	return nil
}

// handleFullRebuild is the job handler for "full_rebuild" jobs.
// For specific resource IDs it rebuilds each one individually.
// For "rebuild all" (empty ResourceIDs) it executes the plan with an empty
// ResourceID, which triggers the plan's paginated ListResources streaming.
func (idx *Indexer) handleFullRebuild(ctx context.Context, p FullRebuildPayload) error {
	logger := slog.With(
		slog.String("jobType", "full_rebuild"),
		slog.String("type", p.Selector.ResourceType),
	)

	cfg := idx.resources.Get(p.Selector.ResourceType)
	if cfg == nil {
		return fmt.Errorf("unknown resource type %q", p.Selector.ResourceType)
	}

	// Resolve which versions to rebuild.
	versions := p.Selector.Versions
	if len(versions) == 0 {
		versions = cfg.SortedVersions()
	}

	// When specific IDs are given, rebuild each individually across the
	// selected versions (same path as incremental change rebuilds).
	if len(p.Selector.ResourceIDs) > 0 {
		return idx.handleFullRebuildByIDs(ctx, logger, p.Selector.ResourceType, p.Selector.ResourceIDs, versions)
	}

	// "Rebuild all": execute each version's plan with an empty ResourceID.
	// The plan streams pages of BuildDocs via provider.ListResources internally.
	return idx.handleFullRebuildAll(ctx, logger, p.Selector.ResourceType, versions, p.Metadata)
}

// handleFullRebuildByIDs rebuilds specific resources across the given versions.
func (idx *Indexer) handleFullRebuildByIDs(ctx context.Context, logger *slog.Logger, resourceType string, ids []string, versions []int) error {
	logger.Info("starting full rebuild for specific IDs", slog.Int("resources", len(ids)), slog.Any("versions", versions))

	var failed int
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := idx.handleRebuildVersions(ctx, resourceType, id, versions); err != nil {
			logger.Warn("rebuild failed for resource", slog.String("id", id), slog.String("error", err.Error()))
			failed++
			continue
		}
	}

	logger.Info("full rebuild complete", slog.Int("total", len(ids)), slog.Int("failed", failed))
	return nil
}

// handleFullRebuildAll streams all resources through each version's plan
// (which internally calls provider.ListResources with pagination) and
// upserts the resulting documents to Elasticsearch.
func (idx *Indexer) handleFullRebuildAll(ctx context.Context, logger *slog.Logger, resourceType string, versions []int, metadata map[string]string) error {
	versionPlans, ok := idx.plans[resourceType]
	if !ok {
		return fmt.Errorf("unknown resource type %q", resourceType)
	}

	logger.Info("starting full rebuild (all resources)", slog.Any("versions", versions))

	// Track relations per resource across versions so we can persist the
	// union after all versions have been processed.
	resourceRelations := make(map[string][]model.Resource)
	// Track which resources we've already cleared old relations for.
	cleaned := make(map[string]bool)

	var totalProcessed, failed int

	for _, v := range versions {
		plan, ok := versionPlans[v]
		if !ok {
			return fmt.Errorf("no plan for resource %q version %d", resourceType, v)
		}

		indexName := es.IndexName(resourceType, v)

		// Execute the plan with empty ResourceID to stream all resources.
		ch := plan.Execute(ctx, projection.BuildRequest{
			ResourceType: resourceType,
			ResourceID:   "",
			Metadata:     metadata,
		})

		for page := range ch {
			if page.Err != nil {
				return fmt.Errorf("plan execution for %s v%d: %w", resourceType, v, page.Err)
			}

			for _, doc := range page.Items {
				id := doc.Root.Id

				// On first encounter, remove stale relations.
				if !cleaned[id] {
					if err := idx.st.RemoveResource(ctx, doc.Root); err != nil {
						logger.Warn("failed to remove relations", slog.String("id", id), slog.String("error", err.Error()))
						failed++
						continue
					}
					cleaned[id] = true
					totalProcessed++
				}

				resourceRelations[id] = append(resourceRelations[id], doc.Relations...)

				if err := idx.es.Upsert(ctx, indexName, id, doc.Doc); err != nil {
					logger.Warn("upsert failed", slog.String("id", id), slog.String("index", indexName), slog.String("error", err.Error()))
					failed++
					continue
				}
			}
		}
	}

	// Persist the union of relations from all versions for each resource.
	for id, rels := range resourceRelations {
		if err := idx.st.AddChildResources(ctx, model.Resource{Type: resourceType, Id: id}, rels); err != nil {
			logger.Warn("failed to persist relations", slog.String("id", id), slog.String("error", err.Error()))
			failed++
		}
	}

	logger.Info("full rebuild complete", slog.Int("total", totalProcessed), slog.Int("failed", failed))
	return nil
}

// handleRebuildVersions rebuilds a single resource for the specified versions.
// It mirrors handleRebuild but only processes the given versions instead of all.
func (idx *Indexer) handleRebuildVersions(ctx context.Context, resourceType, resourceID string, versions []int) error {
	cfg := idx.resources.Get(resourceType)
	versionPlans, ok := idx.plans[resourceType]
	if !ok {
		return fmt.Errorf("unknown resource type %q", resourceType)
	}

	if err := idx.st.RemoveResource(ctx, model.Resource{Type: resourceType, Id: resourceID}); err != nil {
		return fmt.Errorf("removing relations: %w", err)
	}

	var allRelations []model.Resource

	for _, v := range versions {
		plan, ok := versionPlans[v]
		if !ok {
			_ = cfg // keep reference for clarity
			return fmt.Errorf("no plan for resource %q version %d", resourceType, v)
		}

		ch := plan.Execute(ctx, projection.BuildRequest{
			ResourceType: resourceType,
			ResourceID:   resourceID,
			Metadata:     nil,
		})

		var result *projection.BuildDoc
		for r := range ch {
			if r.Err != nil {
				return r.Err
			}
			if len(r.Items) > 0 {
				result = &r.Items[0]
				break
			}
		}

		if result == nil || result.Doc == nil {
			return idx.handleDelete(ctx, RebuildPayload{ResourceType: resourceType, ResourceID: resourceID})
		}

		allRelations = append(allRelations, result.Relations...)

		indexName := es.IndexName(resourceType, v)
		if err := idx.es.Upsert(ctx, indexName, resourceID, result.Doc); err != nil {
			return fmt.Errorf("upsert %s/%s to %s: %w", resourceType, resourceID, indexName, err)
		}
	}

	if err := idx.st.AddChildResources(ctx,
		model.Resource{Type: resourceType, Id: resourceID},
		allRelations,
	); err != nil {
		return fmt.Errorf("persist relations for %s/%s: %w", resourceType, resourceID, err)
	}

	return nil
}
