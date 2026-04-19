package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/es"
	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/projection"
)

// RebuildPayload is the job payload for both "rebuild" and "delete" jobs.
type RebuildPayload struct {
	ResourceType string
	ResourceID   string
	Metadata     map[string]string
}

// handleRebuild fetches the full document from the authoritative source via the
// projection builder and upserts it into Elasticsearch. If the builder returns
// nil (resource no longer exists), it falls through to a delete.
func (idx *Indexer) handleRebuild(ctx context.Context, p RebuildPayload) error {
	logger := slog.With(slog.String("jobType", "rebuild"), slog.String("type", p.ResourceType), slog.String("id", p.ResourceID))

	cfg := idx.resources.Get(p.ResourceType)
	versionPlans, ok := idx.plans[p.ResourceType]
	if !ok {
		return fmt.Errorf("unknown resource type %q", p.ResourceType)
	}

	if err := idx.st.RemoveResource(ctx, model.Resource{Type: p.ResourceType, Id: p.ResourceID}); err != nil {
		return fmt.Errorf("removing relations: %w", err)
	}

	var allRelations []model.Resource

	for _, v := range cfg.SortedVersions() {
		plan, ok := versionPlans[v]
		if !ok {
			return fmt.Errorf("no plan for resource %q version %d", p.ResourceType, v)
		}

		ch := plan.Execute(ctx, projection.BuildRequest{
			ResourceType: p.ResourceType,
			ResourceID:   p.ResourceID,
			Metadata:     p.Metadata,
		})

		var result projection.BuildDoc
		for r := range ch {
			if r.Err != nil {
				return r.Err
			}
			if len(r.Items) > 0 {
				result = r.Items[0]
				break
			}
		}

		// If the builder returns nil the resource was deleted at the source.
		if result.Doc == nil {
			logger.Info("resource no longer exists at source, deleting")
			return idx.handleDelete(ctx, p)
		}

		allRelations = append(allRelations, result.Relations...)

		indexName := es.IndexName(p.ResourceType, v)
		if err := idx.es.Upsert(ctx, indexName, p.ResourceID, result.Doc); err != nil {
			return fmt.Errorf("upsert %s/%s to %s: %w", p.ResourceType, p.ResourceID, indexName, err)
		}
	}

	// Persist the union of relations from all versions so that affected-root
	// lookups cover relations introduced in any version.
	if err := idx.st.AddChildResources(ctx,
		model.Resource{Type: p.ResourceType, Id: p.ResourceID},
		allRelations,
	); err != nil {
		return fmt.Errorf("persist relations for %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	logger.Info("rebuilt document")
	return nil
}

// handleDelete removes the document from Elasticsearch and cleans up relations in PG.
func (idx *Indexer) handleDelete(ctx context.Context, p RebuildPayload) error {
	logger := slog.With(slog.String("jobType", "delete"), slog.String("type", p.ResourceType), slog.String("id", p.ResourceID))

	cfg := idx.resources.Get(p.ResourceType)
	for _, v := range cfg.SortedVersions() {
		indexName := es.IndexName(p.ResourceType, v)
		if err := idx.es.Delete(ctx, indexName, p.ResourceID); err != nil {
			return fmt.Errorf("delete %s/%s from %s: %w", p.ResourceType, p.ResourceID, indexName, err)
		}
	}

	// Remove relation edges from PG so stale roots are no longer affected.
	if err := idx.st.RemoveResource(ctx, model.Resource{Type: p.ResourceType, Id: p.ResourceID}); err != nil {
		return fmt.Errorf("clean up relations for %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	logger.Info("deleted document")
	return nil
}
