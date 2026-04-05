package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/model"
)

// RebuildPayload is the job payload for both "rebuild" and "delete" jobs.
type RebuildPayload struct {
	ResourceType string
	ResourceID   string
}

// handleRebuild fetches the full document from the authoritative source via the
// projection builder and upserts it into Elasticsearch. If the builder returns
// nil (resource no longer exists), it falls through to a delete.
func (idx *Indexer) handleRebuild(ctx context.Context, p RebuildPayload) error {
	logger := slog.With(slog.String("jobType", "rebuild"), slog.String("type", p.ResourceType), slog.String("id", p.ResourceID))

	doc, err := idx.builder.Build(ctx, p.ResourceType, p.ResourceID)
	if err != nil {
		return fmt.Errorf("build document for %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	// If the builder returns nil the resource was deleted at the source.
	if doc == nil {
		logger.Info("resource no longer exists at source, deleting")
		return idx.handleDelete(ctx, p)
	}

	indexName := p.ResourceType + "_search"
	if err := idx.es.Upsert(ctx, indexName, p.ResourceID, doc); err != nil {
		return fmt.Errorf("upsert %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	logger.Info("rebuilt document")
	return nil
}

// handleDelete removes the document from Elasticsearch and cleans up relations in PG.
func (idx *Indexer) handleDelete(ctx context.Context, p RebuildPayload) error {
	logger := slog.With(slog.String("jobType", "delete"), slog.String("type", p.ResourceType), slog.String("id", p.ResourceID))

	indexName := p.ResourceType + "_search"
	if err := idx.es.Delete(ctx, indexName, p.ResourceID); err != nil {
		return fmt.Errorf("delete %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	// Remove relation edges from PG so stale roots are no longer affected.
	if err := idx.st.RemoveResource(ctx, model.Resource{Type: p.ResourceType, Id: p.ResourceID}); err != nil {
		return fmt.Errorf("clean up relations for %s/%s: %w", p.ResourceType, p.ResourceID, err)
	}

	logger.Info("deleted document")
	return nil
}
