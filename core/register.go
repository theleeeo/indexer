package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/source"
)

// RegisterChange handles a single change notification from a source service.
// It determines which root search documents are affected and enqueues
// rebuild (or delete) jobs for each.
func (idx *Indexer) RegisterChange(ctx context.Context, n source.Notification) error {
	if n.ResourceType == "" {
		return &InvalidArgumentError{Msg: "resource_type is required"}
	}
	if n.ResourceID == "" {
		return &InvalidArgumentError{Msg: "resource_id is required"}
	}

	// Determine which root documents are affected.
	roots, err := idx.builder.AffectedRoots(ctx, n.ResourceType, n.ResourceID)
	if err != nil {
		return fmt.Errorf("determining affected roots: %w", err)
	}

	slog.Info("registering change",
		"resource_type", n.ResourceType,
		"resource_id", n.ResourceID,
		"kind", n.Kind.String(),
		"affected_roots", len(roots),
	)

	for _, root := range roots {
		jobType := "rebuild"

		// If this is a delete of a root resource itself, enqueue a delete job.
		if n.Kind == source.ChangeDeleted && root.Type == n.ResourceType && root.Id == n.ResourceID {
			jobType = "delete"
		}

		jobGroup := fmt.Sprintf("%s|%s", root.Type, root.Id)
		if _, err := idx.queue.Enqueue(ctx, jobGroup, jobType, RebuildPayload{
			ResourceType: root.Type,
			ResourceID:   root.Id,
		}, nil); err != nil {
			return fmt.Errorf("enqueue %s job for %s/%s: %w", jobType, root.Type, root.Id, err)
		}
	}

	return nil
}
