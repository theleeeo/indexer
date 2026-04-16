package core

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/theleeeo/indexer/model"
	"github.com/theleeeo/indexer/source"
)

// RegisterChange handles a single change notification from a source service.
// It determines which root search documents are affected and enqueues
// rebuild (or delete) jobs for each.
func (idx *Indexer) RegisterChange(ctx context.Context, n source.Notification) error {
	if err := idx.verifyResourceConfig(n); err != nil {
		return err
	}

	// Track the resource itself in the resources table.
	res := model.Resource{Type: n.ResourceType, Id: n.ResourceID}
	if n.Kind == source.ChangeDeleted {
		if err := idx.st.DeleteResource(ctx, res); err != nil {
			return fmt.Errorf("delete resource %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
	} else {
		if err := idx.st.UpsertResource(ctx, res); err != nil {
			return fmt.Errorf("upsert resource %s/%s: %w", n.ResourceType, n.ResourceID, err)
		}
	}

	roots := []model.Resource{
		// The resource itself is always affected
		{Type: n.ResourceType, Id: n.ResourceID},
	}

	// Determine which parents are affected.
	for _, rCfg := range idx.resources {
		if rCfg.Resource == n.ResourceType {
			continue
		}

		hasRelation := false
		for _, rel := range rCfg.Relations {
			if rel.Resource == n.ResourceType {
				hasRelation = true
				break
			}
		}

		if !hasRelation {
			continue
		}

		// TODO: Get all parents in one go, no matter the type.
		parents, err := idx.st.GetParentResourcesOfType(ctx, model.Resource{Type: n.ResourceType, Id: n.ResourceID}, rCfg.Resource)
		if err != nil {
			return fmt.Errorf("getting parents: %w", err)
		}

		roots = append(roots, parents...)
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
			return fmt.Errorf("enqueueing job for root %s|%s: %w", root.Type, root.Id, err)
		}
	}

	return nil
}
