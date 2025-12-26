package app

import (
	"context"
	"fmt"
	"indexer/gen/index/v1"
	"time"
)

// TODO: Both here and when creating/setting relations, we need to validate that the relations exist in the schema
func (a *App) RegisterCreate(ctx context.Context, p *index.CreatePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	// TODO: Correct "OccurredAt"
	// TODO: Payload not bound to proto
	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "create", time.Now(), p, nil); err != nil {
		return fmt.Errorf("enqueue create job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterUpdate(ctx context.Context, p *index.UpdatePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "update", time.Now(), p, nil); err != nil {
		return fmt.Errorf("enqueue update job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterDelete(ctx context.Context, p *index.DeletePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "delete", time.Now(), p, nil); err != nil {
		return fmt.Errorf("enqueue delete job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterAddRelation(ctx context.Context, p *index.AddRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "add_relation", time.Now(), p, nil); err != nil {
		return fmt.Errorf("enqueue add relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterRemoveRelation(ctx context.Context, p *index.RemoveRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "remove_relation", time.Now(), p, nil); err != nil {
		return fmt.Errorf("enqueue remove relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterSetRelation(ctx context.Context, p *index.SetRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "set_relation", time.Now(), p, nil); err != nil {
		return fmt.Errorf("enqueue set relation job failed: %w", err)
	}

	return nil
}
