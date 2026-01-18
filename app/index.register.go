package app

import (
	"context"
	"fmt"
	"indexer/gen/index/v1"
	"time"
)

// TODO: Both here and when creating/setting relations, we need to validate that the relations exist in the schema
func (a *App) RegisterCreate(ctx context.Context, occuredAt time.Time, p *index.CreatePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	// TODO: Payload not bound to proto
	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "create", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue create job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterUpdate(ctx context.Context, occuredAt time.Time, p *index.UpdatePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "update", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue update job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterDelete(ctx context.Context, occuredAt time.Time, p *index.DeletePayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "delete", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue delete job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterAddRelation(ctx context.Context, occuredAt time.Time, p *index.AddRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "add_relation", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue add relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterRemoveRelation(ctx context.Context, occuredAt time.Time, p *index.RemoveRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "remove_relation", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue remove relation job failed: %w", err)
	}

	return nil
}

func (a *App) RegisterSetRelation(ctx context.Context, occuredAt time.Time, p *index.SetRelationPayload) error {
	_, err := a.verifyResourceConfig(p.Resource, p.ResourceId)
	if err != nil {
		return err
	}

	if occuredAt.IsZero() {
		occuredAt = time.Now()
	}

	if _, err := a.queue.Enqueue(ctx, fmt.Sprintf("%s|%s", p.Resource, p.ResourceId), "set_relation", occuredAt, p, nil); err != nil {
		return fmt.Errorf("enqueue set relation job failed: %w", err)
	}

	return nil
}
