package app

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"indexer/gen/index/v1"
	"indexer/jobqueue"

	"google.golang.org/protobuf/encoding/protojson"
)

func (a *App) HandlerFunc() jobqueue.Handler {
	return func(ctx context.Context, job jobqueue.Job) error {
		switch job.Type {
		case "create":
			p := CreatePayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return a.handleCreate(ctx, job.OccurredAt, p)
		case "update":
			p := &index.UpdatePayload{}
			if err := protojson.Unmarshal(job.Payload, p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return a.handleUpdate(ctx, p)
		case "delete":
			p := &index.DeletePayload{}
			if err := protojson.Unmarshal(job.Payload, p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return a.handleDelete(ctx, p)
		case "add_relation":
			p := AddRelationPayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return a.handleAddRelation(ctx, p)
		case "remove_relation":
			p := &index.RemoveRelationPayload{}
			if err := protojson.Unmarshal(job.Payload, p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return a.handleRemoveRelation(ctx, p)
		case "set_relation":
			p := &index.SetRelationPayload{}
			if err := protojson.Unmarshal(job.Payload, p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return a.handleSetRelation(ctx, p)
		default:
			return fmt.Errorf("unknown job type: %s", job.Type)
		}
	}
}
