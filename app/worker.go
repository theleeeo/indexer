package app

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/theleeeo/indexer/jobqueue"
)

func (a *App) HandlerFunc() jobqueue.Handler {
	return func(ctx context.Context, job jobqueue.Job) error {
		switch job.Type {
		case "rebuild":
			p := RebuildPayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal rebuild payload: %w", err)
			}
			return a.handleRebuild(ctx, p)
		case "delete":
			p := RebuildPayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal delete payload: %w", err)
			}
			return a.handleDelete(ctx, p)
		default:
			return fmt.Errorf("unknown job type: %s", job.Type)
		}
	}
}
