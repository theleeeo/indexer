package core

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/theleeeo/indexer/jobqueue"
)

// HandlerFunc returns a jobqueue.Handler that processes rebuild and delete jobs.
// Pass this to jobqueue.NewWorker to create a worker that processes jobs.
func (idx *Indexer) HandlerFunc() jobqueue.Handler {
	return func(ctx context.Context, job jobqueue.Job) error {
		switch job.Type {
		case "rebuild":
			p := RebuildPayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal rebuild payload: %w", err)
			}
			return idx.handleRebuild(ctx, p)
		case "delete":
			p := RebuildPayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal delete payload: %w", err)
			}
			return idx.handleDelete(ctx, p)
		case "full_rebuild":
			p := FullRebuildPayload{}
			if err := json.Unmarshal(job.Payload, &p); err != nil {
				return fmt.Errorf("failed to unmarshal full_rebuild payload: %w", err)
			}
			return idx.handleFullRebuild(ctx, p)
		default:
			return fmt.Errorf("unknown job type: %s", job.Type)
		}
	}
}
