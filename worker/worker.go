package worker

import (
	"context"
	"fmt"
	"indexer/app"
	"indexer/gen/index/v1"
	"indexer/jobqueue"

	"google.golang.org/protobuf/encoding/protojson"
)

func NewHandlerFunc(app *app.App) jobqueue.Handler {
	return func(ctx context.Context, job jobqueue.Job) error {
		switch job.Type {
		case "create":
			p := &index.CreatePayload{}
			if err := protojson.Unmarshal(job.Payload, p); err != nil {
				return fmt.Errorf("failed to unmarshal payload: %w", err)
			}
			return app.HandleCreate(ctx, p)
		default:
			return fmt.Errorf("unknown job type: %s", job.Type)
		}
	}
}
