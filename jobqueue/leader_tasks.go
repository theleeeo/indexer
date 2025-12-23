package jobqueue

import (
	"context"
	"time"
)

func ReaperTask(q *Queue, interval time.Duration, log Logger) LeaderTask {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return func(ctx context.Context) error {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := q.ReapExpiredRunning(ctx); err != nil && log != nil {
				log.Printf("reaper error: %v", err)
			}

			t := time.NewTimer(interval)
			defer t.Stop()
			select {
			case <-ctx.Done():
			case <-t.C:
			}
		}
	}
}

func CleanerTask(q *Queue, interval time.Duration, log Logger,
	retainSucceeded, retainDead time.Duration, batchSize, maxBatches int,
) LeaderTask {
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	return func(ctx context.Context) error {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := q.CleanupOnce(ctx, retainSucceeded, retainDead, batchSize, maxBatches); err != nil && log != nil {
				log.Printf("cleanup error: %v", err)
			}
			t := time.NewTimer(interval)
			defer t.Stop()
			select {
			case <-ctx.Done():
			case <-t.C:
			}
		}
	}
}
