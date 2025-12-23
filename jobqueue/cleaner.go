package jobqueue

import (
	"context"
)

func (w *Worker) cleanerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		if err := w.q.CleanupOnce(
			ctx,
			w.cfg.RetainSucceeded,
			w.cfg.RetainDead,
			w.cfg.CleanBatchSize,
			w.cfg.MaxBatchesPerClean,
		); err != nil {
			w.logf("cleanup error: %v", err)
		}

		sleepWithJitter(ctx, w.cfg.CleanInterval, w.cfg.CleanJitterPct)
	}
}
