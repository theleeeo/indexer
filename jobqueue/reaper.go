package jobqueue

import (
	"context"
)

func (w *Worker) reaperLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		_ = w.q.ReapExpiredRunning(ctx)

		// sleep with jitter
		pct := w.cfg.ReapJitterPct
		sleepWithJitter(ctx, w.cfg.ReapInterval, pct)
	}
}
