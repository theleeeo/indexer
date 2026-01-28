package jobqueue

import (
	"context"
)

func (w *Worker) reaperLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		if err := w.q.ReapExpiredRunning(ctx); err != nil {
			w.logf("reaper error: %v", err)
		}

		// sleep with jitter
		pct := w.cfg.ReapJitterPct
		sleepWithJitter(ctx, w.cfg.ReapInterval, pct)
	}
}
