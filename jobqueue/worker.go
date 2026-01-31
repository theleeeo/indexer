package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type WorkerConfig struct {
	WorkerID string

	Concurrency int

	// LeaseDuration controls how long a group/job lease lasts without heartbeats.
	LeaseDuration time.Duration
	// HeartbeatInterval controls how often we extend leases while running a job.
	HeartbeatInterval time.Duration

	// PollInterval is the base sleep when no work is found.
	PollInterval time.Duration
	// MaxBatchPerGroup: while holding a group lease, how many jobs to run back-to-back before releasing.
	MaxBatchPerGroup int

	ReapInterval  time.Duration
	ReapJitterPct float64 // e.g. 0.2 means +/-20%

	// Retention windows for completed jobs.
	RetainSucceeded time.Duration
	RetainDead      time.Duration

	// Batch sizes and limits so one cleanup tick can’t run forever.
	CleanBatchSize     int
	MaxBatchesPerClean int

	CleanInterval  time.Duration
	CleanJitterPct float64

	Logger Logger
}

func (c *WorkerConfig) setDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.MaxBatchPerGroup <= 0 {
		c.MaxBatchPerGroup = 10
	}

	if c.ReapInterval <= 0 {
		c.ReapInterval = 30 * time.Second
	}
	if c.ReapJitterPct <= 0 {
		c.ReapJitterPct = 0.2
	}

	if c.RetainSucceeded <= 0 {
		c.RetainSucceeded = 7 * 24 * time.Hour
	}
	if c.RetainDead <= 0 {
		c.RetainDead = 30 * 24 * time.Hour
	}
	if c.CleanBatchSize <= 0 {
		c.CleanBatchSize = 1000
	}
	if c.MaxBatchesPerClean <= 0 {
		c.MaxBatchesPerClean = 10
	}
	if c.CleanInterval <= 0 {
		c.CleanInterval = 1 * time.Hour
	}
	if c.CleanJitterPct <= 0 {
		c.CleanJitterPct = 0.2
	}
}

type Worker struct {
	q       *Queue
	pool    *pgxpool.Pool
	handler Handler
	cfg     WorkerConfig

	stopFetch atomic.Bool

	loopsWG sync.WaitGroup

	// Track in-flight job cancels so Stop(ctx) can force-cancel if deadline hits.
	inFlightMu sync.Mutex
	inFlight   map[uuid.UUID]context.CancelFunc
}

func NewWorker(pool *pgxpool.Pool, handler Handler, cfg WorkerConfig) *Worker {
	cfg.setDefaults()
	return &Worker{
		q:        NewQueue(pool),
		pool:     pool,
		handler:  handler,
		cfg:      cfg,
		inFlight: make(map[uuid.UUID]context.CancelFunc),
	}
}

func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.cfg.Concurrency; i++ {
		w.loopsWG.Go(func() {
			w.loop(ctx)
		})
	}

	w.loopsWG.Go(func() {
		w.reaperLoop(ctx)
	})

	w.loopsWG.Go(func() {
		w.cleanerLoop(ctx)
	})
}

// Stop gracefully stops fetching new jobs and waits until all loops exit.
// If stopCtx expires, it force-cancels in-flight jobs (so handlers can stop), and then returns.
func (w *Worker) Stop(stopCtx context.Context) error {
	w.stopFetch.Store(true)

	done := make(chan struct{})
	go func() {
		w.loopsWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-stopCtx.Done():
		// Force cancel any in-flight jobs
		w.inFlightMu.Lock()
		for _, cancel := range w.inFlight {
			cancel()
		}
		w.inFlightMu.Unlock()

		// Wait a little for loops to actually exit
		select {
		case <-done:
			return nil
		case <-time.After(2 * time.Second):
			return stopCtx.Err()
		}
	}
}

// Wait blocks until all loops exit (useful if you drive shutdown via ctx cancel).
func (w *Worker) Wait() {
	w.loopsWG.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	// small jitter to avoid synchronized polling
	j := time.Duration(rand.Int63n(int64(w.cfg.PollInterval / 2)))
	timer := time.NewTimer(j)
	select {
	case <-ctx.Done():
		timer.Stop()
		return
	case <-timer.C:
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if w.stopFetch.Load() {
			w.logf("stopping fetch loop")
			return
		}

		group, err := w.claimGroup(ctx)
		if err != nil {
			if errors.Is(err, ErrNoWork) {
				sleepWithJitter(ctx, w.cfg.PollInterval, 0.3)
				continue
			}
			w.logf("claimGroup error: %v", err)
			sleepWithJitter(ctx, w.cfg.PollInterval, 0.5)
			continue
		}

		// We hold the group lease now. Run up to MaxBatchPerGroup jobs sequentially.
		for n := 0; n < w.cfg.MaxBatchPerGroup; n++ {
			if ctx.Err() != nil {
				_ = w.releaseGroup(context.Background(), group) // best-effort
				return
			}
			if w.stopFetch.Load() {
				_ = w.releaseGroup(context.Background(), group)
				return
			}

			job, err := w.claimNextJobInGroup(ctx, group)
			if err != nil {
				if errors.Is(err, ErrNoWork) {
					_ = w.releaseGroup(ctx, group)
					break
				}
				w.logf("claimNextJobInGroup error: %v", err)
				_ = w.releaseGroup(ctx, group)
				break
			}

			w.runOne(ctx, group, job)
		}
	}
}

func (w *Worker) runOne(ctx context.Context, group string, job Job) {
	// Create a cancellable context for the handler.
	jobCtx, cancel := context.WithCancel(ctx)

	// Track in-flight cancel
	w.inFlightMu.Lock()
	w.inFlight[job.ID] = cancel
	w.inFlightMu.Unlock()

	leaseLost := make(chan struct{}, 1)

	// Heartbeat loop
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.cfg.HeartbeatInterval)
		defer t.Stop()

		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				ok, err := w.heartbeat(jobCtx, group, job.ID)
				if err != nil {
					w.logf("heartbeat error (job=%s group=%s): %v", job.ID, group, err)
					continue
				}
				if !ok {
					// Lease was lost; cancel handler context so it can stop if it respects ctx.
					select {
					case leaseLost <- struct{}{}:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	// Execute handler
	err := w.handler(jobCtx, job)

	// Stop heartbeat
	cancel()
	<-hbDone

	// Remove from in-flight
	w.inFlightMu.Lock()
	delete(w.inFlight, job.ID)
	w.inFlightMu.Unlock()

	select {
	case <-leaseLost:
		// Another worker likely took over after lease expiry.
		// Best effort: do not try to finalize; it will be reclaimed/reaped.
		w.logf("lease lost while running job=%s group=%s", job.ID, group)
		return
	default:
	}

	// Finalize job
	if finErr := w.finish(ctx, group, job, err); finErr != nil {
		if errors.Is(finErr, ErrLeaseLost) {
			w.logf("finish: lease lost job=%s group=%s", job.ID, group)
			return
		}
		w.logf("finish error job=%s group=%s: %v", job.ID, group, finErr)
	}
}

func (w *Worker) logf(format string, args ...any) {
	if w.cfg.Logger != nil {
		w.cfg.Logger.Printf(format, args...)
	}
}

func micros(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(d / time.Microsecond)
}

func sleepWithJitter(ctx context.Context, base time.Duration, pct float64) {
	if base <= 0 {
		return
	}
	j := 1.0
	if pct > 0 {
		// random in [1-pct, 1+pct]
		j = (1 - pct) + rand.Float64()*(2*pct)
	}
	d := time.Duration(float64(base) * j)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
