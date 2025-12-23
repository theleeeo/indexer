package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type LeaderTask func(ctx context.Context) error

type LeaderElectorConfig struct {
	ID string

	LockName string

	AcquireInterval  time.Duration
	AcquireJitterPct float64
	MonitorInterval  time.Duration

	// Hook: called once when we become leader, before tasks start.
	// If this returns an error, we will relinquish leadership and re-enter election.
	OnStartLeading OnStartLeadingFunc

	// Hook: called once when leadership stops, after tasks have been canceled/stopped.
	// This should not block too long.
	OnStopLeading OnStopLeadingFunc

	// What to do if a leader task returns an error (non-canceled).
	TaskErrorPolicy TaskErrorPolicy

	Logger Logger
}

type LeaderElector struct {
	pool *pgxpool.Pool
	cfg  LeaderElectorConfig

	lockKey int64

	isLeader atomic.Bool

	mu    sync.Mutex
	tasks []namedTask
}

type namedTask struct {
	name string
	fn   LeaderTask
}

type TaskErrorPolicy int

const (
	// TaskErrorDropLeadership cancels all tasks and re-enters election if any task returns an error.
	TaskErrorDropLeadership TaskErrorPolicy = iota

	// TaskErrorLogAndContinue logs the task error but continues leadership (task is not restarted by default).
	TaskErrorLogAndContinue
)

type LeadershipInfo struct {
	ID       string // node/worker id
	LockName string
	LockKey  int64
	// When leadership was acquired
	StartedAt time.Time
}

type OnStartLeadingFunc func(ctx context.Context, info LeadershipInfo) error
type OnStopLeadingFunc func(ctx context.Context, info LeadershipInfo, reason error)

func NewLeaderElector(pool *pgxpool.Pool, cfg LeaderElectorConfig) (*LeaderElector, error) {
	if pool == nil {
		return nil, fmt.Errorf("pgjobq: pool is nil")
	}
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}
	if cfg.LockName == "" {
		return nil, fmt.Errorf("pgjobq: LockName must not be empty")
	}
	if cfg.AcquireInterval <= 0 {
		cfg.AcquireInterval = 2 * time.Second
	}
	if cfg.AcquireJitterPct <= 0 {
		cfg.AcquireJitterPct = 0.2
	}
	if cfg.MonitorInterval <= 0 {
		cfg.MonitorInterval = 5 * time.Second
	}
	if cfg.TaskErrorPolicy != TaskErrorDropLeadership && cfg.TaskErrorPolicy != TaskErrorLogAndContinue {
		cfg.TaskErrorPolicy = TaskErrorDropLeadership
	}

	return &LeaderElector{
		pool:    pool,
		cfg:     cfg,
		lockKey: hashToBigint(cfg.LockName),
	}, nil
}

func (le *LeaderElector) AddTask(name string, fn LeaderTask) {
	if fn == nil {
		return
	}
	le.mu.Lock()
	defer le.mu.Unlock()
	le.tasks = append(le.tasks, namedTask{name: name, fn: fn})
}

func (le *LeaderElector) IsLeader() bool {
	return le.isLeader.Load()
}

// Run blocks until ctx is done. It continuously tries to become leader.
// When leader, it runs all tasks under a leadership context.
// If leadership is lost (e.g. DB connection drops), tasks are canceled and it re-enters election.
func (le *LeaderElector) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		acquiredConn, ok, err := le.tryAcquire(ctx)
		if err != nil {
			le.logf("leader: acquire error: %v", err)
			sleepWithJitter(ctx, le.cfg.AcquireInterval, le.cfg.AcquireJitterPct)
			continue
		}
		if !ok {
			sleepWithJitter(ctx, le.cfg.AcquireInterval, le.cfg.AcquireJitterPct)
			continue
		}

		info := LeadershipInfo{
			ID:        le.cfg.ID,
			LockName:  le.cfg.LockName,
			LockKey:   le.lockKey,
			StartedAt: time.Now(),
		}

		le.isLeader.Store(true)
		le.logf("leader: acquired (%s)", le.cfg.LockName)

		// Start hook (before tasks)
		if le.cfg.OnStartLeading != nil {
			if err := le.cfg.OnStartLeading(ctx, info); err != nil {
				le.logf("leader: OnStartLeading error: %v (relinquish)", err)
				_ = le.bestEffortUnlock(context.Background(), acquiredConn)
				le.isLeader.Store(false)
				acquiredConn.Release()
				sleepWithJitter(ctx, le.cfg.AcquireInterval, le.cfg.AcquireJitterPct)
				continue
			}
		}

		leadErr := le.runAsLeader(ctx, acquiredConn, info)

		le.isLeader.Store(false)
		le.logf("leader: lost (%s): %v", le.cfg.LockName, leadErr)

		// Stop hook (after tasks stop)
		if le.cfg.OnStopLeading != nil {
			// Avoid blocking forever in stop hook
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			le.cfg.OnStopLeading(stopCtx, info, leadErr)
			cancel()
		}

		acquiredConn.Release()

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (le *LeaderElector) tryAcquire(ctx context.Context) (*pgxpool.Conn, bool, error) {
	// Acquire a dedicated connection from the pool for the advisory lock session.
	c, err := le.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}

	var ok bool
	err = c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1::bigint)`, le.lockKey).Scan(&ok)
	if err != nil {
		c.Release()
		return nil, false, err
	}
	if !ok {
		c.Release()
		return nil, false, nil
	}
	return c, true, nil
}

func (le *LeaderElector) runAsLeader(parent context.Context, conn *pgxpool.Conn, info LeadershipInfo) error {
	leadCtx, cancel := context.WithCancel(parent)
	defer cancel()

	le.mu.Lock()
	tasks := make([]namedTask, len(le.tasks))
	copy(tasks, le.tasks)
	le.mu.Unlock()

	taskErrCh := make(chan error, len(tasks))
	var wg sync.WaitGroup

	for _, t := range tasks {
		wg.Add(1)
		go func(t namedTask) {
			defer wg.Done()
			if err := t.fn(leadCtx); err != nil && !errors.Is(err, context.Canceled) {
				taskErrCh <- fmt.Errorf("leader task %q: %w", t.name, err)
			}
		}(t)
	}

	// Monitor conn in a goroutine so we can also react to task errors.
	monErrCh := make(chan error, 1)
	go func() {
		monErrCh <- le.monitorLeaderConn(leadCtx, conn)
	}()

	var reason error

	for reason == nil {
		select {
		case <-leadCtx.Done():
			reason = leadCtx.Err()
		case err := <-monErrCh:
			// monitorLeaderConn returns ctx.Err() on cancel, or real error on conn loss
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				reason = err
			} else if parent.Err() != nil {
				reason = parent.Err()
			} else {
				// canceled due to us stopping
				reason = err
			}
		case err := <-taskErrCh:
			if le.cfg.TaskErrorPolicy == TaskErrorLogAndContinue {
				le.logf("leader: task error (continuing): %v", err)
				// note: task is not restarted in this simple policy
				continue
			}
			// default: drop leadership
			reason = err
		}
	}

	// Stop tasks and unlock.
	cancel()
	wg.Wait()

	_ = le.bestEffortUnlock(context.Background(), conn)
	return reason
}

func (le *LeaderElector) monitorLeaderConn(ctx context.Context, conn *pgxpool.Conn) error {
	t := time.NewTicker(le.cfg.MonitorInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			// Any error here typically indicates connection/session trouble.
			var one int
			if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
				return err
			}
		}
	}
}

func (le *LeaderElector) bestEffortUnlock(ctx context.Context, conn *pgxpool.Conn) error {
	// If the connection is already dead, this will error; that’s fine.
	var ok bool
	return conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1::bigint)`, le.lockKey).Scan(&ok)
}

func (le *LeaderElector) logf(format string, args ...any) {
	if le.cfg.Logger != nil {
		le.cfg.Logger.Printf(format, args...)
	}
}

func hashToBigint(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	// advisory lock key is bigint; treat as signed int64
	return int64(h.Sum64())
}
