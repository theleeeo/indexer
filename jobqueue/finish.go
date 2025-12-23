package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (w *Worker) finish(ctx context.Context, group string, job Job, runErr error) error {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// If we lost the group lease, treat as lease lost.
	var ok bool
	err = tx.QueryRow(ctx, `
		SELECT (locked_by = $1) AND (locked_until IS NOT NULL) AND (locked_until >= now())
		FROM job_groups
		WHERE job_group = $2
	`, w.cfg.WorkerID, group).Scan(&ok)
	if err == nil && !ok {
		_ = tx.Commit(ctx)
		return ErrLeaseLost
	}

	if runErr == nil {
		ct, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status='succeeded',
			    finished_at=now(),
			    locked_until=NULL
			WHERE id=$1 AND locked_by=$2 AND status='running'
		`, job.ID, w.cfg.WorkerID)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			_ = tx.Commit(ctx)
			return ErrLeaseLost
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return nil
	}

	// Decide retry vs dead.
	var retryAfter *time.Duration
	var permanent bool

	var re RetryError
	if errors.As(runErr, &re) {
		d := re.After
		retryAfter = &d
	}

	var pe PermanentError
	if errors.As(runErr, &pe) {
		permanent = true
	}

	// If handler was canceled and we're configured to requeue on cancel:
	if errors.Is(runErr, context.Canceled) {
		d := 0 * time.Second
		retryAfter = &d
		permanent = false
	}

	// Note: attempts was already incremented at claim time.
	attempts := job.Attempts
	maxAttempts := job.MaxAttempts

	if permanent || attempts >= maxAttempts {
		ct, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status='dead',
			    finished_at=now(),
			    last_error=$3,
			    locked_until=NULL
			WHERE id=$1 AND locked_by=$2 AND status='running'
		`, job.ID, w.cfg.WorkerID, truncErr(runErr))
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			_ = tx.Commit(ctx)
			return ErrLeaseLost
		}
		return tx.Commit(ctx)
	}

	// Retryable: if no RetryError was used, apply a default backoff.
	delay := defaultBackoff(attempts)
	if retryAfter != nil {
		delay = *retryAfter
	}

	delayMicros := micros(delay)

	ct, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status='queued',
		    locked_by=NULL,
		    locked_until=NULL,
		    last_error=$3,
		    run_after = now() + ($4::bigint * interval '1 microsecond')
		WHERE id=$1 AND locked_by=$2 AND status='running'
	`, job.ID, w.cfg.WorkerID, truncErr(runErr), delayMicros)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		_ = tx.Commit(ctx)
		return ErrLeaseLost
	}

	return tx.Commit(ctx)
}

func defaultBackoff(attempt int) time.Duration {
	// attempt is 1..N
	// Simple capped exponential-ish backoff
	// 1: 1s, 2: 2s, 3: 4s, 4: 8s, ... cap at 5m
	base := time.Second
	max := 5 * time.Minute

	d := base << (attempt - 1)
	if d > max {
		return max
	}
	return d
}

func truncErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	const max = 2000
	if len(s) > max {
		return fmt.Sprintf("%s…", s[:max])
	}
	return s
}
