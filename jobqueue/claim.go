package jobqueue

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (w *Worker) claimGroup(ctx context.Context) (string, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	leaseMicros := micros(w.cfg.LeaseDuration)

	// Pick an unlocked group that has queued runnable jobs; prefer the group whose next ordering_seq is smallest.
	var group string
	err = tx.QueryRow(ctx, `
		WITH q AS (
		  SELECT job_group, min(ordering_seq) AS next_ts
		  FROM jobs
		  WHERE status='queued' AND run_after <= now()
		  GROUP BY job_group
		),
		candidate AS (
		  SELECT g.job_group
		  FROM job_groups g
		  JOIN q ON q.job_group = g.job_group
		  WHERE g.locked_until IS NULL OR g.locked_until < now()
		  ORDER BY q.next_ts
		  LIMIT 1
		  FOR UPDATE SKIP LOCKED
		)
		UPDATE job_groups g
		SET locked_by = $1,
		    locked_until = now() + ($2::bigint * interval '1 microsecond'),
		    updated_at = now()
		FROM candidate c
		WHERE g.job_group = c.job_group
		RETURNING g.job_group
	`, w.cfg.WorkerID, leaseMicros).Scan(&group)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Commit(ctx)
			return "", ErrNoWork
		}
		return "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return group, nil
}

func (w *Worker) claimNextJobInGroup(ctx context.Context, group string) (Job, error) {
	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	leaseMicros := micros(w.cfg.LeaseDuration)

	// Ensure we still hold the group lease (cheap guard).
	var ok bool
	err = tx.QueryRow(ctx, `
		SELECT (locked_by = $1) AND (locked_until IS NOT NULL) AND (locked_until >= now())
		FROM job_groups
		WHERE job_group = $2
		FOR UPDATE
	`, w.cfg.WorkerID, group).Scan(&ok)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Commit(ctx)
			return Job{}, ErrNoWork
		}
		return Job{}, err
	}
	if !ok {
		_ = tx.Commit(ctx)
		return Job{}, ErrLeaseLost
	}

	// Claim earliest queued runnable job by ordering_seq within this group.
	row := tx.QueryRow(ctx, `
	UPDATE jobs
	SET status='running',
		attempts = attempts + 1,
		locked_by = $1,
		locked_until = now() + ($2::bigint * interval '1 microsecond'),
		started_at = now()
	WHERE id = (
		SELECT id
		FROM jobs
		WHERE job_group = $3
		AND status='queued'
		AND run_after <= now()
		ORDER BY ordering_seq
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	)
	RETURNING
		id, job_group, type, ordering_seq, run_after, status, payload,
		attempts, max_attempts, locked_by, locked_until, started_at, finished_at, last_error
	`, w.cfg.WorkerID, leaseMicros, group)

	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			_ = tx.Commit(ctx)
			return Job{}, ErrNoWork
		}
		return Job{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (w *Worker) releaseGroup(ctx context.Context, group string) error {
	_, err := w.pool.Exec(ctx, `
		UPDATE job_groups
		SET locked_by = NULL,
		    locked_until = NULL,
		    updated_at = now()
		WHERE job_group = $1 AND locked_by = $2
	`, group, w.cfg.WorkerID)
	return err
}

func (w *Worker) heartbeat(ctx context.Context, group string, jobID uuid.UUID) (bool, error) {
	leaseMicros := micros(w.cfg.LeaseDuration)

	// Extend group lease
	ct1, err := w.pool.Exec(ctx, `
		UPDATE job_groups
		SET locked_until = now() + ($1::bigint * interval '1 microsecond'),
		    updated_at = now()
		WHERE job_group = $2 AND locked_by = $3
	`, leaseMicros, group, w.cfg.WorkerID)
	if err != nil {
		return false, err
	}
	if ct1.RowsAffected() == 0 {
		return false, nil
	}

	// Extend job lease
	ct2, err := w.pool.Exec(ctx, `
		UPDATE jobs
		SET locked_until = now() + ($1::bigint * interval '1 microsecond')
		WHERE id = $2 AND locked_by = $3 AND status='running'
	`, leaseMicros, jobID, w.cfg.WorkerID)
	if err != nil {
		return false, err
	}
	if ct2.RowsAffected() == 0 {
		return false, nil
	}

	return true, nil
}
