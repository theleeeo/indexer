package jobqueue

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Optionally expose pool via Queue (if not already).
func (q *Queue) Pool() *pgxpool.Pool { return q.pool }

// ReapExpiredRunning requeues jobs stuck in running whose lease expired.
func (q *Queue) ReapExpiredRunning(ctx context.Context) error {
	_, err := q.pool.Exec(ctx, `
		UPDATE jobs
		SET status='queued',
		    locked_by=NULL,
		    locked_until=NULL,
		    run_after=now(),
		    last_error = COALESCE(last_error,'') || ' | requeued after lease expiry'
		WHERE status='running' AND locked_until IS NOT NULL AND locked_until < now()
	`)
	return err
}

// CleanupOnce deletes old completed jobs and empty groups in bounded batches.
// - retainSucceeded/retainDead: how long to keep succeeded/dead jobs (based on finished_at).
// - batchSize: how many rows to delete per statement.
// - maxBatches: cap to keep the function bounded in time.
func (q *Queue) CleanupOnce(
	ctx context.Context,
	retainSucceeded time.Duration,
	retainDead time.Duration,
	batchSize int,
	maxBatches int,
) error {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if maxBatches <= 0 {
		maxBatches = 10
	}

	if retainSucceeded > 0 {
		if err := q.purgeJobsByRetention(ctx, "succeeded", retainSucceeded, batchSize, maxBatches); err != nil {
			return err
		}
	}
	if retainDead > 0 {
		if err := q.purgeJobsByRetention(ctx, "dead", retainDead, batchSize, maxBatches); err != nil {
			return err
		}
	}

	return q.purgeEmptyGroups(ctx, batchSize, maxBatches)
}

func (q *Queue) purgeJobsByRetention(
	ctx context.Context,
	status string,
	retain time.Duration,
	batchSize int,
	maxBatches int,
) error {
	thresholdMicros := micros(retain)

	for range maxBatches {
		tag, err := q.pool.Exec(ctx, `
			DELETE FROM jobs
			WHERE id IN (
			  SELECT id
			  FROM jobs
			  WHERE status = $1
				AND finished_at IS NOT NULL
				AND finished_at < now() - ($2::bigint * interval '1 microsecond')
			  ORDER BY finished_at
			  LIMIT $3
			  FOR UPDATE SKIP LOCKED
			)
		`, status, thresholdMicros, batchSize)
		if err != nil {
			return err
		}

		if tag.RowsAffected() == 0 {
			return nil
		}
	}

	return nil
}

func (q *Queue) purgeEmptyGroups(ctx context.Context, batchSize int, maxBatches int) error {
	for range maxBatches {
		tag, err := q.pool.Exec(ctx, `
			DELETE FROM job_groups
			WHERE job_group IN (
				SELECT job_group 
				FROM job_groups
				WHERE (locked_until IS NULL OR locked_until < now())
				AND NOT EXISTS (
					SELECT 1 FROM jobs WHERE jobs.job_group = job_groups.job_group
				)
			LIMIT $1
			FOR UPDATE SKIP LOCKED
			)
		`, batchSize)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
	return nil
}
