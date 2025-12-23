package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Queue struct {
	pool *pgxpool.Pool
}

func NewQueue(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

type EnqueueOptions struct {
	RunAfter    *time.Time
	MaxAttempts *int
}

func (q *Queue) Enqueue(
	ctx context.Context,
	jobGroup string,
	jobType string,
	occurredAt time.Time,
	payload any,
	opts *EnqueueOptions,
) (uuid.UUID, error) {
	if jobType == "" {
		return uuid.Nil, fmt.Errorf("type must not be empty")
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return uuid.Nil, err
	}

	runAfter := time.Now()
	if opts != nil && opts.RunAfter != nil {
		runAfter = *opts.RunAfter
	}

	maxAttempts := 0
	if opts != nil && opts.MaxAttempts != nil {
		maxAttempts = *opts.MaxAttempts
	}

	_, err = q.pool.Exec(ctx, `
		INSERT INTO job_groups(job_group) VALUES ($1)
		ON CONFLICT (job_group) DO NOTHING
	`, jobGroup)
	if err != nil {
		return uuid.Nil, err
	}

	var id uuid.UUID
	err = q.pool.QueryRow(ctx, `
		INSERT INTO jobs(job_group, type, occurred_at, run_after, status, payload, max_attempts)
		VALUES ($1, $2, $3, $4, 'queued', $5::jsonb, $6)
		RETURNING id
	`, jobGroup, jobType, occurredAt, runAfter, b, maxAttempts).Scan(&id)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}
