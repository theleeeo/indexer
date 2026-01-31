package jobqueue

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var lockedBy *string
	var lockedUntil *time.Time
	var startedAt *time.Time
	var finishedAt *time.Time
	var lastError *string

	err := row.Scan(
		&j.ID,
		&j.JobGroup,
		&j.Type,
		&j.OrderingSeq,
		&j.RunAfter,
		&j.Status,
		&j.Payload,
		&j.Attempts,
		&j.MaxAttempts,
		&lockedBy,
		&lockedUntil,
		&startedAt,
		&finishedAt,
		&lastError,
	)

	if err != nil {
		return Job{}, err
	}

	j.LockedBy = lockedBy
	j.LockedUntil = lockedUntil
	j.StartedAt = startedAt
	j.FinishedAt = finishedAt
	j.LastError = lastError
	return j, nil
}

var _ = uuid.Nil
