package jobqueue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	StatusQueued    JobStatus = "queued"
	StatusRunning   JobStatus = "running"
	StatusSucceeded JobStatus = "succeeded"
	StatusFailed    JobStatus = "failed"
	StatusDead      JobStatus = "dead"
)

type Job struct {
	ID       uuid.UUID
	JobGroup string

	OccurredAt time.Time
	RunAfter   time.Time

	Type    string
	Payload json.RawMessage

	Attempts    int
	MaxAttempts int

	Status     JobStatus
	StartedAt  *time.Time
	FinishedAt *time.Time
	LastError  *string

	LockedBy    *string
	LockedUntil *time.Time
}

type Handler func(ctx context.Context, job Job) error

type Logger interface {
	Printf(format string, args ...any)
}
