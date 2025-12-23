package jobqueue

import (
	"time"

	"github.com/google/uuid"
)

type JobSort string

const (
	SortOccurredAsc  JobSort = "occurred_asc"
	SortOccurredDesc JobSort = "occurred_desc"
	// SortCreatedDesc  JobSort = "created_desc" // requires created_at
	SortStartedDesc  JobSort = "started_desc"
	SortFinishedDesc JobSort = "finished_desc"
)

type JobQuery struct {
	JobGroup string
	Type     string
	Statuses []JobStatus

	IDPrefix       string // matches id::text ILIKE 'prefix%'
	ErrorContains  string // matches last_error ILIKE '%...%'
	LockedBy       string // matches locked_by = ...
	ExpiredRunning bool   // status='running' AND locked_until < now()

	Since *time.Time
	Until *time.Time

	Limit  int
	Offset int

	IncludePayload bool
	Sort           JobSort
}

type JobListPage struct {
	Jobs  []Job
	Total int64 // total matching rows (uses COUNT(*) query)
}

type Counts struct {
	Total    int64
	ByStatus map[JobStatus]int64
}

type TypeStatusCount struct {
	Type   string
	Status JobStatus
	Count  int64
}

type GroupCounts struct {
	JobGroup string
	Queued   int64
	Running  int64
	Dead     int64
	// The next queued job’s occurred_at (helps show backlog ordering in UI).
	NextOccurredAt *time.Time
}

type ErrorSummary struct {
	Type      string
	JobGroup  *string // optional if grouped by type only
	Status    JobStatus
	ErrorKey  string // short+stable key (hash prefix) for grouping
	ErrorText string // truncated text for display
	Count     int64
	LastSeen  time.Time
}

type JobBasic struct {
	ID         uuid.UUID
	JobGroup   string
	Type       string
	Status     JobStatus
	OccurredAt time.Time
	RunAfter   time.Time

	Attempts    int
	MaxAttempts int

	LockedBy    *string
	LockedUntil *time.Time

	StartedAt  *time.Time
	FinishedAt *time.Time
	LastError  *string

	CreatedAt *time.Time // nil if column not present / not selected
}
