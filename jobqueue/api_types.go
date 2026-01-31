package jobqueue

import (
	"time"
)

type JobSort string

const (
	SortOrderAsc     JobSort = "ordering_seq_asc"
	SortOrderDesc    JobSort = "ordering_seq_desc"
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
