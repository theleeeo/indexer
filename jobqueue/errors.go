package jobqueue

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrNoWork means no eligible group/job was found at this moment.
	ErrNoWork = errors.New("pgjobq: no work available")

	// ErrLeaseLost means we could not extend or finalize a lease, usually because it expired and another worker took over.
	ErrLeaseLost = errors.New("pgjobq: lease lost")
)

type RetryError struct {
	After time.Duration
	Err   error
}

func (e RetryError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("retry after %s", e.After)
	}
	return fmt.Sprintf("retry after %s: %v", e.After, e.Err)
}
func (e RetryError) Unwrap() error { return e.Err }

// RetryAfter wraps an error as retryable after the given delay.
// If err is nil, it still schedules a retry (useful for “not ready yet”).
func RetryAfter(err error, after time.Duration) error {
	return RetryError{After: after, Err: err}
}

type PermanentError struct{ Err error }

func (e PermanentError) Error() string {
	if e.Err == nil {
		return "permanent error"
	}
	return e.Err.Error()
}
func (e PermanentError) Unwrap() error { return e.Err }

// Permanent marks an error as non-retryable (job goes to dead).
func Permanent(err error) error { return PermanentError{Err: err} }
