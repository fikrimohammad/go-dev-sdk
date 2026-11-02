// Package cron defines the job model shared by the cron engine and its
// plug-ins: a [Job] with a [HandlerFunc], the [Toggler] that can disable a job,
// and the [Locker] and [LockToken] that keep a job running on one worker at a
// time. A job's fencing token travels in its context (see [WithFencingToken]).
// The engine that schedules and runs jobs lives in the engine subpackage.
package cron

import (
	"context"
	"time"
)

// HandlerFunc is the business logic executed for a cron job.
type HandlerFunc func(ctx context.Context) error

// Toggler checks whether a job is enabled before execution.
type Toggler interface {
	IsEnabled(ctx context.Context, jobName string) (bool, error)
}

// LockToken represents an acquired distributed lock carrying a fencing token.
type LockToken interface {
	// FencingToken returns the monotonically increasing token or sequence identifier (e.g. integer string, UUID, snowflake).
	FencingToken() string
	// Unlock releases the distributed lock.
	Unlock(ctx context.Context) error
}

// Locker acquires distributed locks for cron jobs.
type Locker interface {
	// Lock attempts to acquire a lock for the given resource key with a TTL.
	// If the lock is already held, it must return ErrLockHeld.
	Lock(ctx context.Context, key string, ttl time.Duration) (LockToken, error)
}

// Job specifies a scheduled task.
type Job struct {
	// Name is the unique identifier of the job. It is required.
	Name string

	// Schedule is a 5-field, 6-field (with seconds), or descriptor expression,
	// such as "0 3 * * *" or "@every 1h". It is required.
	Schedule string

	// Description is a human-readable summary for developers.
	Description string

	// Timeout overrides the engine's GlobalTimeout for this job. Zero uses
	// GlobalTimeout.
	Timeout time.Duration

	// Handler is the business logic to run. It is required.
	Handler HandlerFunc
}
