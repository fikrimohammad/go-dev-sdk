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
	Name        string        // Unique identifier (required)
	Schedule    string        // 5-field, 6-field (seconds), or descriptor expression (required)
	Description string        // Human-readable summary for developers
	Timeout     time.Duration // Per-job timeout override (zero = use GlobalTimeout)
	Handler     HandlerFunc   // The executable function (required)
}

// JobInfo holds snapshot metadata for developer inspection.
type JobInfo struct {
	Name         string    `json:"name"`
	Schedule     string    `json:"schedule"`
	Description  string    `json:"description,omitempty"`
	Timeout      string    `json:"timeout"`
	NextRun      time.Time `json:"next_run"`
	PrevRun      time.Time `json:"prev_run"`
	LastStatus   string    `json:"last_status"` // "idle", "running", "success", "failed", "skipped"
	LastDuration string    `json:"last_duration"`
	LastError    string    `json:"last_error,omitempty"`
}

// JobRunner is the engine interface consumed by the dev server.
type JobRunner interface {
	Jobs() []JobInfo
	Job(name string) (JobInfo, bool)
	Trigger(ctx context.Context, name string) error
}
