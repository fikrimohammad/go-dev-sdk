package cron

import "errors"

var (
	// ErrLockHeld indicates that the distributed lock is already held by another worker.
	ErrLockHeld = errors.New("cron: lock already held")

	// ErrJobDisabled indicates that the job was disabled by the toggler.
	ErrJobDisabled = errors.New("cron: job is disabled")

	// ErrJobNotFound indicates that the requested job is not registered.
	ErrJobNotFound = errors.New("cron: job not found")

	// ErrJobAlreadyExists indicates that a job with the same name is already registered.
	ErrJobAlreadyExists = errors.New("cron: job already exists")

	// ErrJobRunning indicates that the job is currently executing and was skipped.
	ErrJobRunning = errors.New("cron: job is already running")

	// ErrEngineAlreadyStarted indicates that jobs cannot be registered after the engine has started.
	ErrEngineAlreadyStarted = errors.New("cron: cannot register job after engine has started")

	// ErrEngineStopped indicates that the engine has been stopped or is shutting down.
	ErrEngineStopped = errors.New("cron: engine is stopped")

	// ErrEmptyJobName indicates that the job name was empty.
	ErrEmptyJobName = errors.New("cron: job name must not be empty")

	// ErrNilHandler indicates that the job handler was nil.
	ErrNilHandler = errors.New("cron: handler must not be nil")

	// ErrInvalidSchedule indicates that the cron schedule expression could not be parsed.
	ErrInvalidSchedule = errors.New("cron: invalid schedule expression")
)
