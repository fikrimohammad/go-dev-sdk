// Package engine schedules and runs cron jobs from the cron package. It wraps
// robfig/cron for the schedules, and runs each job through a middleware chain
// that applies the toggler, the distributed locker, the timeout, panic
// recovery, logging, metrics, and tracing. Create an Engine with [New], register
// jobs, call [Engine.Start], and call [Engine.Stop] with a context on shutdown.
package engine

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	robfigcron "github.com/robfig/cron/v3"

	"github.com/fikrimohammad/go-servicekit/cron"
	"github.com/fikrimohammad/go-servicekit/observability/logs"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

// Option configures an [Engine] in [New].
type Option func(*Engine)

// WithToggler injects the [cron.Toggler] that can disable a job before it runs.
func WithToggler(t cron.Toggler) Option {
	return func(e *Engine) {
		e.toggler = t
	}
}

// WithLocker injects the [cron.Locker] that keeps a job on one worker at a
// time and supplies its fencing token.
func WithLocker(l cron.Locker) Option {
	return func(e *Engine) {
		e.locker = l
	}
}

// WithMetrics injects the metrics client. Without it, the engine uses
// [metrics.Default].
func WithMetrics(m metrics.Client) Option {
	return func(e *Engine) {
		e.metrics = m
	}
}

// WithTracer injects the tracer client. Without it, the engine uses
// [tracer.Default].
func WithTracer(t tracer.Client) Option {
	return func(e *Engine) {
		e.tracer = t
	}
}

// Engine schedules and runs cron jobs, and instruments each run.
type Engine struct {
	cfg    Config
	cron   *robfigcron.Cron
	parser robfigcron.ScheduleParser

	// mu guards jobs, running, and stopping. Register, Start, and Stop take it
	// for writing; Trigger and the run guard take it for reading.
	mu       sync.RWMutex
	jobs     map[string]cron.Job
	running  bool
	stopping bool

	// stopCtx is canceled by Stop when its deadline expires. robfig/cron runs a
	// scheduled job with no context, so stopCtx is the root of those runs; every
	// run also watches it and is canceled with it.
	stopCtx    context.Context //nolint:containedctx // robfig/cron passes no context to scheduled jobs, so the engine lifetime needs one
	stopCancel context.CancelFunc

	// inFlight is the set of job names currently executing, to prevent local overlap.
	inFlight sync.Map // jobName -> struct{}
	activeWg sync.WaitGroup

	toggler cron.Toggler
	locker  cron.Locker
	metrics metrics.Client
	tracer  tracer.Client
}

// New creates and initializes a new Engine.
func New(cfg Config, opts ...Option) (*Engine, error) {
	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	parser := robfigcron.NewParser(
		robfigcron.SecondOptional |
			robfigcron.Minute |
			robfigcron.Hour |
			robfigcron.Dom |
			robfigcron.Month |
			robfigcron.Dow |
			robfigcron.Descriptor,
	)

	stopCtx, stopCancel := context.WithCancel(context.Background()) //nolint:forbidigo // the engine lifetime is a root that no caller owns; Start and New take no context

	e := &Engine{
		cfg:        cfg,
		cron:       robfigcron.New(robfigcron.WithParser(parser)),
		parser:     parser,
		jobs:       make(map[string]cron.Job),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e, nil
}

// Register registers a job with the engine. Must be called before Start().
func (e *Engine) Register(job cron.Job) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running || e.stopping {
		return cron.ErrEngineAlreadyStarted
	}
	if job.Name == "" {
		return cron.ErrEmptyJobName
	}
	if job.Handler == nil {
		return cron.ErrNilHandler
	}

	schedule, err := e.parser.Parse(job.Schedule)
	if err != nil {
		return fmt.Errorf("%w: %w", cron.ErrInvalidSchedule, err)
	}

	if _, exists := e.jobs[job.Name]; exists {
		return cron.ErrJobAlreadyExists
	}

	jobCopy := job
	e.cron.Schedule(schedule, robfigcron.FuncJob(func() {
		_ = e.runJob(e.stopCtx, jobCopy, "scheduled")
	}))
	e.jobs[job.Name] = jobCopy

	return nil
}

// Trigger triggers a job manually by name.
func (e *Engine) Trigger(ctx context.Context, jobName string) error {
	e.mu.RLock()
	job, exists := e.jobs[jobName]
	e.mu.RUnlock()

	if !exists {
		return cron.ErrJobNotFound
	}

	return e.runJob(ctx, job, "manual")
}

func (e *Engine) runJob(parentCtx context.Context, job cron.Job, trigger string) (err error) {
	// Outermost guard: a panic from a Toggler, Locker, or Unlock would otherwise
	// crash the process on a scheduled run. Handler panics are recovered inside
	// the chain, so this only sees the panics the chain does not own.
	defer func() {
		if r := recover(); r != nil {
			logs.Error(parentCtx, "cron job panicked",
				"job", job.Name,
				"trigger", trigger,
				"panic", r,
				"stack", string(debug.Stack()))
			err = fmt.Errorf("%w: %v", errPanic, r)
		}
	}()

	// Guard against starting jobs during or after shutdown.
	e.mu.RLock()
	if e.stopping {
		e.mu.RUnlock()
		return cron.ErrEngineStopped
	}
	e.activeWg.Add(1)
	e.mu.RUnlock()
	defer e.activeWg.Done()

	timeout := job.Timeout
	if timeout <= 0 {
		timeout = e.cfg.GlobalTimeout
	}

	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	// Watch for engine shutdown to signal cancellation promptly. The watcher
	// exits with the run, because cancel runs when the run returns.
	stopped := e.stopCtx.Done()
	go func() {
		select {
		case <-stopped:
			cancel()
		case <-ctx.Done():
		}
	}()

	m := meta{
		job:      job,
		trigger:  trigger,
		timeout:  timeout,
		inFlight: &e.inFlight,
		toggler:  e.toggler,
		locker:   e.locker,
		metrics:  e.metrics,
		tracer:   e.tracer,
	}

	return m.buildChain(job.Handler)(ctx)
}

// Start begins scheduling and executing jobs.
func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.stopping || e.running {
		return
	}
	e.running = true
	e.cron.Start()
}

// Stop initiates a two-phase graceful shutdown: waits for active jobs up to ctx deadline;
// cancels in-flight job contexts if the deadline expires.
// ctx is required. If it has no deadline, Config.ShutdownTimeout is applied.
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return nil // already stopping or stopped
	}
	e.stopping = true
	e.running = false
	cronDone := e.cron.Stop().Done()
	e.mu.Unlock()

	// Ensure context has a timeout to prevent hanging.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.cfg.ShutdownTimeout)
		defer cancel()
	}

	done := make(chan struct{})
	go func() {
		<-cronDone
		e.activeWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		e.stopCancel()
		return nil
	case <-ctx.Done():
		// Canceling stopCtx cancels every active run through its stop watcher.
		e.stopCancel()
		return ctx.Err()
	}
}
