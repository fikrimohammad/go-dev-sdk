package engine

import (
	"context"
	"fmt"
	"sync"

	robfigcron "github.com/robfig/cron/v3"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// Option configures an Engine instance.
type Option func(*Engine)

// WithToggler injects a custom Toggler.
func WithToggler(t cron.Toggler) Option {
	return func(e *Engine) {
		e.toggler = t
	}
}

// WithLocker injects a custom Locker.
func WithLocker(l cron.Locker) Option {
	return func(e *Engine) {
		e.locker = l
	}
}

// WithMetrics injects a custom metrics client.
func WithMetrics(m metrics.Client) Option {
	return func(e *Engine) {
		e.metrics = m
	}
}

// WithTracer injects a custom tracer client.
func WithTracer(t tracer.Client) Option {
	return func(e *Engine) {
		e.tracer = t
	}
}

// Engine wraps robfig/cron and orchestrates job scheduling, execution, and instrumentation.
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

	// stopCtx is canceled by Stop when its deadline expires; every run derives
	// from it through an AfterFunc watcher.
	stopCtx    context.Context
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

	stopCtx, stopCancel := context.WithCancel(context.Background())

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

func (e *Engine) runJob(parentCtx context.Context, job cron.Job, trigger string) error {
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

	// Watch for engine shutdown to signal cancellation promptly.
	stopWatcher := context.AfterFunc(e.stopCtx, cancel)
	defer stopWatcher()

	// Skip-on-overlap protection: only 1 in-flight execution per job name locally.
	if _, loaded := e.inFlight.LoadOrStore(job.Name, struct{}{}); loaded {
		return cron.ErrJobRunning
	}
	defer e.inFlight.Delete(job.Name)

	m := meta{
		job:     job,
		trigger: trigger,
		timeout: timeout,
		toggler: e.toggler,
		locker:  e.locker,
		metrics: e.metrics,
		tracer:  e.tracer,
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
	cronStopCtx := e.cron.Stop()
	e.mu.Unlock()

	// Ensure context has a timeout to prevent hanging.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.cfg.ShutdownTimeout)
		defer cancel()
	}

	done := make(chan struct{})
	go func() {
		<-cronStopCtx.Done()
		e.activeWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Canceling stopCtx cancels every active run through its AfterFunc watcher.
		e.stopCancel()
		return ctx.Err()
	}
}
