package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	robfigcron "github.com/robfig/cron/v3"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
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
	cfg     Config
	cron    *robfigcron.Cron
	parser  robfigcron.ScheduleParser
	jobs    map[string]cron.Job
	jobsMu  sync.RWMutex
	running atomic.Bool

	// Lifecycle & shutdown synchronization
	stopMu     sync.RWMutex
	stopping   atomic.Bool
	stopCtx    context.Context
	stopCancel context.CancelFunc

	// inFlight tracks currently executing jobs to prevent local overlap.
	inFlight sync.Map // jobName -> context.CancelFunc
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
	if e.running.Load() || e.stopping.Load() {
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
		return errors.Join(cron.ErrInvalidSchedule, err)
	}

	e.jobsMu.Lock()
	defer e.jobsMu.Unlock()

	if _, exists := e.jobs[job.Name]; exists {
		return cron.ErrJobAlreadyExists
	}

	jobCopy := job
	e.cron.Schedule(schedule, robfigcron.FuncJob(func() {
		_ = e.runJob(context.Background(), jobCopy, "scheduled")
	}))
	e.jobs[job.Name] = jobCopy

	return nil
}

// Trigger triggers a job manually by name.
func (e *Engine) Trigger(ctx context.Context, jobName string) error {
	e.jobsMu.RLock()
	job, exists := e.jobs[jobName]
	e.jobsMu.RUnlock()

	if !exists {
		return cron.ErrJobNotFound
	}

	return e.runJob(ctx, job, "manual")
}

func (e *Engine) runJob(parentCtx context.Context, job cron.Job, trigger string) error {
	// Guard against starting jobs during or after shutdown.
	e.stopMu.RLock()
	if e.stopping.Load() {
		e.stopMu.RUnlock()
		return cron.ErrEngineStopped
	}
	e.activeWg.Add(1)
	e.stopMu.RUnlock()
	defer e.activeWg.Done()

	timeout := job.Timeout
	if timeout <= 0 {
		timeout = e.cfg.GlobalTimeout
	}

	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	// Watch for engine shutdown to signal cancellation promptly.
	stopWatcher := context.AfterFunc(e.stopCtx, cancel)
	defer stopWatcher()

	// Skip-on-overlap protection: only 1 in-flight execution per job name locally.
	if _, loaded := e.inFlight.LoadOrStore(job.Name, cancel); loaded {
		logs.Debug(ctx, "cron job skipped: previous run still in-flight", "job", job.Name)
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
	e.stopMu.Lock()
	defer e.stopMu.Unlock()

	if e.stopping.Load() {
		return
	}
	if e.running.CompareAndSwap(false, true) {
		e.cron.Start()
	}
}

// Stop initiates a two-phase graceful shutdown: waits for active jobs up to ctx deadline;
// cancels in-flight job contexts if the deadline expires.
// If the input ctx is nil or has no deadline, Config.ShutdownTimeout is applied.
func (e *Engine) Stop(ctx context.Context) error {
	e.stopMu.Lock()
	if e.stopping.Swap(true) {
		e.stopMu.Unlock()
		return nil // already stopping or stopped
	}
	e.running.Store(false)
	cronStopCtx := e.cron.Stop()
	e.stopMu.Unlock()

	// Ensure context has a timeout to prevent hanging.
	if ctx == nil {
		ctx = context.Background()
	}
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
		// Cancel all active jobs via the engine-level cancel func and in-flight map.
		e.stopCancel()
		e.inFlight.Range(func(key, val any) bool {
			if cancel, ok := val.(context.CancelFunc); ok {
				cancel()
			}
			return true
		})
		return ctx.Err()
	}
}
