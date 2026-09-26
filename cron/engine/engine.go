package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

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

type registeredJob struct {
	job      cron.Job
	entryID  robfigcron.EntryID
	schedule robfigcron.Schedule

	mu         sync.RWMutex
	lastRun    time.Time
	lastStatus string
	lastDur    time.Duration
	lastErr    string
}

// Engine wraps robfig/cron and orchestrates job scheduling, execution, and instrumentation.
type Engine struct {
	cfg     Config
	cron    *robfigcron.Cron
	parser  robfigcron.ScheduleParser
	jobs    map[string]*registeredJob
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
		jobs:       make(map[string]*registeredJob),
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
	entryID := e.cron.Schedule(schedule, robfigcron.FuncJob(func() {
		_ = e.runJob(context.Background(), jobCopy, "scheduled")
	}))

	e.jobs[job.Name] = &registeredJob{
		job:        jobCopy,
		entryID:    entryID,
		schedule:   schedule,
		lastStatus: "idle",
	}

	return nil
}

// Trigger triggers a job manually by name.
func (e *Engine) Trigger(ctx context.Context, jobName string) error {
	e.jobsMu.RLock()
	reg, exists := e.jobs[jobName]
	e.jobsMu.RUnlock()

	if !exists {
		return cron.ErrJobNotFound
	}

	return e.runJob(ctx, reg.job, "manual")
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

	e.updateJobStatus(job.Name, "running", 0, "")

	m := meta{
		job:     job,
		trigger: trigger,
		timeout: timeout,
		toggler: e.toggler,
		locker:  e.locker,
		metrics: e.metrics,
		tracer:  e.tracer,
	}

	chain := m.buildChain(job.Handler)
	start := time.Now()

	err := chain(ctx)
	duration := time.Since(start)

	status := "success"
	errMsg := ""
	if err != nil {
		if errors.Is(err, cron.ErrLockHeld) || errors.Is(err, cron.ErrJobDisabled) {
			status = "skipped"
		} else {
			status = "failed"
			errMsg = err.Error()
		}
	}

	e.updateJobStatus(job.Name, status, duration, errMsg)
	return err
}

func (e *Engine) updateJobStatus(name, status string, dur time.Duration, errMsg string) {
	e.jobsMu.RLock()
	reg, ok := e.jobs[name]
	e.jobsMu.RUnlock()
	if !ok {
		return
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if status == "running" {
		reg.lastRun = time.Now()
	}
	reg.lastStatus = status
	reg.lastDur = dur
	reg.lastErr = errMsg
}

// Jobs returns snapshot metadata for all registered jobs.
func (e *Engine) Jobs() []cron.JobInfo {
	e.jobsMu.RLock()
	defer e.jobsMu.RUnlock()

	cronEntries := e.cron.Entries()
	entryMap := make(map[robfigcron.EntryID]robfigcron.Entry, len(cronEntries))
	for _, entry := range cronEntries {
		entryMap[entry.ID] = entry
	}

	infos := make([]cron.JobInfo, 0, len(e.jobs))
	for _, reg := range e.jobs {
		infos = append(infos, e.snapshotJobWithEntry(reg, entryMap[reg.entryID]))
	}
	return infos
}

// Job returns snapshot metadata for a specific job.
func (e *Engine) Job(name string) (cron.JobInfo, bool) {
	e.jobsMu.RLock()
	reg, ok := e.jobs[name]
	e.jobsMu.RUnlock()
	if !ok {
		return cron.JobInfo{}, false
	}
	entry := e.cron.Entry(reg.entryID)
	return e.snapshotJobWithEntry(reg, entry), true
}

func (e *Engine) snapshotJobWithEntry(reg *registeredJob, entry robfigcron.Entry) cron.JobInfo {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	timeout := reg.job.Timeout
	if timeout <= 0 {
		timeout = e.cfg.GlobalTimeout
	}

	return cron.JobInfo{
		Name:         reg.job.Name,
		Schedule:     reg.job.Schedule,
		Description:  reg.job.Description,
		Timeout:      timeout.String(),
		NextRun:      entry.Next,
		PrevRun:      reg.lastRun,
		LastStatus:   reg.lastStatus,
		LastDuration: reg.lastDur.String(),
		LastError:    reg.lastErr,
	}
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
