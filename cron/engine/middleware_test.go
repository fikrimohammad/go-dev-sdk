package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/cron"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
)

// fakeToggler implements cron.Toggler
type fakeToggler struct {
	enabled bool
	err     error
}

func (f *fakeToggler) IsEnabled(context.Context, string) (bool, error) {
	return f.enabled, f.err
}

// fakeLockToken implements cron.LockToken
type fakeLockToken struct {
	token    string
	unlocked bool
	mu       sync.Mutex
}

func (f *fakeLockToken) FencingToken() string {
	return f.token
}

func (f *fakeLockToken) Unlock(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unlocked = true
	return nil
}

// fakeLocker implements cron.Locker
type fakeLocker struct {
	token cron.LockToken
	err   error
	mu    sync.Mutex
	calls int
}

func (f *fakeLocker) Lock(context.Context, string, time.Duration) (cron.LockToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.token, nil
}

func TestMiddleware_PanicRecovery(t *testing.T) {
	m := meta{
		job:     cron.Job{Name: "panic-job"},
		trigger: "scheduled",
		timeout: time.Minute,
	}

	panickingHandler := func(ctx context.Context) error {
		panic("something went horribly wrong")
	}

	chain := m.buildChain(panickingHandler)
	err := chain(context.Background())
	if err == nil {
		t.Fatal("expected error after panic recovery")
	}
}

func TestMiddleware_TogglerDisabled(t *testing.T) {
	handlerCalled := false
	m := meta{
		job:     cron.Job{Name: "disabled-job"},
		trigger: "scheduled",
		timeout: time.Minute,
		toggler: &fakeToggler{enabled: false},
	}

	chain := m.buildChain(func(ctx context.Context) error {
		handlerCalled = true
		return nil
	})

	err := chain(context.Background())
	if !errors.Is(err, cron.ErrJobDisabled) {
		t.Fatalf("got err %v, want ErrJobDisabled", err)
	}
	if handlerCalled {
		t.Fatal("handler should not have been called when toggler is disabled")
	}
}

func TestMiddleware_TogglerFailOpen(t *testing.T) {
	handlerCalled := false
	m := meta{
		job:     cron.Job{Name: "toggle-err-job"},
		trigger: "scheduled",
		timeout: time.Minute,
		toggler: &fakeToggler{enabled: false, err: errors.New("toggle store timeout")},
	}

	chain := m.buildChain(func(ctx context.Context) error {
		handlerCalled = true
		return nil
	})

	err := chain(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Fatal("handler should be called when toggler fails open")
	}
}

func TestMiddleware_LockerContention(t *testing.T) {
	handlerCalled := false
	m := meta{
		job:     cron.Job{Name: "locked-job"},
		trigger: "scheduled",
		timeout: time.Minute,
		locker:  &fakeLocker{err: cron.ErrLockHeld},
	}

	chain := m.buildChain(func(ctx context.Context) error {
		handlerCalled = true
		return nil
	})

	err := chain(context.Background())
	if !errors.Is(err, cron.ErrLockHeld) {
		t.Fatalf("got err %v, want ErrLockHeld", err)
	}
	if handlerCalled {
		t.Fatal("handler should not be called when lock is held")
	}
}

func TestMiddleware_LockerFailClosed(t *testing.T) {
	handlerCalled := false
	m := meta{
		job:     cron.Job{Name: "infra-err-job"},
		trigger: "scheduled",
		timeout: time.Minute,
		locker:  &fakeLocker{err: errors.New("redis connection refused")},
	}

	chain := m.buildChain(func(ctx context.Context) error {
		handlerCalled = true
		return nil
	})

	err := chain(context.Background())
	if err == nil {
		t.Fatal("expected error when lock infra fails")
	}
	if handlerCalled {
		t.Fatal("handler should not be called when lock infra fails")
	}
}

func TestMiddleware_FencingTokenInjected(t *testing.T) {
	token := &fakeLockToken{token: "snowflake-999"}
	locker := &fakeLocker{token: token}
	var capturedToken string
	var foundToken bool

	m := meta{
		job:     cron.Job{Name: "fencing-job"},
		trigger: "scheduled",
		timeout: time.Minute,
		locker:  locker,
	}

	chain := m.buildChain(func(ctx context.Context) error {
		capturedToken, foundToken = cron.FencingTokenFromContext(ctx)
		return nil
	})

	err := chain(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !foundToken || capturedToken != "snowflake-999" {
		t.Fatalf("expected fencing token %q, got %q, found %v", "snowflake-999", capturedToken, foundToken)
	}
	if !token.unlocked {
		t.Fatal("expected lock to be unlocked after execution")
	}
}

// recordingMetrics implements metrics.Client and keeps every histogram record.
type recordingMetrics struct {
	mu      sync.Mutex
	records []metricRecord
}

type metricRecord struct {
	name  string
	value float64
	attrs map[string]any
}

func (r *recordingMetrics) Add(context.Context, metrics.Instrument, int64, map[string]any) error {
	return nil
}

func (r *recordingMetrics) Record(_ context.Context, in metrics.Instrument, v float64, attrs map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, metricRecord{name: in.Name, value: v, attrs: attrs})
	return nil
}

func (r *recordingMetrics) AddUpDown(context.Context, metrics.Instrument, int64, map[string]any) error {
	return nil
}

func (r *recordingMetrics) RecordGauge(context.Context, metrics.Instrument, float64, map[string]any) error {
	return nil
}

func (r *recordingMetrics) Provider() metric.MeterProvider { return nil }

func (r *recordingMetrics) Stop(context.Context) error { return nil }

// providerTracer adapts an SDK tracer provider to tracer.Client.
type providerTracer struct{ tp trace.TracerProvider }

func (p providerTracer) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return p.tp.Tracer(name, opts...)
}

func (p providerTracer) Provider() trace.TracerProvider { return p.tp }

func (p providerTracer) Stop(context.Context) error { return nil }

// releaseToken is a LockToken that records the context state seen by Unlock.
// The state is read inside Unlock because the caller cancels that context once
// Unlock returns.
type releaseToken struct {
	errAtUnlock  error
	spanAtUnlock trace.SpanContext
	released     chan struct{}
}

func (r *releaseToken) FencingToken() string { return "release-1" }

func (r *releaseToken) Unlock(ctx context.Context) error {
	r.errAtUnlock = ctx.Err()
	r.spanAtUnlock = trace.SpanContextFromContext(ctx)
	r.released <- struct{}{}
	return nil
}

// TestUnlock_AfterCancel stops an engine while a scheduled run is blocked. The
// run is canceled, and Unlock must still get a live context that keeps the job
// span.
func TestUnlock_AfterCancel(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	token := &releaseToken{released: make(chan struct{}, 1)}

	eng, err := New(Config{ShutdownTimeout: 50 * time.Millisecond},
		WithLocker(&fakeLocker{token: token}),
		WithTracer(providerTracer{tp: tp}),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	started := make(chan trace.SpanContext, 1)
	err = eng.Register(cron.Job{
		Name:     "unlock-job",
		Schedule: "@every 1s",
		Handler: func(ctx context.Context) error {
			started <- trace.SpanContextFromContext(ctx)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	eng.Start()

	var jobSC trace.SpanContext
	select {
	case jobSC = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("scheduled run did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := eng.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() got err %v, want DeadlineExceeded", err)
	}

	select {
	case <-token.released:
	case <-time.After(5 * time.Second):
		t.Fatal("lock was not released")
	}
	if token.errAtUnlock != nil {
		t.Fatalf("Unlock ctx Err() got %v, want nil", token.errAtUnlock)
	}
	unlockSC := token.spanAtUnlock
	if !unlockSC.IsValid() {
		t.Fatal("Unlock ctx span context is invalid, want the job span")
	}
	if unlockSC.SpanID() != jobSC.SpanID() || unlockSC.TraceID() != jobSC.TraceID() {
		t.Fatalf("Unlock span got %s/%s, want job span %s/%s",
			unlockSC.TraceID(), unlockSC.SpanID(), jobSC.TraceID(), jobSC.SpanID())
	}
}

// TestJob_PanicIsMeasured checks that a panicking handler is traced as failed
// and produces a duration record with error.type.
func TestJob_PanicIsMeasured(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	rec := &recordingMetrics{}
	m := meta{
		job:     cron.Job{Name: "panic-job"},
		trigger: "manual",
		timeout: time.Minute,
		metrics: rec,
		tracer:  providerTracer{tp: tp},
	}

	chain := m.buildChain(func(ctx context.Context) error {
		panic("handler exploded")
	})
	if err := chain(context.Background()); err == nil {
		t.Fatal("chain() got nil error, want the recovered panic")
	}

	var durations []metricRecord
	for _, r := range rec.records {
		if r.name == "cron.job.duration" {
			durations = append(durations, r)
		}
	}
	if len(durations) != 1 {
		t.Fatalf("cron.job.duration records got %d, want 1", len(durations))
	}
	if _, ok := durations[0].attrs["error.type"]; !ok {
		t.Errorf("cron.job.duration attrs got %v, want error.type", durations[0].attrs)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans got %d, want 1", len(spans))
	}
	if code := spans[0].Status.Code; code != codes.Error {
		t.Errorf("span status got %v, want %v", code, codes.Error)
	}
}

// failingUnlockToken is a LockToken whose Unlock always fails.
type failingUnlockToken struct{}

func (failingUnlockToken) FencingToken() string { return "token-1" }

func (failingUnlockToken) Unlock(context.Context) error { return errors.New("redis down") }

// TestUnlock_FailureKeepsJobOutcome checks that a failed Unlock leaves a
// successful job successful and is recorded as a span event instead.
func TestUnlock_FailureKeepsJobOutcome(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	m := meta{
		job:     cron.Job{Name: "unlock-fails"},
		trigger: "manual",
		timeout: time.Minute,
		locker:  &fakeLocker{token: failingUnlockToken{}},
		tracer:  providerTracer{tp: tp},
	}

	if err := m.buildChain(func(context.Context) error { return nil })(context.Background()); err != nil {
		t.Fatalf("chain() = %v, want nil: a failed unlock must not fail the job", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	if !slices.ContainsFunc(spans[0].Events, func(e sdktrace.Event) bool { return e.Name == "cron.unlock.error" }) {
		t.Errorf("span events = %v, want a cron.unlock.error event", spans[0].Events)
	}
}
