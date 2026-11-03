package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-servicekit/cron"
	"github.com/fikrimohammad/go-servicekit/observability/logs"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
)

// fakeToggler implements cron.Toggler.
type fakeToggler struct {
	enabled bool
	err     error
}

func (f *fakeToggler) IsEnabled(context.Context, string) (bool, error) {
	return f.enabled, f.err
}

// fakeLockToken implements cron.LockToken.
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

// fakeLocker implements cron.Locker.
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
		t.Fatal("chain() got nil error, want the recovered panic")
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
		t.Fatal("chain() got nil error, want the lock failure")
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
		t.Fatalf("fencing token got %q (found %t), want %q", capturedToken, foundToken, "snowflake-999")
	}
	if !token.unlocked {
		t.Fatal("lock unlocked = false, want true")
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
	if got := durations[0].attrs["error.type"]; got != "panic" {
		t.Errorf("cron.job.duration error.type got %v, want %q", got, "panic")
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans got %d, want 1", len(spans))
	}
	if code := spans[0].Status.Code; code != codes.Error {
		t.Errorf("span status got %v, want %v", code, codes.Error)
	}
}

// codedError carries a code the way *errs.Error does, without importing errs.
type codedError struct{ code int }

func (e codedError) Error() string { return "coded failure" }

func (e codedError) Code() int { return e.code }

// TestMetrics_Attributes checks the exact duration instrument and attribute
// sets on success and on each failure class, and that the span carries the
// fencing token while the metric does not.
func TestMetrics_Attributes(t *testing.T) {
	const fence = "fence-7"
	base := map[string]string{
		"cron.job.name":    "attr-job",
		"cron.job.trigger": "scheduled",
	}
	withType := func(v string) map[string]string {
		return maps.Clone(map[string]string{
			"cron.job.name":    "attr-job",
			"cron.job.trigger": "scheduled",
			"error.type":       v,
		})
	}
	tests := []struct {
		name    string
		handler cron.HandlerFunc
		wantErr bool
		want    map[string]string
	}{
		{
			name:    "success",
			handler: func(context.Context) error { return nil },
			want:    base,
		},
		{
			name:    "coded failure",
			handler: func(context.Context) error { return codedError{code: 4001} },
			wantErr: true,
			want:    withType("4001"),
		},
		{
			name:    "plain failure",
			handler: func(context.Context) error { return errors.New("boom with details") },
			wantErr: true,
			want:    withType("_OTHER"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			m := meta{
				job:     cron.Job{Name: "attr-job"},
				trigger: "scheduled",
				timeout: time.Minute,
				locker:  &fakeLocker{token: &fakeLockToken{token: fence}},
				metrics: metrics.Wrap(mp),
				tracer:  providerTracer{tp: tp},
			}

			err := m.buildChain(tc.handler)(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("chain() error got %v, want error %t", err, tc.wantErr)
			}

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatalf("Collect() got err %v, want nil", err)
			}
			var names []string
			var points []metricdata.HistogramDataPoint[float64]
			for _, sm := range rm.ScopeMetrics {
				for _, mt := range sm.Metrics {
					names = append(names, mt.Name)
					h, ok := mt.Data.(metricdata.Histogram[float64])
					if !ok {
						t.Fatalf("metric %q data got %T, want Histogram[float64]", mt.Name, mt.Data)
					}
					if mt.Unit != "s" {
						t.Errorf("metric %q unit got %q, want %q", mt.Name, mt.Unit, "s")
					}
					points = append(points, h.DataPoints...)
				}
			}
			if want := []string{"cron.job.duration"}; !slices.Equal(names, want) {
				t.Fatalf("metric names got %v, want %v", names, want)
			}
			if len(points) != 1 {
				t.Fatalf("data points got %d, want 1", len(points))
			}
			if got := attrMap(points[0].Attributes.ToSlice()); !maps.Equal(got, tc.want) {
				t.Errorf("metric attributes got %v, want %v", got, tc.want)
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("spans got %d, want 1", len(spans))
			}
			if spans[0].Name != "cron.attr-job" {
				t.Errorf("span name got %q, want %q", spans[0].Name, "cron.attr-job")
			}
			if got := attrMap(spans[0].Attributes); !maps.Equal(got, withFence(tc.want, fence)) {
				t.Errorf("span attributes got %v, want %v", got, withFence(tc.want, fence))
			}
		})
	}
}

// attrMap converts attributes to string values for comparison.
func attrMap(kvs []attribute.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value.String()
	}
	return m
}

// withFence returns a copy of want with the fencing token added.
func withFence(want map[string]string, token string) map[string]string {
	m := maps.Clone(want)
	m["cron.job.fencing_token"] = token
	return m
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

// recordedLog is one record seen by recordingLogger.
type recordedLog struct {
	level string
	msg   string
	args  []any
}

// arg returns the value that follows key in the record's key/value args.
func (r recordedLog) arg(key string) any {
	for i := 0; i+1 < len(r.args); i += 2 {
		if r.args[i] == key {
			return r.args[i+1]
		}
	}
	return nil
}

// recordingLogger implements logs.Logger and keeps every record.
type recordingLogger struct {
	mu   sync.Mutex
	recs []recordedLog
}

func (r *recordingLogger) add(level, msg string, args []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, recordedLog{level: level, msg: msg, args: args})
}

func (r *recordingLogger) Debug(_ context.Context, msg string, args ...any) {
	r.add("DEBUG", msg, args)
}

func (r *recordingLogger) Info(_ context.Context, msg string, args ...any) {
	r.add("INFO", msg, args)
}

func (r *recordingLogger) Warn(_ context.Context, msg string, args ...any) {
	r.add("WARN", msg, args)
}

func (r *recordingLogger) Error(_ context.Context, msg string, args ...any) {
	r.add("ERROR", msg, args)
}

// find returns the first record with the given level and message.
func (r *recordingLogger) find(level, msg string) (recordedLog, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.recs {
		if rec.level == level && rec.msg == msg {
			return rec, true
		}
	}
	return recordedLog{}, false
}

// captureLogs makes the default logger record into the returned value until
// the test ends.
func captureLogs(t *testing.T) *recordingLogger {
	t.Helper()
	rl := &recordingLogger{}
	prev := logs.Default()
	logs.SetDefault(rl)
	t.Cleanup(func() { logs.SetDefault(prev) })
	return rl
}

// TestTracer_SkipIsNotError checks that a skipped run leaves the span status
// unset, sets no error.type, and records a cron.job.skipped event with its
// reason.
func TestTracer_SkipIsNotError(t *testing.T) {
	tests := []struct {
		name       string
		toggler    cron.Toggler
		locker     cron.Locker
		wantErr    error
		wantReason string
	}{
		{
			name:       "disabled",
			toggler:    &fakeToggler{enabled: false},
			wantErr:    cron.ErrJobDisabled,
			wantReason: "disabled",
		},
		{
			name:       "lock held",
			locker:     &fakeLocker{err: cron.ErrLockHeld},
			wantErr:    cron.ErrLockHeld,
			wantReason: "lock_held",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			m := meta{
				job:     cron.Job{Name: "skip-job"},
				trigger: "scheduled",
				timeout: time.Minute,
				toggler: tc.toggler,
				locker:  tc.locker,
				tracer:  providerTracer{tp: tp},
			}

			err := m.buildChain(func(context.Context) error { return nil })(context.Background())
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("chain() error got %v, want %v", err, tc.wantErr)
			}

			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("spans got %d, want 1", len(spans))
			}
			span := spans[0]
			if code := span.Status.Code; code != codes.Unset {
				t.Errorf("span status got %v, want %v", code, codes.Unset)
			}
			if _, ok := attrMap(span.Attributes)["error.type"]; ok {
				t.Errorf("span attributes got error.type, want none")
			}
			idx := slices.IndexFunc(span.Events, func(e sdktrace.Event) bool { return e.Name == "cron.job.skipped" })
			if idx < 0 {
				t.Fatalf("span events got %v, want a cron.job.skipped event", span.Events)
			}
			if got := attrMap(span.Events[idx].Attributes)["cron.job.skip_reason"]; got != tc.wantReason {
				t.Errorf("cron.job.skip_reason got %q, want %q", got, tc.wantReason)
			}
		})
	}
}

// TestOverlap_SkipIsLogged checks that a second concurrent trigger returns
// ErrJobRunning, logs the skip at Info, and records it on its span.
func TestOverlap_SkipIsLogged(t *testing.T) {
	logRec := captureLogs(t)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	eng, err := New(Config{}, WithTracer(providerTracer{tp: tp}))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	started := make(chan struct{})
	block := make(chan struct{})
	err = eng.Register(cron.Job{
		Name:     "overlap-job",
		Schedule: "@hourly",
		Handler: func(context.Context) error {
			close(started)
			<-block
			return nil
		},
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		_ = eng.Trigger(context.Background(), "overlap-job")
	})
	<-started

	err = eng.Trigger(context.Background(), "overlap-job")
	close(block)
	wg.Wait()
	if !errors.Is(err, cron.ErrJobRunning) {
		t.Fatalf("overlapping Trigger() got %v, want ErrJobRunning", err)
	}

	rec, ok := logRec.find("INFO", "cron job skipped")
	if !ok {
		t.Fatal("log got no INFO \"cron job skipped\" record, want one")
	}
	if got := rec.arg("reason"); got != cron.ErrJobRunning.Error() {
		t.Errorf("skip log reason got %v, want %q", got, cron.ErrJobRunning.Error())
	}

	recorded := false
	for _, s := range exporter.GetSpans() {
		recorded = recorded || slices.ContainsFunc(s.Events, func(e sdktrace.Event) bool { return e.Name == "cron.job.skipped" })
	}
	if !recorded {
		t.Fatal("no span recorded the cron.job.skipped event, want the overlapping run's span")
	}
}

// TestMetrics_LockBackendFailureMeasured checks that a lock-backend failure is
// measured with error.type, and that a held lock is not measured.
func TestMetrics_LockBackendFailureMeasured(t *testing.T) {
	tests := []struct {
		name      string
		lockErr   error
		wantCount int
		wantType  string
	}{
		{
			name:      "lock backend failure",
			lockErr:   errors.New("redis connection refused"),
			wantCount: 1,
			wantType:  "_OTHER",
		},
		{
			name:      "lock held",
			lockErr:   cron.ErrLockHeld,
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingMetrics{}
			m := meta{
				job:     cron.Job{Name: "lock-metric-job"},
				trigger: "scheduled",
				timeout: time.Minute,
				locker:  &fakeLocker{err: tc.lockErr},
				metrics: rec,
				tracer:  providerTracer{tp: sdktrace.NewTracerProvider()},
			}

			_ = m.buildChain(func(context.Context) error { return nil })(context.Background())

			var samples []metricRecord
			for _, r := range rec.records {
				if r.name == "cron.job.duration" {
					samples = append(samples, r)
				}
			}
			if len(samples) != tc.wantCount {
				t.Fatalf("cron.job.duration samples got %d, want %d", len(samples), tc.wantCount)
			}
			if tc.wantCount == 1 {
				if got := samples[0].attrs["error.type"]; got != tc.wantType {
					t.Errorf("cron.job.duration error.type got %v, want %q", got, tc.wantType)
				}
			}
		})
	}
}

// TestLogger_FencingToken checks that the completed-run log carries the fencing
// token that the locker took.
func TestLogger_FencingToken(t *testing.T) {
	logRec := captureLogs(t)
	m := meta{
		job:     cron.Job{Name: "fence-log-job"},
		trigger: "manual",
		timeout: time.Minute,
		locker:  &fakeLocker{token: &fakeLockToken{token: "fence-7"}},
	}

	if err := m.buildChain(func(context.Context) error { return nil })(context.Background()); err != nil {
		t.Fatalf("chain() got err %v, want nil", err)
	}

	rec, ok := logRec.find("INFO", "cron job completed")
	if !ok {
		t.Fatal("log got no INFO \"cron job completed\" record, want one")
	}
	if got := rec.arg("fencing_token"); got != "fence-7" {
		t.Errorf("completed log fencing_token got %v, want %q", got, "fence-7")
	}
}

// timeoutError is a net.Error whose Timeout reports true.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return false }

// TestErrorType checks the error.type classification of each failure class.
func TestErrorType(t *testing.T) {
	var _ net.Error = timeoutError{}
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "timeout"},
		{name: "wrapped deadline", err: fmt.Errorf("lock: %w", context.DeadlineExceeded), want: "timeout"},
		{name: "canceled", err: context.Canceled, want: "canceled"},
		{name: "net timeout", err: timeoutError{}, want: "timeout"},
		{name: "wrapped coded", err: fmt.Errorf("run: %w", codedError{code: 4001}), want: "4001"},
		{name: "panic", err: fmt.Errorf("%w: boom", errPanic), want: "panic"},
		{name: "other", err: errors.New("boom"), want: "_OTHER"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorType(tc.err); got != tc.want {
				t.Errorf("errorType(%v) got %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
