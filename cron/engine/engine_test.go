package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikrimohammad/go-servicekit/cron"
)

func TestRegister_InvalidSchedule(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	job := cron.Job{Name: "bad-schedule", Schedule: "invalid schedule", Handler: func(ctx context.Context) error { return nil }}
	_, parseErr := eng.parser.Parse(job.Schedule)
	if parseErr == nil {
		t.Fatal("parser accepted an invalid schedule, want an error")
	}

	err = eng.Register(job)
	if !errors.Is(err, cron.ErrInvalidSchedule) {
		t.Fatalf("got err %v, want ErrInvalidSchedule", err)
	}
	if want := cron.ErrInvalidSchedule.Error() + ": "; !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("got err %q, want prefix %q", err, want)
	}
	if !strings.Contains(err.Error(), parseErr.Error()) {
		t.Fatalf("got err %q, want it to contain parser error %q", err, parseErr)
	}
}

// TestRegister_RacesStart registers jobs while Start runs. Each Register must
// either succeed, leaving the job in the map and in the scheduler, or return
// ErrEngineAlreadyStarted.
func TestRegister_RacesStart(t *testing.T) {
	const jobs = 50
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	handler := func(ctx context.Context) error { return nil }
	errs := make([]error, jobs)
	var wg sync.WaitGroup
	for i := range jobs {
		wg.Go(func() {
			errs[i] = eng.Register(cron.Job{
				Name:     fmt.Sprintf("race-%d", i),
				Schedule: "@hourly",
				Handler:  handler,
			})
		})
	}
	wg.Go(func() { eng.Start() })
	wg.Wait()
	defer func() { _ = eng.Stop(context.Background()) }()

	registered := 0
	for i, err := range errs {
		name := fmt.Sprintf("race-%d", i)
		switch {
		case err == nil:
			registered++
			if _, ok := eng.jobs[name]; !ok {
				t.Errorf("Register(%q) returned nil but the job is not in the map", name)
			}
		case errors.Is(err, cron.ErrEngineAlreadyStarted):
		default:
			t.Errorf("Register(%q) got err %v, want nil or ErrEngineAlreadyStarted", name, err)
		}
	}
	if got := len(eng.cron.Entries()); got != registered {
		t.Errorf("scheduled entries got %d, want %d (the successful registrations)", got, registered)
	}
}

func TestEngine_RegisterValidations(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// Empty name
	if err := eng.Register(cron.Job{Schedule: "* * * * *", Handler: func(ctx context.Context) error { return nil }}); !errors.Is(err, cron.ErrEmptyJobName) {
		t.Fatalf("Register(empty name) = %v, want ErrEmptyJobName", err)
	}

	// Nil handler
	if err := eng.Register(cron.Job{Name: "test", Schedule: "* * * * *"}); !errors.Is(err, cron.ErrNilHandler) {
		t.Fatalf("Register(nil handler) = %v, want ErrNilHandler", err)
	}

	// Invalid schedule
	if err := eng.Register(cron.Job{Name: "test", Schedule: "invalid schedule", Handler: func(ctx context.Context) error { return nil }}); !errors.Is(err, cron.ErrInvalidSchedule) {
		t.Fatalf("Register(bad schedule) = %v, want ErrInvalidSchedule", err)
	}

	// Valid 5-field
	if err := eng.Register(cron.Job{Name: "job-5field", Schedule: "0 * * * *", Handler: func(ctx context.Context) error { return nil }}); err != nil {
		t.Fatalf("unexpected error registering 5-field: %v", err)
	}

	// Valid 6-field with seconds
	if err := eng.Register(cron.Job{Name: "job-6field", Schedule: "*/10 * * * * *", Handler: func(ctx context.Context) error { return nil }}); err != nil {
		t.Fatalf("unexpected error registering 6-field: %v", err)
	}

	// Valid descriptor
	if err := eng.Register(cron.Job{Name: "job-descriptor", Schedule: "@every 5s", Handler: func(ctx context.Context) error { return nil }}); err != nil {
		t.Fatalf("unexpected error registering descriptor: %v", err)
	}

	// Duplicate name
	if err := eng.Register(cron.Job{Name: "job-5field", Schedule: "0 * * * *", Handler: func(ctx context.Context) error { return nil }}); !errors.Is(err, cron.ErrJobAlreadyExists) {
		t.Fatalf("Register(duplicate) = %v, want ErrJobAlreadyExists", err)
	}
}

func TestEngine_RegisterAfterStart(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	eng.Start()
	defer func() { _ = eng.Stop(context.Background()) }()

	err = eng.Register(cron.Job{Name: "after-start", Schedule: "* * * * *", Handler: func(ctx context.Context) error { return nil }})
	if !errors.Is(err, cron.ErrEngineAlreadyStarted) {
		t.Fatalf("Register(after Start) = %v, want ErrEngineAlreadyStarted", err)
	}
}

func TestEngine_Trigger(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	called := atomic.Bool{}
	err = eng.Register(cron.Job{
		Name:     "manual-job",
		Schedule: "@hourly",
		Handler: func(ctx context.Context) error {
			called.Store(true)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("failed to register job: %v", err)
	}

	// Unknown job
	if err := eng.Trigger(context.Background(), "unknown"); !errors.Is(err, cron.ErrJobNotFound) {
		t.Fatalf("Trigger(unknown) = %v, want ErrJobNotFound", err)
	}

	// Successful manual trigger
	if err := eng.Trigger(context.Background(), "manual-job"); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}

	if !called.Load() {
		t.Fatal("handler calls = 0, want at least 1")
	}
}

func TestEngine_TimeoutEnforcement(t *testing.T) {
	eng, err := New(Config{GlobalTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	var handlerErr error
	err = eng.Register(cron.Job{
		Name:     "timeout-job",
		Schedule: "@hourly",
		Handler: func(ctx context.Context) error {
			select {
			case <-time.After(500 * time.Millisecond):
				return nil
			case <-ctx.Done():
				handlerErr = ctx.Err()
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	err = eng.Trigger(context.Background(), "timeout-job")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Trigger() = %v, want context.DeadlineExceeded", err)
	}
	if !errors.Is(handlerErr, context.DeadlineExceeded) {
		t.Fatalf("handler context should report DeadlineExceeded, got %v", handlerErr)
	}
}

func TestEngine_SkipOnOverlap(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	started := make(chan struct{})
	block := make(chan struct{})

	err = eng.Register(cron.Job{
		Name:     "slow-job",
		Schedule: "@hourly",
		Handler: func(ctx context.Context) error {
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
		_ = eng.Trigger(context.Background(), "slow-job")
	})

	<-started

	// Second trigger while slow-job is running should fail with ErrJobRunning
	err2 := eng.Trigger(context.Background(), "slow-job")
	if !errors.Is(err2, cron.ErrJobRunning) {
		t.Fatalf("overlapping Trigger() = %v, want ErrJobRunning", err2)
	}

	close(block)
	wg.Wait()
}

func TestEngine_TwoPhaseShutdown(t *testing.T) {
	eng, err := New(Config{
		ShutdownTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	started := make(chan struct{})
	err = eng.Register(cron.Job{
		Name:     "blocking-job",
		Schedule: "@every 1s",
		Handler: func(ctx context.Context) error {
			close(started)
			<-ctx.Done() // wait for cancellation
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	eng.Start()

	go func() {
		_ = eng.Trigger(context.Background(), "blocking-job")
	}()

	<-started

	// Stop without a deadline: the configured 50ms ShutdownTimeout applies and cancels the in-flight job
	err = eng.Stop(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() on timeout = %v, want context.DeadlineExceeded", err)
	}

	// Trigger after stop returns ErrEngineStopped
	errAfterStop := eng.Trigger(context.Background(), "blocking-job")
	if !errors.Is(errAfterStop, cron.ErrEngineStopped) {
		t.Fatalf("Trigger() after Stop = %v, want ErrEngineStopped", errAfterStop)
	}
}

func TestEngine_StopWithoutDeadlineUsesConfig(t *testing.T) {
	eng, err := New(Config{
		ShutdownTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	started := make(chan struct{})
	_ = eng.Register(cron.Job{
		Name:     "slow-stopping-job",
		Schedule: "@every 1s",
		Handler: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	})

	eng.Start()
	go func() {
		_ = eng.Trigger(context.Background(), "slow-stopping-job")
	}()
	<-started

	// Pass context.Background() without explicit deadline: should apply 50ms timeout automatically
	err = eng.Stop(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() = %v, want context.DeadlineExceeded", err)
	}
}

func TestEngine_ConcurrentRunJobAndStop(t *testing.T) {
	eng, err := New(Config{
		GlobalTimeout:   100 * time.Millisecond,
		ShutdownTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	_ = eng.Register(cron.Job{
		Name:     "race-job",
		Schedule: "@every 10ms",
		Handler: func(ctx context.Context) error {
			time.Sleep(5 * time.Millisecond)
			return nil
		},
	})

	eng.Start()

	var wg sync.WaitGroup
	// Concurrently trigger jobs while stopping
	for range 30 {
		wg.Go(func() {
			_ = eng.Trigger(context.Background(), "race-job")
		})
	}

	// Concurrently stop
	go func() {
		time.Sleep(1 * time.Millisecond)
		_ = eng.Stop(context.Background())
	}()

	wg.Wait()
}

func TestEngine_Options(t *testing.T) {
	eng, err := New(Config{},
		WithToggler(&fakeToggler{enabled: true}),
		WithLocker(&fakeLocker{}),
		WithMetrics(nil),
		WithTracer(nil),
	)
	if err != nil {
		t.Fatalf("unexpected error with options: %v", err)
	}
	if eng.toggler == nil || eng.locker == nil {
		t.Fatal("toggler or locker = nil, want both set")
	}
}

// panickingToggler is a cron.Toggler whose IsEnabled panics.
type panickingToggler struct{}

func (panickingToggler) IsEnabled(context.Context, string) (bool, error) {
	panic("toggler exploded")
}

// TestRunJob_PanickingTogglerDoesNotCrash checks that a panic or a nil lock
// token outside the handler comes back as an error from Trigger, not a crash.
func TestRunJob_PanickingTogglerDoesNotCrash(t *testing.T) {
	tests := []struct {
		name         string
		opts         []Option
		wantPanicLog bool
	}{
		{name: "toggler panics", opts: []Option{WithToggler(panickingToggler{})}, wantPanicLog: true},
		{name: "locker returns nil token", opts: []Option{WithLocker(&fakeLocker{})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logRec := captureLogs(t)
			eng, err := New(Config{}, tc.opts...)
			if err != nil {
				t.Fatalf("failed to create engine: %v", err)
			}
			err = eng.Register(cron.Job{
				Name:     "panic-job",
				Schedule: "@hourly",
				Handler:  func(context.Context) error { return nil },
			})
			if err != nil {
				t.Fatalf("register failed: %v", err)
			}

			if err := eng.Trigger(context.Background(), "panic-job"); err == nil {
				t.Fatal("Trigger() got nil error, want the failure")
			}
			_, panicLogged := logRec.find("ERROR", "cron job panicked")
			if panicLogged != tc.wantPanicLog {
				t.Errorf("ERROR \"cron job panicked\" logged got %t, want %t", panicLogged, tc.wantPanicLog)
			}
		})
	}
}

// TestEngine_StopCancelsStopContext checks that a successful Stop releases the
// engine lifetime context.
func TestEngine_StopCancelsStopContext(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	eng.Start()
	if err := eng.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() got err %v, want nil", err)
	}
	if err := eng.stopCtx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("stopCtx Err() got %v, want %v", err, context.Canceled)
	}
}
