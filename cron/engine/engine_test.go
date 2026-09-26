package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikrimohammad/go-dev-sdk/cron"
)

func TestEngine_RegisterValidations(t *testing.T) {
	eng, err := New(Config{})
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// Empty name
	if err := eng.Register(cron.Job{Schedule: "* * * * *", Handler: func(ctx context.Context) error { return nil }}); !errors.Is(err, cron.ErrEmptyJobName) {
		t.Fatalf("expected ErrEmptyJobName, got %v", err)
	}

	// Nil handler
	if err := eng.Register(cron.Job{Name: "test", Schedule: "* * * * *"}); !errors.Is(err, cron.ErrNilHandler) {
		t.Fatalf("expected ErrNilHandler, got %v", err)
	}

	// Invalid schedule
	if err := eng.Register(cron.Job{Name: "test", Schedule: "invalid schedule", Handler: func(ctx context.Context) error { return nil }}); !errors.Is(err, cron.ErrInvalidSchedule) {
		t.Fatalf("expected ErrInvalidSchedule, got %v", err)
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
		t.Fatalf("expected ErrJobAlreadyExists, got %v", err)
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
		t.Fatalf("expected ErrEngineAlreadyStarted, got %v", err)
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
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}

	// Successful manual trigger
	if err := eng.Trigger(context.Background(), "manual-job"); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}

	if !called.Load() {
		t.Fatal("expected handler to have been called")
	}

	info, ok := eng.Job("manual-job")
	if !ok {
		t.Fatal("expected job info to exist")
	}
	if info.LastStatus != "success" {
		t.Fatalf("expected status success, got %s", info.LastStatus)
	}

	jobs := eng.Jobs()
	if len(jobs) != 1 || jobs[0].Name != "manual-job" {
		t.Fatalf("unexpected Jobs() output: %v", jobs)
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
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = eng.Trigger(context.Background(), "slow-job")
	}()

	<-started

	// Second trigger while slow-job is running should fail with ErrJobRunning
	err2 := eng.Trigger(context.Background(), "slow-job")
	if !errors.Is(err2, cron.ErrJobRunning) {
		t.Fatalf("expected ErrJobRunning, got %v", err2)
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

	// Stop with nil context: automatically uses configured 50ms ShutdownTimeout and cancels in-flight job
	var nilCtx context.Context
	err = eng.Stop(nilCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded on stop timeout, got %v", err)
	}

	// Trigger after stop returns ErrEngineStopped
	errAfterStop := eng.Trigger(context.Background(), "blocking-job")
	if !errors.Is(errAfterStop, cron.ErrEngineStopped) {
		t.Fatalf("expected ErrEngineStopped, got %v", errAfterStop)
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
		t.Fatalf("expected deadline exceeded, got %v", err)
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
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = eng.Trigger(context.Background(), "race-job")
		}()
	}

	// Concurrently stop
	go func() {
		time.Sleep(1 * time.Millisecond)
		var nilCtx context.Context
		_ = eng.Stop(nilCtx)
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
		t.Fatal("expected toggler and locker to be set")
	}
}
