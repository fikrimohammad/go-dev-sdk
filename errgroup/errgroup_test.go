package errgroup

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

func TestGroup_Go_HappyPath(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	var counter atomic.Int32
	const n = 10
	for i := 0; i < n; i++ {
		g.Go(func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Wait returned unexpected error: %v", err)
	}
	if got := counter.Load(); got != n {
		t.Fatalf("expected %d tasks to run, got %d", n, got)
	}
}

// ---------------------------------------------------------------------------
// Error propagation
// ---------------------------------------------------------------------------

var errBoom = errors.New("boom")

func TestGroup_Go_FirstErrorIsReturned(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	g.Go(func(ctx context.Context) error {
		return errBoom
	})

	g.Go(func(ctx context.Context) error {
		// Wait for sibling cancellation.
		<-ctx.Done()
		return ctx.Err()
	})

	err := g.Wait()
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected Wait error to be errBoom, got %v", err)
	}

	// Group context must be canceled after Wait returns.
	if err := g.Context().Err(); err == nil {
		t.Fatalf("expected group context to be cancelled after Wait, got nil")
	}
}

// ---------------------------------------------------------------------------
// Panic recovery
// ---------------------------------------------------------------------------

func TestGroup_Go_PanicWithErrorPreservesWrap(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	g.Go(func(ctx context.Context) error {
		panic(errBoom)
	})

	err := g.Wait()
	if err == nil {
		t.Fatal("expected non-nil error from panicking task")
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected errors.Is(err, errBoom) to be true, got err=%v", err)
	}
	if !strings.Contains(err.Error(), "panic recovered") {
		t.Fatalf("expected error to be labelled as recovered panic, got %q", err.Error())
	}
	// Sanity: a stack trace should be included.
	if !strings.Contains(err.Error(), "goroutine") {
		t.Fatalf("expected stack trace in error, got %q", err.Error())
	}
}

func TestGroup_Go_PanicWithStringIsWrapped(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	g.Go(func(ctx context.Context) error {
		panic("something exploded")
	})

	err := g.Wait()
	if err == nil {
		t.Fatal("expected non-nil error from panicking task")
	}
	if !strings.Contains(err.Error(), "something exploded") {
		t.Fatalf("expected panic value to appear in error message, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "panic recovered") {
		t.Fatalf("expected panic-recovered label, got %q", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Concurrency limit
// ---------------------------------------------------------------------------

func TestGroup_MaxConcurrency_LimitsInFlight(t *testing.T) {
	t.Parallel()

	const limit = 3
	const total = 20

	g := New(context.Background(), WithMaxConcurrency(limit))

	var (
		inflight    atomic.Int32
		maxInflight atomic.Int32
	)

	for i := 0; i < total; i++ {
		g.Go(func(ctx context.Context) error {
			cur := inflight.Add(1)
			// Track high-water mark.
			for {
				peak := maxInflight.Load()
				if cur <= peak || maxInflight.CompareAndSwap(peak, cur) {
					break
				}
			}
			// Simulate a bit of work so the limit is actually observable.
			time.Sleep(20 * time.Millisecond)
			inflight.Add(-1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	if got := maxInflight.Load(); got > int32(limit) {
		t.Fatalf("expected at most %d concurrent tasks, observed %d", limit, got)
	}
}

// ---------------------------------------------------------------------------
// TryGo
// ---------------------------------------------------------------------------

func TestGroup_TryGo_ReturnsFalseWhenSaturated(t *testing.T) {
	t.Parallel()

	// Limit = 1, block that slot with a task that waits on a signal.
	g := New(context.Background(), WithMaxConcurrency(1))

	release := make(chan struct{})
	started := make(chan struct{})

	if ok := g.TryGo(func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	}); !ok {
		t.Fatal("expected first TryGo to succeed")
	}

	<-started // ensure the slot is really occupied

	if ok := g.TryGo(func(ctx context.Context) error {
		return nil
	}); ok {
		t.Fatal("expected second TryGo to be rejected while limit is saturated")
	}

	close(release)
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
}

func TestGroup_TryGo_RecoversPanic(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	if ok := g.TryGo(func(ctx context.Context) error {
		panic("try-go boom")
	}); !ok {
		t.Fatal("expected TryGo to succeed")
	}

	err := g.Wait()
	if err == nil {
		t.Fatal("expected non-nil error from panicking TryGo task")
	}
	if !strings.Contains(err.Error(), "try-go boom") {
		t.Fatalf("expected recovered panic message, got %q", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Pre-flight cancellation check
// ---------------------------------------------------------------------------

func TestGroup_Go_SkipsAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	g := New(ctx)

	// Cancel the parent before scheduling.
	cancel()

	var invoked atomic.Bool
	g.Go(func(ctx context.Context) error {
		invoked.Store(true)
		return nil
	})

	err := g.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Wait to return context.Canceled, got %v", err)
	}
	if invoked.Load() {
		t.Fatal("task body should not have been invoked after cancellation")
	}
}

// ---------------------------------------------------------------------------
// SubGroup semantics
// ---------------------------------------------------------------------------

func TestSubGroup_InheritsMaxConcurrency(t *testing.T) {
	t.Parallel()

	parent := New(context.Background(), WithMaxConcurrency(2))
	child := parent.SubGroup()

	if child.maxConcurrency != 2 {
		t.Fatalf("expected sub-group to inherit maxConcurrency=2, got %d", child.maxConcurrency)
	}

	// Behavioural check: child should honour the limit.
	var (
		inflight    atomic.Int32
		maxInflight atomic.Int32
	)
	for i := 0; i < 10; i++ {
		child.Go(func(ctx context.Context) error {
			cur := inflight.Add(1)
			for {
				peak := maxInflight.Load()
				if cur <= peak || maxInflight.CompareAndSwap(peak, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			inflight.Add(-1)
			return nil
		})
	}
	if err := child.Wait(); err != nil {
		t.Fatalf("child.Wait returned error: %v", err)
	}
	if got := maxInflight.Load(); got > 2 {
		t.Fatalf("expected sub-group to observe at most 2 in-flight, got %d", got)
	}
}

func TestSubGroup_ErrorDoesNotCancelParent(t *testing.T) {
	t.Parallel()

	parent := New(context.Background())

	// Run a child that fails.
	child := parent.SubGroup()
	child.Go(func(ctx context.Context) error {
		return errBoom
	})
	if err := child.Wait(); !errors.Is(err, errBoom) {
		t.Fatalf("expected child.Wait to return errBoom, got %v", err)
	}

	// Parent must still be alive.
	if err := parent.Context().Err(); err != nil {
		t.Fatalf("parent context should not be cancelled by child failure, got %v", err)
	}

	// Parent can still run new work.
	var ran atomic.Bool
	parent.Go(func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})
	if err := parent.Wait(); err != nil {
		t.Fatalf("parent.Wait returned unexpected error: %v", err)
	}
	if !ran.Load() {
		t.Fatal("expected parent task to run after child failure")
	}
}

func TestSubGroup_ParentCancellationCancelsChild(t *testing.T) {
	t.Parallel()

	parentCtx, cancelParent := context.WithCancel(context.Background())
	parent := New(parentCtx)
	child := parent.SubGroup()

	started := make(chan struct{})
	var observedErr error
	var mu sync.Mutex

	child.Go(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		mu.Lock()
		observedErr = ctx.Err()
		mu.Unlock()
		return ctx.Err()
	})

	<-started
	cancelParent()

	err := child.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected child.Wait to return context.Canceled, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(observedErr, context.Canceled) {
		t.Fatalf("expected child task to observe context.Canceled, got %v", observedErr)
	}
}

// ---------------------------------------------------------------------------
// Nil context / defaults
// ---------------------------------------------------------------------------

func TestNew_NilContextFallsBackToBackground(t *testing.T) {
	t.Parallel()

	g := New(nil) //nolint:staticcheck // intentional nil-ctx test
	if g.Context() == nil {
		t.Fatal("expected group context to be non-nil after nil-ctx fallback")
	}

	var ran atomic.Bool
	g.Go(func(ctx context.Context) error {
		if ctx == nil {
			t.Errorf("task received nil ctx")
		}
		ran.Store(true)
		return nil
	})
	if err := g.Wait(); err != nil {
		t.Fatalf("Wait returned error: %v", err)
	}
	if !ran.Load() {
		t.Fatal("expected task to run under Background fallback ctx")
	}
}

func TestNew_ZeroMaxConcurrencyMeansUnlimited(t *testing.T) {
	t.Parallel()

	// Explicitly pass 0 — must not deadlock the way SetLimit(0) would.
	g := New(context.Background(), WithMaxConcurrency(0))

	var counter atomic.Int32
	const n = 50
	for i := 0; i < n; i++ {
		g.Go(func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	// Give Wait a reasonable timeout so a regression here shows as a failure,
	// not a hang.
	done := make(chan error, 1)
	go func() { done <- g.Wait() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return within 2s — SetLimit(0) may be blocking tasks")
	}

	if got := counter.Load(); got != n {
		t.Fatalf("expected %d tasks to run, got %d", n, got)
	}
}

// ---------------------------------------------------------------------------
// Context() accessor
// ---------------------------------------------------------------------------

func TestGroup_Context_IsCancelledOnFirstError(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	g.Go(func(ctx context.Context) error {
		return errBoom
	})

	// Wait to let the error propagate.
	_ = g.Wait()

	if err := g.Context().Err(); err == nil {
		t.Fatal("expected group ctx to be cancelled after a task error")
	}
}
