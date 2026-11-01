package errgroup

import (
	"context"
	"errors"
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
	for range n {
		g.Go(func(ctx context.Context) error {
			counter.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Wait returned unexpected error: %v", err)
	}
	if got := counter.Load(); got != n {
		t.Fatalf("Go(%d tasks) ran = %d, want %d", n, got, n)
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
		t.Fatalf("Wait() = %v, want errBoom", err)
	}

	// Group context must be canceled after Wait returns.
	if err := g.Context().Err(); err == nil {
		t.Fatal("Context().Err() = nil, want canceled after Wait")
	}
}

// ---------------------------------------------------------------------------
// Panic recovery
// ---------------------------------------------------------------------------

func TestGroup_Go_PanicWithErrorPreservesWrap(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("sentinel")
	g := New(context.Background())

	g.Go(func(ctx context.Context) error {
		panic(sentinel)
	})

	err := g.Wait()
	if !errors.Is(err, sentinel) {
		t.Fatalf("Wait() error = %v, want errors.Is sentinel", err)
	}
	pe, ok := errors.AsType[*PanicError](err)
	if !ok {
		t.Fatalf("Wait() error = %T, want *PanicError in chain", err)
	}
	if len(pe.Stack) == 0 {
		t.Error("PanicError.Stack is empty, want a stack trace")
	}
}

func TestGroup_Go_PanicWithStringIsWrapped(t *testing.T) {
	t.Parallel()

	g := New(context.Background())

	g.Go(func(ctx context.Context) error {
		panic("boom")
	})

	err := g.Wait()
	pe, ok := errors.AsType[*PanicError](err)
	if !ok {
		t.Fatalf("Wait() error = %T, want *PanicError in chain", err)
	}
	if pe.Value != "boom" {
		t.Errorf("PanicError.Value = %v, want %q", pe.Value, "boom")
	}
	if len(pe.Stack) == 0 {
		t.Error("PanicError.Stack is empty, want a stack trace")
	}
	if got := pe.Unwrap(); got != nil {
		t.Errorf("PanicError.Unwrap() = %v, want nil", got)
	}
	if got, want := pe.Error(), "errgroup: task panicked: boom"; got != want {
		t.Errorf("PanicError.Error() = %q, want %q", got, want)
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

	for range total {
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
		t.Fatalf("max in-flight = %d, want at most %d", got, limit)
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
		t.Fatal("TryGo(first) = false, want true")
	}

	<-started // ensure the slot is really occupied

	if ok := g.TryGo(func(ctx context.Context) error {
		return nil
	}); ok {
		t.Fatal("TryGo(saturated) = true, want false")
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
		t.Fatal("TryGo(panic boom) = false, want true")
	}

	pe, ok := errors.AsType[*PanicError](g.Wait())
	if !ok {
		t.Fatal("Wait() error is not a *PanicError from panicking TryGo task")
	}
	if pe.Value != "try-go boom" {
		t.Errorf("PanicError.Value = %v, want %q", pe.Value, "try-go boom")
	}
}

func TestGroup_TryGo_PanicWithErrorPreservesWrap(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("sentinel")
	g := New(context.Background())

	if ok := g.TryGo(func(ctx context.Context) error {
		panic(sentinel)
	}); !ok {
		t.Fatal("TryGo(panic sentinel) = false, want true")
	}

	err := g.Wait()
	if !errors.Is(err, sentinel) {
		t.Fatalf("Wait() error = %v, want errors.Is sentinel", err)
	}
	if _, ok := errors.AsType[*PanicError](err); !ok {
		t.Fatalf("Wait() error = %T, want *PanicError in chain", err)
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
		t.Fatalf("Wait() = %v, want context.Canceled", err)
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
		t.Fatalf("SubGroup().maxConcurrency = %d, want 2", child.maxConcurrency)
	}

	// Behavioural check: child should honour the limit.
	var (
		inflight    atomic.Int32
		maxInflight atomic.Int32
	)
	for range 10 {
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
		t.Fatalf("sub-group max in-flight = %d, want at most 2", got)
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
		t.Fatalf("child.Wait() = %v, want errBoom", err)
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
		t.Fatal("parent task ran = false, want true after child failure")
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
		t.Fatalf("child.Wait() = %v, want context.Canceled", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(observedErr, context.Canceled) {
		t.Fatalf("child task ctx.Err() = %v, want context.Canceled", observedErr)
	}
}

// ---------------------------------------------------------------------------
// Nil context / defaults
// ---------------------------------------------------------------------------

func TestNew_ZeroMaxConcurrencyMeansUnlimited(t *testing.T) {
	t.Parallel()

	// Explicitly pass 0 — must not deadlock the way SetLimit(0) would.
	g := New(context.Background(), WithMaxConcurrency(0))

	var counter atomic.Int32
	const n = 50
	for range n {
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
		t.Fatalf("Go(%d tasks) ran = %d, want %d", n, got, n)
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
		t.Fatal("Context().Err() = nil, want canceled after task error")
	}
}
