// Package errgroup provides a concurrency-limited, panic-safe wrapper around
// golang.org/x/sync/errgroup.
//
// Compared to the upstream errgroup, this package:
//   - Enforces a maxConcurrency limit via functional options.
//   - Recovers from panics inside tasks and converts them into errors,
//     preserving the original error type (via %w) and the panic stack trace.
//   - Skips scheduling tasks whose parent context is already canceled.
//   - Supports child groups (SubGroup) that inherit the parent's context
//     and default concurrency limit.
package errgroup

import (
	"context"
	"fmt"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
)

// Group is a concurrency-limited, panic-safe wrapper over errgroup.Group.
//
// The zero value is not usable; always construct a Group with New.
// A Group is safe for concurrent use by multiple goroutines calling Go / TryGo,
// following the same rules as the upstream errgroup.Group.
type Group struct {
	// eg is the underlying errgroup that owns goroutine lifecycle,
	// first-error reporting, and the shared cancellation ctx.
	eg *errgroup.Group

	// ctx is the group's cancellation-linked context, derived from the ctx
	// passed to New via errgroup.WithContext. It is cancelled on the first
	// task error, the first recovered panic, or when Wait returns.
	ctx context.Context

	// maxConcurrency is the configured limit propagated to child groups
	// via SubGroup. A value <= 0 means "no limit".
	maxConcurrency int
}

// New returns a Group whose internal context is derived from ctx and cancelled
// on the first task error, the first recovered panic, or when Wait returns.
//
// A nil ctx is treated as context.Background(); callers are encouraged to pass
// context.Background() explicitly rather than relying on this fallback.
//
// Options are applied left-to-right; see WithMaxConcurrency for the primary
// knob. If maxConcurrency <= 0, no concurrency limit is enforced.
func New(ctx context.Context, opts ...Option) *Group {
	o := &options{maxConcurrency: DefaultMaxConcurrency}
	for _, opt := range opts {
		opt(o)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	eg, egCtx := errgroup.WithContext(ctx)
	if o.maxConcurrency > 0 {
		// SetLimit(0) would block *all* new goroutines — treat 0 as "unlimited"
		// to match errgroup's default and avoid a silent deadlock.
		eg.SetLimit(o.maxConcurrency)
	}

	return &Group{
		eg:             eg,
		ctx:            egCtx,
		maxConcurrency: o.maxConcurrency,
	}
}

// Go schedules f to be executed on a group-managed goroutine.
//
// If the group's context is already canceled when f would start, f is not
// invoked and the cancellation error is reported to the group instead.
//
// Panics inside f are recovered and returned as an error wrapped with a
// captured stack trace. When the panic value is itself an error, it is
// wrapped with %w so callers can use errors.Is / errors.As against it.
//
// Only the first non-nil error observed by the group is retained; Wait
// returns that error and cancels the group's context.
func (g *Group) Go(f func(ctx context.Context) error) {
	g.eg.Go(g.adaptTask(f))
}

// TryGo behaves like Go but returns false without scheduling if the
// configured concurrency limit has been reached.
//
// TryGo shares Go's panic-recovery and pre-flight cancellation semantics.
func (g *Group) TryGo(f func(ctx context.Context) error) bool {
	return g.eg.TryGo(g.adaptTask(f))
}

// Wait blocks until all scheduled tasks have returned, then returns the
// first non-nil error (if any) reported to the group.
//
// After Wait returns, the group's context is cancelled and the Group must
// not be reused.
func (g *Group) Wait() error {
	return g.eg.Wait()
}

// Context returns the group's cancellation-linked context. Callers can use it
// to plumb cancellation into non-task work (e.g. cleanup, follow-up requests)
// that should abort when any task in the group fails.
func (g *Group) Context() context.Context {
	return g.ctx
}

// SubGroup returns a child Group whose context is derived from g.Context()
// and whose maxConcurrency defaults to the parent's (overridable via opts).
//
// The child installs its own errgroup.WithContext layer, so an error or
// panic inside the child cancels the child's context only — it does not
// propagate to the parent. Cancelling the parent, however, will cancel the
// child (because the child's ctx descends from the parent's).
//
// This is useful for scoping a batch of related tasks whose failures should
// not tear down the parent group.
func (g *Group) SubGroup(opts ...Option) *Group {
	subGroupOptions := []Option{WithMaxConcurrency(g.maxConcurrency)}
	subGroupOptions = append(subGroupOptions, opts...)
	return New(g.ctx, subGroupOptions...)
}

// adaptTask wraps a user-supplied task function with:
//  1. A pre-flight check on the group's context — tasks scheduled after the
//     group is already canceled return the cancellation error immediately
//     without invoking fn.
//  2. Panic recovery — panics are turned into wrapped errors carrying the
//     captured debug.Stack(). When the panic value is an error, %w is used
//     so callers can still errors.Is / errors.As it.
//
// The returned closure matches errgroup.Group.Go's expected signature.
func (g *Group) adaptTask(fn func(ctx context.Context) error) func() error {
	return func() (err error) {
		defer func() {
			if e := recover(); e != nil {
				if pe, ok := e.(error); ok {
					err = fmt.Errorf("panic recovered: %w\n%s", pe, debug.Stack())
				} else {
					err = fmt.Errorf("panic recovered: %v\n%s", e, debug.Stack())
				}
			}
		}()

		// Fast path: if the group is already cancelled, do not even invoke fn.
		// This keeps queued-behind-SetLimit tasks from doing pointless work
		// after a sibling has already failed.
		if ctxErr := g.ctx.Err(); ctxErr != nil {
			err = ctxErr
			return
		}

		if fnErr := fn(g.ctx); fnErr != nil {
			err = fnErr
			return
		}

		return
	}
}
