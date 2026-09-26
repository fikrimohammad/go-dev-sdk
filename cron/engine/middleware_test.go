package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/fikrimohammad/go-dev-sdk/cron"
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
