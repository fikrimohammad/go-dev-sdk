package confloader

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
)

// mockClient is an in-memory client used to exercise the loader without a
// real etcd/infisical instance. It records the sequence of fetched values so
// tests can simulate changes and transient failures.
type mockClient struct {
	mu      sync.Mutex
	store   map[string]map[string]string // folder -> key -> value
	rev     map[string]map[string]int64  // folder -> key -> revision counter
	failFor map[string]map[string]error  // folder -> key -> injected error
	calls   int64
	delay   time.Duration // optional per-fetch sleep to force overlap in tests
}

func newMockClient() *mockClient {
	return &mockClient{
		store:   map[string]map[string]string{},
		rev:     map[string]map[string]int64{},
		failFor: map[string]map[string]error{},
	}
}

func (m *mockClient) set(folder, key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.store[folder] == nil {
		m.store[folder] = map[string]string{}
		m.rev[folder] = map[string]int64{}
	}
	if _, ok := m.rev[folder][key]; !ok {
		m.rev[folder][key] = 0
	}
	m.store[folder][key] = value
	m.rev[folder][key]++
}

func (m *mockClient) fail(folder, key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failFor[folder] == nil {
		m.failFor[folder] = map[string]error{}
	}
	m.failFor[folder][key] = err
}

func (m *mockClient) clearFail(folder, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failFor[folder] != nil {
		delete(m.failFor[folder], key)
	}
}

func mockConfig() Config {
	return Config{
		Provider:         ProviderEtcd, // irrelevant; we inject the client
		Endpoint:         "localhost:2379",
		AuthClientID:     "user",
		AuthClientSecret: "pass",
		Namespace:        "proj",
		Watcher:          DefaultWatcherConfig(),
	}
}

// withMock builds a Loader for testConfig backed by the in-memory client.
func withMock(t *testing.T, mc client.Client, cfg Config, opts ...Option) *Loader[testConfig] {
	t.Helper()
	all := append([]Option{WithClient(mc)}, opts...)
	ldr, err := New[testConfig](context.Background(), cfg, all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ldr
}

type dbConfig struct {
	Host string `json:"host" yaml:"host"`
	Port int    `json:"port" yaml:"port"`
}

type testConfig struct {
	DBConfig Getter[dbConfig] `conf:"folder=default,key=db_config"`
	Debug    Getter[bool]     `conf:"folder=settings,key=debug"`
	Missing  Getter[string]   `conf:"folder=settings,key=absent"`
}

func TestLoaderInitialFetchAndGet(t *testing.T) {
	mc := newMockClient()
	mc.set("default", "db_config", `{"host":"localhost","port":5432}`)
	mc.set("settings", "debug", "true")

	ldr := withMock(t, mc, mockConfig())
	defer func() { _ = ldr.Stop() }()

	db, err := ldr.Data().DBConfig.Get(context.Background())
	if err != nil {
		t.Fatalf("DBConfig: %v", err)
	}
	if db.Host != "localhost" || db.Port != 5432 {
		t.Fatalf("unexpected db config: %+v", db)
	}
	debug, err := ldr.Data().Debug.Get(context.Background())
	if err != nil {
		t.Fatalf("Debug: %v", err)
	}
	if !debug {
		t.Fatalf("expected debug true")
	}
}

func TestLoaderDefaultFolderFallback(t *testing.T) {
	mc := newMockClient()
	// debug only present in the "default" folder; requested from "settings".
	mc.set("default", "debug", "false")

	ldr := withMock(t, mc, mockConfig())
	defer func() { _ = ldr.Stop() }()

	debug, err := ldr.Data().Debug.Get(context.Background())
	if err != nil {
		t.Fatalf("Debug fallback: %v", err)
	}
	if debug {
		t.Fatalf("expected default-folder fallback value false")
	}
}

func TestLoaderNotFound(t *testing.T) {
	mc := newMockClient()
	ldr := withMock(t, mc, mockConfig())
	defer func() { _ = ldr.Stop() }()

	if _, err := ldr.Data().Missing.Get(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLoaderPollingDetectsChange(t *testing.T) {
	mc := newMockClient()
	mc.set("default", "db_config", `{"host":"old","port":1}`)

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 50 * time.Millisecond
	cfg.Watcher.PollingMaxRetries = 1
	cfg.Watcher.PollingRetryDelay = 10 * time.Millisecond

	ldr := withMock(t, mc, cfg)
	defer func() { _ = ldr.Stop() }()

	// The cache is populated by the initial fetch.
	db, err := ldr.Data().DBConfig.Get(context.Background())
	if err != nil {
		t.Fatalf("initial Get: %v", err)
	}
	if db.Host != "old" {
		t.Fatalf("expected old host, got %s", db.Host)
	}

	// Mutate the client; polling should pick it up so the next Get returns it.
	mc.set("default", "db_config", `{"host":"new","port":2}`)

	deadline := time.Now().Add(2 * time.Second)
	for {
		db, _ = ldr.Data().DBConfig.Get(context.Background())
		if db.Host == "new" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("polling did not pick up change; last host=%s", db.Host)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLoaderStaleRefreshesOnRead(t *testing.T) {
	mc := newMockClient()
	mc.set("default", "db_config", `{"host":"good","port":1}`)

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 40 * time.Millisecond
	cfg.Watcher.PollingMaxRetries = 1
	cfg.Watcher.PollingRetryDelay = 10 * time.Millisecond

	ldr := withMock(t, mc, cfg)
	defer func() { _ = ldr.Stop() }()

	db, err := ldr.Data().DBConfig.Get(context.Background())
	if err != nil || db.Host != "good" {
		t.Fatalf("initial Get: %v %+v", err, db)
	}

	// Inject a client failure on the next poll.
	mc.fail("default", "db_config", errors.New("backend down"))

	// Wait for at least one poll cycle to fail and mark the entry stale.
	deadline := time.Now().Add(2 * time.Second)
	for !ldr.IsStale() {
		if time.Now().After(deadline) {
			t.Fatalf("loader never reported stale after injected failure")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// StaleReason exposes the underlying cause for callers that log but proceed.
	if reason := ldr.StaleReason("default", "db_config"); reason == nil {
		t.Fatalf("expected StaleReason to return the underlying error")
	}

	// Heal the client; a Get on the stale entry must refresh directly from the
	// source and return the fresh value (no longer stale, no error).
	mc.clearFail("default", "db_config")
	deadline = time.Now().Add(2 * time.Second)
	for {
		db, err = ldr.Data().DBConfig.Get(context.Background())
		if err == nil && db.Host == "good" && !ldr.IsStale() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale entry not refreshed on read: err=%v host=%s stale=%v", err, db.Host, ldr.IsStale())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestLoaderStaleHardFailure verifies that a stale entry whose on-read refresh
// still fails surfaces the source error directly (never ErrStale): Get is pure
// read-through and reports the backend's own failure.
func TestLoaderStaleHardFailure(t *testing.T) {
	mc := newMockClient()
	mc.set("default", "debug", "true")

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 40 * time.Millisecond
	cfg.Watcher.PollingMaxRetries = 1
	cfg.Watcher.PollingRetryDelay = 10 * time.Millisecond

	ldr := withMock(t, mc, cfg)
	defer func() { _ = ldr.Stop() }()

	if _, err := ldr.Data().Debug.Get(context.Background()); err != nil {
		t.Fatalf("initial fetch should succeed: %v", err)
	}

	// Permanent backend failure.
	mc.fail("default", "debug", errors.New("backend down"))
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := ldr.Data().Debug.Get(context.Background())
		if err != nil && !errors.Is(err, ErrStale) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("getter never returned the source error (got %v)", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestGetterGetWithDefault verifies the safe-fallback variant: on a failed
// read it returns the supplied default instead of an error, and on success it
// returns the value.
func TestGetterGetWithDefault(t *testing.T) {
	mc := newMockClient()
	mc.set("settings", "debug", "true")

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 5 * time.Second // no poll interference

	ldr, err := New[testConfig](context.Background(), cfg, WithClient(mc), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ldr.Stop() }()

	// Success path returns the fetched value.
	got := ldr.Data().Debug.GetWithDefault(context.Background(), false)
	if !got {
		t.Fatalf("expected true from GetWithDefault success")
	}

	// Failure path (absent key) returns the default, no error.
	mc2 := newMockClient() // "debug" never set
	ldr2, err := New[testConfig](context.Background(), cfg, WithClient(mc2), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = ldr2.Stop() }()

	if v := ldr2.Data().Debug.GetWithDefault(context.Background(), true); v != true {
		t.Fatalf("expected default true on absent key, got %v", v)
	}
}

// TestLoaderPanicRecovered verifies that a client panic during a refresh does
// not kill the polling loop: the entry is marked stale (so getters fail) and
// the loop keeps running and recovers once the client stops panicking.
func TestLoaderPanicRecovered(t *testing.T) {
	mc := newMockClient()
	mc.set("default", "debug", "true")

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 40 * time.Millisecond
	cfg.Watcher.PollingMaxRetries = 1
	cfg.Watcher.PollingRetryDelay = 10 * time.Millisecond

	ldr := withMock(t, mc, cfg)
	defer func() { _ = ldr.Stop() }()

	if _, err := ldr.Data().Debug.Get(context.Background()); err != nil {
		t.Fatalf("initial fetch should succeed: %v", err)
	}

	// Make the next refresh panic.
	mc.fail("default", "debug", errPanic{})

	deadline := time.Now().Add(2 * time.Second)
	for !ldr.IsStale() {
		if time.Now().After(deadline) {
			t.Fatalf("loader never became stale after client panic")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The loop must still be alive: healing yields a clean fetch.
	mc.clearFail("default", "debug")
	deadline = time.Now().Add(2 * time.Second)
	for {
		_, err := ldr.Data().Debug.Get(context.Background())
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("loader did not recover after panic healed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// errPanic is an error value that makes the mock client panic on Fetch, to
// exercise the polling loop's panic recovery.
type errPanic struct{}

func (errPanic) Error() string { return "boom" }

func (m *mockClient) Fetch(_ context.Context, folder, key string) (client.Fetched, error) {
	atomic.AddInt64(&m.calls, 1)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if fm, ok := m.failFor[folder]; ok {
		if err, ok := fm[key]; ok && err != nil {
			if _, isPanic := err.(errPanic); isPanic {
				panic(err)
			}
			return client.Fetched{}, err
		}
	}
	fm, ok := m.store[folder]
	if !ok {
		return client.Fetched{}, client.ErrNotFound
	}
	v, ok := fm[key]
	if !ok {
		return client.Fetched{}, client.ErrNotFound
	}
	return client.Fetched{Value: v, Revision: strconv.FormatInt(m.rev[folder][key], 10)}, nil
}

func (m *mockClient) Close() error { return nil }

func TestLoaderParseFailure(t *testing.T) {
	mc := newMockClient()
	mc.set("default", "db_config", `not-json`) // invalid for struct

	ldr := withMock(t, mc, mockConfig())
	defer func() { _ = ldr.Stop() }()

	if _, err := ldr.Data().DBConfig.Get(context.Background()); err == nil {
		t.Fatalf("expected parse error")
	}
}

// TestLoaderLazyStart verifies that WithInitialFetch(false) lets New return
// even when the backend is down; a Get later fetches directly from the source.
func TestLoaderLazyStart(t *testing.T) {
	mc := newMockClient()
	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 40 * time.Millisecond
	cfg.Watcher.PollingMaxRetries = 1
	cfg.Watcher.PollingRetryDelay = 10 * time.Millisecond

	ldr, err := New[testConfig](context.Background(), cfg, WithClient(mc), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("lazy New should not block: %v", err)
	}
	defer func() { _ = ldr.Stop() }()

	mc.set("settings", "debug", "true")

	// No poll has necessarily run; Get fetches on demand.
	got, err := ldr.Data().Debug.Get(context.Background())
	if err != nil {
		t.Fatalf("lazy Get should have fetched on demand: %v", err)
	}
	if !got {
		t.Fatalf("expected true from on-demand lazy fetch")
	}
}

// TestLoaderLazyGetTriggersFetch verifies that, on a lazy-started loader, the
// first Get() of a never-fetched key triggers an on-demand fetch from the
// source instead of misreporting the cold key as ErrNotFound (the polling
// delay risk you raised).
func TestLoaderLazyGetTriggersFetch(t *testing.T) {
	mc := newMockClient()
	mc.set("settings", "debug", "true")

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 5 * time.Second // deliberately long: would
	cfg.Watcher.PollingMaxRetries = 1             // otherwise starve a poll
	cfg.Watcher.PollingRetryDelay = 10 * time.Millisecond

	ldr, err := New[testConfig](context.Background(), cfg, WithClient(mc), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("lazy New should not block: %v", err)
	}
	defer func() { _ = ldr.Stop() }()

	// No poll has run (interval is 5s), yet Get() must fetch on demand.
	got, err := ldr.Data().Debug.Get(context.Background())
	if err != nil {
		t.Fatalf("cold Get should have fetched on demand: %v", err)
	}
	if !got {
		t.Fatalf("expected true from on-demand cold fetch")
	}
	if mc.calls == 0 {
		t.Fatalf("expected at least one backend call from cold Get")
	}
}

// TestLoaderLazyGetColdNotFound verifies that a genuinely absent cold key still
// surfaces ErrNotFound (not a zero/false positive) after the on-demand fetch.
func TestLoaderLazyGetColdNotFound(t *testing.T) {
	mc := newMockClient() // "debug" never set

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 5 * time.Second
	cfg.Watcher.PollingMaxRetries = 0
	cfg.Watcher.PollingRetryDelay = 5 * time.Millisecond

	ldr, err := New[testConfig](context.Background(), cfg, WithClient(mc), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("lazy New should not block: %v", err)
	}
	defer func() { _ = ldr.Stop() }()

	if _, err := ldr.Data().Debug.Get(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for cold absent key, got %v", err)
	}
}

// TestLoaderLazyGetConcurrentCoalesces verifies that many concurrent Get()s on
// a cold key trigger a single backend fetch (coalesced via the entry lock),
// not one-per-caller.
func TestLoaderLazyGetConcurrentCoalesces(t *testing.T) {
	mc := newMockClient()
	mc.set("settings", "debug", "true")

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 5 * time.Second
	cfg.Watcher.PollingMaxRetries = 1
	cfg.Watcher.PollingRetryDelay = 5 * time.Millisecond

	ldr, err := New[testConfig](context.Background(), cfg, WithClient(mc), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("lazy New should not block: %v", err)
	}
	defer func() { _ = ldr.Stop() }()

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, gerr := ldr.Data().Debug.Get(ctx)
			if gerr != nil {
				errs <- gerr
				return
			}
			if !got {
				errs <- fmt.Errorf("expected true")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent cold Get failed: %v (calls=%d)", e, atomic.LoadInt64(&mc.calls))
	}
	if mc.calls != 1 {
		t.Fatalf("expected exactly one coalesced cold fetch, got %d", mc.calls)
	}
}

// TestLoaderConcurrentStaleCoalesces verifies that many concurrent Get()s on a
// STALE entry trigger a single backend refresh, not one-per-caller. This is the
// case the fetchMu (separate from the data lock) fixes: the freshness re-check
// under fetchMu lets late waiters skip the redundant fetch once the first
// caller repopulates the entry. A per-fetch delay forces the goroutines to
// overlap so a herd would be observable.
func TestLoaderConcurrentStaleCoalesces(t *testing.T) {
	mc := newMockClient()
	mc.set("settings", "debug", "true")

	cfg := mockConfig()
	cfg.Watcher.PollingInterval = 5 * time.Second // keep the poll out of the way
	cfg.Watcher.PollingMaxRetries = 0

	ldr, err := New[testConfig](context.Background(), cfg, WithClient(mc), WithInitialFetch(false))
	if err != nil {
		t.Fatalf("lazy New: %v", err)
	}
	defer func() { _ = ldr.Stop() }()

	// Populate once, then force the entry into a stale state.
	if _, err := ldr.Data().Debug.Get(context.Background()); err != nil {
		t.Fatalf("warm-up Get: %v", err)
	}
	entry := ldr.entries["Debug"]
	entry.mu.Lock()
	entry.stale = true
	entry.fetchErr = errors.New("previous refresh failed")
	entry.mu.Unlock()

	// From now on, count fetches and make each one slow so callers overlap.
	atomic.StoreInt64(&mc.calls, 0)
	mc.delay = 30 * time.Millisecond

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, gerr := ldr.Data().Debug.Get(ctx)
			if gerr != nil {
				errs <- gerr
				return
			}
			if !got {
				errs <- fmt.Errorf("expected true")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent stale Get failed: %v (calls=%d)", e, atomic.LoadInt64(&mc.calls))
	}
	if n := atomic.LoadInt64(&mc.calls); n != 1 {
		t.Fatalf("expected exactly one coalesced stale refresh, got %d", n)
	}
}
