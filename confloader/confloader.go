// Package confloader loads and watches hot configuration / secrets from
// pluggable backends (etcd, infisical, ...).
//
// Devs declare their config as a struct of Getter[T] fields, each tagged with
// `conf:"folder=...,key=..."`. The loader eagerly fetches every entry (unless
// started lazily), keeps them fresh via a background cache-with-polling loop,
// and exposes typed, thread-safe accessors. A Getter.Get performs a direct
// source fetch whenever the cached value is missing or stale, so reads never
// block on the polling loop and never misreport a cold or stale key.
package confloader

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
	"github.com/fikrimohammad/go-dev-sdk/confloader/client/etcd"
	"github.com/fikrimohammad/go-dev-sdk/confloader/client/infisical"
)

// Getter is the typed accessor for a single config value. It is created by the
// loader and bound to a (folder, key) pair plus a cache slot.
//
//	Get(ctx) (T, error)  returns the latest value, fetching directly from the
//	  source when the cached value is missing or stale. It surfaces the raw
//	  source error (e.g. timeout, provider down, or a definitive ErrNotFound
//	  when the key is absent in both the requested folder and the default
//	  folder). Get never returns ErrStale: a stale entry is refreshed on read
//	  and either yields the fresh value or the source's own error. Per Go
//	  convention the returned T is the zero value on error — callers MUST
//	  check err.
//
//	GetWithDefault(ctx, def) T  is the same read but never fails: on any error
//	  it returns def, so callers that can tolerate drift get a safe fallback
//	  without error handling.
type Getter[T any] struct {
	// Core binds this accessor to the loader's cache. It is an exported field
	// of an unexported interface type so reflection can populate it; devs
	// should treat it as opaque and only call Get / GetWithDefault.
	Core getterImpl
}

// getterImpl is the un-typed behaviour shared by every Getter[T].
type getterImpl interface {
	get(ctx context.Context) (any, error)
}

// Get returns the latest value, fetching directly from the source when the
// cache is missing or stale. It returns the source's own error on failure
// (timeout, provider down, or ErrNotFound for a definitively absent key)
func (g Getter[T]) Get(ctx context.Context) (T, error) {
	var zero T
	v, err := g.Core.get(ctx)
	if err != nil {
		return zero, err
	}
	return v.(T), nil
}

// GetWithDefault returns the latest value, or def if the read fails for any
// reason (including a source error or a definitively absent key). It never
// returns an error, giving callers a safe fallback path.
func (g Getter[T]) GetWithDefault(ctx context.Context, def T) T {
	v, err := g.Core.get(ctx)
	if err != nil {
		return def
	}
	return v.(T)
}

// getterCore implements getterImpl against a loaderCore's cache.
type getterCore struct {
	name   string
	loader *loaderCore
}

// get returns the value for the entry, refreshing directly from the source
// when the cache is missing or stale. On a cache hit it returns immediately
// (no I/O, no coupling to the polling loop). It never reports staleness: a
// stale entry is refreshed on read, so the result is either the fresh value or
// the source's own error.
func (c *getterCore) get(ctx context.Context) (any, error) {
	entry, ok := c.loader.entries[c.name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown entry %q", ErrProviderFailed, c.name)
	}

	if needRefresh(entry) {
		m, ok := c.loader.metaForField(c.name)
		if ok {
			c.loader.refreshOne(ctx, m, false)
		}
	}

	val, has, _, _, ferr := entry.get()
	if !has {
		if ferr != nil {
			return nil, ferr
		}
		return nil, ErrNotFound
	}
	if ferr != nil {
		// The read-through fetch failed (timeout, source down, ...). Surface
		// the source's own error rather than a stale value or ErrStale.
		return nil, ferr
	}
	return val, nil
}

// needRefresh reports whether the entry must be fetched before serving a read:
// it has never been fetched (cold) or its last refresh failed (stale). This is
// a lock-free pre-check; refreshOne re-validates under the entry lock.
func needRefresh(entry *cacheEntry) bool {
	entry.mu.RLock()
	defer entry.mu.RUnlock()
	return entry.lastFetch.IsZero() || entry.stale
}

// cacheEntry is one cached (value, revision) slot, guarded for concurrent use
// by the polling goroutine and the Getter callers.
type cacheEntry struct {
	mu sync.RWMutex

	// fetchMu serializes backend fetches for this entry so concurrent cold or
	// stale reads coalesce into a single call. It is distinct from mu: the
	// network fetch is done under fetchMu but NOT under mu, so readers of the
	// cached fields never block on the network.
	fetchMu sync.Mutex

	typ reflect.Type // expected value type (set once at build time)

	value    any    // parsed value (typed)
	revision string // client revision string
	hasValue bool   // false until first successful fetch

	fetchErr   error // last fetch error (nil if last fetch succeeded)
	lastFetch  time.Time
	stale      bool // true when lastFetch failed and we are serving old data
	lastChange time.Time
}

func (e *cacheEntry) get() (any, bool, time.Time, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.value, e.hasValue, e.lastChange, e.stale, e.fetchErr
}

// loaderCore holds the provider-agnostic, non-generic state of a Loader.
// Splitting it out lets Getter closures reference the cache without needing
// the concrete config type T.
type loaderCore struct {
	cfg     Config
	client  client.Client
	handler func(folder, key string, err error)

	metas   []getterMeta
	entries map[string]*cacheEntry // keyed by meta.name (immutable after New)
	byKey   map[string]*cacheEntry // keyed by "folder/key" (immutable after New)

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// Loader fetches and watches config from a client using a cache-with-polling
// model. Devs declare their config as a struct of Getter[T] fields tagged with
// `conf:"folder=...,key=..."`; the loader eagerly fetches every entry, keeps
// them fresh on a polling interval, and exposes typed getters that refresh
// directly from the source on a miss or a stale entry.
//
// Concurrency: one Loader may be shared across goroutines. Each Getter is safe
// for concurrent use. Call Stop to release resources.
type Loader[T any] struct {
	*loaderCore
	data T
}

// Option configures a Loader at construction time.
type Option func(*options)

type options struct {
	client       client.Client
	initialFetch bool
	errorHandler func(folder, key string, err error)
}

// WithClient injects a pre-built client, overriding the provider factory. Used
// by tests and by callers who want a custom backend without editing this
// package.
func WithClient(c client.Client) Option {
	return func(o *options) { o.client = c }
}

// WithInitialFetch controls whether New performs a blocking initial fetch of
// every entry. When false, entries are populated on the first successful
// polling cycle (lazy start) — New returns immediately even if the backend is
// down.
func WithInitialFetch(do bool) Option {
	return func(o *options) { o.initialFetch = do }
}

// WithErrorHandler registers a callback invoked for every refresh error
// (transient failures and parse errors), useful for logging/metrics.
func WithErrorHandler(h func(folder, key string, err error)) Option {
	return func(o *options) { o.errorHandler = h }
}

// New builds a Loader for config struct T, connects to the configured client
// (unless one is injected via WithClient), and — unless WithInitialFetch(false)
// is given — performs the initial fetch of every Getter, bounded by ctx.
//
// Callers must call Stop when finished.
func New[T any](ctx context.Context, cfg Config, opts ...Option) (*Loader[T], error) {
	var zero T
	tType := reflect.TypeOf(zero)
	if tType == nil || tType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("confloader: T must be a struct, got %T", zero)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	o := options{initialFetch: true}
	for _, opt := range opts {
		opt(&o)
	}

	metas, err := reflectGetters(tType)
	if err != nil {
		return nil, err
	}

	var c client.Client
	if o.client != nil {
		c = o.client
	} else {
		c, err = newClient(cfg)
		if err != nil {
			return nil, err
		}
	}

	core := &loaderCore{
		cfg:     cfg,
		client:  c,
		handler: o.errorHandler,
		metas:   metas,
		entries: make(map[string]*cacheEntry, len(metas)),
		byKey:   make(map[string]*cacheEntry, len(metas)),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	l := &Loader[T]{loaderCore: core}

	for _, m := range metas {
		e := &cacheEntry{typ: m.typ}
		core.entries[m.name] = e
		core.byKey[m.folder+"/"+m.key] = e
	}
	if err := l.buildData(); err != nil {
		_ = c.Close()
		return nil, err
	}

	if o.initialFetch {
		l.refreshAll(ctx)
	}

	l.startPolling()
	return l, nil
}

// newClient builds the concrete client for cfg.Provider.
func newClient(cfg Config) (client.Client, error) {
	switch cfg.Provider {
	case ProviderEtcd:
		return etcd.New(etcd.Config{
			Endpoint:         cfg.Endpoint,
			AuthClientID:     cfg.AuthClientID,
			AuthClientSecret: cfg.AuthClientSecret,
			Namespace:        cfg.Namespace,
		})
	case ProviderInfisical:
		return infisical.New(infisical.Config{
			Endpoint:         cfg.Endpoint,
			AuthClientID:     cfg.AuthClientID,
			AuthClientSecret: cfg.AuthClientSecret,
			Namespace:        cfg.Namespace,
			Environment:      cfg.Environment,
		})
	default:
		return nil, ErrUnsupportedProvider
	}
}

// Data returns the app config struct populated with typed Getter fields.
// Call Get on a field to read the current value.
func (l *Loader[T]) Data() T {
	return l.data
}

// buildData uses reflection to set each Getter[T_i] field on a fresh T to a
// Getter[T_i] bound (via a getterCore) to the matching cache entry.
func (l *Loader[T]) buildData() error {
	dataVal := reflect.New(reflect.TypeOf(l.data)).Elem()
	dataType := dataVal.Type()

	for i := 0; i < dataType.NumField(); i++ {
		field := dataType.Field(i)
		ft := field.Type
		// Only Getter[T] fields (detected by their Get method) get a
		// bound accessor; everything else is left untouched.
		if _, ok := ft.MethodByName("Get"); !ok {
			continue
		}
		meta, ok := l.metaForField(field.Name)
		if !ok {
			continue
		}
		// field.Type is the concrete Getter[T_i]; allocate it and set its
		// exported core field to a getterCore bound to this loader.
		gv := reflect.New(ft)
		core := &getterCore{name: meta.name, loader: l.loaderCore}
		gv.Elem().FieldByName("Core").Set(reflect.ValueOf(core))
		dataVal.Field(i).Set(gv.Elem())
	}

	l.data = dataVal.Interface().(T)
	return nil
}

func (c *loaderCore) metaForField(name string) (getterMeta, bool) {
	for _, m := range c.metas {
		if m.name == name {
			return m, true
		}
	}
	return getterMeta{}, false
}

// refreshAll fetches every entry once, applying the per-entry retry policy.
func (c *loaderCore) refreshAll(ctx context.Context) {
	for _, m := range c.metas {
		c.refreshOne(ctx, m, true)
	}
}

// refreshOne refreshes a single entry. Backend I/O runs under the entry's
// fetchMu (never under mu), so readers of the cached fields never block on the
// network. When force is false (on-read refresh from Get), it re-checks
// freshness under fetchMu and skips the fetch if another caller already
// populated the entry, so concurrent cold/stale reads coalesce into a single
// successful fetch. When force is true (initial fetch / polling), it always
// fetches so changes are detected via the revision.
func (c *loaderCore) refreshOne(ctx context.Context, m getterMeta, force bool) {
	entry := c.entries[m.name]

	// Serialize fetches for this entry; the data lock (mu) is NOT held here.
	entry.fetchMu.Lock()
	defer entry.fetchMu.Unlock()

	if !force {
		entry.mu.RLock()
		fresh := !entry.lastFetch.IsZero() && !entry.stale
		entry.mu.RUnlock()
		if fresh {
			return
		}
	}

	newVal, newRev, fetchErr := c.fetchWithFallback(ctx, m)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if fetchErr != nil {
		// A definitive "not found" is a legitimate absence, not staleness:
		// keep serving ErrNotFound on access but do not flag the entry stale.
		if fetchErr == client.ErrNotFound {
			entry.fetchErr = client.ErrNotFound
			entry.stale = false
			entry.lastFetch = time.Now() // set last so waiters see the full result
			return
		}
		// Transient provider error or parse error: keep the last good value
		// (if any) but mark the entry stale so staleness is inspectable. Mark
		// lastFetch as attempted so a cold (lazy-start) Get() does not
		// re-trigger the fetch on every call — polling retries on its interval.
		entry.fetchErr = fetchErr
		entry.stale = entry.hasValue
		if c.handler != nil {
			c.handler(m.folder, m.key, fetchErr)
		}
		entry.lastFetch = time.Now() // set last so waiters see the full result
		return
	}

	entry.fetchErr = nil
	if entry.hasValue && newRev != entry.revision {
		entry.lastChange = time.Now()
	}
	entry.value = newVal
	entry.revision = newRev
	entry.hasValue = true
	entry.stale = false
	entry.lastFetch = time.Now() // set last so waiters see the full result
}

// fetchWithFallback fetches (folder, key); on ErrNotFound it retries the
// standardized "default" folder fallback (unless already in default). It
// applies the retry policy on transient provider errors and parse failures.
func (c *loaderCore) fetchWithFallback(ctx context.Context, m getterMeta) (any, string, error) {
	val, rev, err := c.fetchRetried(ctx, m.folder, m.key, m.typ)
	if err == nil {
		return val, rev, nil
	}
	if err == client.ErrNotFound && m.folder != DefaultFolder {
		val, rev, ferr := c.fetchRetried(ctx, DefaultFolder, m.key, m.typ)
		if ferr == nil {
			return val, rev, nil
		}
		// Fallback also failed; prefer the original NotFound semantics.
		if ferr == client.ErrNotFound {
			return nil, "", client.ErrNotFound
		}
		return nil, "", ferr
	}
	return nil, "", err
}

// fetchRetried fetches a single (folder, key) with retry/backoff on transient
// provider errors. ErrNotFound is a definitive answer and is NOT retried.
func (c *loaderCore) fetchRetried(ctx context.Context, folder, key string, typ reflect.Type) (any, string, error) {
	delay := c.cfg.Watcher.PollingRetryDelay
	var lastErr error
	for attempt := 0; attempt <= c.cfg.Watcher.PollingMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, "", ctx.Err()
			case <-time.After(delay):
			}
		}
		f, err := c.client.Fetch(ctx, folder, key)
		if err == nil {
			val, perr := parseValue(f.Value, typ)
			if perr != nil {
				return nil, "", perr
			}
			return val, f.Revision, nil
		}
		if err == client.ErrNotFound {
			return nil, "", client.ErrNotFound
		}
		lastErr = err
		if attempt < c.cfg.Watcher.PollingMaxRetries {
			delay = time.Duration(float64(delay) * c.cfg.Watcher.PollingRetryBackoff)
		}
	}
	return nil, "", lastErr
}

// startPolling launches the background refresh loop. Ticks are self-rescheduling
// (the next tick only fires after the current refresh completes), so a slow or
// retrying backend can never cause overlapping refreshes. A panic in a client
// is recovered and converted into a stale state rather than silently killing
// the loop forever.
func (c *loaderCore) startPolling() {
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(c.cfg.Watcher.PollingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				func() {
					defer func() {
						if r := recover(); r != nil {
							// A panicking client must not kill the loop. Mark
							// every entry stale with a recovered-panic error so
							// getters fail loudly and the loop keeps running.
							c.markAllStale(fmt.Errorf("confloader: client panicked during refresh: %v", r))
						}
					}()
					ctx, cancel := context.WithTimeout(context.Background(), c.cfg.Watcher.PollingInterval)
					defer cancel()
					c.refreshAll(ctx)
				}()
			}
		}
	}()
}

// markAllStale flags every entry as serving a stale last-known-good value with
// the given error. Used when a refresh cannot complete (panic, etc.).
func (c *loaderCore) markAllStale(err error) {
	for _, e := range c.entries {
		e.mu.Lock()
		e.fetchErr = err
		e.stale = e.hasValue
		e.lastFetch = time.Now()
		e.mu.Unlock()
	}
	for _, m := range c.metas {
		if c.handler != nil {
			c.handler(m.folder, m.key, err)
		}
	}
}

// Stop halts the polling loop and closes the client connection. It is safe to
// call multiple times and from multiple goroutines.
func (c *loaderCore) Stop() error {
	var stopErr error
	c.once.Do(func() {
		close(c.stop)
		<-c.done
		stopErr = c.client.Close()
	})
	return stopErr
}

// IsStale reports whether any cached entry is currently serving a stale value
// (its last background refresh failed with a transient error while a
// last-known-good value remains cached). It is an inspectable health signal:
// a stale entry will be re-fetched on the next Get or polling cycle. A
// legitimately absent config (ErrNotFound) is NOT considered stale. Useful for
// health checks and alerting on drift.
func (c *loaderCore) IsStale() bool {
	for _, e := range c.entries {
		e.mu.RLock()
		stale := e.stale
		e.mu.RUnlock()
		if stale {
			return true
		}
	}
	return false
}

// StaleReason returns the underlying error for the named entry (folder, key)
// if it is currently stale, or nil otherwise. Callers that can tolerate drift
// can use this to log the cause, but note that Get itself refreshes a stale
// entry on read, so StaleReason primarily reflects the background loop's view.
func (c *loaderCore) StaleReason(folder, key string) error {
	e, ok := c.byKey[folder+"/"+key]
	if !ok {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.stale {
		return e.fetchErr
	}
	return nil
}
