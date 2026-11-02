package consumer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-dev-sdk/identity"
)

func TestNew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "valid app name",
			appName: "test-app",
			wantErr: false,
		},
		{
			name:    "empty app name",
			appName: "",
			wantErr: true,
		},
		{
			name:    "whitespace app name",
			appName: "  ",
			wantErr: true,
		},
		{
			name:    "app name with dots",
			appName: "test.app",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := New(identity.Info{Name: tt.appName})
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.appName == "test.app" && !errors.Is(err, ErrInvalidAppName) {
				t.Errorf("New() error = %v, want %v", err, ErrInvalidAppName)
			}
			if (tt.appName == "" || tt.appName == "  ") && !errors.Is(err, identity.ErrName) {
				t.Errorf("New() error = %v, want %v", err, identity.ErrName)
			}
			if !tt.wantErr && c == nil {
				t.Error("New() returned nil consumer without error")
			}
			if !tt.wantErr && c.appName != tt.appName {
				t.Errorf("New() appName = %q, want %q", c.appName, tt.appName)
			}
		})
	}
}

func TestConsumerRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name:    "missing endpoints",
			cfg:     Config{Topic: "test-topic", Group: "test-group"},
			wantErr: ErrEndpointsRequired,
		},
		{
			name:    "missing topic",
			cfg:     Config{Endpoints: []string{"localhost:4222"}, Group: "test-group"},
			wantErr: ErrTopicRequired,
		},
		{
			name:    "missing group",
			cfg:     Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic"},
			wantErr: ErrGroupRequired,
		},
		{
			name:    "topic with dots",
			cfg:     Config{Endpoints: []string{"localhost:4222"}, Topic: "test.topic", Group: "test-group"},
			wantErr: ErrInvalidTopic,
		},
		{
			name:    "group with dots",
			cfg:     Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic", Group: "test.group"},
			wantErr: ErrInvalidGroup,
		},
		{
			name:    "tag with dots",
			cfg:     Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic", Group: "test-group", Tags: []string{"test.tag"}},
			wantErr: ErrInvalidTag,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := New(testAppInfo)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			handler := func(ctx context.Context, msgBody []byte) error { return nil }
			err = c.Register(tt.cfg, handler)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Register() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConsumerRegisterDuplicateTopicGroup(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := Config{
		Endpoints: []string{"localhost:4222"},
		Topic:     "test-topic",
		Group:     "test-group",
	}
	handler := func(ctx context.Context, msgBody []byte) error { return nil }

	if err := c.Register(cfg, handler); err != nil {
		t.Fatalf("Register() first error = %v", err)
	}

	err = c.Register(cfg, handler)
	if !errors.Is(err, ErrConsumerExists) {
		t.Errorf("Register() second error = %v, wantErr %v", err, ErrConsumerExists)
	}
}

func TestConsumerRegisterDifferentGroups(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	handler := func(ctx context.Context, msgBody []byte) error { return nil }
	for _, group := range []string{"group1", "group2"} {
		cfg := Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic", Group: group}
		if err := c.Register(cfg, handler); err != nil {
			t.Fatalf("Register(%s) error = %v", group, err)
		}
	}

	if got := len(c.regs); got != 2 {
		t.Errorf("len(regs) = %d, want 2", got)
	}
}

func TestConsumerRegisterDoesNotDial(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var dials atomic.Int32
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		dials.Add(1)
		return &mockConsumer{}, nil
	}

	cfg := Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic", Group: "test-group"}
	handler := func(ctx context.Context, msgBody []byte) error { return nil }
	if err := c.Register(cfg, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if got := dials.Load(); got != 0 {
		t.Errorf("factory calls after Register = %d, want 0", got)
	}
}

func TestConsumerStartIdempotent(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c)

	var creates atomic.Int32
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		creates.Add(1)
		return &mockConsumer{}, nil
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() second call error = %v", err)
	}

	if got := creates.Load(); got != 2 {
		t.Errorf("factory calls = %d, want 2", got)
	}
}

func TestConsumerShutdownIdempotent(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c)

	var shutdowns atomic.Int32
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		return &mockConsumer{shutdownFunc: func(context.Context) error {
			shutdowns.Add(1)
			return nil
		}}, nil
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() second call error = %v", err)
	}

	if got := shutdowns.Load(); got != 2 {
		t.Errorf("consumer Shutdown calls = %d, want 2", got)
	}
}

func TestConsumerStartError(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c)

	wantErr := errors.New("connect failed")
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		return nil, wantErr
	}

	err = c.Start(context.Background())
	if !errors.Is(err, wantErr) {
		t.Errorf("Start() error = %v, want %v", err, wantErr)
	}
}

func TestConsumerShutdownError(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c)

	wantErr := errors.New("shutdown failed")
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		return &mockConsumer{shutdownErr: wantErr}, nil
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	err = c.Shutdown(context.Background())
	if !errors.Is(err, wantErr) {
		t.Errorf("Shutdown() error = %v, want %v", err, wantErr)
	}
}

func TestConsumerRegisterWithHandler(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var handlerCalled atomic.Bool
	handler := func(ctx context.Context, msgBody []byte) error {
		handlerCalled.Store(true)
		return nil
	}

	cfg := Config{
		Endpoints: []string{"localhost:4222"},
		Topic:     "test-topic",
		Group:     "test-group",
	}

	var registeredHandler func(msg jetstream.Msg)
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		return &mockConsumer{
			consumeFunc: func(h func(msg jetstream.Msg)) error {
				registeredHandler = h
				return nil
			},
		}, nil
	}

	if err := c.Register(cfg, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if registeredHandler == nil {
		t.Fatal("Consume() was not called on factory consumer")
	}

	msg := &mockMsg{data: []byte("test")}
	registeredHandler(msg)

	if !handlerCalled.Load() {
		t.Error("handler was not called")
	}
	if !msg.acked {
		t.Error("message was not acked")
	}
}

func TestConsumerStartRollbackOnFailure(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c)

	failing := c.buildGroupName("topic2", "group2")
	var (
		mu       sync.Mutex
		created  []string
		shutdown []string
	)
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		mu.Lock()
		created = append(created, durableName)
		mu.Unlock()

		cn := &mockConsumer{shutdownFunc: func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			shutdown = append(shutdown, durableName)
			return nil
		}}
		if durableName == failing {
			cn.consumeErr = errors.New("subscribe failed")
		}
		return cn, nil
	}

	err = c.Start(context.Background())
	if err == nil {
		t.Fatal("Start() error = nil, want error")
	}

	// Map order is random, so the failing consumer may run before or after the
	// other one. Either way, every consumer that was created must be shut down.
	mu.Lock()
	defer mu.Unlock()
	slices.Sort(created)
	slices.Sort(shutdown)
	if !slices.Equal(shutdown, created) {
		t.Errorf("shutdown = %v, want every created consumer %v", shutdown, created)
	}
	if !slices.Contains(shutdown, failing) {
		t.Errorf("shutdown = %v, want failing consumer %s shut down", shutdown, failing)
	}
	if c.started {
		t.Error("started = true after Start() failure, want false")
	}
}

func TestConsumerRegisterAfterStart(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	cfg := Config{
		Endpoints: []string{"localhost:4222"},
		Topic:     "new-topic",
		Group:     "new-group",
	}
	handler := func(ctx context.Context, msgBody []byte) error { return nil }

	err = c.Register(cfg, handler)
	if err == nil {
		t.Error("Register() after Start() should fail")
	}
}

func TestConsumerRegisterNilHandler(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := Config{
		Endpoints: []string{"localhost:4222"},
		Topic:     "test-topic",
		Group:     "test-group",
	}
	err = c.Register(cfg, nil)
	if err == nil {
		t.Fatal("Register(nil handler) expected error, got nil")
	}
}

// regression: Shutdown without Start must not dial or leak a connection.
func TestShutdown_WithoutStart(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var creates atomic.Int32
	c.consumerFactory = func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
		creates.Add(1)
		return &mockConsumer{}, nil
	}

	cfg := Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic", Group: "test-group"}
	if err := c.Register(cfg, func(ctx context.Context, msgBody []byte) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if err := c.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error = %v, want nil", err)
	}
	if got := creates.Load(); got != 0 {
		t.Errorf("factory calls = %d, want 0", got)
	}
}

// regression: an in-flight handler that ignores ctx must not hold Shutdown past its
// deadline.
func TestShutdown_RespectsDeadline(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entered := make(chan struct{}, 1)
	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders", Group: "billing"}
	err = c.Register(cfg, func(ctx context.Context, msgBody []byte) error {
		entered <- struct{}{}
		time.Sleep(5 * time.Second) // ignores ctx on purpose
		return nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := nc.Publish("orders", []byte("slow")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = c.Shutdown(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed > time.Second {
		t.Errorf("Shutdown() took %v, want about 300ms", elapsed)
	}
}

// regression: the default filter delivers data subjects only, never schedules.
func TestConsumer_DefaultFilterSkipsSchedules(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var (
		mu       sync.Mutex
		received []string
		done     = make(chan struct{}, 1)
	)
	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders", Group: "billing"}
	err = c.Register(cfg, func(ctx context.Context, msgBody []byte) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, string(msgBody))
		if len(received) == 2 {
			done <- struct{}{}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	for _, subj := range []string{"orders", "orders.created", "schedules.orders.x"} {
		if err := nc.Publish(subj, []byte(subj)); err != nil {
			t.Fatalf("Publish(%s) error = %v", subj, err)
		}
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive both data messages")
	}
	// Give a schedule message time to arrive if the filter were wrong.
	time.Sleep(time.Second)

	mu.Lock()
	defer mu.Unlock()
	// Handlers run concurrently, so the arrival order is not fixed.
	slices.Sort(received)
	want := []string{"orders", "orders.created"}
	if !slices.Equal(received, want) {
		t.Errorf("received = %v, want %v", received, want)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

// registerTwo registers two consumers on different topics so lifecycle tests have
// more than one durable consumer to act on.
func registerTwo(t *testing.T, c *Consumer) {
	t.Helper()

	handler := func(ctx context.Context, msgBody []byte) error { return nil }
	for _, cfg := range []Config{
		{Endpoints: []string{"localhost:4222"}, Topic: "topic1", Group: "group1"},
		{Endpoints: []string{"localhost:4222"}, Topic: "topic2", Group: "group2"},
	} {
		if err := c.Register(cfg, handler); err != nil {
			t.Fatalf("Register(%s) error = %v", cfg.Topic, err)
		}
	}
}

// regression: Start must return within its context when every server is
// unreachable, instead of waiting out the AckWait-sized dial timeout.
func TestStart_BoundedByContext(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cfg := Config{
		Endpoints: []string{"nats://10.255.255.1:4222", "nats://10.255.255.2:4222"},
		Topic:     "orders",
		Group:     "billing",
	}
	if err := c.Register(cfg, func(context.Context, []byte) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- c.Start(ctx) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start() error = nil, want non-nil")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("Start() took %v, want about 300ms", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start() did not return within 10s of a 300ms context")
	}
}
