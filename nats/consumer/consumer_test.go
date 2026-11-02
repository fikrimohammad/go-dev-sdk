package consumer

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/identity"
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

			handler := func(ctx context.Context, msg Message) error { return nil }
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
	handler := func(ctx context.Context, msg Message) error { return nil }

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

	handler := func(ctx context.Context, msg Message) error { return nil }
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

// TestConsumerRegisterDoesNotDial checks that Register stores the config without
// dialing: no client connects and no stream is created.
func TestConsumerRegisterDoesNotDial(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders", Group: "billing"}
	handler := func(ctx context.Context, msg Message) error { return nil }
	if err := c.Register(cfg, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if got := ns.NumClients(); got != 0 {
		t.Fatalf("Register() clients = %d, want 0", got)
	}
	nc := connectTestClient(t, ns)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	if _, err := js.Stream(context.Background(), "orders"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("Register() stream error = %v, want %v", err, jetstream.ErrStreamNotFound)
	}
}

func TestConsumerStartIdempotent(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c, ns)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() second call error = %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	if got := ns.NumClients(); got != 2 {
		t.Errorf("Start() clients = %d, want 2 (one per consumer)", got)
	}
}

func TestConsumerShutdownIdempotent(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registerTwo(t, c, ns)

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() second call error = %v", err)
	}
	waitClients(t, ns, 0)
}

// TestConsumerStartError checks that Start fails with the context error when the
// server cannot be reached within its context.
func TestConsumerStartError(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cfg := Config{Endpoints: []string{deadEndpoint}, Topic: "orders", Group: "billing"}
	if err := c.Register(cfg, func(context.Context, Message) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := c.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Start() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

// TestConsumerShutdownError checks that Shutdown returns the context error when a
// handler is still running at its deadline.
func TestConsumerShutdownError(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	entered := make(chan struct{}, 1)
	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders", Group: "billing"}
	if err := c.Register(cfg, func(context.Context, Message) error {
		entered <- struct{}{}
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := connectTestClient(t, ns).Publish("orders", []byte("stuck")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestConsumerRegisterWithHandler(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	handled := make(chan struct{}, 1)
	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders", Group: "billing"}
	if err := c.Register(cfg, func(ctx context.Context, msg Message) error {
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := connectTestClient(t, ns).Publish("orders", []byte("hi")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	// The successful handler acked the message, so nothing is left unacked. The
	// durable consumer is named after the app, the topic, and the group.
	js, err := jetstream.New(connectTestClient(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	cons, err := js.Consumer(context.Background(), "orders", "test-app_orders_billing")
	if err != nil {
		t.Fatalf("Consumer() error = %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := cons.Info(context.Background())
		if err != nil {
			t.Fatalf("Info() error = %v", err)
		}
		if info.NumAckPending == 0 {
			if info.Delivered.Consumer != 1 {
				t.Errorf("delivered = %d, want 1", info.Delivered.Consumer)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("NumAckPending = %d after the handler returned, want 0", info.NumAckPending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConsumerStartRollbackOnFailure checks that a failed Start leaves no connection
// open. One consumer's server is reachable and the other's is not. Start visits the
// consumers in map order, so the reachable one is connected only when it comes
// first; either way its connection must be closed when Start fails.
func TestConsumerStartRollbackOnFailure(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handler := func(ctx context.Context, msg Message) error { return nil }
	if err := c.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "topic1", Group: "group1"}, handler); err != nil {
		t.Fatalf("Register(topic1) error = %v", err)
	}
	if err := c.Register(Config{Endpoints: []string{deadEndpoint}, Topic: "topic2", Group: "group2"}, handler); err != nil {
		t.Fatalf("Register(topic2) error = %v", err)
	}

	// Start runs in name order, so topic1 connects before topic2 fails, and
	// the rollback must close topic1's connection.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := c.Start(ctx); err == nil {
		t.Fatal("Start() error = nil, want error")
	}
	waitClients(t, ns, 0)
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
	handler := func(ctx context.Context, msg Message) error { return nil }

	err = c.Register(cfg, handler)
	if !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("Register() after Start() = %v, want ErrAlreadyStarted", err)
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
		t.Fatal("Register(nil handler) = nil, want error")
	}
}

// regression: Shutdown without Start must not dial or leak a connection.
func TestShutdown_WithoutStart(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := Config{Endpoints: []string{ns.ClientURL()}, Topic: "test-topic", Group: "test-group"}
	if err := c.Register(cfg, func(ctx context.Context, msg Message) error { return nil }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	if err := c.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error = %v, want nil", err)
	}
	if got := ns.NumClients(); got != 0 {
		t.Errorf("Shutdown() clients = %d, want 0", got)
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
	err = c.Register(cfg, func(ctx context.Context, msg Message) error {
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
	err = c.Register(cfg, func(ctx context.Context, msg Message) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, string(msg.Body()))
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

// registerTwo registers two consumers on different topics of ns, so lifecycle tests
// have more than one durable consumer to act on.
func registerTwo(t *testing.T, c *Consumer, ns *natsserver.Server) {
	t.Helper()

	handler := func(ctx context.Context, msg Message) error { return nil }
	for _, cfg := range []Config{
		{Endpoints: []string{ns.ClientURL()}, Topic: "topic1", Group: "group1"},
		{Endpoints: []string{ns.ClientURL()}, Topic: "topic2", Group: "group2"},
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
	if err := c.Register(cfg, func(context.Context, Message) error { return nil }); err != nil {
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
