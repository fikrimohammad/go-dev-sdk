package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
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
			wantErr: false,
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
			c, err := New(appinfo.Info{Name: tt.appName})
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.appName == "test.app" && !errors.Is(err, ErrInvalidAppName) {
				t.Errorf("New() error = %v, want %v", err, ErrInvalidAppName)
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
	cfg = cfg.SetDefaults()

	handler := func(ctx context.Context, msgBody []byte) error { return nil }

	mock := &mockConsumer{}
	c.setConsumer(cfg.Topic, cfg.Group, mock)

	err = c.Register(cfg, handler)
	if !errors.Is(err, ErrConsumerExists) {
		t.Errorf("Register() error = %v, wantErr %v", err, ErrConsumerExists)
	}
}

func TestConsumerRegisterDifferentGroups(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock1 := &mockConsumer{}
	c.setConsumer("test-topic", "group1", mock1)

	mock2 := &mockConsumer{}
	c.setConsumer("test-topic", "group2", mock2)

	c1, err := c.getConsumer("test-topic", "group1")
	if err != nil {
		t.Errorf("getConsumer(group1) error = %v", err)
	}
	if c1 == nil {
		t.Error("getConsumer(group1) returned nil")
	}

	c2, err := c.getConsumer("test-topic", "group2")
	if err != nil {
		t.Errorf("getConsumer(group2) error = %v", err)
	}
	if c2 == nil {
		t.Error("getConsumer(group2) returned nil")
	}
}

func TestConsumerStartIdempotent(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock1 := &mockConsumer{}
	mock2 := &mockConsumer{}
	c.setConsumer("topic1", "group1", mock1)
	c.setConsumer("topic2", "group2", mock2)

	if err := c.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var startCount atomic.Int32
	mock1.startFunc = func() error {
		startCount.Add(1)
		return nil
	}
	mock2.startFunc = func() error {
		startCount.Add(1)
		return nil
	}

	if err := c.Start(); err != nil {
		t.Fatalf("Start() second call error = %v", err)
	}

	if count := startCount.Load(); count != 0 {
		t.Errorf("Start() called mock Start %d times, want 0", count)
	}
}

func TestConsumerShutdownIdempotent(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock1 := &mockConsumer{}
	mock2 := &mockConsumer{}
	c.setConsumer("topic1", "group1", mock1)
	c.setConsumer("topic2", "group2", mock2)

	if err := c.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := c.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	var shutdownCount atomic.Int32
	mock1.shutdownFunc = func() error {
		shutdownCount.Add(1)
		return nil
	}
	mock2.shutdownFunc = func() error {
		shutdownCount.Add(1)
		return nil
	}

	if err := c.Shutdown(); err != nil {
		t.Fatalf("Shutdown() second call error = %v", err)
	}

	if count := shutdownCount.Load(); count != 0 {
		t.Errorf("Shutdown() called mock Shutdown %d times, want 0", count)
	}
}

func TestConsumerBuildGroupName(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		topic string
		group string
		want  string
	}{
		{"topic1", "group1", "test-app_topic1_group1"},
		{"topic2", "group2", "test-app_topic2_group2"},
		{"my-topic", "my-group", "test-app_my-topic_my-group"},
	}

	for _, tt := range tests {
		name := tt.topic + "/" + tt.group
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := c.buildGroupName(tt.topic, tt.group)
			if got != tt.want {
				t.Errorf("buildGroupName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConsumerGetConsumer(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Run("existing topic and group", func(t *testing.T) {
		t.Parallel()
		mock := &mockConsumer{}
		c.setConsumer("existing-topic", "existing-group", mock)

		cn, err := c.getConsumer("existing-topic", "existing-group")
		if err != nil {
			t.Errorf("getConsumer() error = %v", err)
		}
		if cn == nil {
			t.Error("getConsumer() returned nil consumer")
		}
	})

	t.Run("nonexistent topic and group", func(t *testing.T) {
		t.Parallel()
		cn, err := c.getConsumer("nonexistent-topic", "nonexistent-group")
		if !errors.Is(err, ErrConsumerNotFound) {
			t.Errorf("getConsumer() error = %v, wantErr %v", err, ErrConsumerNotFound)
		}
		if cn != nil {
			t.Error("getConsumer() returned non-nil consumer for nonexistent topic and group")
		}
	})
}

func TestConsumerStartError(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockConsumer{startErr: errors.New("start failed")}
	c.setConsumer("test-topic", "test-group", mock)

	err = c.Start()
	if err == nil {
		t.Error("Start() expected error, got nil")
	}
}

func TestConsumerShutdownError(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockConsumer{shutdownErr: errors.New("shutdown failed")}
	c.setConsumer("test-topic", "test-group", mock)

	if err := c.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	err = c.Shutdown()
	if err == nil {
		t.Error("Shutdown() expected error, got nil")
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
	mock := &mockConsumer{
		consumeFunc: func(h func(msg jetstream.Msg)) error {
			registeredHandler = h
			return nil
		},
	}

	c.consumerFactory = func(cfg Config, durableName string) (jsConsumer, error) {
		return mock, nil
	}

	err = c.Register(cfg, handler)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
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

	mock1 := &mockConsumer{}
	mock2 := &mockConsumer{startErr: errors.New("start failed")}
	c.setConsumer("topic1", "group1", mock1)
	c.setConsumer("topic2", "group2", mock2)

	err = c.Start()
	if err == nil {
		t.Error("Start() expected error, got nil")
	}

	if c.started.Load() {
		t.Error("started flag should be false after Start() failure")
	}
}

func TestConsumerRegisterAfterStart(t *testing.T) {
	t.Parallel()

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockConsumer{}
	c.setConsumer("topic1", "group1", mock)

	if err := c.Start(); err != nil {
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
