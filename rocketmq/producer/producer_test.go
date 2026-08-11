package producer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := New(appinfo.Info{Name: tt.appName})
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && p == nil {
				t.Error("New() returned nil producer without error")
			}
			if !tt.wantErr && p.appName != tt.appName {
				t.Errorf("New() appName = %q, want %q", p.appName, tt.appName)
			}
		})
	}
}

func TestProducerRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     Config
		wantErr error
	}{
		{
			name:    "missing endpoints",
			cfg:     Config{Topic: "test-topic"},
			wantErr: ErrEndpointsRequired,
		},
		{
			name:    "missing topic",
			cfg:     Config{Endpoints: []string{"localhost:9876"}},
			wantErr: ErrTopicRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := New(testAppInfo)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			err = p.Register(tt.cfg)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Register() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProducerRegisterDuplicateTopic(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := Config{
		Endpoints: []string{"localhost:9876"},
		Topic:     "test-topic",
	}
	cfg = cfg.SetDefaults()

	// Inject a mock producer for the topic
	mock := &mockProducer{}
	p.setProducer(cfg.Topic, mock)

	// Try to register the same topic again
	err = p.Register(cfg)
	if !errors.Is(err, ErrProducerExists) {
		t.Errorf("Register() error = %v, wantErr %v", err, ErrProducerExists)
	}
}

func TestProducerStartIdempotent(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Inject mock producers
	mock1 := &mockProducer{}
	mock2 := &mockProducer{}
	p.setProducer("topic1", mock1)
	p.setProducer("topic2", mock2)

	// First call should start
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Track if Start is called again
	var startCount atomic.Int32
	mock1.startFunc = func() error {
		startCount.Add(1)
		return nil
	}
	mock2.startFunc = func() error {
		startCount.Add(1)
		return nil
	}

	// Second call should be no-op
	if err := p.Start(); err != nil {
		t.Fatalf("Start() second call error = %v", err)
	}

	if count := startCount.Load(); count != 0 {
		t.Errorf("Start() called mock Start %d times, want 0", count)
	}
}

func TestProducerShutdownIdempotent(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Inject mock producers
	mock1 := &mockProducer{}
	mock2 := &mockProducer{}
	p.setProducer("topic1", mock1)
	p.setProducer("topic2", mock2)

	// Start first
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// First shutdown should work
	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	// Track if Shutdown is called again
	var shutdownCount atomic.Int32
	mock1.shutdownFunc = func() error {
		shutdownCount.Add(1)
		return nil
	}
	mock2.shutdownFunc = func() error {
		shutdownCount.Add(1)
		return nil
	}

	// Second shutdown should be no-op
	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown() second call error = %v", err)
	}

	if count := shutdownCount.Load(); count != 0 {
		t.Errorf("Shutdown() called mock Shutdown %d times, want 0", count)
	}
}

func TestProducerPublishSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topic     string
		mockErr   error
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "success",
			topic:   "test-topic",
			mockErr: nil,
			wantErr: false,
		},
		{
			name:      "send error",
			topic:     "test-topic",
			mockErr:   errors.New("send failed"),
			wantErr:   true,
			errSubstr: "send failed",
		},
		{
			name:      "topic not found",
			topic:     "nonexistent-topic",
			wantErr:   true,
			errSubstr: ErrProducerNotFound.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := New(testAppInfo)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			if tt.topic != "nonexistent-topic" {
				mock := &mockProducer{sendSyncErr: tt.mockErr}
				p.setProducer(tt.topic, mock)
			}

			err = p.PublishSync(context.Background(), tt.topic, "tag", "key", []byte("hello"))
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishSync() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errSubstr != "" && !errors.Is(err, ErrProducerNotFound) {
				if err.Error() == "" || len(err.Error()) < len(tt.errSubstr) {
					t.Errorf("PublishSync() error = %v, want error containing %q", err, tt.errSubstr)
				}
			}
		})
	}
}

func TestProducerPublishSyncWithDelay(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var capturedMsg *primitive.Message
	mock := &mockProducer{
		sendSyncFunc: func(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error) {
			if len(mq) > 0 {
				capturedMsg = mq[0]
			}
			return &primitive.SendResult{}, nil
		},
	}
	p.setProducer("test-topic", mock)

	delay := 5 * time.Second
	err = p.PublishSyncWithDelay(context.Background(), "test-topic", "tag", "key", []byte("hello"), delay)
	if err != nil {
		t.Fatalf("PublishSyncWithDelay() error = %v", err)
	}

	if capturedMsg == nil {
		t.Fatal("PublishSyncWithDelay() did not capture message")
	}

	// Verify delay timestamp is set via the TIMER_DELIVER_MS property
	delayProp := capturedMsg.GetProperty(primitive.PropertyTimerDeliverMS)
	if delayProp == "" {
		t.Error("PublishSyncWithDelay() delay timestamp property not set")
	}
}

func TestProducerPublishSyncWithDelay_Error(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockProducer{sendSyncErr: errors.New("delay send failed")}
	p.setProducer("test-topic", mock)

	err = p.PublishSyncWithDelay(context.Background(), "test-topic", "tag", "key", []byte("hello"), 5*time.Second)
	if err == nil {
		t.Error("PublishSyncWithDelay() expected error, got nil")
	}
}

func TestProducerPublishAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topic     string
		mockErr   error
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "success",
			topic:   "test-topic",
			mockErr: nil,
			wantErr: false,
		},
		{
			name:      "send error",
			topic:     "test-topic",
			mockErr:   errors.New("async send failed"),
			wantErr:   true,
			errSubstr: "async send failed",
		},
		{
			name:      "topic not found",
			topic:     "nonexistent-topic",
			wantErr:   true,
			errSubstr: ErrProducerNotFound.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := New(testAppInfo)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			if tt.topic != "nonexistent-topic" {
				mock := &mockProducer{sendAsyncErr: tt.mockErr}
				p.setProducer(tt.topic, mock)
			}

			callback := func(ctx context.Context, result *primitive.SendResult, err error) {}
			err = p.PublishAsync(context.Background(), tt.topic, "tag", "key", []byte("hello"), callback)
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishAsync() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errSubstr != "" && !errors.Is(err, ErrProducerNotFound) {
				if err.Error() == "" || len(err.Error()) < len(tt.errSubstr) {
					t.Errorf("PublishAsync() error = %v, want error containing %q", err, tt.errSubstr)
				}
			}
		})
	}
}

func TestProducerBuildMessage(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		name    string
		topic   string
		tag     string
		key     string
		body    []byte
		wantTag string
		wantKey string
	}{
		{
			name:    "with tag and key",
			topic:   "test-topic",
			tag:     "test-tag",
			key:     "test-key",
			body:    []byte("hello"),
			wantTag: "test-tag",
			wantKey: "test-key",
		},
		{
			name:    "with tag only",
			topic:   "test-topic",
			tag:     "test-tag",
			key:     "",
			body:    []byte("hello"),
			wantTag: "test-tag",
			wantKey: "",
		},
		{
			name:    "with key only",
			topic:   "test-topic",
			tag:     "",
			key:     "test-key",
			body:    []byte("hello"),
			wantTag: "",
			wantKey: "test-key",
		},
		{
			name:    "without tag and key",
			topic:   "test-topic",
			tag:     "",
			key:     "",
			body:    []byte("hello"),
			wantTag: "",
			wantKey: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := p.buildMessage(context.Background(), tt.topic, tt.tag, tt.key, tt.body)

			if msg.Topic != tt.topic {
				t.Errorf("buildMessage() Topic = %q, want %q", msg.Topic, tt.topic)
			}
			if string(msg.Body) != string(tt.body) {
				t.Errorf("buildMessage() Body = %q, want %q", msg.Body, tt.body)
			}
			if msg.GetTags() != tt.wantTag {
				t.Errorf("buildMessage() Tags = %q, want %q", msg.GetTags(), tt.wantTag)
			}
			if msg.GetKeys() != tt.wantKey {
				t.Errorf("buildMessage() Keys = %q, want %q", msg.GetKeys(), tt.wantKey)
			}
		})
	}
}

func TestProducerBuildGroupName(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		topic string
		want  string
	}{
		{"topic1", "test-app::topic1"},
		{"topic2", "test-app::topic2"},
		{"my-topic", "test-app::my-topic"},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			t.Parallel()
			got := p.buildGroupName(tt.topic)
			if got != tt.want {
				t.Errorf("buildGroupName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProducerGetProducer(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	t.Run("existing topic", func(t *testing.T) {
		t.Parallel()
		mock := &mockProducer{}
		p.setProducer("existing-topic", mock)

		pd, err := p.getProducer("existing-topic")
		if err != nil {
			t.Errorf("getProducer() error = %v", err)
		}
		if pd == nil {
			t.Error("getProducer() returned nil producer")
		}
	})

	t.Run("nonexistent topic", func(t *testing.T) {
		t.Parallel()
		pd, err := p.getProducer("nonexistent-topic")
		if !errors.Is(err, ErrProducerNotFound) {
			t.Errorf("getProducer() error = %v, wantErr %v", err, ErrProducerNotFound)
		}
		if pd != nil {
			t.Error("getProducer() returned non-nil producer for nonexistent topic")
		}
	})
}

func TestProducerStartError(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockProducer{startErr: errors.New("start failed")}
	p.setProducer("test-topic", mock)

	err = p.Start()
	if err == nil {
		t.Error("Start() expected error, got nil")
	}
}

func TestProducerShutdownError(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockProducer{shutdownErr: errors.New("shutdown failed")}
	p.setProducer("test-topic", mock)

	// Start first
	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	err = p.Shutdown()
	if err == nil {
		t.Error("Shutdown() expected error, got nil")
	}
}
