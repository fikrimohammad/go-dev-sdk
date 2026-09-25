package producer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
			p, err := New(appinfo.Info{Name: tt.appName})
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.appName == "test.app" && !errors.Is(err, ErrInvalidAppName) {
				t.Errorf("New() error = %v, want %v", err, ErrInvalidAppName)
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
			cfg:     Config{Endpoints: []string{"localhost:4222"}},
			wantErr: ErrTopicRequired,
		},
		{
			name:    "topic with dots",
			cfg:     Config{Endpoints: []string{"localhost:4222"}, Topic: "test.topic"},
			wantErr: ErrInvalidTopic,
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
		Endpoints: []string{"localhost:4222"},
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
		tag       string
		mockErr   error
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "success",
			topic:   "test-topic",
			tag:     "tag",
			mockErr: nil,
			wantErr: false,
		},
		{
			name:      "send error",
			topic:     "test-topic",
			tag:       "tag",
			mockErr:   errors.New("send failed"),
			wantErr:   true,
			errSubstr: "send failed",
		},
		{
			name:      "topic not found",
			topic:     "nonexistent-topic",
			tag:       "tag",
			wantErr:   true,
			errSubstr: ErrProducerNotFound.Error(),
		},
		{
			name:      "tag with dots",
			topic:     "test-topic",
			tag:       "order.created",
			mockErr:   nil,
			wantErr:   true,
			errSubstr: ErrInvalidTag.Error(),
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

			err = p.PublishSync(context.Background(), tt.topic, tt.tag, "key", []byte("hello"))
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishSync() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errSubstr != "" && !errors.Is(err, ErrProducerNotFound) && !errors.Is(err, ErrInvalidTag) {
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

	var capturedMsg *nats.Msg
	mock := &mockProducer{
		sendSyncFunc: func(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
			capturedMsg = msg
			return &jetstream.PubAck{Stream: "test-topic", Sequence: 1}, nil
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

	// Verify Nats-Schedule and Nats-Schedule-Target header
	sched := capturedMsg.Header.Get("Nats-Schedule")
	if sched == "" {
		t.Error("PublishSyncWithDelay() Nats-Schedule header not set")
	}
	target := capturedMsg.Header.Get("Nats-Schedule-Target")
	if target != "test-topic.tag" {
		t.Errorf("PublishSyncWithDelay() target = %q, want %q", target, "test-topic.tag")
	}

	// Verify tag with dots is rejected
	err = p.PublishSyncWithDelay(context.Background(), "test-topic", "has.dot", "key", []byte("hello"), delay)
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("PublishSyncWithDelay(tag with dots) = %v, want %v", err, ErrInvalidTag)
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
		tag       string
		mockErr   error
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "success",
			topic:   "test-topic",
			tag:     "tag",
			mockErr: nil,
			wantErr: false,
		},
		{
			name:      "send error",
			topic:     "test-topic",
			tag:       "tag",
			mockErr:   errors.New("async send failed"),
			wantErr:   true,
			errSubstr: "async send failed",
		},
		{
			name:      "topic not found",
			topic:     "nonexistent-topic",
			tag:       "tag",
			wantErr:   true,
			errSubstr: ErrProducerNotFound.Error(),
		},
		{
			name:      "tag with dots",
			topic:     "test-topic",
			tag:       "order.created",
			mockErr:   nil,
			wantErr:   true,
			errSubstr: ErrInvalidTag.Error(),
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

			callback := func(ctx context.Context, result *jetstream.PubAck, err error) {}
			err = p.PublishAsync(context.Background(), tt.topic, tt.tag, "key", []byte("hello"), callback)
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishAsync() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errSubstr != "" && !errors.Is(err, ErrProducerNotFound) && !errors.Is(err, ErrInvalidTag) {
				if err.Error() == "" || len(err.Error()) < len(tt.errSubstr) {
					t.Errorf("PublishAsync() error = %v, want error containing %q", err, tt.errSubstr)
				}
			}
		})
	}
}

func TestProducerBuildMsg(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tests := []struct {
		name        string
		topic       string
		tag         string
		key         string
		body        []byte
		wantSubject string
		wantTag     string
		wantKey     string
	}{
		{
			name:        "with tag and key",
			topic:       "test-topic",
			tag:         "test-tag",
			key:         "test-key",
			body:        []byte("hello"),
			wantSubject: "test-topic.test-tag",
			wantTag:     "test-tag",
			wantKey:     "test-key",
		},
		{
			name:        "with tag only",
			topic:       "test-topic",
			tag:         "test-tag",
			key:         "",
			body:        []byte("hello"),
			wantSubject: "test-topic.test-tag",
			wantTag:     "test-tag",
			wantKey:     "",
		},
		{
			name:        "with key only",
			topic:       "test-topic",
			tag:         "",
			key:         "test-key",
			body:        []byte("hello"),
			wantSubject: "test-topic",
			wantTag:     "",
			wantKey:     "test-key",
		},
		{
			name:        "without tag and key",
			topic:       "test-topic",
			tag:         "",
			key:         "",
			body:        []byte("hello"),
			wantSubject: "test-topic",
			wantTag:     "",
			wantKey:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msg := p.buildMsg(context.Background(), tt.topic, tt.tag, tt.key, tt.body)

			if msg.Subject != tt.wantSubject {
				t.Errorf("buildMsg() Subject = %q, want %q", msg.Subject, tt.wantSubject)
			}
			if string(msg.Data) != string(tt.body) {
				t.Errorf("buildMsg() Body = %q, want %q", msg.Data, tt.body)
			}
			if msg.Header.Get("Nats-Msg-Tag") != tt.wantTag {
				t.Errorf("buildMsg() Tags = %q, want %q", msg.Header.Get("Nats-Msg-Tag"), tt.wantTag)
			}
			if msg.Header.Get("Nats-Msg-Id") != tt.wantKey {
				t.Errorf("buildMsg() Keys = %q, want %q", msg.Header.Get("Nats-Msg-Id"), tt.wantKey)
			}
		})
	}
}

func TestProducerRegisterAfterStart(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockProducer{}
	p.setProducer("topic1", mock)

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	cfg := Config{
		Endpoints: []string{"localhost:4222"},
		Topic:     "new-topic",
	}
	err = p.Register(cfg)
	if err == nil {
		t.Error("Register() after Start() should fail")
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

	if err := p.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	err = p.Shutdown()
	if err == nil {
		t.Error("Shutdown() expected error, got nil")
	}
}
