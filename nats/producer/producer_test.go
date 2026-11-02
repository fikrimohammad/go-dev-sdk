package producer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
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
			p, err := New(identity.Info{Name: tt.appName})
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
			if !tt.wantErr && p == nil {
				t.Error("New() returned nil producer without error")
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

// TestRegister_NoIO checks that Register stores the config without dialing: no
// client connects and no stream is created.
func TestRegister_NoIO(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if got := ns.NumClients(); got != 0 {
		t.Fatalf("Register() clients = %d, want 0", got)
	}
	js, err := jetstream.New(dialNATS(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	if _, err := js.Stream(context.Background(), "orders"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("Register() stream error = %v, want %v", err, jetstream.ErrStreamNotFound)
	}
}

func TestProducerRegisterDuplicateTopic(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cfg := Config{Endpoints: []string{"localhost:4222"}, Topic: "test-topic"}
	if err := p.Register(cfg); err != nil {
		t.Fatalf("Register() first call error = %v", err)
	}

	err = p.Register(cfg)
	if !errors.Is(err, ErrProducerExists) {
		t.Errorf("Register() error = %v, wantErr %v", err, ErrProducerExists)
	}
}

// TestStart_PreservesOperatorStreamSettings checks that Start keeps the settings
// an operator set on an existing stream and only adds the SDK subjects.
func TestStart_PreservesOperatorStreamSettings(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	ctx := context.Background()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "orders",
		Subjects: []string{"orders"},
		MaxAge:   time.Hour,
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	s, err := js.Stream(ctx, "orders")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	got := s.CachedInfo().Config

	if got.MaxAge != time.Hour {
		t.Errorf("Start() MaxAge = %v, want %v", got.MaxAge, time.Hour)
	}
	for _, want := range []string{"orders", "orders.>", "schedules.orders.>"} {
		if !slices.Contains(got.Subjects, want) {
			t.Errorf("Start() Subjects = %v, want to include %q", got.Subjects, want)
		}
	}
	if !got.AllowMsgSchedules {
		t.Error("Start() AllowMsgSchedules = false, want true")
	}
}

// TestProducerStartIdempotent checks that a second Start neither dials again nor
// creates a second connection for a topic.
func TestProducerStartIdempotent(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, topic := range []string{"topic1", "topic2"} {
		if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: topic}); err != nil {
			t.Fatalf("Register(%s) error = %v", topic, err)
		}
	}

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() second call error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	if got := ns.NumClients(); got != 2 {
		t.Errorf("Start() clients = %d, want 2 (one per topic)", got)
	}
}

// TestStart_FailureClosesOpenedConnections checks that a failed Start closes the
// connections it already opened, and that a later Start succeeds once the failing
// server is up. The "bad" topic points at a port nothing listens on, so its dial
// fails at the deadline, after the "good" topic has connected.
func TestStart_FailureClosesOpenedConnections(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	port := freePort(t)

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "good"}); err != nil {
		t.Fatalf("Register(good) error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{fmt.Sprintf("nats://127.0.0.1:%d", port)}, Topic: "bad"}); err != nil {
		t.Fatalf("Register(bad) error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := p.Start(ctx); err == nil {
		t.Fatal("Start() error = nil, want error")
	}
	waitClients(t, ns, 0)

	// The failing server comes up, and a later Start succeeds.
	startJetStreamServerOn(t, port)
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() retry error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
}

// TestPublishAsync_AckTimeout checks that an ack that never arrives fails the
// callback. The server is stopped before the publish. The client either fails the
// pending ack as soon as it sees the disconnect, or at Config.Timeout, whichever
// comes first, so both outcomes are accepted.
func TestPublishAsync_AckTimeout(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "reporting", Timeout: 200 * time.Millisecond}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	ns.Shutdown()
	ns.WaitForShutdown()
	got := make(chan error, 1)
	if err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "", Key: "", Body: []byte("hello")}, func(_ context.Context, _ *jetstream.PubAck, err error) {
		got <- err
	}); err != nil {
		t.Fatalf("PublishAsync() error = %v", err)
	}

	select {
	case err := <-got:
		if !errors.Is(err, jetstream.ErrAsyncPublishTimeout) && !errors.Is(err, nats.ErrDisconnected) {
			t.Errorf("callback error = %v, want an ack failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback was not called after the publish timeout")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = p.Shutdown(ctx)
}

func TestProducerShutdownIdempotent(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, topic := range []string{"topic1", "topic2"} {
		if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: topic}); err != nil {
			t.Fatalf("Register(%s) error = %v", topic, err)
		}
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() second call error = %v", err)
	}
	waitClients(t, ns, 0)
}

// TestShutdown_WithoutStart checks that Shutdown before Start is a no-op.
func TestShutdown_WithoutStart(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error = %v, want nil", err)
	}
}

// TestShutdown_WaitsForAsyncPublish checks that Shutdown returns only after the
// callback of a pending async publish has run. The callback is slow on purpose.
func TestShutdown_WaitsForAsyncPublish(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "reporting")

	var called atomic.Bool
	err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "", Key: "", Body: []byte("hello")}, func(context.Context, *jetstream.PubAck, error) {
		time.Sleep(200 * time.Millisecond)
		called.Store(true)
	})
	if err != nil {
		t.Fatalf("PublishAsync() error = %v", err)
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if !called.Load() {
		t.Error("Shutdown() returned before the async callback ran")
	}
}

// TestShutdown_Deadline checks that an async callback still running when ctx ends
// does not hold Shutdown: Shutdown returns the context error and closes the
// connection. The callback waits on release, so it never returns in time.
func TestShutdown_Deadline(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "reporting")
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	pd := p.run.producers["reporting"]

	err := p.PublishAsync(context.Background(), Message{Topic: "reporting", Tag: "", Key: "", Body: []byte("hello")}, func(context.Context, *jetstream.PubAck, error) {
		<-release
	})
	if err != nil {
		t.Fatalf("PublishAsync() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = p.Shutdown(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed > time.Second {
		t.Errorf("Shutdown() took %v, want about 200ms", elapsed)
	}
	if !pd.nc.IsClosed() {
		t.Error("Shutdown() returned with the connection open, want it closed")
	}
}

func TestProducerPublishSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		topic      string
		tag        string
		body       string
		smallLimit int32 // MaxMsgSize of the "small" stream, created before Start when set
		wantErr    error
		wantAPI    uint16 // JetStream API error code, when the send fails inside the server
	}{
		{name: "success", topic: "orders", tag: "tag", body: "hello"},
		{name: "send error", topic: "small", tag: "tag", body: "0123456789", smallLimit: 4, wantAPI: 10054},
		{name: "topic not found", topic: "nonexistent-topic", tag: "tag", body: "hello", wantErr: ErrProducerNotFound},
		{name: "tag with dots", topic: "orders", tag: "order.created", body: "hello", wantErr: ErrInvalidTag},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ns := startJetStreamServer(t)
			if tt.smallLimit > 0 {
				createStream(t, ns, jetstream.StreamConfig{Name: "small", Subjects: []string{"small"}, MaxMsgSize: tt.smallLimit})
			}
			p := startTopics(t, ns, []string{"orders", "small"})

			err := p.PublishSync(context.Background(), Message{Topic: tt.topic, Tag: tt.tag, Key: "key", Body: []byte(tt.body)})
			if tt.wantErr == nil && tt.wantAPI == 0 {
				if err != nil {
					t.Fatalf("PublishSync() error = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("PublishSync() error = nil, want error")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("PublishSync() error = %v, want %v", err, tt.wantErr)
			}
			if apiErr, ok := errors.AsType[*jetstream.APIError](err); tt.wantAPI != 0 && (!ok || uint16(apiErr.ErrorCode) != tt.wantAPI) {
				t.Errorf("PublishSync() error = %v, want JetStream API error %d", err, tt.wantAPI)
			}
		})
	}
}

func TestProducerPublishSyncWithDelay(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "orders")

	err := p.PublishSyncWithDelay(context.Background(), Message{Topic: "orders", Tag: "tag", Key: "key", Body: []byte("hello")}, 5*time.Second)
	if err != nil {
		t.Fatalf("PublishSyncWithDelay() error = %v", err)
	}

	js, err := jetstream.New(dialNATS(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	stored := readMsgs(t, js, "orders", "schedules.orders.>", 1)[0]
	if sched := stored.Header.Get("Nats-Schedule"); !strings.HasPrefix(sched, "@at ") {
		t.Errorf("PublishSyncWithDelay() Nats-Schedule = %q, want @at <RFC3339Nano time>", sched)
	}
	if target := stored.Header.Get("Nats-Schedule-Target"); target != "orders.tag" {
		t.Errorf("PublishSyncWithDelay() target = %q, want %q", target, "orders.tag")
	}

	// Verify tag with dots is rejected
	err = p.PublishSyncWithDelay(context.Background(), Message{Topic: "orders", Tag: "has.dot", Key: "key", Body: []byte("hello")}, 5*time.Second)
	if !errors.Is(err, ErrInvalidTag) {
		t.Errorf("PublishSyncWithDelay(tag with dots) = %v, want %v", err, ErrInvalidTag)
	}
}

// TestProducerPublishSyncWithDelay_ZeroDelay checks that a delay of zero sends the
// message at once, as PublishSync does, and stores no schedule message.
func TestProducerPublishSyncWithDelay_ZeroDelay(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "orders")

	if err := p.PublishSyncWithDelay(context.Background(), Message{Topic: "orders", Tag: "now", Key: "k", Body: []byte("hello")}, 0); err != nil {
		t.Fatalf("PublishSyncWithDelay() error = %v", err)
	}

	js := jsContext(t, ns)
	if got := readMsgs(t, js, "orders", "orders.now", 1)[0].Subject; got != "orders.now" {
		t.Errorf("subject = %q, want %q", got, "orders.now")
	}
	// The ack of the publish means the server has stored everything it will store.
	readMsgs(t, js, "orders", "schedules.orders.>", 0)
}

func TestProducerPublishSyncWithDelay_Error(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	createStream(t, ns, jetstream.StreamConfig{Name: "small", Subjects: []string{"small"}, MaxMsgSize: 4})
	p := startJetStreamProducer(t, ns, "small")

	err := p.PublishSyncWithDelay(context.Background(), Message{Topic: "small", Tag: "tag", Key: "key", Body: []byte("0123456789")}, 5*time.Second)
	if _, ok := errors.AsType[*jetstream.APIError](err); !ok {
		t.Errorf("PublishSyncWithDelay() error = %v, want a JetStream API error", err)
	}
}

func TestProducerPublishAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		topic   string
		tag     string
		body    []byte
		wantErr error
		wantAny bool // any send error, from the transport
	}{
		{name: "success", topic: "orders", tag: "tag", body: []byte("hello")},
		{name: "send error", topic: "orders", tag: "tag", body: make([]byte, 2<<20), wantAny: true},
		{name: "topic not found", topic: "nonexistent-topic", tag: "tag", body: []byte("hello"), wantErr: ErrProducerNotFound},
		{name: "tag with dots", topic: "orders", tag: "order.created", body: []byte("hello"), wantErr: ErrInvalidTag},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ns := startJetStreamServer(t)
			p := startJetStreamProducer(t, ns, "orders")

			callback := func(ctx context.Context, result *jetstream.PubAck, err error) {}
			err := p.PublishAsync(context.Background(), Message{Topic: tt.topic, Tag: tt.tag, Key: "key", Body: tt.body}, callback)
			switch {
			case tt.wantErr != nil:
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("PublishAsync() error = %v, want %v", err, tt.wantErr)
				}
			case tt.wantAny:
				if err == nil {
					t.Error("PublishAsync() error = nil, want error")
				}
			default:
				if err != nil {
					t.Errorf("PublishAsync() error = %v, want nil", err)
				}
			}
		})
	}
}

func TestProducerBuildMsg(t *testing.T) {
	t.Parallel()

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
			msg := buildMsg(Message{Topic: tt.topic, Tag: tt.tag, Key: tt.key, Body: tt.body})

			if msg.Subject != tt.wantSubject {
				t.Errorf("buildMsg() Subject = %q, want %q", msg.Subject, tt.wantSubject)
			}
			if string(msg.Data) != string(tt.body) {
				t.Errorf("buildMsg() Body = %q, want %q", msg.Data, tt.body)
			}
			if msg.Header.Get(wire.HeaderTag) != tt.wantTag {
				t.Errorf("buildMsg() Tags = %q, want %q", msg.Header.Get(wire.HeaderTag), tt.wantTag)
			}
			if msg.Header.Get(wire.HeaderKey) != tt.wantKey {
				t.Errorf("buildMsg() Keys = %q, want %q", msg.Header.Get(wire.HeaderKey), tt.wantKey)
			}
			if got := msg.Header.Get("Nats-Msg-Id"); got != "" {
				t.Errorf("buildMsg() Nats-Msg-Id = %q, want none", got)
			}
		})
	}
}

func TestProducerRegisterAfterStart(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	startJetStreamProducer(t, ns, "topic1")

	p := startJetStreamProducer(t, ns, "topic2")
	err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "new-topic"})
	if !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("Register() after Start() = %v, want ErrAlreadyStarted", err)
	}
}

func TestProducerFor(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "existing-topic")

	t.Run("existing topic", func(t *testing.T) {
		t.Parallel()
		pd, err := p.producerFor("existing-topic")
		if err != nil {
			t.Errorf("producerFor() error = %v", err)
		}
		if pd == nil {
			t.Error("producerFor() returned nil producer")
		}
	})

	t.Run("nonexistent topic", func(t *testing.T) {
		t.Parallel()
		pd, err := p.producerFor("nonexistent-topic")
		if !errors.Is(err, ErrProducerNotFound) {
			t.Errorf("producerFor() error = %v, wantErr %v", err, ErrProducerNotFound)
		}
		if pd != nil {
			t.Error("producerFor() returned non-nil producer for nonexistent topic")
		}
	})
}

// TestProducerStartError checks that Start fails when the server cannot be reached
// within its context.
func TestProducerStartError(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{deadEndpoint}, Topic: "test-topic"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := p.Start(ctx); err == nil {
		t.Error("Start() = nil, want error")
	}
}

// TestProducerShutdownError checks that Shutdown returns the context error when a
// drain does not finish. A subscription on the producer's own connection blocks, so
// the drain waits for it past ctx.
func TestProducerShutdownError(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "orders")
	pd := p.run.producers["orders"]

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	entered := make(chan struct{}, 1)
	if _, err := pd.nc.Subscribe("hold", func(*nats.Msg) {
		entered <- struct{}{}
		<-release
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := pd.nc.Publish("hold", []byte("x")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

// regression: Start must return within its context when every server is
// unreachable, instead of waiting out one full dial timeout per server.
func TestStart_BoundedByContext(t *testing.T) {
	t.Parallel()

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{
		Endpoints: []string{"nats://10.255.255.1:4222", "nats://10.255.255.2:4222"},
		Topic:     "orders",
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- p.Start(ctx) }()

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

// regression: a second Start/Shutdown cycle must not share the pending-callback
// WaitGroup of the first run. A callback left pending by run one must not hold
// Shutdown of run two.
func TestPublishAsync_AcrossRuns(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	// Run one: the callback blocks, so Shutdown hits its deadline.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() run one error = %v", err)
	}
	if err := p.PublishAsync(context.Background(), Message{Topic: "orders", Tag: "", Key: "", Body: []byte("one")}, func(context.Context, *jetstream.PubAck, error) {
		<-release
	}); err != nil {
		t.Fatalf("PublishAsync() run one error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() run one error = %v, want %v", err, context.DeadlineExceeded)
	}

	// Run two: its only publish is acked, so Shutdown must return nil.
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() run two error = %v", err)
	}
	if err := p.PublishAsync(context.Background(), Message{Topic: "orders", Tag: "", Key: "", Body: []byte("two")}, nil); err != nil {
		t.Fatalf("PublishAsync() run two error = %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() run two error = %v, want nil", err)
	}
}

// TestDefaultDrain_Deadline checks that the default producer drain closes the
// connection and returns the context error when a delivery handler blocks the
// drain past its deadline.
func TestStreamProducer_Drain_Deadline(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	t.Cleanup(func() { close(release) })
	if _, err := nc.Subscribe("blocked", func(*nats.Msg) {
		entered <- struct{}{}
		<-release
	}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := nc.Publish("blocked", []byte("hold")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	<-entered

	pd := &streamProducer{nc: nc, js: js}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := pd.drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Drain() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if !nc.IsClosed() {
		t.Error("Drain() returned with the connection open, want it closed")
	}
}

// TestStart_RejectsIncompatibleStream checks that Start fails with the topic and
// the incompatible setting when an existing stream cannot take schedules.
func TestStart_RejectsIncompatibleStream(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	ctx := context.Background()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "orders",
		Subjects:  []string{"orders"},
		DenyPurge: true,
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}

	p, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: "orders"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	err = p.Start(ctx)
	const want = "orders: wire: stream orders: DenyPurge is incompatible with message schedules"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Start() error = %v, want it to contain %q", err, want)
	}
}

// TestPublishSyncWithDelay_DeliversOnceAtDelay publishes two delayed messages
// concurrently and reads the data subjects through a raw consumer. Each message
// arrives once, no earlier than its delay, with its tag, key, and traceparent, and
// no schedule message reaches the consumer.
func TestPublishSyncWithDelay_DeliversOnceAtDelay(t *testing.T) {
	useTraceContextPropagator(t)
	ns := startJetStreamServer(t)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(&recordingExporter{})))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	p := startJetStreamProducer(t, ns, "orders", WithTracer(tracer.Wrap(tp)))

	ctx := context.Background()
	js, err := jetstream.New(dialNATS(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	stream, err := js.Stream(ctx, "orders")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubjects: wire.DataSubjects("orders"),
		AckPolicy:      jetstream.AckNonePolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("CreateConsumer() error = %v", err)
	}

	cases := []struct {
		tag, key string
		delay    time.Duration
		sent     time.Time
	}{
		{tag: "first", key: "order-1", delay: time.Second},
		{tag: "second", key: "order-2", delay: 1500 * time.Millisecond},
	}
	var wg sync.WaitGroup
	for i := range cases {
		wg.Go(func() {
			c := &cases[i]
			c.sent = time.Now()
			if err := p.PublishSyncWithDelay(ctx, Message{Topic: "orders", Tag: c.tag, Key: c.key, Body: []byte(c.tag)}, c.delay); err != nil {
				t.Errorf("PublishSyncWithDelay(%s) error = %v", c.tag, err)
			}
		})
	}
	wg.Wait()

	type delivery struct {
		subject, tag, key, traceparent string
		at                             time.Time
	}
	var got []delivery
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}
		for m := range batch.Messages() {
			h := m.Headers()
			got = append(got, delivery{m.Subject(), h.Get(wire.HeaderTag), h.Get(wire.HeaderKey), h.Get("traceparent"), time.Now()})
		}
		if err := batch.Error(); err != nil {
			t.Fatalf("batch error = %v", err)
		}
	}

	if len(got) != len(cases) {
		t.Fatalf("delivered %d messages, want %d: %+v", len(got), len(cases), got)
	}
	for _, d := range got {
		if strings.HasPrefix(d.subject, "schedules.") {
			t.Errorf("schedule message delivered on subject %q", d.subject)
		}
	}
	for _, c := range cases {
		i := slices.IndexFunc(got, func(d delivery) bool { return d.tag == c.tag })
		if i < 0 {
			t.Errorf("no delivery with tag %q", c.tag)
			continue
		}
		d := got[i]
		if want := "orders." + c.tag; d.subject != want {
			t.Errorf("subject = %q, want %q", d.subject, want)
		}
		if d.key != c.key {
			t.Errorf("key = %q, want %q", d.key, c.key)
		}
		if d.traceparent == "" {
			t.Errorf("traceparent of %q = empty, want set", c.tag)
		}
		if earliest := c.sent.Add(c.delay - 100*time.Millisecond); d.at.Before(earliest) {
			t.Errorf("%q delivered at %v, want no earlier than %v", c.tag, d.at, earliest)
		}
	}
}

// TestPublishSync_SameKeyNotDeduplicated sends two messages with one key and
// different bodies. Both must be stored, and neither may carry Nats-Msg-Id.
func TestPublishSync_SameKeyNotDeduplicated(t *testing.T) {
	ns := startJetStreamServer(t)
	p := startJetStreamProducer(t, ns, "orders")

	ctx := context.Background()
	for _, body := range []string{"created", "updated"} {
		if err := p.PublishSync(ctx, Message{Topic: "orders", Tag: "", Key: "order-42", Body: []byte(body)}); err != nil {
			t.Fatalf("PublishSync(%s) error = %v", body, err)
		}
	}

	js, err := jetstream.New(dialNATS(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	stream, err := js.Stream(ctx, "orders")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info() error = %v", err)
	}
	if got := info.State.Msgs; got != 2 {
		t.Fatalf("stream Msgs = %d, want 2", got)
	}
	for seq := uint64(1); seq <= 2; seq++ {
		raw, err := stream.GetMsg(ctx, seq)
		if err != nil {
			t.Fatalf("GetMsg(%d) error = %v", seq, err)
		}
		if got := raw.Header.Get("Nats-Msg-Id"); got != "" {
			t.Errorf("message %d Nats-Msg-Id = %q, want none", seq, got)
		}
	}
}

// regression: a caller ctx that ends after the message is handed to the transport
// does not fail the publish; the callback still receives the ack.
func TestStreamProducer_PublishAsyncIgnoresCallerCancel(t *testing.T) {
	t.Parallel()

	ns := startJetStreamServer(t)
	nc := dialNATS(t, ns)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     "orders",
		Subjects: []string{"orders"},
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}

	pd := &streamProducer{nc: nc, js: js}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	if err := pd.publishAsync(ctx, nats.NewMsg("orders"), func(_ context.Context, _ *jetstream.PubAck, err error) {
		got <- err
	}); err != nil {
		t.Fatalf("PublishAsync() error = %v", err)
	}
	cancel()

	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("callback error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback was not called")
	}
}
