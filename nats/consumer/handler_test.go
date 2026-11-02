package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/fikrimohammad/go-dev-sdk/nats/internal/wire"
)

// regression: Shutdown cancels a running handler's ctx, and the handler's deadline is
// HandlerTimeout after it started.
func TestHandler_ContextCanceledOnShutdown(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	const handlerTimeout = 2 * time.Second
	type result struct {
		err      error
		deadline time.Time
	}
	results := make(chan result, 1)
	started := make(chan struct{}, 1)
	cfg := Config{
		Endpoints:      []string{ns.ClientURL()},
		Topic:          "orders",
		Group:          "billing",
		HandlerTimeout: handlerTimeout,
	}
	startTestConsumer(t, c, cfg, func(ctx context.Context, msg Message) error {
		deadline, _ := ctx.Deadline()
		started <- struct{}{}
		<-ctx.Done()
		results <- result{err: ctx.Err(), deadline: deadline}
		return ctx.Err()
	})

	publishedAt := time.Now()
	if err := nc.Publish("orders", []byte("blocked")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	select {
	case got := <-results:
		if !errors.Is(got.err, context.Canceled) {
			t.Errorf("handler ctx.Err() = %v, want %v", got.err, context.Canceled)
		}
		if diff := got.deadline.Sub(publishedAt) - handlerTimeout; diff < -200*time.Millisecond || diff > 200*time.Millisecond {
			t.Errorf("handler deadline off by %v, want about HandlerTimeout (%v) from start", diff, handlerTimeout)
		}
	default:
		t.Fatal("handler did not return after Shutdown")
	}
}

// regression: a handler that runs longer than AckWait keeps its message through
// InProgress heartbeats, so the server does not redeliver it.
func TestHandler_HeartbeatPreventsRedelivery(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var deliveries atomic.Int32
	cfg := Config{
		Endpoints:        []string{ns.ClientURL()},
		Topic:            "orders",
		Group:            "billing",
		Timeout:          time.Second,
		HandlerTimeout:   5 * time.Second,
		MaxRetryAttempts: 5,
	}
	startTestConsumer(t, c, cfg, func(ctx context.Context, msg Message) error {
		deliveries.Add(1)
		time.Sleep(2500 * time.Millisecond)
		return nil
	})

	if err := nc.Publish("orders", []byte("slow")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	time.Sleep(4500 * time.Millisecond)

	if got := deliveries.Load(); got != 1 {
		t.Fatalf("deliveries = %d, want 1", got)
	}
}

// regression: a handler error wrapping ErrNoRetry terms the message, so it is
// never redelivered even after AckWait.
func TestHandler_ErrNoRetryTerms(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var deliveries atomic.Int32
	cfg := Config{
		Endpoints:        []string{ns.ClientURL()},
		Topic:            "orders",
		Group:            "billing",
		Timeout:          500 * time.Millisecond,
		MaxRetryAttempts: 5,
	}
	startTestConsumer(t, c, cfg, func(ctx context.Context, msg Message) error {
		deliveries.Add(1)
		return fmt.Errorf("%w: x", ErrNoRetry)
	})

	if err := nc.Publish("orders", []byte("poison")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	time.Sleep(2 * time.Second)

	if got := deliveries.Load(); got != 1 {
		t.Fatalf("deliveries = %d, want 1", got)
	}
}

// regression: the handler sees the key, tag, subject, body, stream sequence, and
// delivery count, and the delivery count grows on redelivery.
func TestHandler_MessageMetadata(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	type seen struct {
		key, tag, subject string
		body              string
		sequence, count   uint64
	}
	seenCh := make(chan seen, 2)
	cfg := Config{
		Endpoints: []string{ns.ClientURL()},
		Topic:     "orders",
		Group:     "billing",
	}
	startTestConsumer(t, c, cfg, func(ctx context.Context, msg Message) error {
		seenCh <- seen{
			key:      msg.Key(),
			tag:      msg.Tag(),
			subject:  msg.Subject(),
			body:     string(msg.Body()),
			sequence: msg.Sequence(),
			count:    msg.DeliveryCount(),
		}
		if msg.DeliveryCount() == 1 {
			return errors.New("first delivery fails")
		}
		return nil
	})

	raw := &nats.Msg{Subject: "orders.created", Data: []byte("payload"), Header: nats.Header{}}
	raw.Header.Set(wire.HeaderKey, "k1")
	raw.Header.Set(wire.HeaderTag, "created")
	if err := nc.PublishMsg(raw); err != nil {
		t.Fatalf("PublishMsg() error = %v", err)
	}

	want := []seen{
		{key: "k1", tag: "created", subject: "orders.created", body: "payload", sequence: 1, count: 1},
		{key: "k1", tag: "created", subject: "orders.created", body: "payload", sequence: 1, count: 2},
	}
	for i := range want {
		select {
		case got := <-seenCh:
			if got != want[i] {
				t.Errorf("delivery %d = %+v, want %+v", i+1, got, want[i])
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("delivery %d was not received", i+1)
		}
	}
}

// regression: a message whose JetStream metadata is missing is nak'd before the
// handler runs.
func TestHandler_NoMetadataIsNakked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		metaErr error
	}{
		{name: "metadata error", metaErr: errors.New("not a JetStream message")},
		{name: "nil metadata"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var called atomic.Bool
			a, _, _ := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
				called.Store(true)
				return nil
			})

			msg := &mockMsg{subject: "reporting.export", data: []byte("hi"), metaErr: tc.metaErr}
			if err := a.HandleMsg(context.Background(), msg); err == nil {
				t.Fatal("HandleMsg() error = nil, want error")
			}
			if called.Load() {
				t.Fatal("handler was called, want it skipped")
			}
			if !msg.nacked {
				t.Fatal("message was not nacked")
			}
		})
	}
}

// regression: a failed ack is logged at Warn and noted on the process span.
func TestSettle_ErrorIsLogged(t *testing.T) {
	rl := captureLogs(t)
	a, _, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error { return nil })

	msg := testMsg("hello")
	msg.ackErr = errors.New("ack timed out")
	if err := a.HandleMsg(context.Background(), msg); err != nil {
		t.Fatalf("HandleMsg() error = %v", err)
	}

	rec, ok := rl.find("WARN", "message settle failed")
	if !ok {
		t.Fatal("no WARN record for the failed ack")
	}
	if got := rec.arg("topic"); got != "reporting" {
		t.Errorf("topic = %v, want reporting", got)
	}
	if got := rec.arg("group"); got != "export_consumer" {
		t.Errorf("group = %v, want export_consumer", got)
	}
	if got := rec.arg("sequence"); got != uint64(1) {
		t.Errorf("sequence = %v, want 1", got)
	}
	if got := rec.arg("error"); got != "ack timed out" {
		t.Errorf("error = %v, want ack timed out", got)
	}

	sp, ok := ex.last()
	if !ok {
		t.Fatal("no span recorded")
	}
	for _, ev := range sp.Events() {
		if ev.Name != "messaging.settle.error" {
			continue
		}
		for _, kv := range ev.Attributes {
			if string(kv.Key) == "error.message" && kv.Value.AsString() == "ack timed out" {
				return
			}
		}
	}
	t.Fatal("span has no messaging.settle.error event with error.message")
}
