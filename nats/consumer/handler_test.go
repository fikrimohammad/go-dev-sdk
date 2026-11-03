package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
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
// handler runs, and the skip is logged at Warn.
func TestHandler_NoMetadataIsNakked(t *testing.T) {
	tests := []struct {
		name    string
		metaErr error
	}{
		{name: "metadata error", metaErr: errors.New("not a JetStream message")},
		{name: "nil metadata"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rl := captureLogs(t)

			var called atomic.Bool
			a, _, _ := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
				called.Store(true)
				return nil
			})

			msg := &unmetaMsg{metaErr: tc.metaErr}
			if err := a.HandleMsg(context.Background(), msg); err == nil {
				t.Fatal("HandleMsg() error = nil, want error")
			}
			if called.Load() {
				t.Fatal("handler was called, want it skipped")
			}
			if !msg.nacked.Load() {
				t.Fatal("message was not nacked")
			}

			rec, ok := rl.find("WARN", "message without JetStream metadata")
			if !ok {
				t.Fatal("no WARN record for the message without metadata")
			}
			if got := rec.arg("topic"); got != "reporting" {
				t.Errorf("topic = %v, want reporting", got)
			}
			if got := rec.arg("group"); got != "export_consumer" {
				t.Errorf("group = %v, want export_consumer", got)
			}
			if got := rec.arg("subject"); got != "reporting.export" {
				t.Errorf("subject = %v, want reporting.export", got)
			}
		})
	}
}

// regression: a failed ack is logged at Warn and noted on the process span. The
// connection is closed before the handler returns, so the ack cannot reach the
// server.
func TestSettle_ErrorIsLogged(t *testing.T) {
	rl := captureLogs(t)
	a, _, ex := setupAdapter(t, 0, func(ctx context.Context, msg Message) error { return nil })

	msg, f := deliveredReport(t, "hello")
	f.nc.Close()
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
	if got := rec.arg("error"); got != nats.ErrConnectionClosed.Error() {
		t.Errorf("error = %v, want %v", got, nats.ErrConnectionClosed)
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
			if string(kv.Key) == "error.message" && kv.Value.AsString() == nats.ErrConnectionClosed.Error() {
				return
			}
		}
	}
	t.Fatal("span has no messaging.settle.error event with error.message")
}

// regression: with MaxConcurrent=1, a buffered message is not redelivered while the
// only handler slot is busy, because MaxAckPending caps the unacked messages.
func TestHandler_BufferedMessageNotRedelivered(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var mu sync.Mutex
	deliveries := make(map[string]int)
	done := make(chan struct{}, 2)
	cfg := Config{
		Endpoints:      []string{ns.ClientURL()},
		Topic:          "orders",
		Group:          "billing",
		Timeout:        time.Second,
		HandlerTimeout: 5 * time.Second,
		MaxConcurrent:  1,
	}
	startTestConsumer(t, c, cfg, func(ctx context.Context, msg Message) error {
		mu.Lock()
		deliveries[string(msg.Body())]++
		mu.Unlock()
		time.Sleep(2500 * time.Millisecond)
		done <- struct{}{}
		return nil
	})

	for _, body := range []string{"a", "b"} {
		if err := nc.Publish("orders", []byte(body)); err != nil {
			t.Fatalf("Publish(%s) error = %v", body, err)
		}
	}
	for range 2 {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("both messages were not handled")
		}
	}
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, body := range []string{"a", "b"} {
		if got := deliveries[body]; got != 1 {
			t.Errorf("deliveries of %q = %d, want 1", body, got)
		}
	}
}

// regression: the heartbeat stops at the handler deadline, so a handler that ignores
// its ctx and outlives it lets AckWait redeliver the message.
func TestHandler_HeartbeatStopsAtDeadline(t *testing.T) {
	t.Parallel()

	ns := startTestNATSServer(t)
	nc := connectTestClient(t, ns)

	c, err := New(testAppInfo)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var calls atomic.Int32
	cfg := Config{
		Endpoints:        []string{ns.ClientURL()},
		Topic:            "orders",
		Group:            "billing",
		Timeout:          time.Second,
		HandlerTimeout:   time.Second,
		MaxRetryAttempts: 5,
	}
	startTestConsumer(t, c, cfg, func(ctx context.Context, msg Message) error {
		if calls.Add(1) == 1 {
			time.Sleep(4 * time.Second)
		}
		return nil
	})

	if err := nc.Publish("orders", []byte("stuck")); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	time.Sleep(4500 * time.Millisecond)

	if got := calls.Load(); got < 2 {
		t.Fatalf("deliveries = %d, want at least 2", got)
	}
}

// regression: a zero heartbeat interval starts no ticker, since time.NewTicker
// panics on a non-positive interval, and the handler still runs.
func TestHandler_ZeroHeartbeatIntervalRuns(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	a, _, _ := setupAdapter(t, 0, func(ctx context.Context, msg Message) error {
		called.Store(true)
		return nil
	})
	a.heartbeatEvery = 0

	msg, _ := deliveredReport(t, "hi")
	if err := a.HandleMsg(context.Background(), msg); err != nil {
		t.Fatalf("HandleMsg() error = %v", err)
	}
	if !called.Load() {
		t.Fatal("handler was not called")
	}
}

// regression: once the run context is canceled, a message is nak'd at once, without
// the retry backoff, and the handler does not run.
func TestHandler_CanceledContextNaksWithoutRunning(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	a, _, _ := setupAdapter(t, 2*time.Second, func(ctx context.Context, msg Message) error {
		called.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg, f := deliveredReport(t, "late")
	if err := a.HandleMsg(ctx, msg); !errors.Is(err, context.Canceled) {
		t.Fatalf("HandleMsg() error = %v, want %v", err, context.Canceled)
	}
	if called.Load() {
		t.Fatal("handler was called, want it skipped")
	}

	// A nak with no backoff brings the message back at once, well inside the 2 s
	// RetryBackoff that setupAdapter configured.
	again, ok := f.next(t, time.Second)
	if !ok {
		t.Fatal("message was not redelivered at once after the nak")
	}
	if got := deliveries(t, again); got != 2 {
		t.Errorf("deliveries = %d, want 2", got)
	}
}
