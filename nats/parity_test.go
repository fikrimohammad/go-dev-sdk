package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/nats/consumer"
	"github.com/fikrimohammad/go-dev-sdk/nats/producer"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

type orderEvent struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func startTestNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()

	opts := &natsserver.Options{
		Port:      -1, // Pick a random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create NATS test server: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS test server failed to become ready for connections")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns
}

// TestRegistrationDotRejection tests that the SDK strictly rejects dots
// across all fields (Topic, Group, Tag, and appinfo.Name).
func TestRegistrationDotRejection(t *testing.T) {
	ns := startTestNATSServer(t)
	serverURL := ns.ClientURL()

	// 1. Producer registration rejects Topic containing dots
	p, err := producer.New(appinfo.Info{Name: "order_producer"})
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	err = p.Register(producer.Config{
		Endpoints: []string{serverURL},
		Topic:     "oec.order.event",
	})
	if !errors.Is(err, producer.ErrInvalidTopic) {
		t.Errorf("p.Register(topic with dots) = %v, want %v", err, producer.ErrInvalidTopic)
	}

	// 2. Producer publish rejects Tag containing dots
	if err := p.Register(producer.Config{
		Endpoints: []string{serverURL},
		Topic:     "oec_order_event",
	}); err != nil {
		t.Fatalf("p.Register: %v", err)
	}
	err = p.PublishSync(context.Background(), "oec_order_event", "order.created", "key", []byte("data"))
	if !errors.Is(err, producer.ErrInvalidTag) {
		t.Errorf("p.PublishSync(tag with dots) = %v, want %v", err, producer.ErrInvalidTag)
	}

	// 3. Consumer creation rejects appinfo.Name containing dots
	_, err = consumer.New(appinfo.Info{Name: "oec.pay.billing"})
	if !errors.Is(err, consumer.ErrInvalidAppName) {
		t.Errorf("consumer.New(appName with dots) = %v, want %v", err, consumer.ErrInvalidAppName)
	}

	// 4. Consumer registration rejects Topic containing dots
	c, err := consumer.New(appinfo.Info{Name: "oec_pay_billing"})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	err = c.Register(consumer.Config{
		Endpoints: []string{serverURL},
		Topic:     "oec.order.event",
		Group:     "order_created",
	}, func(ctx context.Context, body []byte) error { return nil })
	if !errors.Is(err, consumer.ErrInvalidTopic) {
		t.Errorf("c.Register(topic with dots) = %v, want %v", err, consumer.ErrInvalidTopic)
	}

	// 5. Consumer registration rejects Group containing dots
	err = c.Register(consumer.Config{
		Endpoints: []string{serverURL},
		Topic:     "oec_order_event",
		Group:     "order.created",
	}, func(ctx context.Context, body []byte) error { return nil })
	if !errors.Is(err, consumer.ErrInvalidGroup) {
		t.Errorf("c.Register(group with dots) = %v, want %v", err, consumer.ErrInvalidGroup)
	}

	// 6. Consumer registration rejects Tags containing dots
	err = c.Register(consumer.Config{
		Endpoints: []string{serverURL},
		Topic:     "oec_order_event",
		Group:     "order_created",
		Tags:      []string{"order.created"},
	}, func(ctx context.Context, body []byte) error { return nil })
	if !errors.Is(err, consumer.ErrInvalidTag) {
		t.Errorf("c.Register(tags with dots) = %v, want %v", err, consumer.ErrInvalidTag)
	}
}

// TestMultiConsumerParity tests the multi-consumer message pattern:
// 1. Topic (stream): oec_order_event
// 2. Tag: order_created
// 3. Consumer groups:
//   - oec_pay_billing_oec_order_event_order_created (Billing service)
//   - oec_promo_consumer_oec_order_event_order_created (Promo service)
//
// Each consumer group has its own independent offset and concurrency, receives
// each message published to the tag, and filters out other tags (e.g. order_cancelled).
func TestMultiConsumerParity(t *testing.T) {
	ns := startTestNATSServer(t)
	serverURL := ns.ClientURL()

	const (
		topicName   = "oec_order_event"
		targetTag   = "order_created"
		otherTag    = "order_cancelled"
		billingApp  = "oec_pay_billing"
		promoApp    = "oec_promo_consumer"
		groupName   = "order_created"
		numMessages = 5
	)

	// Set up OpenTelemetry tracer provider for trace propagation verification
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// 1. Create Producer
	p, err := producer.New(appinfo.Info{Name: "oec_order_producer"}, producer.WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}

	err = p.Register(producer.Config{
		Endpoints:        []string{serverURL},
		Topic:            topicName,
		Timeout:          10 * time.Second,
		MaxRetryAttempts: 3,
	})
	if err != nil {
		t.Fatalf("p.Register: %v", err)
	}

	if err := p.Start(); err != nil {
		t.Fatalf("p.Start: %v", err)
	}
	defer func() { _ = p.Shutdown() }()

	// 2. Create Billing Consumer Group (oec_pay_billing_oec_order_event_order_created)
	cBilling, err := consumer.New(appinfo.Info{Name: billingApp}, consumer.WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("consumer.New (billing): %v", err)
	}

	var (
		billingReceived sync.Map // map[string]int (orderID -> count)
		billingWg       sync.WaitGroup
	)
	billingWg.Add(numMessages)

	err = cBilling.Register(consumer.Config{
		Endpoints:        []string{serverURL},
		Topic:            topicName,
		Group:            groupName,
		Tags:             []string{targetTag},
		Timeout:          10 * time.Second,
		MaxRetryAttempts: 3,
		MaxConcurrent:    5,
		ConsumeFromWhere: consumer.ConsumeFromFirst,
	}, func(ctx context.Context, body []byte) error {
		var evt orderEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			return err
		}
		// Verify trace context was propagated
		if id := tracer.TraceIDFrom(ctx); id == "" {
			t.Errorf("[billing] missing trace ID in context")
		}

		if _, loaded := billingReceived.LoadOrStore(evt.OrderID, 1); !loaded {
			billingWg.Done()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cBilling.Register: %v", err)
	}

	// 3. Create Promo Consumer Group (oec_promo_consumer_oec_order_event_order_created)
	cPromo, err := consumer.New(appinfo.Info{Name: promoApp}, consumer.WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("consumer.New (promo): %v", err)
	}

	var (
		promoReceived sync.Map // map[string]int (orderID -> count)
		promoWg       sync.WaitGroup
	)
	promoWg.Add(numMessages)

	err = cPromo.Register(consumer.Config{
		Endpoints:        []string{serverURL},
		Topic:            topicName,
		Group:            groupName,
		Tags:             []string{targetTag},
		Timeout:          10 * time.Second,
		MaxRetryAttempts: 3,
		MaxConcurrent:    10,
		ConsumeFromWhere: consumer.ConsumeFromFirst,
	}, func(ctx context.Context, body []byte) error {
		var evt orderEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			return err
		}
		// Verify trace context was propagated
		if id := tracer.TraceIDFrom(ctx); id == "" {
			t.Errorf("[promo] missing trace ID in context")
		}

		if _, loaded := promoReceived.LoadOrStore(evt.OrderID, 1); !loaded {
			promoWg.Done()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cPromo.Register: %v", err)
	}

	// 4. Start both consumers
	if err := cBilling.Start(); err != nil {
		t.Fatalf("cBilling.Start: %v", err)
	}
	defer func() { _ = cBilling.Shutdown() }()

	if err := cPromo.Start(); err != nil {
		t.Fatalf("cPromo.Start: %v", err)
	}
	defer func() { _ = cPromo.Shutdown() }()

	// 5. Publish `order_created` messages with W3C parent trace
	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "order-producer-trace")
	defer parentSpan.End()

	for i := 1; i <= numMessages; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		payload, _ := json.Marshal(orderEvent{
			OrderID: orderID,
			Amount:  float64(i * 100),
		})

		err := p.PublishSync(parentCtx, topicName, targetTag, orderID, payload)
		if err != nil {
			t.Fatalf("PublishSync %s: %v", orderID, err)
		}
	}

	// 6. Publish an `order_cancelled` message with a different tag
	// Neither billing nor promo groups should receive this.
	cancelledPayload, _ := json.Marshal(orderEvent{
		OrderID: "order-cancelled-999",
		Amount:  999.0,
	})
	err = p.PublishSync(parentCtx, topicName, otherTag, "order-cancelled-999", cancelledPayload)
	if err != nil {
		t.Fatalf("PublishSync cancelled: %v", err)
	}

	// 7. Wait for both consumers to receive all 5 target messages
	billingDone := make(chan struct{})
	go func() {
		billingWg.Wait()
		close(billingDone)
	}()

	promoDone := make(chan struct{})
	go func() {
		promoWg.Wait()
		close(promoDone)
	}()

	select {
	case <-billingDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for billing consumer to receive all messages")
	}

	select {
	case <-promoDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for promo consumer to receive all messages")
	}

	// 8. Verify message isolation & filtering
	for i := 1; i <= numMessages; i++ {
		orderID := fmt.Sprintf("order-%d", i)
		if _, ok := billingReceived.Load(orderID); !ok {
			t.Errorf("billing did not receive %s", orderID)
		}
		if _, ok := promoReceived.Load(orderID); !ok {
			t.Errorf("promo did not receive %s", orderID)
		}
	}

	if _, ok := billingReceived.Load("order-cancelled-999"); ok {
		t.Errorf("billing unexpectedly received order_cancelled message")
	}
	if _, ok := promoReceived.Load("order-cancelled-999"); ok {
		t.Errorf("promo unexpectedly received order_cancelled message")
	}
}

// TestConsumerRetryRedelivery tests that when a handler returns an error,
// the message is negatively acknowledged (NAK) and redelivered.
func TestConsumerRetryRedelivery(t *testing.T) {
	ns := startTestNATSServer(t)
	serverURL := ns.ClientURL()

	const (
		topicName = "oec_order_event"
		tag       = "order_retry"
	)

	p, err := producer.New(appinfo.Info{Name: "order_producer"})
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	if err := p.Register(producer.Config{
		Endpoints: []string{serverURL},
		Topic:     topicName,
	}); err != nil {
		t.Fatalf("p.Register: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("p.Start: %v", err)
	}
	defer func() { _ = p.Shutdown() }()

	c, err := consumer.New(appinfo.Info{Name: "order_worker"})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}

	var attempts atomic.Int32
	delivered := make(chan struct{})

	err = c.Register(consumer.Config{
		Endpoints:        []string{serverURL},
		Topic:            topicName,
		Group:            "retry_worker",
		Tags:             []string{tag},
		Timeout:          2 * time.Second,
		MaxRetryAttempts: 3,
		RetryBackoff:     50 * time.Millisecond,
	}, func(ctx context.Context, body []byte) error {
		cnt := attempts.Add(1)
		if cnt == 1 {
			// Fail on first attempt to trigger NAK redelivery
			return fmt.Errorf("simulated temporary failure")
		}
		// Succeed on second attempt
		close(delivered)
		return nil
	})
	if err != nil {
		t.Fatalf("c.Register: %v", err)
	}

	if err := c.Start(); err != nil {
		t.Fatalf("c.Start: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	if err := p.PublishSync(context.Background(), topicName, tag, "retry-order", []byte("retry-data")); err != nil {
		t.Fatalf("PublishSync: %v", err)
	}

	select {
	case <-delivered:
		if attempts.Load() != 2 {
			t.Errorf("expected 2 attempts (1 failure + 1 redelivery), got %d", attempts.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for redelivery, attempts = %d", attempts.Load())
	}
}

// TestConsumerConcurrentExecution verifies that MaxConcurrent allows multiple messages
// to be processed concurrently in parallel goroutines.
func TestConsumerConcurrentExecution(t *testing.T) {
	ns := startTestNATSServer(t)
	serverURL := ns.ClientURL()

	const (
		topicName = "concurrent_topic"
		groupName = "concurrent_group"
		tag       = "task"
		numMsgs   = 5
	)

	p, err := producer.New(appinfo.Info{Name: "concurrent_prod"})
	if err != nil {
		t.Fatalf("producer.New: %v", err)
	}
	if err := p.Register(producer.Config{
		Endpoints: []string{serverURL},
		Topic:     topicName,
	}); err != nil {
		t.Fatalf("p.Register: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("p.Start: %v", err)
	}
	defer func() { _ = p.Shutdown() }()

	c, err := consumer.New(appinfo.Info{Name: "concurrent_app"})
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}

	var (
		inFlight    atomic.Int32
		maxObserved atomic.Int32
		allEntered  = make(chan struct{})
		wg          sync.WaitGroup
	)
	wg.Add(numMsgs)

	err = c.Register(consumer.Config{
		Endpoints:        []string{serverURL},
		Topic:            topicName,
		Group:            groupName,
		Tags:             []string{tag},
		MaxConcurrent:    numMsgs,
		ConsumeFromWhere: consumer.ConsumeFromFirst,
	}, func(ctx context.Context, body []byte) error {
		cur := inFlight.Add(1)
		for {
			old := maxObserved.Load()
			if cur <= old || maxObserved.CompareAndSwap(old, cur) {
				break
			}
		}

		if cur == numMsgs {
			close(allEntered)
		}

		select {
		case <-allEntered:
		case <-time.After(3 * time.Second):
		}

		inFlight.Add(-1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("c.Register: %v", err)
	}

	if err := c.Start(); err != nil {
		t.Fatalf("c.Start: %v", err)
	}
	defer func() { _ = c.Shutdown() }()

	for i := 0; i < numMsgs; i++ {
		if err := p.PublishSync(context.Background(), topicName, tag, fmt.Sprintf("id-%d", i), []byte("test")); err != nil {
			t.Fatalf("PublishSync: %v", err)
		}
	}

	wg.Wait()

	if maxObserved.Load() < 2 {
		t.Errorf("expected concurrent execution (maxObserved > 1), got %d", maxObserved.Load())
	}
}
