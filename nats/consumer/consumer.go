// Package consumer runs durable NATS JetStream consumers. A [Consumer] passes each
// delivered message to a [HandlerFunc] and settles the message from the handler's
// error.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

// Message is one delivered message, as a handler sees it. Its values are
// parsed once when the message arrives.
type Message interface {
	// Key returns the business key set by the producer (Sdk-Msg-Key), or "".
	Key() string
	// Tag returns the producer's tag (Sdk-Msg-Tag), or "".
	Tag() string
	// Subject returns the NATS subject the message was published to.
	Subject() string
	// Body returns the message payload.
	Body() []byte
	// Sequence is the stream sequence: unique per stored message.
	Sequence() uint64
	// DeliveryCount is 1 on first delivery and grows on each redelivery.
	DeliveryCount() uint64
	// Timestamp returns when the message was stored in the stream.
	Timestamp() time.Time
}

// jsConsumer is one durable JetStream consumer run by a Consumer.
type jsConsumer interface {
	// Consume starts delivering each message to handler.
	Consume(handler func(msg jetstream.Msg)) error
	// Shutdown stops intake, waits for in-flight handlers, and drains the
	// connection. It returns ctx.Err() if ctx ends first.
	Shutdown(ctx context.Context) error
}

// consumerFactory connects to the server and returns the durable consumer
// durableName for cfg. The stream and the durable consumer are ensured within ctx.
type consumerFactory func(ctx context.Context, cfg Config, durableName string) (jsConsumer, error)

// defaultJSConsumer is the jsConsumer backed by a NATS connection and a
// JetStream pull consumer.
type defaultJSConsumer struct {
	nc         *nats.Conn
	consumer   jetstream.Consumer
	cfg        Config
	consumeCtx jetstream.ConsumeContext
	wg         sync.WaitGroup
}

// Consume starts pulling messages and runs handler on up to MaxConcurrent
// messages at a time.
func (c *defaultJSConsumer) Consume(handler func(msg jetstream.Msg)) error {
	sem := make(chan struct{}, c.cfg.MaxConcurrent)
	consumeCtx, err := c.consumer.Consume(func(msg jetstream.Msg) {
		sem <- struct{}{}
		c.wg.Add(1)
		go func() {
			defer func() {
				<-sem
				c.wg.Done()
			}()
			handler(msg)
		}()
	}, jetstream.PullMaxMessages(c.cfg.MaxConcurrent))
	if err != nil {
		return fmt.Errorf("consumer: consume start: %w", err)
	}

	c.consumeCtx = consumeCtx
	return nil
}

// Shutdown stops intake, waits for in-flight handlers, and drains the connection.
// When ctx ends first it closes the connection and returns ctx.Err().
func (c *defaultJSConsumer) Shutdown(ctx context.Context) error {
	if err := c.quiesce(ctx); err != nil {
		c.nc.Close()
		return err
	}
	return wire.Drain(ctx, c.nc)
}

// quiesce stops new deliveries and waits for the handlers already running. Once
// the consume context is closed, no callback can add to the WaitGroup.
func (c *defaultJSConsumer) quiesce(ctx context.Context) error {
	if c.consumeCtx != nil {
		c.consumeCtx.Drain()
		select {
		case <-c.consumeCtx.Closed():
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// defaultConsumerFactory connects, ensures the stream exists, and creates or
// updates the durable consumer. On error it closes the connection it opened.
func defaultConsumerFactory(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
	// The dial timeout is left to wire, which caps it by ctx. cfg.Timeout is the
	// AckWait-sized ack timeout, not a dial timeout.
	nc, err := wire.Connect(ctx, durableName, wire.Options{
		Endpoints:       cfg.Endpoints,
		Token:           cfg.Token,
		Username:        cfg.Username,
		Password:        cfg.Password,
		NKey:            cfg.NKey,
		CredentialsFile: cfg.CredentialsFile,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("consumer: create jetstream: %w", err)
	}

	// The producer owns the stream, so an existing one is never updated.
	stream, err := js.Stream(ctx, cfg.Topic)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		stream, err = js.CreateStream(ctx, wire.StreamConfig(cfg.Topic))
	}
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("consumer: ensure stream %s: %w", cfg.Topic, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, cfg.buildConsumerConfig(durableName))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("consumer: create consumer %s: %w", durableName, err)
	}

	return &defaultJSConsumer{nc: nc, consumer: cons, cfg: cfg}, nil
}

// Option configures a Consumer returned by New.
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented messages. A nil
// client falls back to the OTel global meter provider (otel.SetMeterProvider).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented messages. A nil
// client falls back to the OTel global tracer provider (otel.SetTracerProvider).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// registration is a consumer accepted by Register and not yet started.
type registration struct {
	cfg     Config
	handler *handlerAdapter
}

// Consumer runs the registered JetStream consumers. Register stores them, Start
// connects and begins delivery, and Shutdown stops them.
type Consumer struct {
	appName         string
	consumerFactory consumerFactory
	metrics         metrics.Client
	tracer          tracer.Client

	// mu guards the fields below. Start and Shutdown hold it for their whole run.
	mu      sync.Mutex
	regs    map[string]registration // keyed by durable name
	running []jsConsumer
	started bool
	// cancelRun cancels runCtx, the context every handler call derives from. It
	// is set by Start and called by Shutdown before the consumers are drained.
	cancelRun context.CancelFunc
}

// New returns an empty, ready-to-use Consumer. The service identity is taken
// from info (its Name seeds the consumer group names). A missing service name
// is rejected with identity.ErrName; a name containing dots is rejected with
// ErrInvalidAppName because NATS uses dots as subject separators. Metrics and
// tracing go through the OTel global providers (via metrics.Default and tracer.Default)
// unless overridden via WithMetrics/WithTracer.
func New(info identity.Info, opts ...Option) (*Consumer, error) {
	if err := info.Validate(); err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	if strings.Contains(info.Name, ".") {
		return nil, ErrInvalidAppName
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	return &Consumer{
		appName:         info.Name,
		consumerFactory: defaultConsumerFactory,
		regs:            make(map[string]registration),
		metrics:         o.metrics,
		tracer:          o.tracer,
	}, nil
}

// Register validates cfg and stores it with handlerFunc. It does not connect;
// Start does. Register fails after Start.
func (c *Consumer) Register(cfg Config, handlerFunc HandlerFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return ErrAlreadyStarted
	}

	if handlerFunc == nil {
		return errors.New("consumer: handler function is nil")
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("consumer: register config invalid: %w", err)
	}

	name := c.buildGroupName(cfg.Topic, cfg.Group)
	if _, ok := c.regs[name]; ok {
		return fmt.Errorf("%w: %s", ErrConsumerExists, name)
	}

	addr, port := wire.ServerAddrPort(cfg.Endpoints)
	adapter, err := newHandlerAdapter(meta{
		topic:      cfg.Topic,
		group:      cfg.Group,
		serverAddr: addr,
		serverPort: port,
		propagator: otel.GetTextMapPropagator(),
		metrics:    c.metrics,
		tracer:     c.tracer,
	}, cfg, handlerFunc)
	if err != nil {
		return fmt.Errorf("consumer: create handler adapter: %w", err)
	}

	c.regs[name] = registration{cfg: cfg, handler: adapter}
	return nil
}

// Start connects every registered consumer, ensures its stream and durable
// consumer exist, and starts delivery. ctx bounds the setup only; handlers run
// until Shutdown. If any consumer fails, Start shuts down the ones already started
// and returns the error. Start does nothing once it has succeeded.
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	// Handlers outlive Start, so runCtx keeps Start's values but not its deadline.
	runCtx, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	var running []jsConsumer
	for name, reg := range c.regs {
		cn, err := c.startOne(ctx, runCtx, name, reg)
		if err != nil {
			cancelRun()
			return errors.Join(err, shutdownAll(ctx, running))
		}
		running = append(running, cn)
	}

	c.running = running
	c.cancelRun = cancelRun
	c.started = true
	return nil
}

// startOne creates the durable consumer for reg and starts its delivery. Handler
// calls derive from runCtx. On a failure after creation it shuts that consumer
// down.
func (c *Consumer) startOne(ctx, runCtx context.Context, name string, reg registration) (jsConsumer, error) {
	cn, err := c.consumerFactory(ctx, reg.cfg, name)
	if err != nil {
		return nil, fmt.Errorf("consumer: create %s: %w", name, err)
	}

	handle := func(msg jetstream.Msg) {
		_ = reg.handler.HandleMsg(runCtx, msg)
	}
	if err := cn.Consume(handle); err != nil {
		return nil, errors.Join(fmt.Errorf("consumer: subscribe %s: %w", name, err), cn.Shutdown(ctx))
	}
	return cn, nil
}

// Shutdown cancels the context of in-flight handlers, stops intake for every
// started consumer, waits for the handlers, and drains the connections, all
// bounded by ctx. It returns ctx.Err() when ctx ends first. Shutdown does nothing
// if Start has not succeeded, and a second call does nothing.
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	// Cancel before draining, so a handler that honors its ctx returns and the
	// drain does not wait out its full deadline.
	c.cancelRun()
	running := c.running
	c.running = nil
	c.cancelRun = nil
	c.started = false
	return shutdownAll(ctx, running)
}

// shutdownAll shuts down every consumer in cns and joins their errors.
func shutdownAll(ctx context.Context, cns []jsConsumer) error {
	var errs []error
	for _, cn := range cns {
		if err := cn.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (c *Consumer) buildGroupName(topic, group string) string {
	return fmt.Sprintf("%s_%s_%s", c.appName, topic, group)
}
