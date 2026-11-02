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

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/nats/internal/wire"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

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
	return c.drainConn(ctx)
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

// drainConn drains the connection and waits until it is closed. nats.Conn.Drain
// is asynchronous, so the wait polls IsClosed. ctx bounds the wait.
func (c *defaultJSConsumer) drainConn(ctx context.Context) error {
	if err := c.nc.Drain(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
		c.nc.Close()
		return fmt.Errorf("consumer: drain connection: %w", err)
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !c.nc.IsClosed() {
		select {
		case <-ctx.Done():
			c.nc.Close()
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

// defaultConsumerFactory connects, ensures the stream exists, and creates or
// updates the durable consumer. On error it closes the connection it opened.
func defaultConsumerFactory(ctx context.Context, cfg Config, durableName string) (jsConsumer, error) {
	nc, err := wire.Connect(durableName, wire.Options{
		Endpoints:       cfg.Endpoints,
		Timeout:         cfg.Timeout,
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
		return errors.New("consumer: cannot register after Start")
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
	}, cfg.RetryBackoff, handlerFunc)
	if err != nil {
		return fmt.Errorf("consumer: create handler adapter: %w", err)
	}

	c.regs[name] = registration{cfg: cfg, handler: adapter}
	return nil
}

// Start connects every registered consumer, ensures its stream and durable
// consumer exist, and starts delivery. ctx bounds the setup. If any consumer
// fails, Start shuts down the ones already started and returns the error. Start
// does nothing once it has succeeded.
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	var running []jsConsumer
	for name, reg := range c.regs {
		cn, err := c.startOne(ctx, name, reg)
		if err != nil {
			return errors.Join(err, shutdownAll(ctx, running))
		}
		running = append(running, cn)
	}

	c.running = running
	c.started = true
	return nil
}

// startOne creates the durable consumer for reg and starts its delivery. On a
// failure after creation it shuts that consumer down.
func (c *Consumer) startOne(ctx context.Context, name string, reg registration) (jsConsumer, error) {
	cn, err := c.consumerFactory(ctx, reg.cfg, name)
	if err != nil {
		return nil, fmt.Errorf("consumer: create %s: %w", name, err)
	}

	if err := cn.Consume(reg.handler.Handle); err != nil {
		return nil, errors.Join(fmt.Errorf("consumer: subscribe %s: %w", name, err), cn.Shutdown(ctx))
	}
	return cn, nil
}

// Shutdown stops intake for every started consumer, waits for in-flight
// handlers, and drains the connections, all bounded by ctx. It returns ctx.Err()
// when ctx ends first. Shutdown does nothing if Start has not succeeded, and a
// second call does nothing.
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	running := c.running
	c.running = nil
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
