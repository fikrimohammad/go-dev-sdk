package consumer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

type jsConsumer interface {
	Start() error
	Shutdown() error
	Consume(handler func(msg jetstream.Msg)) error
}

type consumerFactory func(cfg Config, durableName string) (jsConsumer, error)

type defaultJSConsumer struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	consumer   jetstream.Consumer
	consumeCtx jetstream.ConsumeContext
	cfg        Config
	handler    func(msg jetstream.Msg)
	mu         sync.Mutex
	wg         sync.WaitGroup
	started    bool
}

func (c *defaultJSConsumer) Consume(handler func(msg jetstream.Msg)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handler = handler
	return nil
}

func (c *defaultJSConsumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	if c.handler == nil {
		return errors.New("consumer: handler is nil")
	}

	opts := []jetstream.PullConsumeOpt{}
	maxConcurrent := c.cfg.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrent
	}
	opts = append(opts, jetstream.PullMaxMessages(maxConcurrent))

	sem := make(chan struct{}, maxConcurrent)
	workerHandler := func(msg jetstream.Msg) {
		sem <- struct{}{}
		c.wg.Add(1)
		go func() {
			defer func() {
				<-sem
				c.wg.Done()
			}()
			c.handler(msg)
		}()
	}

	consumeCtx, err := c.consumer.Consume(workerHandler, opts...)
	if err != nil {
		return fmt.Errorf("consumer: consume start: %w", err)
	}

	c.consumeCtx = consumeCtx
	c.started = true
	return nil
}

func (c *defaultJSConsumer) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return nil
	}

	if c.consumeCtx != nil {
		c.consumeCtx.Stop()
		c.consumeCtx = nil
	}

	c.wg.Wait()

	if c.nc != nil {
		_ = c.nc.Drain()
	}

	c.started = false
	return nil
}

func defaultConsumerFactory(cfg Config, durableName string) (jsConsumer, error) {
	opts := []nats.Option{
		nats.Name(durableName),
		nats.Timeout(cfg.Timeout),
		nats.RetryOnFailedConnect(true),
	}

	if cfg.Token != "" {
		opts = append(opts, nats.Token(cfg.Token))
	}
	if cfg.Username != "" || cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
	}
	if cfg.NKey != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.NKey)
		if err != nil {
			return nil, fmt.Errorf("consumer: nkey option: %w", err)
		}
		opts = append(opts, opt)
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}

	endpoints := strings.Join(cfg.Endpoints, ",")
	nc, err := nats.Connect(endpoints, opts...)
	if err != nil {
		return nil, fmt.Errorf("consumer: connect to %s: %w", endpoints, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("consumer: create jetstream: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.Topic,
		Subjects:          []string{cfg.Topic, cfg.Topic + ".>"},
		AllowMsgSchedules: true,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("consumer: ensure stream %s: %w", cfg.Topic, err)
	}

	consumerConfig := cfg.buildConsumerConfig(durableName)
	cons, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("consumer: create consumer %s: %w", durableName, err)
	}

	return &defaultJSConsumer{
		nc:       nc,
		js:       js,
		consumer: cons,
		cfg:      cfg,
	}, nil
}

// Option configures a Consumer returned by New.
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented messages. A nil
// client falls back to the package-level default (metrics.SetDefault).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented messages. A nil
// client falls back to the package-level default (tracer.SetDefault).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

type Consumer struct {
	appName         string
	consumers       sync.Map
	started         atomic.Bool
	consumerFactory consumerFactory
	mu              sync.Mutex

	metrics metrics.Client
	tracer  tracer.Client
}

// New returns an empty, ready-to-use Consumer. The service identity is taken
// from info (its Name seeds the consumer group names). Metrics and tracing use
// the package-level defaults unless overridden via WithMetrics/WithTracer.
func New(info appinfo.Info, opts ...Option) (*Consumer, error) {
	if info.Name == "" {
		return nil, errors.New("appinfo name is empty")
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
		consumers:       sync.Map{},
		consumerFactory: defaultConsumerFactory,
		metrics:         o.metrics,
		tracer:          o.tracer,
	}, nil
}

// Register configures and registers a consumer for cfg.Topic and cfg.Group.
func (c *Consumer) Register(cfg Config, handlerFunc HandlerFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started.Load() {
		return errors.New("consumer: cannot register after Start")
	}

	if handlerFunc == nil {
		return errors.New("consumer: handler function is nil")
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("consumer: register config invalid: %w", err)
	}

	cr, err := c.getConsumer(cfg.Topic, cfg.Group)
	if err != nil && !errors.Is(err, ErrConsumerNotFound) {
		return fmt.Errorf("consumer: get existing consumer: %w", err)
	}

	if cr != nil {
		key := c.buildGroupName(cfg.Topic, cfg.Group)
		return fmt.Errorf("%w: %s", ErrConsumerExists, key)
	}

	groupName := c.buildGroupName(cfg.Topic, cfg.Group)
	cn, err := c.consumerFactory(cfg, groupName)
	if err != nil {
		return fmt.Errorf("consumer: create consumer %s: %w", groupName, err)
	}

	addr, port := serverAddrPort(cfg.Endpoints)
	consumerHandlerAdapter, err := newHandlerAdapter(meta{
		topic:      cfg.Topic,
		group:      cfg.Group,
		serverAddr: addr,
		serverPort: port,
		propagator: otel.GetTextMapPropagator(),
		metrics:    c.metrics,
		tracer:     c.tracer,
	}, cfg.RetryBackoff, handlerFunc)
	if err != nil {
		_ = cn.Shutdown()
		return fmt.Errorf("consumer: create handler adapter: %w", err)
	}

	err = cn.Consume(consumerHandlerAdapter.Handle)
	if err != nil {
		_ = cn.Shutdown()
		return fmt.Errorf("consumer: subscribe %s: %w", groupName, err)
	}

	c.setConsumer(cfg.Topic, cfg.Group, cn)
	return nil
}

func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started.CompareAndSwap(false, true) {
		return nil
	}

	consumers, err := c.getAllConsumers()
	if err != nil {
		c.started.Store(false)
		return err
	}

	var errs []error
	for _, cn := range consumers {
		if err := cn.Start(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		c.started.Store(false)
		return errors.Join(errs...)
	}

	return nil
}

func (c *Consumer) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started.CompareAndSwap(true, false) {
		return nil
	}

	consumers, err := c.getAllConsumers()
	if err != nil {
		return err
	}

	var errs []error
	for _, cn := range consumers {
		if err := cn.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (c *Consumer) getConsumer(topic, group string) (jsConsumer, error) {
	key := c.buildGroupName(topic, group)
	consumerRaw, ok := c.consumers.Load(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrConsumerNotFound, key)
	}

	cn, ok := consumerRaw.(jsConsumer)
	if !ok {
		return nil, fmt.Errorf("consumer: invalid consumer type: %s", key)
	}

	return cn, nil
}

func (c *Consumer) getAllConsumers() ([]jsConsumer, error) {
	var (
		consumers []jsConsumer
		errs      []error
	)

	c.consumers.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			errs = append(errs, fmt.Errorf("consumer: invalid consumer key: %v", k))
			return true
		}

		cn, ok := v.(jsConsumer)
		if !ok {
			errs = append(errs, fmt.Errorf("consumer: invalid consumer type: %s", key))
			return true
		}

		consumers = append(consumers, cn)
		return true
	})

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return consumers, nil
}

func (c *Consumer) setConsumer(topic, group string, cn jsConsumer) {
	key := c.buildGroupName(topic, group)
	c.consumers.Store(key, cn)
}

func (c *Consumer) buildGroupName(topic, group string) string {
	return fmt.Sprintf("%s_%s_%s", c.appName, topic, group)
}

// serverAddrPort splits the first endpoint into host and port.
func serverAddrPort(endpoints []string) (string, int) {
	if len(endpoints) == 0 {
		return "", 0
	}
	ep := endpoints[0]
	if strings.Contains(ep, "://") {
		u, err := url.Parse(ep)
		if err == nil {
			host := u.Hostname()
			port, _ := strconv.Atoi(u.Port())
			if port == 0 {
				port = 4222
			}
			return host, port
		}
	}
	host, portStr, err := net.SplitHostPort(ep)
	if err != nil {
		return ep, 4222
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 4222
	}
	return host, port
}
