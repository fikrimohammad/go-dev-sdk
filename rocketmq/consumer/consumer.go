package consumer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"go.opentelemetry.io/otel"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

type mqConsumer interface {
	Start() error
	Shutdown() error
	Subscribe(topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
}

type consumerFactory func(opts ...consumer.Option) (mqConsumer, error)

func defaultConsumerFactory(opts ...consumer.Option) (mqConsumer, error) {
	return rocketmq.NewPushConsumer(opts...)
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

func (c *Consumer) Register(cfg Config, handlerFunc HandlerFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started.Load() {
		return errors.New("consumer: cannot register after Start")
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
	cn, err := c.consumerFactory(cfg.buildConsumerOptions(groupName)...)
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
	}, handlerFunc)
	if err != nil {
		return fmt.Errorf("consumer: create handler adapter: %w", err)
	}

	err = cn.Subscribe(cfg.Topic, cfg.buildMessageSelectorConfig(), consumerHandlerAdapter.Handle)
	if err != nil {
		return fmt.Errorf("consumer: subscribe %s: %w", groupName, err)
	}

	c.setConsumer(cfg.Topic, cfg.Group, cn)
	return nil
}

func (c *Consumer) Start() error {
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

func (c *Consumer) getConsumer(topic, group string) (mqConsumer, error) {
	key := c.buildGroupName(topic, group)
	consumerRaw, ok := c.consumers.Load(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrConsumerNotFound, key)
	}

	cn, ok := consumerRaw.(mqConsumer)
	if !ok {
		return nil, fmt.Errorf("consumer: invalid consumer type: %s", key)
	}

	return cn, nil
}

func (c *Consumer) getAllConsumers() ([]mqConsumer, error) {
	var (
		consumers []mqConsumer
		errs      []error
	)

	c.consumers.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			errs = append(errs, fmt.Errorf("consumer: invalid consumer key: %v", k))
			return true
		}

		cn, ok := v.(mqConsumer)
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

func (c *Consumer) setConsumer(topic, group string, cn mqConsumer) {
	key := c.buildGroupName(topic, group)
	c.consumers.Store(key, cn)
}

func (c *Consumer) buildGroupName(topic, group string) string {
	return fmt.Sprintf("%s_%s_%s", c.appName, topic, group)
}

// serverAddrPort splits the first endpoint (host:port) into its parts.
func serverAddrPort(endpoints []string) (string, int) {
	if len(endpoints) == 0 {
		return "", 0
	}
	host, portStr, err := net.SplitHostPort(endpoints[0])
	if err != nil {
		return endpoints[0], 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 0
	}
	return host, port
}
