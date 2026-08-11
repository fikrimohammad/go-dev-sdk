package producer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"golang.org/x/sync/errgroup"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// mqProducer defines the subset of rocketmq.Producer used by this package.
type mqProducer interface {
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
	SendAsync(ctx context.Context, mq func(ctx context.Context, result *primitive.SendResult, err error),
		msg ...*primitive.Message) error
}

// producerFactory creates an mqProducer from the given options.
type producerFactory func(opts ...producer.Option) (mqProducer, error)

// defaultProducerFactory returns the real rocketmq.NewProducer wrapped to
// satisfy mqProducer (rocketmq.Producer is a superset of mqProducer).
func defaultProducerFactory(opts ...producer.Option) (mqProducer, error) {
	return rocketmq.NewProducer(opts...)
}

// Option configures a Producer returned by New.
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented sends. A nil client
// falls back to the package-level default (metrics.SetDefault).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented sends. A nil client
// falls back to the package-level default (tracer.SetDefault).
func WithTracer(t tracer.Client) Option {
	return func(o *options) { o.tracer = t }
}

// Client is the send surface of a RocketMQ producer, implemented by *Producer.
// It lets callers (e.g. the repository layer) depend on the contract without
// the manager lifecycle.
type Client interface {
	// PublishSync sends a message synchronously to the producer registered for topic.
	PublishSync(ctx context.Context, topic, tag, key string, message []byte) error
	// PublishSyncWithDelay sends a message that becomes consumable only after delay.
	PublishSyncWithDelay(ctx context.Context, topic, tag, key string, message []byte, delay time.Duration) error
	// PublishAsync sends a message asynchronously; callback receives the result or error.
	PublishAsync(ctx context.Context, topic, tag, key string, message []byte,
		callback func(ctx context.Context, result *primitive.SendResult, err error)) error
}

// Producer owns a set of RocketMQ producers keyed by topic and coordinates
// their lifecycle. It is safe for concurrent use.
//
// Usage lifecycle:
//  1. Create with New()
//  2. Register topics with Register()
//  3. Call Start() to start all registered producers
//  4. Use PublishSync/PublishAsync to send messages
//  5. Call Shutdown() when done
type Producer struct {
	appName         string
	producers       sync.Map // map[string]mqProducer
	producerMeta    sync.Map // map[string]producerMeta, server address for telemetry
	started         atomic.Bool
	producerFactory producerFactory

	metrics metrics.Client
	tracer  tracer.Client
}

var _ Client = (*Producer)(nil)

// New returns an empty, ready-to-use Producer. The service identity is taken
// from info (its Name seeds the producer group names). Metrics and tracing use
// the package-level defaults unless overridden via WithMetrics/WithTracer.
func New(info appinfo.Info, opts ...Option) (*Producer, error) {
	if info.Name == "" {
		return nil, errors.New("appinfo name is empty")
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}

	return &Producer{
		appName:         info.Name,
		producers:       sync.Map{},
		producerMeta:    sync.Map{},
		producerFactory: defaultProducerFactory,
		metrics:         o.metrics,
		tracer:          o.tracer,
	}, nil
}

// Register creates and stores a producer for cfg.Topic.
func (p *Producer) Register(cfg Config) error {
	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}

	pr, err := p.getProducer(cfg.Topic)
	if err != nil && !errors.Is(err, ErrProducerNotFound) {
		return err
	}

	if pr != nil {
		return fmt.Errorf("%w: %s", ErrProducerExists, cfg.Topic)
	}

	pd, err := p.producerFactory(
		producer.WithGroupName(p.buildGroupName(cfg.Topic)),
		producer.WithNsResolver(primitive.NewPassthroughResolver(cfg.Endpoints)),
		producer.WithSendMsgTimeout(cfg.Timeout),
		producer.WithRetry(cfg.MaxRetryAttempts),
	)
	if err != nil {
		return fmt.Errorf("producer: create for topic %s: %w", cfg.Topic, err)
	}

	addr, port := serverAddrPort(cfg.Endpoints)
	p.setProducer(cfg.Topic, pd)
	p.producerMeta.Store(cfg.Topic, producerMeta{serverAddr: addr, serverPort: port})
	return nil
}

// PublishSync sends a message synchronously to the producer registered for topic.
func (p *Producer) PublishSync(ctx context.Context, topic, tag, key string, message []byte) error {
	msg := p.buildMessage(ctx, topic, tag, key, message)
	_, err := p.metaFor(topic).instrumentPublish(ctx, topic, tag, key, len(message), func(ctx context.Context) (*primitive.SendResult, error) {
		return p.sendSync(ctx, topic, msg)
	})
	return err
}

// PublishSyncWithDelay sends a message that becomes consumable only after delay.
func (p *Producer) PublishSyncWithDelay(ctx context.Context, topic, tag, key string, message []byte, delay time.Duration) error {
	msg := p.buildMessage(ctx, topic, tag, key, message).WithDelayTimestamp(time.Now().Add(delay))
	_, err := p.metaFor(topic).instrumentPublish(ctx, topic, tag, key, len(message), func(ctx context.Context) (*primitive.SendResult, error) {
		return p.sendSync(ctx, topic, msg)
	})
	return err
}

// PublishAsync sends a message asynchronously; callback receives the result or error.
func (p *Producer) PublishAsync(ctx context.Context, topic, tag, key string, message []byte,
	callback func(ctx context.Context, result *primitive.SendResult, err error)) error {

	if callback == nil {
		callback = func(context.Context, *primitive.SendResult, error) {}
	}

	msg := p.buildMessage(ctx, topic, tag, key, message)
	return p.metaFor(topic).instrumentPublishAsync(ctx, topic, tag, key, len(message), func(ctx context.Context, cb func(context.Context, *primitive.SendResult, error)) error {
		pd, err := p.getProducer(topic)
		if err != nil {
			return err
		}

		if err := pd.SendAsync(ctx, cb, msg); err != nil {
			return fmt.Errorf("producer: async send to topic %s: %w", topic, err)
		}

		return nil
	}, callback)
}

// Start starts every registered producer. It is idempotent; calling it
// multiple times after the first successful call is a no-op.
func (p *Producer) Start() error {
	if !p.started.CompareAndSwap(false, true) {
		return nil
	}

	producers, err := p.getAllProducers()
	if err != nil {
		return err
	}

	var errGroup errgroup.Group
	for topic, pd := range producers {
		errGroup.Go(func() error {
			if pErr := pd.Start(); pErr != nil {
				return fmt.Errorf("start topic %s: %w", topic, pErr)
			}
			return nil
		})
	}

	return errGroup.Wait()
}

// Shutdown shuts every registered producer down. It is idempotent; calling it
// multiple times after the first successful call is a no-op.
func (p *Producer) Shutdown() error {
	if !p.started.CompareAndSwap(true, false) {
		return nil
	}

	producers, err := p.getAllProducers()
	if err != nil {
		return err
	}

	var errGroup errgroup.Group
	for topic, pd := range producers {
		errGroup.Go(func() error {
			if pErr := pd.Shutdown(); pErr != nil {
				return fmt.Errorf("shutdown topic %s: %w", topic, pErr)
			}
			return nil
		})
	}

	return errGroup.Wait()
}

// sendSync resolves the producer and performs a synchronous send.
func (p *Producer) sendSync(ctx context.Context, topic string, msg *primitive.Message) (*primitive.SendResult, error) {
	pd, err := p.getProducer(topic)
	if err != nil {
		return nil, err
	}

	result, err := pd.SendSync(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("producer: sync send to topic %s: %w", topic, err)
	}

	return result, nil
}

// metaFor returns the telemetry metadata for the given topic.
func (p *Producer) metaFor(topic string) meta {
	m := meta{metrics: p.metrics, tracer: p.tracer}
	if raw, ok := p.producerMeta.Load(topic); ok {
		if pm, ok := raw.(producerMeta); ok {
			m.serverAddr, m.serverPort = pm.serverAddr, pm.serverPort
		}
	}
	return m
}

// getProducer returns the producer for topic or ErrProducerNotFound.
func (p *Producer) getProducer(topic string) (mqProducer, error) {
	pRaw, ok := p.producers.Load(topic)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrProducerNotFound, topic)
	}

	pd, ok := pRaw.(mqProducer)
	if !ok {
		return nil, fmt.Errorf("producer: invalid producer type: %s", topic)
	}

	return pd, nil
}

func (p *Producer) getAllProducers() (map[string]mqProducer, error) {
	var (
		result = make(map[string]mqProducer)
		errs   []error
	)

	p.producers.Range(func(key, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			errs = append(errs, fmt.Errorf("producer: invalid producer key type: %s", key))
			return true
		}

		pd, ok := value.(mqProducer)
		if !ok {
			errs = append(errs, fmt.Errorf("producer: invalid producer type: %s", key))
			return true
		}

		result[k] = pd
		return true
	})

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return result, nil
}

func (p *Producer) setProducer(topic string, pd mqProducer) {
	p.producers.Store(topic, pd)
}

// buildMessage constructs a message with a tag, a single key, and the W3C
// trace context injected into its properties so consumers can continue the
// trace.
func (p *Producer) buildMessage(ctx context.Context, topic, tag, key string, body []byte) *primitive.Message {
	msg := primitive.NewMessage(topic, body)
	msg.WithProperties(map[string]string{
		primitive.PropertyProducerGroup: p.buildGroupName(topic),
	})

	if tag != "" {
		msg.WithTag(tag)
	}

	if key != "" {
		msg.WithKeys([]string{key})
	}

	injectTraceContext(ctx, msg)

	return msg
}

func (p *Producer) buildGroupName(topic string) string {
	return fmt.Sprintf("%s::%s", p.appName, topic)
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
