package producer

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
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// jsProducer defines the subset of NATS JetStream publisher operations used by this package.
type jsProducer interface {
	Start() error
	Shutdown() error
	Publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error)
	PublishAsync(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error
}

// producerFactory creates a jsProducer from the given configuration.
type producerFactory func(cfg Config) (jsProducer, error)

type defaultJSProducer struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg Config
}

func (p *defaultJSProducer) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Timeout)
	defer cancel()

	_, err := p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              p.cfg.Topic,
		Subjects:          []string{p.cfg.Topic, p.cfg.Topic + ".>"},
		AllowMsgSchedules: true,
	})
	if err != nil {
		return fmt.Errorf("producer: ensure stream %s: %w", p.cfg.Topic, err)
	}
	return nil
}

func (p *defaultJSProducer) Shutdown() error {
	if p.nc != nil {
		return p.nc.Drain()
	}
	return nil
}

func (p *defaultJSProducer) Publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
	return p.js.PublishMsg(ctx, msg)
}

func (p *defaultJSProducer) PublishAsync(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error {
	ackF, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return err
	}

	go func() {
		select {
		case ack := <-ackF.Ok():
			cb(ctx, ack, nil)
		case err := <-ackF.Err():
			cb(ctx, nil, err)
		case <-ctx.Done():
			cb(ctx, nil, ctx.Err())
		}
	}()

	return nil
}

func defaultProducerFactory(cfg Config) (jsProducer, error) {
	opts := []nats.Option{
		nats.Name(cfg.Topic),
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
			return nil, fmt.Errorf("producer: nkey option: %w", err)
		}
		opts = append(opts, opt)
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredentialsFile))
	}

	endpoints := strings.Join(cfg.Endpoints, ",")
	nc, err := nats.Connect(endpoints, opts...)
	if err != nil {
		return nil, fmt.Errorf("producer: connect to %s: %w", endpoints, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("producer: create jetstream: %w", err)
	}

	return &defaultJSProducer{
		nc:  nc,
		js:  js,
		cfg: cfg,
	}, nil
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

// Client is the send surface of a NATS JetStream producer, implemented by *Producer.
// It lets callers (e.g. the repository layer) depend on the contract without
// the manager lifecycle.
type Client interface {
	// PublishSync sends a message synchronously to the producer registered for topic.
	PublishSync(ctx context.Context, topic, tag, key string, message []byte) error
	// PublishSyncWithDelay sends a message that becomes consumable only after delay.
	PublishSyncWithDelay(ctx context.Context, topic, tag, key string, message []byte, delay time.Duration) error
	// PublishAsync sends a message asynchronously; callback receives the result or error.
	PublishAsync(ctx context.Context, topic, tag, key string, message []byte,
		callback func(ctx context.Context, result *jetstream.PubAck, err error)) error
}

// Producer owns a set of NATS JetStream producers keyed by topic and coordinates
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
	producers       sync.Map // map[string]jsProducer
	producerMeta    sync.Map // map[string]producerMeta, server address for telemetry
	started         atomic.Bool
	producerFactory producerFactory
	mu              sync.Mutex

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
	if strings.Contains(info.Name, ".") {
		return nil, ErrInvalidAppName
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
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started.Load() {
		return errors.New("producer: cannot register after Start")
	}

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

	pd, err := p.producerFactory(cfg)
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
	if strings.Contains(tag, ".") {
		return ErrInvalidTag
	}
	msg := p.buildMsg(ctx, topic, tag, key, message)
	_, err := p.metaFor(topic).instrumentPublish(ctx, topic, tag, key, msg.Subject, len(message), func(ctx context.Context) (*jetstream.PubAck, error) {
		return p.sendSync(ctx, topic, msg)
	})
	return err
}

// PublishSyncWithDelay sends a message that becomes consumable only after delay using NATS 2.12+ message schedule.
func (p *Producer) PublishSyncWithDelay(ctx context.Context, topic, tag, key string, message []byte, delay time.Duration) error {
	if strings.Contains(tag, ".") {
		return ErrInvalidTag
	}
	if delay <= 0 {
		return p.PublishSync(ctx, topic, tag, key, message)
	}
	msg := p.buildMsg(ctx, topic, tag, key, message)
	targetSubject := msg.Subject
	scheduleAt := time.Now().Add(delay).UTC().Format(time.RFC3339)
	msg.Header.Set("Nats-Schedule", "@at "+scheduleAt)
	msg.Header.Set("Nats-Schedule-Target", targetSubject)

	_, err := p.metaFor(topic).instrumentPublish(ctx, topic, tag, key, targetSubject, len(message), func(ctx context.Context) (*jetstream.PubAck, error) {
		return p.sendSync(ctx, topic, msg)
	})
	return err
}

// PublishAsync sends a message asynchronously; callback receives the result or error.
func (p *Producer) PublishAsync(ctx context.Context, topic, tag, key string, message []byte,
	callback func(ctx context.Context, result *jetstream.PubAck, err error)) error {

	if strings.Contains(tag, ".") {
		return ErrInvalidTag
	}

	if callback == nil {
		callback = func(context.Context, *jetstream.PubAck, error) {}
	}

	msg := p.buildMsg(ctx, topic, tag, key, message)
	targetSubject := msg.Subject
	return p.metaFor(topic).instrumentPublishAsync(ctx, topic, tag, key, targetSubject, len(message), func(ctx context.Context, cb func(context.Context, *jetstream.PubAck, error)) error {
		pd, err := p.getProducer(topic)
		if err != nil {
			return err
		}

		if err := pd.PublishAsync(ctx, msg, cb); err != nil {
			return fmt.Errorf("producer: async publish to topic %s: %w", topic, err)
		}

		return nil
	}, callback)
}

// Start starts every registered producer. It is idempotent; calling it
// multiple times after the first successful call is a no-op.
func (p *Producer) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started.CompareAndSwap(false, true) {
		return nil
	}

	producers, err := p.getAllProducers()
	if err != nil {
		p.started.Store(false)
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

	if err := errGroup.Wait(); err != nil {
		p.started.Store(false)
		return err
	}

	return nil
}

// Shutdown shuts every registered producer down. It is idempotent; calling it
// multiple times after the first successful call is a no-op.
func (p *Producer) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
func (p *Producer) sendSync(ctx context.Context, topic string, msg *nats.Msg) (*jetstream.PubAck, error) {
	pd, err := p.getProducer(topic)
	if err != nil {
		return nil, err
	}

	result, err := pd.Publish(ctx, msg)
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
func (p *Producer) getProducer(topic string) (jsProducer, error) {
	pRaw, ok := p.producers.Load(topic)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrProducerNotFound, topic)
	}

	pd, ok := pRaw.(jsProducer)
	if !ok {
		return nil, fmt.Errorf("producer: invalid producer type: %s", topic)
	}

	return pd, nil
}

func (p *Producer) getAllProducers() (map[string]jsProducer, error) {
	var (
		result = make(map[string]jsProducer)
		errs   []error
	)

	p.producers.Range(func(key, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			errs = append(errs, fmt.Errorf("producer: invalid producer key type: %s", key))
			return true
		}

		pd, ok := value.(jsProducer)
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

func (p *Producer) setProducer(topic string, pd jsProducer) {
	p.producers.Store(topic, pd)
}

// buildSubject constructs the target NATS subject. If tag is provided,
// it becomes topic.tag. If tag is empty or equal to topic, topic is returned.
func buildSubject(topic, tag string) string {
	if tag == "" || tag == topic {
		return topic
	}
	return topic + "." + tag
}

// buildMsg constructs a NATS message with subject, data, headers, and injected trace context.
func (p *Producer) buildMsg(ctx context.Context, topic, tag, key string, body []byte) *nats.Msg {
	subject := buildSubject(topic, tag)
	msg := nats.NewMsg(subject)
	msg.Data = body

	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}

	if key != "" {
		msg.Header.Set("Nats-Msg-Id", key)
	}
	if tag != "" {
		msg.Header.Set("Nats-Msg-Tag", tag)
	}

	injectTraceContext(ctx, msg)

	return msg
}

// serverAddrPort splits the first endpoint (host:port or nats://host:port) into its parts.
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
