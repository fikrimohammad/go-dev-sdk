package producer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/nats/internal/wire"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
)

// jsProducer defines the subset of NATS JetStream publisher operations used by this package.
type jsProducer interface {
	Publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error)
	PublishAsync(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error
	// Drain closes the connection after pending publishes are flushed. It closes
	// the connection at once if ctx ends first.
	Drain(ctx context.Context) error
	// Close closes the connection without flushing.
	Close()
}

// producerFactory connects to the server for cfg and ensures the stream exists. It
// is bounded by ctx.
type producerFactory func(ctx context.Context, cfg Config) (jsProducer, error)

type defaultJSProducer struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func (p *defaultJSProducer) Drain(ctx context.Context) error {
	if p.nc.IsClosed() {
		return nil
	}

	closed := make(chan struct{})
	p.nc.SetClosedHandler(func(*nats.Conn) { close(closed) })
	if err := p.nc.Drain(); err != nil {
		p.nc.Close()
		return fmt.Errorf("producer: drain: %w", err)
	}

	select {
	case <-closed:
		return nil
	case <-ctx.Done():
		p.nc.Close()
		return ctx.Err()
	}
}

func (p *defaultJSProducer) Close() {
	p.nc.Close()
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

// defaultProducerFactory connects to the first reachable server in cfg.Endpoints
// and ensures the stream for cfg.Topic exists.
func defaultProducerFactory(ctx context.Context, cfg Config) (jsProducer, error) {
	nc, err := wire.Connect(cfg.Topic, wire.Options{
		Endpoints:       cfg.Endpoints,
		Timeout:         cfg.Timeout,
		Token:           cfg.Token,
		Username:        cfg.Username,
		Password:        cfg.Password,
		NKey:            cfg.NKey,
		CredentialsFile: cfg.CredentialsFile,
	})
	if err != nil {
		return nil, fmt.Errorf("producer: %w", err)
	}

	// An async publish whose ack never arrives fails after cfg.Timeout instead
	// of waiting until the connection closes.
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncTimeout(cfg.Timeout))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("producer: create jetstream: %w", err)
	}

	if err := ensureStream(ctx, js, cfg.Topic); err != nil {
		nc.Close()
		return nil, fmt.Errorf("producer: ensure stream %s: %w", cfg.Topic, err)
	}

	return &defaultJSProducer{nc: nc, js: js}, nil
}

// ensureStream creates the stream for topic when it is missing. An existing stream
// keeps its operator settings and gets only the SDK subjects, and it is updated
// only when those subjects are missing.
func ensureStream(ctx context.Context, js jetstream.JetStream, topic string) error {
	s, err := js.Stream(ctx, topic)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		_, err = js.CreateStream(ctx, wire.StreamConfig(topic))
		return err
	}
	if err != nil {
		return err
	}

	patched, changed := wire.PatchStream(s.CachedInfo().Config, topic)
	if !changed {
		return nil
	}
	_, err = js.UpdateStream(ctx, patched)
	return err
}

// Option configures a Producer returned by New.
type Option func(*options)

type options struct {
	metrics metrics.Client
	tracer  tracer.Client
}

// WithMetrics injects a metrics client for the instrumented sends. A nil client
// falls back to the OTel global meter provider (otel.SetMeterProvider).
func WithMetrics(m metrics.Client) Option {
	return func(o *options) { o.metrics = m }
}

// WithTracer injects a tracer client for the instrumented sends. A nil client
// falls back to the OTel global tracer provider (otel.SetTracerProvider).
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
//  2. Register topics with Register(), which validates and stores the config only
//  3. Call Start(ctx) to connect and ensure the stream for every registered topic
//  4. Use PublishSync/PublishAsync to send messages
//  5. Call Shutdown(ctx) when done
type Producer struct {
	appName         string
	producerFactory producerFactory

	metrics metrics.Client
	tracer  tracer.Client

	// pending counts the async publishes whose callbacks have not returned yet.
	pending sync.WaitGroup

	mu           sync.RWMutex
	started      bool
	configs      map[string]Config     // registered configs, keyed by topic
	producers    map[string]jsProducer // connected producers, set by Start
	producerMeta map[string]producerMeta
}

var _ Client = (*Producer)(nil)

// New returns an empty, ready-to-use Producer. The service identity is taken
// from info (its Name seeds the producer group names). A missing service name
// is rejected with identity.ErrName; a name containing dots is rejected with
// ErrInvalidAppName because NATS uses dots as subject separators. Metrics and
// tracing go through the OTel global providers (via metrics.Default and tracer.Default)
// unless overridden via WithMetrics/WithTracer.
func New(info identity.Info, opts ...Option) (*Producer, error) {
	if err := info.Validate(); err != nil {
		return nil, fmt.Errorf("producer: %w", err)
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
		producerFactory: defaultProducerFactory,
		metrics:         o.metrics,
		tracer:          o.tracer,
		configs:         make(map[string]Config),
	}, nil
}

// Register validates cfg and stores it for cfg.Topic. It does not connect; Start
// does that. It returns ErrProducerExists when the topic is already registered.
func (p *Producer) Register(cfg Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return errors.New("producer: cannot register after Start")
	}

	cfg = cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}

	if _, ok := p.configs[cfg.Topic]; ok {
		return fmt.Errorf("%w: %s", ErrProducerExists, cfg.Topic)
	}

	p.configs[cfg.Topic] = cfg
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
// Start must have succeeded for the topic. Shutdown waits for callback to return.
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
		pd, err := p.acquire(topic)
		if err != nil {
			return err
		}

		done := func(cctx context.Context, ack *jetstream.PubAck, sendErr error) {
			defer p.pending.Done()
			cb(cctx, ack, sendErr)
		}
		if err := pd.PublishAsync(ctx, msg, done); err != nil {
			p.pending.Done()
			return fmt.Errorf("producer: async publish to topic %s: %w", topic, err)
		}

		return nil
	}, callback)
}

// Start connects a producer for every registered topic and ensures each stream
// exists. The work is bounded by ctx. If any topic fails, Start closes the
// connections it opened and returns the error, and a later Start may retry. It is
// idempotent: once started, further calls return nil.
func (p *Producer) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return nil
	}

	cfgs := make([]Config, 0, len(p.configs))
	for _, cfg := range p.configs {
		cfgs = append(cfgs, cfg)
	}

	pds := make([]jsProducer, len(cfgs))
	g, gctx := errgroup.WithContext(ctx)
	for i, cfg := range cfgs {
		g.Go(func() error {
			pd, err := p.producerFactory(gctx, cfg)
			if err != nil {
				return fmt.Errorf("producer: start topic %s: %w", cfg.Topic, err)
			}
			pds[i] = pd
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		for _, pd := range pds {
			if pd != nil {
				pd.Close()
			}
		}
		return err
	}

	p.producers = make(map[string]jsProducer, len(cfgs))
	p.producerMeta = make(map[string]producerMeta, len(cfgs))
	for i, cfg := range cfgs {
		p.producers[cfg.Topic] = pds[i]
		addr, port := wire.ServerAddrPort(cfg.Endpoints)
		p.producerMeta[cfg.Topic] = producerMeta{serverAddr: addr, serverPort: port}
	}
	p.started = true
	return nil
}

// Shutdown waits for pending async publish callbacks, then drains every connection.
// Both steps are bounded by ctx. If ctx ends first, Shutdown closes the connections
// without draining and returns ctx.Err(). Without a successful Start, it is a
// no-op. It is idempotent.
func (p *Producer) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = false
	producers := p.producers
	p.producers = nil
	p.mu.Unlock()

	// No Add can run now: acquire needs the producers map that was just cleared.
	pendingDone := make(chan struct{})
	go func() {
		p.pending.Wait()
		close(pendingDone)
	}()

	select {
	case <-pendingDone:
	case <-ctx.Done():
		for _, pd := range producers {
			pd.Close()
		}
		return ctx.Err()
	}

	var errs []error
	for topic, pd := range producers {
		if err := pd.Drain(ctx); err != nil {
			errs = append(errs, fmt.Errorf("producer: shutdown topic %s: %w", topic, err))
		}
	}
	return errors.Join(errs...)
}

// sendSync resolves the producer and performs a synchronous send.
func (p *Producer) sendSync(ctx context.Context, topic string, msg *nats.Msg) (*jetstream.PubAck, error) {
	pd, err := p.producerFor(topic)
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
	p.mu.RLock()
	defer p.mu.RUnlock()

	pm := p.producerMeta[topic]
	return meta{serverAddr: pm.serverAddr, serverPort: pm.serverPort, metrics: p.metrics, tracer: p.tracer}
}

// producerFor returns the started producer for topic or ErrProducerNotFound.
func (p *Producer) producerFor(topic string) (jsProducer, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pd, ok := p.producers[topic]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrProducerNotFound, topic)
	}
	return pd, nil
}

// acquire returns the started producer for topic and counts one pending async
// publish. The caller must call p.pending.Done once the callback has returned, or
// right away if the publish fails.
func (p *Producer) acquire(topic string) (jsProducer, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	pd, ok := p.producers[topic]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrProducerNotFound, topic)
	}
	p.pending.Add(1)
	return pd, nil
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
