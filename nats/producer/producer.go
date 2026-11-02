// Package producer publishes messages to NATS JetStream streams. A [Producer]
// owns the stream of each registered topic and sends messages synchronously,
// delayed through message schedules, or asynchronously.
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

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
	"github.com/fikrimohammad/go-servicekit/observability/metrics"
	"github.com/fikrimohammad/go-servicekit/observability/tracer"
)

// streamProducer is the JetStream publisher of one topic, on its own NATS
// connection.
type streamProducer struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// drain closes the connection after pending publishes are flushed. It closes the
// connection at once if ctx ends first.
func (p *streamProducer) drain(ctx context.Context) error {
	return wire.Drain(ctx, p.nc)
}

// close closes the connection without flushing.
func (p *streamProducer) close() {
	p.nc.Close()
}

// publish sends msg and waits for the server's ack.
func (p *streamProducer) publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
	return p.js.PublishMsg(ctx, msg)
}

// publishAsync hands msg to the transport and calls cb once the ack arrives or
// the send fails.
func (p *streamProducer) publishAsync(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error {
	ackF, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		return err
	}

	// Wait only for the ack. The message is already with the transport, so the
	// caller's ctx ending must not fail it. WithPublishAsyncTimeout bounds the wait.
	go func() {
		select {
		case ack := <-ackF.Ok():
			cb(ctx, ack, nil)
		case err := <-ackF.Err():
			cb(ctx, nil, err)
		}
	}()

	return nil
}

// dialProducer connects to the first reachable server in cfg.Endpoints and
// ensures the stream for cfg.Topic exists. It is bounded by ctx.
func dialProducer(ctx context.Context, cfg Config) (*streamProducer, error) {
	nc, err := wire.Connect(ctx, cfg.Topic, wire.Options{
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

	return &streamProducer{nc: nc, js: js}, nil
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

	patched, changed, err := wire.PatchStream(s.CachedInfo().Config, topic)
	if err != nil {
		return err // already names the stream; the caller adds the producer prefix
	}
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

// Message is one message to publish.
type Message struct {
	// Topic is the stream the message is published to.
	Topic string
	// Tag is an optional sub-subject. It is published as Topic.Tag and carried in
	// the Sdk-Msg-Tag header. It must not contain dots.
	Tag string
	// Key is an optional business key carried in the Sdk-Msg-Key header. It is
	// never used for deduplication.
	Key string
	// Body is the message payload.
	Body []byte
}

// Client is the send surface of a NATS JetStream producer, implemented by *Producer.
// It lets callers (e.g. the repository layer) depend on the contract without
// the manager lifecycle.
type Client interface {
	// PublishSync sends a message synchronously to the producer registered for msg.Topic.
	PublishSync(ctx context.Context, msg Message) error
	// PublishSyncWithDelay sends a message that becomes consumable only after delay.
	PublishSyncWithDelay(ctx context.Context, msg Message, delay time.Duration) error
	// PublishAsync sends a message asynchronously; callback receives the result or error.
	PublishAsync(ctx context.Context, msg Message,
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
	metrics metrics.Client
	tracer  tracer.Client

	mu      sync.RWMutex
	configs map[string]Config // registered configs, keyed by topic
	// run holds the state of the current Start, or nil before Start and after
	// Shutdown.
	run *run
}

// run is the state of one Start/Shutdown cycle. Each cycle has its own pending
// WaitGroup, so a callback left over from an earlier cycle cannot affect a later
// one.
type run struct {
	producers    map[string]*streamProducer // connected producers, keyed by topic
	producerMeta map[string]producerMeta
	// pending counts the async publishes whose callbacks have not returned yet.
	pending *sync.WaitGroup
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
		metrics: o.metrics,
		tracer:  o.tracer,
		configs: make(map[string]Config),
	}, nil
}

// Register validates cfg and stores it for cfg.Topic. It does not connect; Start
// does that. It returns ErrProducerExists when the topic is already registered.
func (p *Producer) Register(cfg Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.run != nil {
		return ErrAlreadyStarted
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

// PublishSync sends msg synchronously to the producer registered for msg.Topic. It
// returns once the server acknowledges the message.
func (p *Producer) PublishSync(ctx context.Context, msg Message) error {
	if strings.Contains(msg.Tag, ".") {
		return ErrInvalidTag
	}
	nm := buildMsg(msg)
	target := nm.Subject
	return p.publishSync(ctx, msg, nm, target)
}

// PublishSyncWithDelay sends msg so that it is delivered once, at delay after the
// call, using a NATS message schedule. The message goes to a schedule subject
// with the data subject in Nats-Schedule-Target. The server copies the body, the
// SDK headers, and the trace context to the delivered message. The server stores
// the delivered message under a new stream sequence, so the consumer's message id
// differs from the one on this publish span, which is the schedule message's
// sequence; correlate the two with Key or the trace. A delay of zero or less sends
// the message at once,
// as PublishSync does. The call returns once the server accepts the schedule.
func (p *Producer) PublishSyncWithDelay(ctx context.Context, msg Message, delay time.Duration) error {
	if strings.Contains(msg.Tag, ".") {
		return ErrInvalidTag
	}
	if delay <= 0 {
		return p.PublishSync(ctx, msg)
	}
	target := buildSubject(msg.Topic, msg.Tag)
	nm := buildMsg(msg)
	nm.Subject = wire.ScheduleSubject(msg.Topic)
	nm.Header.Set("Nats-Schedule", "@at "+time.Now().Add(delay).UTC().Format(time.RFC3339Nano))
	nm.Header.Set("Nats-Schedule-Target", target)
	return p.publishSync(ctx, msg, nm, target)
}

// publishSync sends nm, the wire form of msg, inside a producer span. target is
// the data subject the message is delivered to, which is nm.Subject for a plain
// publish and the schedule target for a delayed one.
func (p *Producer) publishSync(ctx context.Context, msg Message, nm *nats.Msg, target string) error {
	_, err := p.metaFor(msg.Topic).instrumentPublish(ctx, msg, target, nm,
		func(ctx context.Context) (*jetstream.PubAck, error) {
			return p.sendSync(ctx, msg.Topic, nm)
		})
	return err
}

// PublishAsync sends msg asynchronously; callback receives the result or error.
// Start must have succeeded for the topic. Shutdown waits for callback to return.
func (p *Producer) PublishAsync(ctx context.Context, msg Message,
	callback func(ctx context.Context, result *jetstream.PubAck, err error)) error {

	if strings.Contains(msg.Tag, ".") {
		return ErrInvalidTag
	}

	if callback == nil {
		callback = func(context.Context, *jetstream.PubAck, error) {}
	}

	nm := buildMsg(msg)
	return p.metaFor(msg.Topic).instrumentPublishAsync(ctx, msg, nm.Subject, nm, func(ctx context.Context, cb func(context.Context, *jetstream.PubAck, error)) error {
		pd, pending, err := p.acquire(msg.Topic)
		if err != nil {
			return err
		}

		done := func(cctx context.Context, ack *jetstream.PubAck, sendErr error) {
			defer pending.Done()
			cb(cctx, ack, sendErr)
		}
		if err := pd.publishAsync(ctx, nm, done); err != nil {
			pending.Done()
			return fmt.Errorf("producer: async publish to topic %s: %w", msg.Topic, err)
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

	if p.run != nil {
		return nil
	}

	cfgs := make([]Config, 0, len(p.configs))
	for _, cfg := range p.configs {
		cfgs = append(cfgs, cfg)
	}

	pds := make([]*streamProducer, len(cfgs))
	g, gctx := errgroup.WithContext(ctx)
	for i, cfg := range cfgs {
		g.Go(func() error {
			pd, err := dialProducer(gctx, cfg)
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
				pd.close()
			}
		}
		return err
	}

	r := &run{
		producers:    make(map[string]*streamProducer, len(cfgs)),
		producerMeta: make(map[string]producerMeta, len(cfgs)),
		pending:      &sync.WaitGroup{},
	}
	for i, cfg := range cfgs {
		r.producers[cfg.Topic] = pds[i]
		addr, port := wire.ServerAddrPort(cfg.Endpoints)
		r.producerMeta[cfg.Topic] = producerMeta{serverAddr: addr, serverPort: port}
	}
	p.run = r
	return nil
}

// Shutdown waits for pending async publish callbacks, then drains every connection.
// Both steps are bounded by ctx. If ctx ends first, Shutdown closes the connections
// without draining and returns ctx.Err(). Without a successful Start, it is a
// no-op. It is idempotent.
func (p *Producer) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	r := p.run
	if r == nil {
		p.mu.Unlock()
		return nil
	}
	p.run = nil
	p.mu.Unlock()

	// No Add can run now: acquire needs the run that was just cleared.
	pendingDone := make(chan struct{})
	go func() {
		r.pending.Wait()
		close(pendingDone)
	}()

	select {
	case <-pendingDone:
	case <-ctx.Done():
		for _, pd := range r.producers {
			pd.close()
		}
		return ctx.Err()
	}

	var errs []error
	for topic, pd := range r.producers {
		if err := pd.drain(ctx); err != nil {
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

	result, err := pd.publish(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("producer: sync send to topic %s: %w", topic, err)
	}

	return result, nil
}

// metaFor returns the telemetry metadata for the given topic.
func (p *Producer) metaFor(topic string) meta {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var pm producerMeta
	if p.run != nil {
		pm = p.run.producerMeta[topic]
	}
	return meta{topic: topic, serverAddr: pm.serverAddr, serverPort: pm.serverPort, metrics: p.metrics, tracer: p.tracer}
}

// producerFor returns the started producer for topic or ErrProducerNotFound.
func (p *Producer) producerFor(topic string) (*streamProducer, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.run != nil {
		if pd, ok := p.run.producers[topic]; ok {
			return pd, nil
		}
	}
	return nil, fmt.Errorf("%w: %s", ErrProducerNotFound, topic)
}

// acquire returns the started producer for topic and counts one pending async
// publish in the current run. The caller must call Done on the returned WaitGroup
// once the callback has returned, or right away if the publish fails.
func (p *Producer) acquire(topic string) (*streamProducer, *sync.WaitGroup, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.run != nil {
		if pd, ok := p.run.producers[topic]; ok {
			p.run.pending.Add(1)
			return pd, p.run.pending, nil
		}
	}
	return nil, nil, fmt.Errorf("%w: %s", ErrProducerNotFound, topic)
}

// buildSubject constructs the target NATS subject. If tag is provided,
// it becomes topic.tag. If tag is empty or equal to topic, topic is returned.
func buildSubject(topic, tag string) string {
	if tag == "" || tag == topic {
		return topic
	}
	return topic + "." + tag
}

// buildMsg constructs the NATS message for msg with its body and SDK headers. It
// never sets Nats-Msg-Id, so the key cannot deduplicate messages. The trace context
// is injected later, inside the producer span.
func buildMsg(msg Message) *nats.Msg {
	nm := nats.NewMsg(buildSubject(msg.Topic, msg.Tag))
	nm.Data = msg.Body

	if msg.Key != "" {
		nm.Header.Set(wire.HeaderKey, msg.Key)
	}
	if msg.Tag != "" {
		nm.Header.Set(wire.HeaderTag, msg.Tag)
	}

	return nm
}
