package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/nats/internal/wire"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
)

// settleErrorEvent is the span event added when an ack, nak, term, or InProgress
// call fails.
const settleErrorEvent = "messaging.settle.error"

// HandlerFunc processes one delivered message. Its error decides the outcome: nil
// acks the message, an error matching ErrNoRetry terms it, and any other error
// naks it for redelivery after Config.RetryBackoff. ctx is canceled when
// Config.HandlerTimeout passes or when Shutdown starts.
type HandlerFunc func(ctx context.Context, msg Message) error

// message is the Message handed to handlers. newMessage parses it once, so the
// middleware and the handler read the same values.
type message struct {
	raw           jetstream.Msg
	key           string
	tag           string
	id            string
	subject       string
	body          []byte
	sequence      uint64
	deliveryCount uint64
	timestamp     time.Time
}

// Key returns the business key set by the producer, or "".
func (m *message) Key() string { return m.key }

// Tag returns the producer's tag, or "".
func (m *message) Tag() string { return m.tag }

// Subject returns the NATS subject the message was published to.
func (m *message) Subject() string { return m.subject }

// Body returns the message payload.
func (m *message) Body() []byte { return m.body }

// Sequence returns the stream sequence.
func (m *message) Sequence() uint64 { return m.sequence }

// DeliveryCount returns 1 on first delivery and grows on each redelivery.
func (m *message) DeliveryCount() uint64 { return m.deliveryCount }

// Timestamp returns when the message was stored in the stream.
func (m *message) Timestamp() time.Time { return m.timestamp }

// newMessage parses the headers, subject, and JetStream metadata of raw. The tag
// falls back to the subject suffix after "<topic>.". It fails when raw has no
// JetStream metadata.
func newMessage(raw jetstream.Msg, topic string) (*message, error) {
	md, err := raw.Metadata()
	if err != nil {
		return nil, fmt.Errorf("consumer: message metadata: %w", err)
	}
	if md == nil {
		return nil, errors.New("consumer: message has no metadata")
	}

	headers := raw.Headers()
	subject := raw.Subject()
	tag := headers.Get(wire.HeaderTag)
	if after, ok := strings.CutPrefix(subject, topic+"."); tag == "" && ok {
		tag = after
	}

	return &message{
		raw:           raw,
		key:           headers.Get(wire.HeaderKey),
		tag:           tag,
		id:            strconv.FormatUint(md.Sequence.Stream, 10),
		subject:       subject,
		body:          raw.Data(),
		sequence:      md.Sequence.Stream,
		deliveryCount: md.NumDelivered,
		timestamp:     md.Timestamp,
	}, nil
}

// handlerAdapter runs a HandlerFunc for each delivered message. It routes every
// message through the middleware chain and settles it from the handler's result.
type handlerAdapter struct {
	meta           meta
	handlerFunc    HandlerFunc
	handlerTimeout time.Duration
	heartbeatEvery time.Duration
	chain          consumeHandler
}

// newHandlerAdapter returns an adapter for handlerFunc. cfg must already have its
// defaults applied.
func newHandlerAdapter(m meta, cfg Config, handlerFunc HandlerFunc) (*handlerAdapter, error) {
	if handlerFunc == nil {
		return nil, errors.New("handler function is nil")
	}

	m.retryBackoff = cfg.RetryBackoff
	m.base = m.baseAttrs()
	h := &handlerAdapter{
		meta:           m,
		handlerFunc:    handlerFunc,
		handlerTimeout: cfg.HandlerTimeout,
		heartbeatEvery: cfg.Timeout / 2,
	}
	// The tracer is outermost, so settling happens inside the process span. Panic
	// recovery is innermost, so a panic is traced, logged, and measured.
	h.chain = m.tracerMW(m.logger(m.metricsMW(m.panicRecovery(h.call))))
	return h, nil
}

// HandleMsg runs one delivered message through the middleware chain, which also
// settles it. It returns the handler's error. A message without JetStream
// metadata is nak'd, and the handler is not called.
func (h *handlerAdapter) HandleMsg(ctx context.Context, raw jetstream.Msg) error {
	msg, err := newMessage(raw, h.meta.topic)
	if err != nil {
		logs.Warn(ctx, "message without JetStream metadata",
			"topic", h.meta.topic,
			"group", h.meta.group,
			"subject", raw.Subject(),
		)
		h.meta.settle(ctx, 0, "nak", raw.Nak)
		return err
	}
	// Once the run context is canceled (Shutdown has begun), the message goes back to
	// the server at once, without the retry backoff, and the handler does not run.
	if err := ctx.Err(); err != nil {
		h.meta.settle(ctx, msg.sequence, "nak", raw.Nak)
		return err
	}
	return h.chain(ctx, msg)
}

// call runs the handler under the HandlerTimeout deadline. While it runs, a
// heartbeat keeps the message from being redelivered.
func (h *handlerAdapter) call(ctx context.Context, msg *message) error {
	ctx, cancel := context.WithTimeout(ctx, h.handlerTimeout)
	defer cancel()

	stop := h.startHeartbeat(ctx, msg)
	defer stop()
	return h.handlerFunc(ctx, msg)
}

// startHeartbeat sends InProgress every heartbeatEvery until the returned stop
// is called. stop waits for the last heartbeat, so the settle step never races it.
func (h *handlerAdapter) startHeartbeat(ctx context.Context, msg *message) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	// time.NewTicker panics on a non-positive interval, so no heartbeat runs then.
	if h.heartbeatEvery > 0 {
		wg.Go(func() {
			ticker := time.NewTicker(h.heartbeatEvery)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					// The handler deadline has passed, so stop renewing the ack and let
					// AckWait redeliver the message.
					return
				case <-ticker.C:
					h.meta.settle(ctx, msg.sequence, "in_progress", msg.raw.InProgress)
				}
			}
		})
	}
	return func() {
		close(done)
		wg.Wait()
	}
}

// settleOutcome acknowledges msg from the handler's error: nil acks, an error
// matching ErrNoRetry terms, and any other error naks, with RetryBackoff when it
// is set.
func (m meta) settleOutcome(ctx context.Context, msg *message, err error) {
	switch {
	case err == nil:
		m.settle(ctx, msg.sequence, "ack", msg.raw.Ack)
	case errors.Is(err, ErrNoRetry):
		m.settle(ctx, msg.sequence, "term", msg.raw.Term)
	case m.retryBackoff > 0:
		m.settle(ctx, msg.sequence, "nak", func() error { return msg.raw.NakWithDelay(m.retryBackoff) })
	default:
		m.settle(ctx, msg.sequence, "nak", msg.raw.Nak)
	}
}

// settle runs one settle call: ack, nak, term, or InProgress. A failure is logged
// at Warn and added to the span in ctx as a messaging.settle.error event. It is
// not returned, because the JetStream callback has no caller to receive it.
func (m meta) settle(ctx context.Context, sequence uint64, op string, fn func() error) {
	err := fn()
	if err == nil {
		return
	}

	logs.Warn(ctx, "message settle failed",
		"operation", op,
		"topic", m.topic,
		"group", m.group,
		"sequence", sequence,
		"error", err.Error(),
	)
	trace.SpanFromContext(ctx).AddEvent(settleErrorEvent,
		trace.WithAttributes(attribute.String("error.message", err.Error())))
}
