package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// HandlerFunc processes the body of a single consumed message. Returning an
// error asks the broker to redeliver the message (via NAK).
type HandlerFunc func(ctx context.Context, msgBody []byte) error

// handlerAdapter adapts a HandlerFunc to the NATS JetStream consume callback and
// routes every message through the instrumentation middleware chain
// (panic recovery → tracer → logger → metrics).
type handlerAdapter struct {
	meta         meta
	retryBackoff time.Duration
	handlerFunc  HandlerFunc
	chain        consumeHandler
}

func newHandlerAdapter(m meta, retryBackoff time.Duration, handlerFunc HandlerFunc) (*handlerAdapter, error) {
	if handlerFunc == nil {
		return nil, errors.New("handler function is nil")
	}

	base := func(ctx context.Context, msg jetstream.Msg) error {
		var data []byte
		if msg != nil {
			data = msg.Data()
		}
		return handlerFunc(ctx, data)
	}

	chain := m.panicRecovery(m.tracerMW(m.logger(m.metricsMW(base))))

	return &handlerAdapter{
		meta:         m,
		retryBackoff: retryBackoff,
		handlerFunc:  handlerFunc,
		chain:        chain,
	}, nil
}

// Handle satisfies the jetstream.MessageHandler callback signature.
func (h *handlerAdapter) Handle(msg jetstream.Msg) {
	_ = h.HandleMsg(context.Background(), msg)
}

// HandleMsg processes a single message through the middleware chain and
// acknowledges (Ack) or negative-acknowledges (Nak) the message.
func (h *handlerAdapter) HandleMsg(ctx context.Context, msg jetstream.Msg) error {
	err := h.consumeOne(ctx, msg)
	if err != nil {
		if h.retryBackoff > 0 {
			_ = msg.NakWithDelay(h.retryBackoff)
		} else {
			_ = msg.Nak()
		}
		return err
	}

	_ = msg.Ack()
	return nil
}

// consumeOne routes a single message through the middleware chain: the user
// handler is wrapped by metrics, then logger, then tracer, then panic
// recovery, mirroring the apiserver middleware order.
func (h *handlerAdapter) consumeOne(ctx context.Context, msg jetstream.Msg) error {
	return h.chain(ctx, msg)
}
