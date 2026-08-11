package consumer

import (
	"context"
	"errors"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// HandlerFunc processes the body of a single consumed message. Returning an
// error asks the broker to redeliver the message (ConsumeRetryLater).
type HandlerFunc func(ctx context.Context, msgBody []byte) error

// handlerAdapter adapts a HandlerFunc to the RocketMQ consume callback and
// routes every message through the instrumentation middleware chain
// (panic recovery → tracer → logger → metrics).
type handlerAdapter struct {
	meta        meta
	handlerFunc HandlerFunc
}

func newHandlerAdapter(m meta, handlerFunc HandlerFunc) (*handlerAdapter, error) {
	if handlerFunc == nil {
		return nil, errors.New("handler function is nil")
	}

	return &handlerAdapter{
		meta:        m,
		handlerFunc: handlerFunc,
	}, nil
}

func (h *handlerAdapter) Handle(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	var errs []error
	for _, msg := range msgs {
		if err := h.consumeOne(ctx, msg); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return consumer.ConsumeRetryLater, errors.Join(errs...)
	}
	return consumer.ConsumeSuccess, nil
}

// consumeOne routes a single message through the middleware chain: the user
// handler is wrapped by metrics, then logger, then tracer, then panic
// recovery, mirroring the apiserver middleware order.
func (h *handlerAdapter) consumeOne(ctx context.Context, msg *primitive.MessageExt) error {
	base := func(ctx context.Context, m *primitive.MessageExt) error {
		return h.handlerFunc(ctx, m.Body)
	}

	chain := h.meta.panicRecovery(h.meta.tracerMW(h.meta.logger(h.meta.metricsMW(base))))
	return chain(ctx, msg)
}
