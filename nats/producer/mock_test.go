// This file holds the transport mock for package producer tests.

package producer

import (
	"context"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/identity"
)

// testAppInfo is the service identity used across the producer tests.
var testAppInfo = identity.Info{Name: "test-app"}

// mockProducer implements jsProducer for testing. Its async publish succeeds
// unless sendAsyncErr is set, and it never calls the callback on a failed send,
// as the jsProducer contract requires.
type mockProducer struct {
	drainErr  error
	drainFunc func(ctx context.Context) error
	closes    atomic.Int32

	sendSyncErr  error
	sendAsyncErr error

	sendSyncFunc  func(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error)
	sendAsyncFunc func(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error
}

func (m *mockProducer) Drain(ctx context.Context) error {
	if m.drainFunc != nil {
		return m.drainFunc(ctx)
	}
	return m.drainErr
}

func (m *mockProducer) Close() {
	m.closes.Add(1)
}

func (m *mockProducer) Publish(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error) {
	if m.sendSyncFunc != nil {
		return m.sendSyncFunc(ctx, msg)
	}
	return &jetstream.PubAck{}, m.sendSyncErr
}

func (m *mockProducer) PublishAsync(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error {
	if m.sendAsyncFunc != nil {
		return m.sendAsyncFunc(ctx, msg, cb)
	}
	if m.sendAsyncErr != nil {
		return m.sendAsyncErr
	}
	cb(ctx, &jetstream.PubAck{}, nil)
	return nil
}
