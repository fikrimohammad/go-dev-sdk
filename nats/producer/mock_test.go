package producer

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

// testAppInfo is the service identity used across the producer tests.
var testAppInfo = appinfo.Info{Name: "test-app"}

// mockProducer implements jsProducer for testing.
type mockProducer struct {
	startErr     error
	shutdownErr  error
	sendSyncErr  error
	sendAsyncErr error

	startFunc     func() error
	shutdownFunc  func() error
	sendSyncFunc  func(ctx context.Context, msg *nats.Msg) (*jetstream.PubAck, error)
	sendAsyncFunc func(ctx context.Context, msg *nats.Msg, cb func(ctx context.Context, ack *jetstream.PubAck, err error)) error
}

func (m *mockProducer) Start() error {
	if m.startFunc != nil {
		return m.startFunc()
	}
	return m.startErr
}

func (m *mockProducer) Shutdown() error {
	if m.shutdownFunc != nil {
		return m.shutdownFunc()
	}
	return m.shutdownErr
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
	if cb != nil {
		cb(ctx, &jetstream.PubAck{}, m.sendAsyncErr)
	}
	return m.sendAsyncErr
}
