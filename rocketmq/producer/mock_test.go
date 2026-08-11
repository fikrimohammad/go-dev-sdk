package producer

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

// testAppInfo is the service identity used across the producer tests.
var testAppInfo = appinfo.Info{Name: "test-app"}

// mockProducer implements mqProducer for testing.
type mockProducer struct {
	startErr     error
	shutdownErr  error
	sendSyncErr  error
	sendAsyncErr error

	startFunc     func() error
	shutdownFunc  func() error
	sendSyncFunc  func(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
	sendAsyncFunc func(ctx context.Context, callback func(ctx context.Context, result *primitive.SendResult, err error), msg ...*primitive.Message) error
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

func (m *mockProducer) SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error) {
	if m.sendSyncFunc != nil {
		return m.sendSyncFunc(ctx, mq...)
	}
	return &primitive.SendResult{}, m.sendSyncErr
}

func (m *mockProducer) SendAsync(ctx context.Context, callback func(ctx context.Context, result *primitive.SendResult, err error), msg ...*primitive.Message) error {
	if m.sendAsyncFunc != nil {
		return m.sendAsyncFunc(ctx, callback, msg...)
	}
	return m.sendAsyncErr
}
