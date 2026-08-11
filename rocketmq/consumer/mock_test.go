package consumer

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

// testAppInfo is the service identity used across the consumer tests.
var testAppInfo = appinfo.Info{Name: "test-app"}

// mockConsumer implements mqConsumer for testing.
type mockConsumer struct {
	startErr     error
	shutdownErr  error
	subscribeErr error

	startFunc     func() error
	shutdownFunc  func() error
	subscribeFunc func(topic string, selector consumer.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
}

func (m *mockConsumer) Start() error {
	if m.startFunc != nil {
		return m.startFunc()
	}
	return m.startErr
}

func (m *mockConsumer) Shutdown() error {
	if m.shutdownFunc != nil {
		return m.shutdownFunc()
	}
	return m.shutdownErr
}

func (m *mockConsumer) Subscribe(topic string, selector consumer.MessageSelector, f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error {
	if m.subscribeFunc != nil {
		return m.subscribeFunc(topic, selector, f)
	}
	return m.subscribeErr
}
