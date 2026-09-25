package consumer

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
)

// testAppInfo is the service identity used across the consumer tests.
var testAppInfo = appinfo.Info{Name: "test-app"}

// mockConsumer implements jsConsumer for testing.
type mockConsumer struct {
	startErr    error
	shutdownErr error
	consumeErr  error

	startFunc    func() error
	shutdownFunc func() error
	consumeFunc  func(handler func(msg jetstream.Msg)) error
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

func (m *mockConsumer) Consume(handler func(msg jetstream.Msg)) error {
	if m.consumeFunc != nil {
		return m.consumeFunc(handler)
	}
	return m.consumeErr
}

// mockMsg implements jetstream.Msg for testing.
type mockMsg struct {
	subject  string
	data     []byte
	headers  nats.Header
	acked    bool
	nacked   bool
	nakDelay time.Duration
	meta     *jetstream.MsgMetadata
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return m.meta, nil
}

func (m *mockMsg) Data() []byte {
	return m.data
}

func (m *mockMsg) Headers() nats.Header {
	return m.headers
}

func (m *mockMsg) Subject() string {
	return m.subject
}

func (m *mockMsg) Reply() string {
	return ""
}

func (m *mockMsg) Ack() error {
	m.acked = true
	return nil
}

func (m *mockMsg) DoubleAck(_ context.Context) error {
	m.acked = true
	return nil
}

func (m *mockMsg) Nak() error {
	m.nacked = true
	return nil
}

func (m *mockMsg) NakWithDelay(delay time.Duration) error {
	m.nacked = true
	m.nakDelay = delay
	return nil
}

func (m *mockMsg) Term() error {
	return nil
}

func (m *mockMsg) TermWithReason(reason string) error {
	return nil
}

func (m *mockMsg) InProgress() error {
	return nil
}
