// This file holds the transport mocks for package consumer tests: a consumer, and
// a JetStream message that records its settlement.
package consumer

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/identity"
)

// testAppInfo is the service identity used across the consumer tests.
var testAppInfo = identity.Info{Name: "test-app"}

// mockConsumer implements jsConsumer for testing.
type mockConsumer struct {
	consumeErr  error
	shutdownErr error

	consumeFunc  func(handler func(msg jetstream.Msg)) error
	shutdownFunc func(ctx context.Context) error
}

func (m *mockConsumer) Consume(handler func(msg jetstream.Msg)) error {
	if m.consumeFunc != nil {
		return m.consumeFunc(handler)
	}
	return m.consumeErr
}

func (m *mockConsumer) Shutdown(ctx context.Context) error {
	if m.shutdownFunc != nil {
		return m.shutdownFunc(ctx)
	}
	return m.shutdownErr
}

// mockMsg implements jetstream.Msg for testing.
type mockMsg struct {
	subject  string
	data     []byte
	headers  nats.Header
	acked    bool
	nacked   bool
	termed   bool
	nakDelay time.Duration
	meta     *jetstream.MsgMetadata
	metaErr  error
	ackErr   error
}

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return m.meta, m.metaErr
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
	return m.ackErr
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
	m.termed = true
	return nil
}

func (m *mockMsg) TermWithReason(reason string) error {
	return nil
}

func (m *mockMsg) InProgress() error {
	return nil
}
