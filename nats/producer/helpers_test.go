// This file holds the shared test helpers for package producer: an in-process
// JetStream server, stub transports, and a registration shortcut.

package producer

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// useTraceContextPropagator makes W3C trace context the global propagator for the
// test and restores the previous one when the test ends.
func useTraceContextPropagator(t *testing.T) {
	t.Helper()

	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })
}

// dialNATS returns a connection to ns for reads in the test. The connection
// closes when the test ends.
func dialNATS(t *testing.T, ns *natsserver.Server) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// startJetStreamProducer returns a Producer for topic on ns, configured with opts.
// It is started, and shut down when the test ends.
func startJetStreamProducer(t *testing.T, ns *natsserver.Server, topic string, opts ...Option) *Producer {
	t.Helper()

	p, err := New(testAppInfo, opts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: topic}); err != nil {
		t.Fatalf("Register(%s) error = %v", topic, err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// startJetStreamServer runs an in-process NATS server with JetStream enabled and
// stops it when the test ends.
func startJetStreamServer(t *testing.T) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS test server not ready for connections")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return ns
}

// testConfig returns a valid Config for topic that points at a local endpoint.
func testConfig(topic string) Config {
	return Config{Endpoints: []string{"localhost:4222"}, Topic: topic}
}

// stubFactory returns a producerFactory that hands out the stub transport
// registered for each topic.
func stubFactory(pds map[string]jsProducer) producerFactory {
	return func(_ context.Context, cfg Config) (jsProducer, error) {
		pd, ok := pds[cfg.Topic]
		if !ok {
			return nil, fmt.Errorf("no stub for topic %s", cfg.Topic)
		}
		return pd, nil
	}
}

// startStubbed registers every topic in pds, starts p with the stub transports,
// and shuts p down when the test ends.
func startStubbed(t *testing.T, p *Producer, pds map[string]jsProducer) {
	t.Helper()

	p.producerFactory = stubFactory(pds)
	for topic := range pds {
		if err := p.Register(testConfig(topic)); err != nil {
			t.Fatalf("Register(%s) error = %v", topic, err)
		}
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
}
