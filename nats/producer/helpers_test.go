// This file holds the shared test helpers for package producer: an in-process
// JetStream server, stub transports, and a registration shortcut.

package producer

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

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
