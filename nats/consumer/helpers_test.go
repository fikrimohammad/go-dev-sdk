package consumer

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startTestNATSServer starts an in-process NATS server with JetStream enabled
// and stops it when the test ends.
func startTestNATSServer(t *testing.T) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("natsserver.NewServer() error = %v", err)
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

// connectTestClient returns a plain NATS client for ns that closes when the test
// ends. Tests use it to publish raw messages.
func connectTestClient(t *testing.T, ns *natsserver.Server) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(nc.Close)

	return nc
}
