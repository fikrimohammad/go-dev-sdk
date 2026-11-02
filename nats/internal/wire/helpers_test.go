// This file holds the shared test helper for package wire: an in-process
// JetStream server.

package wire

import (
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// startServer runs an in-process JetStream server on a random port and stops it
// when the test ends. A non-empty token enables token authentication.
func startServer(t *testing.T, token string) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port:          -1,
		JetStream:     true,
		StoreDir:      t.TempDir(),
		Authorization: token,
	})
	if err != nil {
		t.Fatalf("NewServer(): got error %v, want nil", err)
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready for connections within 10s")
	}
	return ns
}

// deadEndpoint returns a NATS URL on a loopback port that nothing listens on. The
// port is taken from a listener that is closed again, so a dial to it is refused.
func deadEndpoint(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return "nats://" + addr
}
