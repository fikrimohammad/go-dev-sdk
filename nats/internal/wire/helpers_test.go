// This file holds the shared test helper for package wire: an in-process
// JetStream server.

package wire

import (
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
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready for connections within 10s")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns
}
