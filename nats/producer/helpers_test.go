// This file holds the shared test helpers for package producer: an in-process
// JetStream server, a started producer, and metric and attribute comparisons.

package producer

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fikrimohammad/go-servicekit/identity"
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

	return startTopics(t, ns, []string{topic}, opts...)
}

// startTopics returns a Producer registered for every topic on ns and started. It
// is shut down when the test ends.
func startTopics(t *testing.T, ns *natsserver.Server, topics []string, opts ...Option) *Producer {
	t.Helper()

	p, err := New(testAppInfo, opts...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, topic := range topics {
		if err := p.Register(Config{Endpoints: []string{ns.ClientURL()}, Topic: topic}); err != nil {
			t.Fatalf("Register(%s) error = %v", topic, err)
		}
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

// startJetStreamServer runs an in-process NATS server with JetStream enabled on a
// random port, and stops it when the test ends.
func startJetStreamServer(t *testing.T) *natsserver.Server {
	t.Helper()

	return startJetStreamServerOn(t, -1)
}

// startJetStreamServerOn runs an in-process NATS server with JetStream enabled on
// port, which -1 picks at random, and stops it when the test ends.
func startJetStreamServerOn(t *testing.T, port int) *natsserver.Server {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Port:      port,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	go ns.Start()

	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS test server not ready for connections")
	}

	return ns
}

// freePort returns a loopback TCP port that nothing listens on at the time of the
// call.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// deadEndpoint returns a NATS URL on a loopback port that nothing listens on, so a
// dial to it is refused.
func deadEndpoint(t *testing.T) string {
	t.Helper()

	return "nats://127.0.0.1:" + strconv.Itoa(freePort(t))
}

// createStream creates the stream cfg on ns, through a connection of its own, and
// returns the JetStream context for later reads.
func createStream(t *testing.T, ns *natsserver.Server, cfg jetstream.StreamConfig) jetstream.JetStream { //nolint:ireturn // returns the nats.go jetstream.JetStream interface, as the library does
	t.Helper()

	js, err := jetstream.New(dialNATS(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	if _, err := js.CreateStream(context.Background(), cfg); err != nil {
		t.Fatalf("CreateStream(%s) error = %v", cfg.Name, err)
	}
	return js
}

// jsContext returns a JetStream context on a connection of its own, which closes
// when the test ends.
func jsContext(t *testing.T, ns *natsserver.Server) jetstream.JetStream { //nolint:ireturn // returns the nats.go jetstream.JetStream interface, as the library does
	t.Helper()

	js, err := jetstream.New(dialNATS(t, ns))
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	return js
}

// readMsgs returns the messages stored on subjects matching filter, in stream
// order, read through an ack-none consumer so the reads leave no state behind.
func readMsgs(t *testing.T, js jetstream.JetStream, topic, filter string, n int) []*nats.Msg {
	t.Helper()

	ctx := context.Background()
	stream, err := js.Stream(ctx, topic)
	if err != nil {
		t.Fatalf("Stream(%s) error = %v", topic, err)
	}
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubjects: []string{filter},
		AckPolicy:      jetstream.AckNonePolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		t.Fatalf("CreateConsumer(%s) error = %v", filter, err)
	}

	var out []*nats.Msg
	for deadline := time.Now().Add(5 * time.Second); len(out) < n && time.Now().Before(deadline); {
		batch, err := cons.Fetch(n-len(out), jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Fetch(%s) error = %v", filter, err)
		}
		for m := range batch.Messages() {
			out = append(out, &nats.Msg{Subject: m.Subject(), Data: m.Data(), Header: m.Headers()})
		}
	}
	if len(out) != n {
		t.Fatalf("read %d messages on %s, want %d", len(out), filter, n)
	}
	return out
}

// testAppInfo is the service identity used across the producer tests.
var testAppInfo = identity.Info{Name: "test-app"}

// waitClients waits until ns has n client connections. The server sees a closed
// client a moment after the client returns, so the count is polled.
func waitClients(t *testing.T, ns *natsserver.Server, n int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for ns.NumClients() != n {
		if time.Now().After(deadline) {
			t.Fatalf("server clients = %d, want %d", ns.NumClients(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// collectMetrics reads every metric that reader has collected, keyed by name.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() error = %v, want nil", err)
	}
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// pointStrings returns one canonical "key=value" string per attribute set, sorted,
// so data points can be compared without caring about their order.
func pointStrings(sets ...attribute.Set) []string {
	out := make([]string, 0, len(sets))
	for _, s := range sets {
		parts := make([]string, 0, s.Len())
		for _, kv := range s.ToSlice() {
			parts = append(parts, string(kv.Key)+"="+kv.Value.String())
		}
		slices.Sort(parts)
		out = append(out, strings.Join(parts, ","))
	}
	slices.Sort(out)
	return out
}

// mapStrings is pointStrings for expected attributes given as a map.
func mapStrings(sets ...map[string]string) []string {
	out := make([]string, 0, len(sets))
	for _, m := range sets {
		parts := make([]string, 0, len(m))
		for k, v := range m {
			parts = append(parts, k+"="+v)
		}
		slices.Sort(parts)
		out = append(out, strings.Join(parts, ","))
	}
	slices.Sort(out)
	return out
}
