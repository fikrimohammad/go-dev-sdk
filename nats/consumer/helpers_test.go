// This file holds the shared test helpers for package consumer: an in-process
// JetStream server, real pull-consumer fixtures, a recording logger, and metric
// collection.
package consumer

import (
	"context"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fikrimohammad/go-servicekit/identity"
	"github.com/fikrimohammad/go-servicekit/nats/internal/wire"
	"github.com/fikrimohammad/go-servicekit/observability/logs"
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

// testAppInfo is the service identity used across the consumer tests.
var testAppInfo = identity.Info{Name: "test-app"}

// deadEndpoint is an address nothing listens on, so a dial to it never connects.
const deadEndpoint = "nats://127.0.0.1:1"

// pullFixture is a stream for one topic on a server, with a durable pull consumer
// on it, so handler tests run on real deliveries. Its connection closes when the
// test ends.
type pullFixture struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	cons jetstream.Consumer
}

// newPullFixture creates the stream for topic on ns, with its data subjects, and a
// durable consumer that waits 30 s for an ack before it redelivers.
func newPullFixture(t *testing.T, ns *natsserver.Server, topic string) pullFixture {
	t.Helper()

	nc := connectTestClient(t, ns)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	ctx := context.Background()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     topic,
		Subjects: []string{topic, topic + ".>"},
	})
	if err != nil {
		t.Fatalf("CreateStream(%s) error = %v", topic, err)
	}
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "fixture",
		AckWait:   30 * time.Second,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("CreateConsumer() error = %v", err)
	}
	return pullFixture{nc: nc, js: js, cons: cons}
}

// publish stores one message on subject with header and returns once the server
// has stored it.
func (f pullFixture) publish(t *testing.T, subject, body string, header nats.Header) {
	t.Helper()

	msg := nats.NewMsg(subject)
	msg.Data = []byte(body)
	msg.Header = header
	if _, err := f.js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatalf("PublishMsg(%s) error = %v", subject, err)
	}
}

// next returns the next message the consumer delivers within wait, and false when
// none arrives in time.
func (f pullFixture) next(t *testing.T, wait time.Duration) (jetstream.Msg, bool) { //nolint:ireturn // returns the nats.go jetstream.Msg interface, as the library does
	t.Helper()

	batch, err := f.cons.Fetch(1, jetstream.FetchMaxWait(wait))
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	var out jetstream.Msg
	for m := range batch.Messages() {
		out = m
	}
	if err := batch.Error(); err != nil {
		t.Fatalf("batch error = %v", err)
	}
	return out, out != nil
}

// fetch returns the next message the consumer delivers, failing the test when none
// arrives within 5 s.
func (f pullFixture) fetch(t *testing.T) jetstream.Msg { //nolint:ireturn // returns the nats.go jetstream.Msg interface, as the library does
	t.Helper()

	msg, ok := f.next(t, 5*time.Second)
	if !ok {
		t.Fatal("no message delivered within 5s")
	}
	return msg
}

// awaitPending waits until the number of delivered, unacked messages is want. The
// server applies an ack a moment after the client sends it, so the count is polled.
func (f pullFixture) awaitPending(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := f.cons.Info(context.Background())
		if err != nil {
			t.Fatalf("Info() error = %v", err)
		}
		if info.NumAckPending == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("NumAckPending = %d, want %d", info.NumAckPending, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// reportTag is the header of the messages published by deliveredReport.
var reportTag = nats.Header{wire.HeaderTag: []string{"export_report_process"}}

// deliveredReport publishes body on the reporting topic with the tag used by the
// adapter tests, and returns it as the pull consumer delivers it, with the fixture
// for ack and redelivery checks.
func deliveredReport(t *testing.T, body string) (jetstream.Msg, pullFixture) { //nolint:ireturn // returns the nats.go jetstream.Msg interface, as the library does
	t.Helper()

	f := newPullFixture(t, startTestNATSServer(t), "reporting")
	f.publish(t, "reporting.export_report_process", body, cloneHeader(reportTag))
	return f.fetch(t), f
}

// cloneHeader returns a copy of h, so each publish owns its headers.
func cloneHeader(h nats.Header) nats.Header {
	out := make(nats.Header, len(h))
	for k, v := range h {
		out[k] = slices.Clone(v)
	}
	return out
}

// deliveries returns how many times the server has delivered msg, from its
// metadata: 1 on first delivery, and one more on each redelivery.
func deliveries(t *testing.T, msg jetstream.Msg) uint64 {
	t.Helper()

	md, err := msg.Metadata()
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	return md.NumDelivered
}

// serverAttrs returns the server.address and server.port a consumer on ns reports,
// read from the URL the consumer dials.
func serverAttrs(t *testing.T, ns *natsserver.Server) (string, int) {
	t.Helper()

	u, err := url.Parse(ns.ClientURL())
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("port of %s error = %v", u, err)
	}
	return u.Hostname(), port
}

// unmetaMsg is a jetstream.Msg whose metadata cannot be read. A delivered message
// always has metadata, and the jetstream package offers no way to build a message
// without it, so the no-metadata path of the adapter is driven by this fake. It
// embeds a nil jetstream.Msg, so only the methods the adapter calls are defined.
type unmetaMsg struct {
	jetstream.Msg
	metaErr error
	nacked  atomic.Bool
}

// Metadata returns no metadata, and the configured error.
func (m *unmetaMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, m.metaErr }

// Subject returns the subject of the message.
func (m *unmetaMsg) Subject() string { return "reporting.export" }

// Nak records the negative ack.
func (m *unmetaMsg) Nak() error {
	m.nacked.Store(true)
	return nil
}

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

// startTestConsumer registers cfg with handler on c, starts c, and shuts it down
// when the test ends.
func startTestConsumer(t *testing.T, c *Consumer, cfg Config, handler HandlerFunc) {
	t.Helper()

	if err := c.Register(cfg, handler); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
	})
}

// recordedLog is one record seen by recordingLogger.
type recordedLog struct {
	level string
	msg   string
	args  []any
}

// arg returns the value that follows key in the record's key/value args.
func (r recordedLog) arg(key string) any {
	for i := 0; i+1 < len(r.args); i += 2 {
		if r.args[i] == key {
			return r.args[i+1]
		}
	}
	return nil
}

// recordingLogger implements logs.Logger and keeps every record.
type recordingLogger struct {
	mu   sync.Mutex
	recs []recordedLog
}

func (r *recordingLogger) add(level, msg string, args []any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, recordedLog{level: level, msg: msg, args: args})
}

func (r *recordingLogger) Debug(_ context.Context, msg string, args ...any) {
	r.add("DEBUG", msg, args)
}

func (r *recordingLogger) Info(_ context.Context, msg string, args ...any) {
	r.add("INFO", msg, args)
}

func (r *recordingLogger) Warn(_ context.Context, msg string, args ...any) {
	r.add("WARN", msg, args)
}

func (r *recordingLogger) Error(_ context.Context, msg string, args ...any) {
	r.add("ERROR", msg, args)
}

// find returns the first record with the given level and message.
func (r *recordingLogger) find(level, msg string) (recordedLog, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.recs {
		if rec.level == level && rec.msg == msg {
			return rec, true
		}
	}
	return recordedLog{}, false
}

// captureLogs makes the default logger record into the returned value until the
// test ends.
func captureLogs(t *testing.T) *recordingLogger {
	t.Helper()

	rl := &recordingLogger{}
	prev := logs.Default()
	logs.SetDefault(rl)
	t.Cleanup(func() { logs.SetDefault(prev) })
	return rl
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
