// This file holds the shared test helpers for package consumer: an in-process
// JetStream server, a recording logger, and metric collection.
package consumer

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
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
