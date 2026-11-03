package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/fikrimohammad/go-servicekit/rest/internal/semconv"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// listenDelay is how long listenDelayLogger holds the "HTTP server listening"
// line. It is much longer than shutdownPoll, so a shutdown in that window
// certainly happens while netpoll has no server to stop.
const listenDelay = 100 * time.Millisecond

// listenDelayArmed is true only while TestRun_cancelInListenWindow runs. It
// turns on the delay in listenDelayLogger, so no other test is slowed down.
var listenDelayArmed atomic.Bool

// listenDelayLogger is a Hertz logger that holds the "HTTP server listening"
// line while listenDelayArmed is set. netpoll logs that line after it sets the
// listener and before it starts serving, so the delay opens the window in
// which a shutdown is refused.
type listenDelayLogger struct {
	hlog.FullLogger
}

// Infof waits listenDelay before it logs the listening line while the delay is
// armed, and logs every other line at once.
func (l listenDelayLogger) Infof(format string, v ...any) {
	if listenDelayArmed.Load() && strings.Contains(format, "HTTP server listening") {
		<-time.After(listenDelay)
	}
	l.FullLogger.Infof(format, v...)
}

// TestMain installs listenDelayLogger as Hertz's global logger once, before any
// test starts a goroutine. Hertz's SetLogger is not safe to call while another
// goroutine reads the logger, and a shutdown-timeout test leaves a connection
// goroutine behind that does read it, so no test swaps the logger later.
func TestMain(m *testing.M) {
	hlog.SetLogger(listenDelayLogger{FullLogger: hlog.DefaultLogger()})
	os.Exit(m.Run())
}

// installDefaultTelemetry installs a recording tracer provider and a manual
// metrics reader as the OTel global providers until the test ends. It returns
// the reader and the span recorder.
func installDefaultTelemetry(t *testing.T) (*sdkmetric.ManualReader, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prevTracer, prevMeter := otel.GetTracerProvider(), otel.GetMeterProvider()
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		if otel.GetTracerProvider() != prevTracer {
			otel.SetTracerProvider(prevTracer)
		}
		if otel.GetMeterProvider() != prevMeter {
			otel.SetMeterProvider(prevMeter)
		}
		_ = tp.Shutdown(context.WithoutCancel(t.Context()))
		_ = mp.Shutdown(context.WithoutCancel(t.Context()))
	})
	return reader, rec
}

// requestDurationCount returns how many http.server.request.duration
// measurements reader has collected. It fails the test if the collection fails
// or the instrument is not a float64 histogram.
func requestDurationCount(t *testing.T, reader *sdkmetric.ManualReader) uint64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("reader.Collect() error = %v", err)
	}
	var n uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != semconv.ServerRequestDuration.Name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s data = %T, want metricdata.Histogram[float64]", m.Name, m.Data)
			}
			for _, dp := range h.DataPoints {
				n += dp.Count
			}
		}
	}
	return n
}

func TestNew_unsetTelemetryUsesDefaults(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
	}{
		{name: "no options"},
		{name: "nil clients", opts: []Option{WithMetrics(nil), WithTracer(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(Config{}, tt.opts...)
			if err != nil {
				t.Fatalf("New(Config{}) error = %v", err)
			}
			// The defaults are installed after New, so this also checks that
			// they are read per request, not captured by New.
			reader, rec := installDefaultTelemetry(t)
			perform(s, http.MethodGet, "/missing", nil)
			if got := requestDurationCount(t, reader); got != 1 {
				t.Errorf("default metrics got %d duration records, want 1", got)
			}
			if got := len(rec.Ended()); got != 1 {
				t.Errorf("default tracer got %d ended spans, want 1", got)
			}
		})
	}
}

func TestNew_invalidConfig(t *testing.T) {
	s, err := New(Config{Addr: "nohostport"})
	if err == nil {
		t.Fatal("New(Config{Addr: \"nohostport\"}) error = nil, want non-nil")
	}
	if s != nil {
		t.Errorf("New(Config{Addr: \"nohostport\"}) = %v, want nil on error", s)
	}
}

func TestHertz(t *testing.T) {
	s, err := New(Config{})
	if err != nil {
		t.Fatalf("New(Config{}) error = %v", err)
	}
	if got := s.Hertz(); got != s.hz {
		t.Errorf("Hertz() = %p, want %p", got, s.hz)
	}
}

func TestRun_cancelReleasesPort(t *testing.T) {
	addr := freeAddr(t)
	s, err := New(Config{Addr: addr})
	if err != nil {
		t.Fatalf("New(Config{Addr: %q}) error = %v", addr, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()

	waitListening(t, addr)
	cancel()
	if err := waitRun(t, errc); err != nil {
		t.Errorf("Run(ctx) after cancel = %v, want nil", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("net.Listen(%q) after Run returned error = %v, want port released", addr, err)
	}
	_ = ln.Close()
}

func TestRun_cancelBeforeServing(t *testing.T) {
	addr := freeAddr(t)
	s, err := New(Config{Addr: addr})
	if err != nil {
		t.Fatalf("New(Config{Addr: %q}) error = %v", addr, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Run(ctx); err != nil {
		t.Errorf("Run(cancelled ctx) = %v, want nil", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("net.Listen(%q) after Run returned error = %v, want port released", addr, err)
	}
	_ = ln.Close()
}

func TestRun_cancelInListenWindow(t *testing.T) {
	listenDelayArmed.Store(true)
	t.Cleanup(func() { listenDelayArmed.Store(false) })

	addr := freeAddr(t)
	s, err := New(Config{Addr: addr})
	if err != nil {
		t.Fatalf("New(Config{Addr: %q}) error = %v", addr, err)
	}
	// Cancelling before Run starts makes the shutdown run as soon as the
	// listener exists, which is inside the window that listenDelayLogger holds.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()

	if err := waitRun(t, errc); err != nil {
		t.Errorf("Run(cancelled ctx) in the listen window = %v, want nil", err)
	}
	if conn, err := net.Dial("tcp", addr); err == nil {
		_ = conn.Close()
		t.Errorf("net.Dial(%q) after Run returned succeeded, want the server stopped", addr)
	}
}

func TestRun_bindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(127.0.0.1:0) error = %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	addr := ln.Addr().String()
	s, err := New(Config{Addr: addr})
	if err != nil {
		t.Fatalf("New(Config{Addr: %q}) error = %v", addr, err)
	}
	// netpoll panics on a bind failure and serve recovers it. The test checks
	// only that Run returns a non-nil error promptly, so it neither crashes
	// nor hangs.
	errc := make(chan error, 1)
	go func() { errc <- s.Run(t.Context()) }()
	if err := waitRun(t, errc); err == nil {
		t.Errorf("Run(ctx) on busy %s = nil, want non-nil error", addr)
	}
}

func TestRun_shutdownTimeout(t *testing.T) {
	buf := captureLogs(t)
	addr := freeAddr(t)
	s, err := New(Config{Addr: addr, ShutdownTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("New(Config{Addr: %q}) error = %v", addr, err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	s.Hertz().GET("/slow", func(_ context.Context, _ *app.RequestContext) {
		defer close(handlerDone)
		close(started)
		<-release
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	waitListening(t, addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial(%q) error = %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := conn.Write([]byte("GET /slow HTTP/1.1\r\nHost: test\r\n\r\n")); err != nil {
		t.Fatalf("write request to %q error = %v", addr, err)
	}
	select {
	case <-started:
	case <-time.After(testTimeout):
		t.Fatalf("handler for GET /slow did not start within %v", testTimeout)
	}
	// Run returns without waiting for this request, so it finishes after the
	// test body. Cleanups run last-in first-out, so this one runs before the
	// connection close above and before captureLogs restores the logger. It
	// waits for the handler to return and for the access log line that the
	// logging middleware writes after it, so nothing from this request reaches
	// another test's logger.
	t.Cleanup(func() {
		close(release)
		select {
		case <-handlerDone:
		case <-time.After(testTimeout):
			t.Fatalf("handler for GET /slow did not return within %v after release", testTimeout)
		}
		buf.waitContains(t, `"url.path":"/slow"`)
	})

	cancel()
	err = waitRun(t, errc)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run(ctx) with a request in flight past ShutdownTimeout = %v, want DeadlineExceeded", err)
	}
}

func TestHertz_useAppliesToLaterRoutes(t *testing.T) {
	s := newTestServer(t)
	s.Hertz().GET("/before", okHandler)
	s.Hertz().Use(func(ctx context.Context, c *app.RequestContext) {
		c.Response.Header.Set("X-Team", "mw")
		c.Next(ctx)
	})
	s.Hertz().GET("/after", okHandler)

	if got := perform(s, "GET", "/after", nil).Header().Get("X-Team"); got != "mw" {
		t.Errorf("GET /after X-Team = %q, want %q: a route registered after Use runs the middleware", got, "mw")
	}
	if got := perform(s, "GET", "/before", nil).Header().Get("X-Team"); got != "" {
		t.Errorf("GET /before X-Team = %q, want none: Use doesn't reach a route registered before it", got)
	}
}

func TestRequestIDFrom_absent(t *testing.T) {
	if got := RequestIDFrom(t.Context()); got != "" {
		t.Errorf("RequestIDFrom(context without an ID) = %q, want empty", got)
	}
}

func TestNew_trailingSlashIs404(t *testing.T) {
	buf := captureLogs(t)
	s, mc, rec := newTelemetryServer(t, Config{})
	s.Hertz().GET("/users/:id", okHandler)
	recorded := perform(s, "GET", "/users/42/", nil, ut.Header{Key: "X-Request-ID", Value: "req-slash"})

	if recorded.Code != http.StatusNotFound {
		t.Errorf("GET /users/42/ status = %d, want %d: no redirect for a trailing slash", recorded.Code, http.StatusNotFound)
	}
	if got := recorded.Header().Get("Location"); got != "" {
		t.Errorf("GET /users/42/ Location = %q, want no redirect", got)
	}
	if got := recorded.Header().Get("X-Request-ID"); got != "req-slash" {
		t.Errorf("GET /users/42/ X-Request-ID = %q, want %q", got, "req-slash")
	}
	if got := decodeProblem(t, recorded)["status"]; got != float64(http.StatusNotFound) {
		t.Errorf("GET /users/42/ problem status = %v, want %d", got, http.StatusNotFound)
	}
	onlySpan(t, rec)
	if n := len(mc.Records()); n != 1 {
		t.Errorf("duration records = %d, want 1", n)
	}
	requestLog(t, buf)
}
