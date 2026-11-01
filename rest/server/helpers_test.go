// Shared fixtures for the rest/server tests. This file holds the free
// loopback address, the bounded waits for a listener and for Run to return,
// and the timeout those waits share. It also holds the server start helper,
// the in-process and real-socket request helpers, the raw-socket exchange for
// responses that Hertz doesn't frame normally, test servers, the span recorder,
// the recording metrics client, and the log capture for the default logger.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// testTimeout bounds every wait on the server in these tests.
const testTimeout = 5 * time.Second

// freeAddr returns a 127.0.0.1 address whose port was free when the call
// returned. The probe listener is closed before returning, so another process
// could take the port afterwards.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(127.0.0.1:0) error = %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close probe listener on %s: %v", addr, err)
	}
	return addr
}

// waitListening dials addr until a connection succeeds, which shows that the
// server has bound the port. It polls every few milliseconds and fails the
// test after testTimeout.
func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.NewTimer(testTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("server did not listen on %s within %v: last dial error %v", addr, testTimeout, err)
		case <-tick.C:
		}
	}
}

// waitRun returns the error that Run sent on errc. It fails the test if Run
// has not returned after testTimeout.
func waitRun(t *testing.T, errc <-chan error) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(testTimeout):
		t.Fatalf("Run did not return within %v", testTimeout)
		return nil
	}
}

// newTestServer returns a Server with the default config and no-op telemetry
// clients. It fails the test if New returns an error.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	return newTestServerCfg(t, Config{})
}

// newTestServerCfg returns a Server with cfg and no-op telemetry clients. It
// fails the test if New returns an error.
func newTestServerCfg(t *testing.T, cfg Config) *Server {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(%+v) error = %v", cfg, err)
	}
	return s
}

// metricCall is one recorded measurement: the instrument, the value, and a copy
// of the attributes.
type metricCall struct {
	instrument metrics.Instrument
	value      float64
	attrs      map[string]any
}

// recordingMetrics captures the Record and AddUpDown calls, in order. The
// embedded no-op client supplies the other methods. The accessors return
// copies, so a test can read them while the server is still running.
type recordingMetrics struct {
	metrics.Client
	mu      sync.Mutex
	records []metricCall
	upDowns []metricCall
}

// newRecordingMetrics returns a recordingMetrics with no calls yet.
func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{Client: metrics.Noop()}
}

// Record captures a histogram measurement.
func (m *recordingMetrics) Record(_ context.Context, instrument metrics.Instrument, value float64, attrs map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, metricCall{instrument: instrument, value: value, attrs: maps.Clone(attrs)})
	return nil
}

// AddUpDown captures an up-down counter change.
func (m *recordingMetrics) AddUpDown(_ context.Context, instrument metrics.Instrument, value int64, attrs map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upDowns = append(m.upDowns, metricCall{instrument: instrument, value: float64(value), attrs: maps.Clone(attrs)})
	return nil
}

// Records returns a copy of the Record calls so far.
func (m *recordingMetrics) Records() []metricCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.records)
}

// UpDowns returns a copy of the AddUpDown calls so far.
func (m *recordingMetrics) UpDowns() []metricCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.upDowns)
}

// newTelemetryServer returns a Server with cfg whose metrics go to the returned
// recordingMetrics and whose tracer records ended spans into the returned
// recorder. The provider is shut down when the test ends. It fails the test if
// New returns an error.
func newTelemetryServer(t *testing.T, cfg Config) (*Server, *recordingMetrics, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.WithoutCancel(t.Context())) })
	mc := newRecordingMetrics()
	s, err := New(cfg, WithMetrics(mc), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New(%+v) error = %v", cfg, err)
	}
	return s, mc, rec
}

// newTracedServer returns a Server with the default config and a recording
// metrics client that the test ignores, whose tracer records ended spans into
// the returned recorder. The provider is shut down when the test ends. It
// fails the test if New returns an error.
func newTracedServer(t *testing.T) (*Server, *tracetest.SpanRecorder) {
	t.Helper()
	return newTracedServerCfg(t, Config{})
}

// newTracedServerCfg is newTracedServer with cfg in place of the default
// config.
func newTracedServerCfg(t *testing.T, cfg Config) (*Server, *tracetest.SpanRecorder) {
	t.Helper()
	s, _, rec := newTelemetryServer(t, cfg)
	return s, rec
}

// perform sends one request through the server's engine without a socket and
// returns the recorded response. Pass a nil body for a request without one.
func perform(s *Server, method, path string, body *ut.Body, headers ...ut.Header) *ut.ResponseRecorder {
	return ut.PerformRequest(s.Hertz().Engine, method, path, body, headers...)
}

// sendUT returns a send function that performs one request through the
// server's engine without a socket, and returns the status code.
func sendUT(method, path string, headers ...ut.Header) func(*testing.T, *Server) int {
	return func(_ *testing.T, s *Server) int {
		return perform(s, method, path, nil, headers...).Code
	}
}

// startServer runs s on its configured address and waits until the port
// accepts connections. The returned stop cancels the run, waits for Run to
// return, and returns Run's error. Later calls to stop return the same error.
// The test cleanup calls stop too, so a failed test still shuts the server
// down.
func startServer(t *testing.T, s *Server) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	waitListening(t, s.cfg.Addr)

	var (
		once   sync.Once
		runErr error
	)
	stop = func() error {
		once.Do(func() {
			cancel()
			runErr = waitRun(t, errc)
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// sendOverSocket returns a send function that starts the server on its
// configured address, sends one request with method, path, and headers over a
// real connection, and stops the server before it returns the status code. The
// client closes each connection after its response, so the shutdown doesn't
// wait for one.
func sendOverSocket(method, path string, headers ...ut.Header) func(*testing.T, *Server) int {
	return func(t *testing.T, s *Server) int {
		t.Helper()
		stop := startServer(t, s)
		client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
		req, err := http.NewRequestWithContext(t.Context(), method, "http://"+s.cfg.Addr+path, nil)
		if err != nil {
			t.Fatalf("NewRequest(%s %q) error = %v", method, path, err)
		}
		for _, h := range headers {
			req.Header.Set(h.Key, h.Value)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s %s over the socket error = %v", method, path, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err := stop(); err != nil {
			t.Errorf("Run after %s %s = %v, want nil", method, path, err)
		}
		return resp.StatusCode
	}
}

// rawExchange sends request to addr over a plain TCP connection and returns
// every byte the server sends before it closes the connection. The request
// must ask for Connection: close, or the server keeps the connection open and
// the read fails at the deadline.
func rawExchange(t *testing.T, addr, request string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("net.Dial(%q) error = %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("SetDeadline error = %v", err)
	}
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatalf("write request to %q error = %v", addr, err)
	}
	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response from %q error = %v, want the server to close the connection", addr, err)
	}
	return string(raw)
}

// logBuffer collects log output. Its writes are guarded by a mutex, because
// the logs handlers write from more than one place. Each write also signals
// wrote, so a test can wait for a line that a request writes late.
type logBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	wrote chan struct{}
}

// Write appends p to the buffer and signals wrote without blocking.
func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	n, err := b.buf.Write(p)
	b.mu.Unlock()
	select {
	case b.wrote <- struct{}{}:
	default:
	}
	return n, err
}

// waitContains returns once the buffer contains want. It fails the test if
// that doesn't happen within testTimeout.
func (b *logBuffer) waitContains(t *testing.T, want string) {
	t.Helper()
	deadline := time.NewTimer(testTimeout)
	defer deadline.Stop()
	for !strings.Contains(b.String(), want) {
		select {
		case <-b.wrote:
		case <-deadline.C:
			t.Fatalf("log output does not contain %q within %v; log output:\n%s", want, testTimeout, b.String())
		}
	}
}

// String returns everything written so far.
func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs makes the default logger write JSON at debug level to the
// returned buffer, and restores the previous default when the test ends. The
// caller must not run in parallel with other tests that log, because the
// default logger is package state.
func captureLogs(t *testing.T) *logBuffer {
	t.Helper()
	buf := &logBuffer{wrote: make(chan struct{}, 1)}
	l, err := logs.New(identity.Info{Name: "test"}, logs.Config{
		Format:      logs.FormatJSON,
		Level:       "debug",
		Writer:      buf,
		ErrorWriter: buf,
	})
	if err != nil {
		t.Fatalf("logs.New error = %v", err)
	}
	prev := logs.Default()
	logs.SetDefault(l)
	t.Cleanup(func() { logs.SetDefault(prev) })
	return buf
}

// decodeProblem parses the body of rec as a JSON object. It fails the test if
// the body is not one.
func decodeProblem(t *testing.T, rec *ut.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body %q is not a JSON object: %v", rec.Body.String(), err)
	}
	return body
}
