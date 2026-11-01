// Shared fixtures for the rest/client tests. This file holds the httptest
// vendor and proxy servers, including a slow vendor that counts its hits, a
// drip vendor that trickles its body, and a scripted vendor that answers each
// request from a list of steps. It also holds a proxy that never answers, a
// proxy that records its Proxy-Authorization header, a local address that
// refuses connections, the GET and generic request helpers that send a request
// through a client, and the log capture for the default logger. It also holds
// the telemetry fixtures: a span recorder, a recording metrics client, and a
// client built on both, with the helpers that read their attributes. The
// vendors that must send bytes no net/http server can send, such as a 1xx
// response before the final one, are raw TCP servers. The file also holds the
// request paths that must reach a vendor exactly as sent.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newVendor starts an httptest server that answers with handler. The server
// is closed when the test ends.
func newVendor(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// newProxy starts an httptest server that acts as a forward proxy. It answers
// 200 OK and sends the request target it received, as the absolute-form URI,
// on the returned channel. The channel holds one target for each request, so
// a test reads it after the call returns. The server is closed when the test
// ends.
func newProxy(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	targets := make(chan string, 8)
	srv := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case targets <- r.RequestURI:
		default:
		}
		w.WriteHeader(http.StatusOK)
	})
	return srv, targets
}

// get sends a GET for url through c and returns the response status and body.
// The Hertz objects are released before it returns, so the caller keeps only
// the copied body.
func get(t *testing.T, c *hclient.Client, url string) (int, string, error) {
	t.Helper()
	return sendGET(t.Context(), c, url)
}

// sendGET is get with the context of the call given by the caller, so a test
// can set a deadline or cancel the call.
func sendGET(ctx context.Context, c *hclient.Client, url string) (int, string, error) {
	req := protocol.AcquireRequest()
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseRequest(req)
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.SetMethod(consts.MethodGet)
	if err := c.Do(ctx, req, resp); err != nil {
		return 0, "", err
	}
	return resp.StatusCode(), string(resp.Body()), nil
}

// newSlowVendor starts an httptest server that waits for delay before it
// answers 200 OK. The wait ends early when the client goes away. The returned
// counter holds the number of requests the vendor received. The server is
// closed when the test ends.
func newSlowVendor(t *testing.T, delay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(delay):
		}
		_, _ = io.WriteString(w, "vendor body")
	})
	return srv, &hits
}

// send sends a request with method to url through c. setup, when not nil, changes
// the request first, for example to set a body or a header. It returns the status,
// and the Hertz objects are released before it returns.
func send(ctx context.Context, c *hclient.Client, method, url string, setup func(*protocol.Request)) (int, error) {
	req := protocol.AcquireRequest()
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseRequest(req)
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.SetMethod(method)
	if setup != nil {
		setup(req)
	}
	if err := c.Do(ctx, req, resp); err != nil {
		return 0, err
	}
	return resp.StatusCode(), nil
}

// vendorStep is one scripted answer of a scriptedVendor.
type vendorStep struct {
	status     int           // status to answer with
	retryAfter string        // Retry-After header value, when not empty
	delay      time.Duration // wait before answering, when not zero
	drop       bool          // close the connection without answering
}

// seenRequest is what a scriptedVendor records about one request.
type seenRequest struct {
	method         string
	body           string
	idempotencyKey string
}

// scriptedVendor answers its requests from a list of steps, one step for each
// request. When the steps run out, the last step repeats. It counts the hits
// and records each request it receives.
type scriptedVendor struct {
	URL  string
	hits atomic.Int64
	mu   sync.Mutex
	seen []seenRequest
}

// newScriptedVendor starts an httptest server that answers with steps. The
// server is closed when the test ends.
func newScriptedVendor(t *testing.T, steps ...vendorStep) *scriptedVendor {
	t.Helper()
	v := &scriptedVendor{}
	srv := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		n := int(v.hits.Add(1)) - 1
		v.mu.Lock()
		v.seen = append(v.seen, seenRequest{
			method:         r.Method,
			body:           string(body),
			idempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		v.mu.Unlock()

		step := steps[min(n, len(steps)-1)]
		if step.drop {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				_ = conn.Close()
			}
			return
		}
		if step.delay > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(step.delay):
			}
		}
		if step.retryAfter != "" {
			w.Header().Set("Retry-After", step.retryAfter)
		}
		w.WriteHeader(step.status)
		_, _ = io.WriteString(w, "vendor body")
	})
	v.URL = srv.URL
	return v
}

// requests returns a copy of what the vendor has received, in order.
func (v *scriptedVendor) requests() []seenRequest {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.seen)
}

// logRecord decodes the only line in buf as a JSON log record. It fails the
// test when buf does not hold exactly one line.
func logRecord(t *testing.T, buf *logBuffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("log output = %q, want exactly one line", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log line %q is not JSON: %v", lines[0], err)
	}
	return rec
}

// logBuffer collects log output. Its writes are guarded by a mutex, because
// the logs handlers write from more than one place.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p to the buffer.
func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
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
	buf := &logBuffer{}
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

// okHandler answers 200 OK with a fixed body. It stands in for a vendor that
// works.
func okHandler(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "vendor body")
}

// newDripVendor starts an httptest server that sends size bytes of body, one
// byte every interval, after its headers. Its Content-Length is size, so a
// client waits for the whole body. The wait ends early when the client goes
// away. The server is closed when the test ends.
func newDripVendor(t *testing.T, size int, interval time.Duration) *httptest.Server {
	t.Helper()
	return newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		for range size {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(interval):
			}
			_, _ = io.WriteString(w, "x")
			w.(http.Flusher).Flush()
		}
	})
}

// newStallingProxy starts a TCP listener that accepts connections and never
// answers them, so a CONNECT through it stalls. It returns the proxy URL. The
// listener and every connection it accepted are closed, and their goroutines
// have ended, by the time the test ends.
func newStallingProxy(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var (
			wg    sync.WaitGroup
			conns []net.Conn
		)
		defer func() {
			for _, c := range conns {
				_ = c.Close()
			}
			wg.Wait()
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
			wg.Go(func() {
				_, _ = io.Copy(io.Discard, conn)
			})
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
	return "http://" + ln.Addr().String()
}

// newHeaderProxy starts an httptest server that acts as a forward proxy. It
// answers 200 OK and sends the Proxy-Authorization header of each request on
// the returned channel, which holds up to eight values. The server is closed
// when the test ends.
func newHeaderProxy(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	auths := make(chan string, 8)
	srv := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case auths <- r.Header.Get("Proxy-Authorization"):
		default:
		}
		w.WriteHeader(http.StatusOK)
	})
	return srv, auths
}

// closedAddress returns the host and port of a local address that refuses
// connections, because the listener behind it is already closed.
func closedAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen error = %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("listener Close error = %v", err)
	}
	return addr
}

// metricCall is one recorded Record call: the instrument, the value, and a copy
// of the attributes. Each attribute keeps its Go type, so an int stays an int.
type metricCall struct {
	instrument metrics.Instrument
	value      float64
	attrs      map[string]any
}

// recordingMetrics captures the Record calls, in order. The embedded no-op client
// supplies the other methods. Records returns a copy, so a test can read it while
// the client is still in use.
type recordingMetrics struct {
	metrics.Client
	mu      sync.Mutex
	records []metricCall
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

// Records returns a copy of the Record calls so far.
func (m *recordingMetrics) Records() []metricCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.records)
}

// telemetry holds the fixtures that a client under test reports to: the tracer
// provider, whose ended spans go to spans, and the recording metrics client.
type telemetry struct {
	provider *sdktrace.TracerProvider
	spans    *tracetest.SpanRecorder
	metrics  *recordingMetrics
}

// newTelemetryClient returns a client built from cfg, with telemetry fixtures
// that record its spans and measurements. The provider is shut down when the
// test ends. It fails the test if New returns an error.
func newTelemetryClient(t *testing.T, cfg Config) (*hclient.Client, *telemetry) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.WithoutCancel(t.Context())) })
	tel := &telemetry{provider: tp, spans: rec, metrics: newRecordingMetrics()}
	c, err := New(cfg, WithMetrics(tel.metrics), WithTracer(tracer.Wrap(tp)))
	if err != nil {
		t.Fatalf("New(%+v) error = %v", cfg, err)
	}
	return c, tel
}

// clientSpans returns the ended spans of the rest/client scope, in the order
// they ended. The caller's own spans are left out.
func clientSpans(rec *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.InstrumentationScope().Name == tracerScope {
			spans = append(spans, s)
		}
	}
	return spans
}

// spanAttributes returns the attributes of a span keyed by name. An integer
// becomes an int, the type that metricCall.attrs holds, so one expectation can
// describe a span and a metric.
func spanAttributes(kvs []attribute.KeyValue) map[attribute.Key]any {
	attrs := make(map[attribute.Key]any, len(kvs))
	for _, kv := range kvs {
		if kv.Value.Type() == attribute.INT64 {
			attrs[kv.Key] = int(kv.Value.AsInt64())
			continue
		}
		attrs[kv.Key] = kv.Value.AsInterface()
	}
	return attrs
}

// metricAttributes returns the attributes of one Record call keyed by name, in
// the same form as spanAttributes.
func metricAttributes(attrs map[string]any) map[attribute.Key]any {
	out := make(map[attribute.Key]any, len(attrs))
	for k, v := range attrs {
		out[attribute.Key(k)] = v
	}
	return out
}

// checkAttributeSet reports the difference between got and want, as sets of
// keys and values. what names the set in the message.
func checkAttributeSet(t *testing.T, what string, got, want map[attribute.Key]any) {
	t.Helper()
	if !maps.Equal(got, want) {
		t.Errorf("%s attributes = %v, want %v", what, got, want)
	}
}

// sentPaths are request paths with escaped slashes, escaped reserved bytes,
// double slashes, and dot segments. Hertz's default path handling would decode,
// clean, or re-encode each one, so each must reach the vendor exactly as given.
var sentPaths = []struct {
	name string
	path string // path and query, appended to the vendor URL
}{
	{name: "encoded dot segments", path: "/a/..%2F..%2Fadmin"},
	{name: "encoded slash in a segment", path: "/group%2Fproject/x"},
	{name: "empty segment", path: "/a//b"},
	{name: "encoded semicolon", path: "/a%3Bb"},
	{name: "encoded plus", path: "/key%2Bplus"},
	{name: "dot segments", path: "/a/./b/../c"},
}

// newRequestURIVendor starts an httptest server that answers 200 OK and sends
// the request URI of each request it receives on the returned channel, exactly
// as it came off the wire. The channel holds up to eight values. The server is
// closed when the test ends.
func newRequestURIVendor(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	uris := make(chan string, 8)
	srv := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case uris <- r.RequestURI:
		default:
		}
		okHandler(w, r)
	})
	return srv, uris
}

// newEarlyHintsVendor starts a raw TCP server that speaks keep-alive HTTP/1.1.
// A GET for /hints gets a 103 Early Hints before its 200 OK, which net/http
// servers cannot send in this order, and the 200 follows only after the next
// request on the connection. Every other GET gets a plain 200 OK whose body is
// "for " and the path. The listener and its connections are closed,
// and their goroutines have ended, by the time the test ends. It returns the
// base URL.
func newEarlyHintsVendor(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen error = %v", err)
	}
	var (
		mu         sync.Mutex
		conns      []net.Conn
		wg         sync.WaitGroup
		acceptDone = make(chan struct{})
	)
	go func() {
		defer close(acceptDone)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Go(func() { serveEarlyHints(conn) })
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-acceptDone
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return "http://" + ln.Addr().String()
}

// serveEarlyHints answers the GET requests on conn until the connection ends.
// For /hints it writes the 103 first, then holds the 200 OK until the next
// request arrives on the same connection. The 200 therefore comes after the
// client has read the 103, as a late final response does, and it is still on
// the connection when a reused connection would read it. A client that closed
// the connection instead ends the loop with no response owed.
func serveEarlyHints(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		path, err := readRequestPath(r)
		if err != nil {
			return
		}
		if path == "/hints" {
			if _, err := io.WriteString(conn, "HTTP/1.1 103 Early Hints\r\nLink: </a.css>; rel=preload\r\n\r\n"); err != nil {
				return
			}
			next, err := readRequestPath(r)
			if err != nil {
				return
			}
			if err := writeEarlyHintsFinal(conn, path); err != nil {
				return
			}
			path = next
		}
		if err := writeEarlyHintsFinal(conn, path); err != nil {
			return
		}
	}
}

// readRequestPath reads one request head from r and returns the path of its
// request line. It returns an error when the connection ends first.
func readRequestPath(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", io.ErrUnexpectedEOF
	}
	for {
		h, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(h) == "" {
			return fields[1], nil
		}
	}
}

// writeEarlyHintsFinal writes the 200 OK for path, whose body is "for " and the
// path.
func writeEarlyHintsFinal(conn net.Conn, path string) error {
	body := "for " + path
	_, err := io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"+body)
	return err
}
