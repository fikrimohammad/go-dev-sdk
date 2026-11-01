// Parity tests for the rest module, in the module root like nats/parity_test.go.
// They check that the same request emits the same telemetry through
// client.New's Do and through client.RoundTripper, and that a rest/client call
// to a rest/server route starts a separate trace with no trace context, baggage,
// or request ID on the wire. The root has no other Go files, so the telemetry
// fixtures live here.
package rest_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-dev-sdk/identity"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"github.com/fikrimohammad/go-dev-sdk/rest/client"
	"github.com/fikrimohammad/go-dev-sdk/rest/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// clientDurationName and clientDurationUnit are the public contract for the
// client duration metric.
const (
	clientDurationName = "http.client.request.duration"
	clientDurationUnit = "s"
)

// parityVendorDelay is how long a slow vendor waits when the caller does not
// go away first. It bounds the test if a timeout is never seen.
const parityVendorDelay = 2 * time.Second

// parityRequest is what a scenario sends. The vendor records the same three
// fields of every request it receives, so a retry can be checked for the body
// and the key it carried.
type parityRequest struct {
	method         string
	body           string // request body, empty for none
	idempotencyKey string // Idempotency-Key header, empty for none
}

// parityVendor is a vendor test server. Its URL stays the same for every run of
// a scenario, so both call paths see the same server.address and server.port.
type parityVendor struct {
	URL string
	// hits counts the requests answered in the current run. Each run resets it,
	// so each run sees the same script.
	hits atomic.Int32
	mu   sync.Mutex
	// seen holds what each request of the current run carried, in order.
	seen []parityRequest
}

// reset clears the counts and the record of the previous run.
func (v *parityVendor) reset() {
	v.hits.Store(0)
	v.mu.Lock()
	defer v.mu.Unlock()
	v.seen = nil
}

// received returns a copy of what the vendor received in the current run.
func (v *parityVendor) received() []parityRequest {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.seen)
}

// newParityVendor starts an httptest server that answers with statuses in
// order, repeating the last one. A slow vendor instead waits until the caller
// goes away. The server is closed when the test ends.
func newParityVendor(t *testing.T, statuses []int, slow bool) *parityVendor {
	t.Helper()
	v := &parityVendor{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		v.mu.Lock()
		v.seen = append(v.seen, parityRequest{
			method:         r.Method,
			body:           string(body),
			idempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		v.mu.Unlock()
		n := int(v.hits.Add(1)) - 1
		if slow {
			select {
			case <-r.Context().Done():
			case <-time.After(parityVendorDelay):
			}
			return
		}
		w.WriteHeader(statuses[min(n, len(statuses)-1)])
	}))
	t.Cleanup(srv.Close)
	v.URL = srv.URL
	return v
}

// metricCall is one metrics call. Its value is left out, because it is a
// duration that differs from run to run.
type metricCall struct {
	kind       string
	instrument metrics.Instrument
	attrs      map[string]any
}

// recordingMetrics keeps every metrics call in order. It embeds a no-op client
// for Provider and Stop, so the fixture needs no direct import of otel/metric.
type recordingMetrics struct {
	metrics.Client
	mu    sync.Mutex
	calls []metricCall
}

// Add records an Add call.
func (m *recordingMetrics) Add(_ context.Context, ins metrics.Instrument, _ int64, attrs map[string]any) error {
	m.record("add", ins, attrs)
	return nil
}

// Record records a Record call.
func (m *recordingMetrics) Record(_ context.Context, ins metrics.Instrument, _ float64, attrs map[string]any) error {
	m.record("record", ins, attrs)
	return nil
}

// AddUpDown records an AddUpDown call.
func (m *recordingMetrics) AddUpDown(_ context.Context, ins metrics.Instrument, _ int64, attrs map[string]any) error {
	m.record("add_up_down", ins, attrs)
	return nil
}

// RecordGauge records a RecordGauge call.
func (m *recordingMetrics) RecordGauge(_ context.Context, ins metrics.Instrument, _ float64, attrs map[string]any) error {
	m.record("record_gauge", ins, attrs)
	return nil
}

// record appends one call under the lock.
func (m *recordingMetrics) record(kind string, ins metrics.Instrument, attrs map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, metricCall{kind: kind, instrument: ins, attrs: attrs})
}

// snapshot returns a copy of the calls so far.
func (m *recordingMetrics) snapshot() []metricCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.calls)
}

// telemetry is one fresh set of telemetry for a run: the span recorder behind
// the tracer client, the tracer client, and the recording metrics client.
type telemetry struct {
	spans *tracetest.SpanRecorder
	tc    tracer.Client
	mc    *recordingMetrics
}

// newTelemetry returns fresh telemetry. The tracer provider is shut down when
// the test ends.
func newTelemetry(t *testing.T) *telemetry {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return &telemetry{
		spans: sr,
		tc:    tracer.Wrap(tp),
		mc:    &recordingMetrics{Client: metrics.Noop()},
	}
}

// spanShape is what a span shows a telemetry backend, without its IDs and
// times, so two runs can be compared.
type spanShape struct {
	scope     string
	name      string
	kind      trace.SpanKind
	hasParent bool
	code      codes.Code
	desc      string
	attrs     map[string]any
}

// shapeSpans returns the shape of each span, in the order they ended.
func shapeSpans(spans []sdktrace.ReadOnlySpan) []spanShape {
	shapes := make([]spanShape, 0, len(spans))
	for _, s := range spans {
		attrs := make(map[string]any)
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.AsInterface()
		}
		status := s.Status()
		shapes = append(shapes, spanShape{
			scope:     s.InstrumentationScope().Name,
			name:      s.Name(),
			kind:      s.SpanKind(),
			hasParent: s.Parent().IsValid(),
			code:      status.Code,
			desc:      status.Description,
			attrs:     attrs,
		})
	}
	return shapes
}

// sameSpan reports whether two span shapes are equal.
func sameSpan(a, b spanShape) bool {
	return a.scope == b.scope && a.name == b.name && a.kind == b.kind &&
		a.hasParent == b.hasParent && a.code == b.code && a.desc == b.desc &&
		maps.Equal(a.attrs, b.attrs)
}

// sameCall reports whether two metrics calls have the same kind, instrument
// metadata, and attributes.
func sameCall(a, b metricCall) bool {
	return a.kind == b.kind && a.instrument == b.instrument && maps.Equal(a.attrs, b.attrs)
}

// parityRun is what one call path left behind: the outcome of the call, the
// telemetry it emitted, and what the vendor received for each attempt.
type parityRun struct {
	status   int
	err      error
	spans    []spanShape
	calls    []metricCall
	received []parityRequest
}

// parityWant is the outcome that every call path must report for a scenario.
type parityWant struct {
	status   int   // response status, zero when the call fails
	err      error // error that the call must match with errors.Is; nil on success
	attempts int   // attempts sent, which is also the client span and duration record count
}

// sender sends the request pr to url through c and returns the response status,
// or the error of the call.
type sender func(ctx context.Context, c *hclient.Client, url string, pr parityRequest) (int, error)

// sendDo sends pr through c.Do.
func sendDo(ctx context.Context, c *hclient.Client, url string, pr parityRequest) (int, error) {
	req := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(req)
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.SetMethod(pr.method)
	if pr.body != "" {
		req.SetBody([]byte(pr.body))
	}
	if pr.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", pr.idempotencyKey)
	}
	if err := c.Do(ctx, req, resp); err != nil {
		return 0, err
	}
	return resp.StatusCode(), nil
}

// sendRoundTrip sends pr through a net/http client that uses
// client.RoundTripper.
func sendRoundTrip(ctx context.Context, c *hclient.Client, url string, pr parityRequest) (int, error) {
	hc := &http.Client{Transport: client.RoundTripper(c)}
	req, err := http.NewRequestWithContext(ctx, pr.method, url, strings.NewReader(pr.body))
	if err != nil {
		return 0, err
	}
	if pr.idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", pr.idempotencyKey)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// runParity resets the vendor, builds a client with fresh telemetry, sends pr
// through send, and returns what it emitted and what the vendor received.
func runParity(t *testing.T, cfg client.Config, vendor *parityVendor, send sender, pr parityRequest) parityRun {
	t.Helper()
	vendor.reset()
	tel := newTelemetry(t)
	c, err := client.New(cfg, client.WithMetrics(tel.mc), client.WithTracer(tel.tc))
	if err != nil {
		t.Fatalf("client.New(%+v) error = %v", cfg, err)
	}
	t.Cleanup(c.CloseIdleConnections)
	status, err := send(t.Context(), c, vendor.URL+"/v1/invoices?id=42", pr)
	return parityRun{
		status:   status,
		err:      err,
		spans:    shapeSpans(tel.spans.Ended()),
		calls:    tel.mc.snapshot(),
		received: vendor.received(),
	}
}

// checkRun reports where a run differs from the outcome and telemetry that its
// scenario expects. Every attempt must send pr again, with the same body and
// key.
func checkRun(t *testing.T, path string, run parityRun, want parityWant, pr parityRequest) {
	t.Helper()
	if run.status != want.status {
		t.Errorf("%s status = %d, want %d", path, run.status, want.status)
	}
	if !errors.Is(run.err, want.err) {
		t.Errorf("%s err = %v, want %v", path, run.err, want.err)
	}
	if len(run.spans) != want.attempts {
		t.Errorf("%s client spans = %d, want %d", path, len(run.spans), want.attempts)
	}
	if len(run.calls) != want.attempts {
		t.Errorf("%s duration records = %d, want %d", path, len(run.calls), want.attempts)
	}
	if len(run.received) != want.attempts {
		t.Errorf("%s vendor requests = %d, want %d", path, len(run.received), want.attempts)
	}
	for i, got := range run.received {
		if got != pr {
			t.Errorf("%s vendor request %d = %+v, want %+v", path, i, got, pr)
		}
	}
	for _, c := range run.calls {
		if c.kind != "record" || c.instrument.Name != clientDurationName || c.instrument.Unit != clientDurationUnit {
			t.Errorf("%s metric call = %s %+v, want a record of %s in %s",
				path, c.kind, c.instrument, clientDurationName, clientDurationUnit)
		}
	}
}

// compareRuns reports where the RoundTripper run's telemetry differs from the
// Do run's.
func compareRuns(t *testing.T, do, rt parityRun) {
	t.Helper()
	if !slices.EqualFunc(rt.spans, do.spans, sameSpan) {
		t.Errorf("RoundTripper client spans = %+v, want %+v (the Do run)", rt.spans, do.spans)
	}
	if !slices.EqualFunc(rt.calls, do.calls, sameCall) {
		t.Errorf("RoundTripper metric calls = %+v, want %+v (the Do run)", rt.calls, do.calls)
	}
	if !slices.Equal(rt.received, do.received) {
		t.Errorf("RoundTripper vendor requests = %+v, want %+v (the Do run)", rt.received, do.received)
	}
}

// TestClientParity_DoMatchesRoundTripper runs each scenario through c.Do and
// through the RoundTripper, and checks that both paths emit the same client
// spans and duration records.
func TestClientParity_DoMatchesRoundTripper(t *testing.T) {
	get := parityRequest{method: http.MethodGet}
	tests := []struct {
		name     string
		statuses []int
		slow     bool
		cfg      client.Config
		req      parityRequest
		want     parityWant
	}{
		{
			name:     "200",
			statuses: []int{http.StatusOK},
			req:      get,
			want:     parityWant{status: http.StatusOK, attempts: 1},
		},
		{
			name:     "503 twice then 200",
			statuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK},
			cfg:      client.Config{MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond},
			req:      get,
			want:     parityWant{status: http.StatusOK, attempts: 3},
		},
		{
			name:     "POST with Idempotency-Key 503 then 200 sends the same body again",
			statuses: []int{http.StatusServiceUnavailable, http.StatusOK},
			cfg:      client.Config{MaxAttempts: 3, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond},
			req:      parityRequest{method: http.MethodPost, body: `{"invoice":42}`, idempotencyKey: "invoice-42"},
			want:     parityWant{status: http.StatusOK, attempts: 2},
		},
		{
			name: "attempt timeout",
			slow: true,
			cfg:  client.Config{MaxAttempts: 1, AttemptTimeout: 50 * time.Millisecond},
			req:  get,
			want: parityWant{err: context.DeadlineExceeded, attempts: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discardLogs(t)
			vendor := newParityVendor(t, tt.statuses, tt.slow)
			doRun := runParity(t, tt.cfg, vendor, sendDo, tt.req)
			rtRun := runParity(t, tt.cfg, vendor, sendRoundTrip, tt.req)
			checkRun(t, "Do", doRun, tt.want, tt.req)
			checkRun(t, "RoundTripper", rtRun, tt.want, tt.req)
			compareRuns(t, doRun, rtRun)
		})
	}
}

// TestClientToServer_SeparateTraces sends a rest/client call to a rest/server
// route. It checks that the two sides record separate traces, and that the
// request leaves the client with no trace context, baggage, or request ID.
func TestClientToServer_SeparateTraces(t *testing.T) {
	discardLogs(t)
	tel := newTelemetry(t)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	addr := freeAddr(t)
	srv, err := server.New(server.Config{Addr: addr, ShutdownTimeout: 2 * time.Second}, server.WithMetrics(tel.mc), server.WithTracer(tel.tc))
	if err != nil {
		t.Fatalf("server.New error = %v", err)
	}
	// The handler sends what it received before it answers, so a completed
	// call always leaves the value in the buffer.
	received := make(chan map[string]string, 1)
	srv.Hertz().GET("/ping", func(_ context.Context, c *app.RequestContext) {
		received <- map[string]string{
			"traceparent":  string(c.Request.Header.Peek("traceparent")),
			"tracestate":   string(c.Request.Header.Peek("tracestate")),
			"baggage":      string(c.Request.Header.Peek("baggage")),
			"X-Request-ID": string(c.Request.Header.Peek("X-Request-ID")),
		}
		c.String(http.StatusOK, "pong")
	})
	startServer(t, srv, addr)

	// Registered after startServer, so it runs before the server stops.
	c, err := client.New(client.Config{AttemptTimeout: 5 * time.Second}, client.WithMetrics(tel.mc), client.WithTracer(tel.tc))
	if err != nil {
		t.Fatalf("client.New error = %v", err)
	}
	t.Cleanup(c.CloseIdleConnections)

	bag, err := baggage.Parse("tenant=acme")
	if err != nil {
		t.Fatalf("baggage.Parse error = %v", err)
	}
	ctx, caller := tel.tc.Tracer("parity.caller").Start(baggage.ContextWithBaggage(t.Context(), bag), "caller")
	req := protocol.AcquireRequest()
	defer protocol.ReleaseRequest(req)
	resp := protocol.AcquireResponse()
	defer protocol.ReleaseResponse(resp)
	req.SetRequestURI("http://" + addr + "/ping")
	req.SetMethod(http.MethodGet)
	err = c.Do(ctx, req, resp)
	caller.End()
	if err != nil {
		t.Fatalf("c.Do(GET /ping) error = %v", err)
	}
	if got := resp.StatusCode(); got != http.StatusOK {
		t.Errorf("c.Do(GET /ping) status = %d, want %d", got, http.StatusOK)
	}
	requestID := string(resp.Header.Peek("X-Request-ID"))

	var wire map[string]string
	select {
	case wire = <-received:
	default:
		t.Fatal("the server handler did not run")
	}
	for name, value := range wire {
		if value != "" {
			t.Errorf("server received %s = %q, want no such header", name, value)
		}
	}
	if len(requestID) != 26 {
		t.Errorf("response X-Request-ID = %q, want the 26-character ID that the server generated", requestID)
	}

	var clientSpan, serverSpan sdktrace.ReadOnlySpan
	for _, s := range tel.spans.Ended() {
		switch s.InstrumentationScope().Name {
		case "rest.client":
			clientSpan = s
		case "rest.server":
			serverSpan = s
		}
	}
	if clientSpan == nil || serverSpan == nil {
		t.Fatalf("client span found = %t, server span found = %t, want both", clientSpan != nil, serverSpan != nil)
	}
	if got, want := clientSpan.SpanContext().TraceID(), caller.SpanContext().TraceID(); got != want {
		t.Errorf("client span trace ID = %s, want the caller's trace ID %s", got, want)
	}
	if got := serverSpan.SpanContext().TraceID(); got == clientSpan.SpanContext().TraceID() {
		t.Errorf("server span trace ID = %s, want a trace separate from the client span", got)
	}
	if n := len(serverSpan.Links()); n != 0 {
		t.Errorf("server span links = %d, want 0 because the client injects no context", n)
	}
	if serverSpan.Parent().IsValid() {
		t.Errorf("server span parent = %s, want a new root", serverSpan.Parent().SpanID())
	}
}

// discardLogs sends the default logger to io.Discard for the test, so access
// and outcome lines stay out of test output, and restores the logger when the
// test ends.
func discardLogs(t *testing.T) {
	t.Helper()
	l, err := logs.New(identity.Info{Name: "test"}, logs.Config{
		Format:      logs.FormatJSON,
		Level:       "debug",
		Writer:      io.Discard,
		ErrorWriter: io.Discard,
	})
	if err != nil {
		t.Fatalf("logs.New error = %v", err)
	}
	prev := logs.Default()
	logs.SetDefault(l)
	t.Cleanup(func() { logs.SetDefault(prev) })
}

// freeAddr returns a 127.0.0.1 address that was free a moment ago. The caller
// binds it right after, so the window for another process to take it is short.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen error = %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("listener Close error = %v", err)
	}
	return addr
}

// startServer runs srv in a goroutine and waits until it accepts connections
// at addr. The cleanup cancels Run and waits for it to return, so no server
// goroutine outlives the test.
func startServer(t *testing.T, srv *server.Server, addr string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	var runErr error
	go func() {
		defer close(stopped)
		runErr = srv.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
		if runErr != nil {
			t.Errorf("server Run() error = %v, want nil after a graceful shutdown", runErr)
		}
	})

	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", addr); err == nil {
			_ = conn.Close()
			return
		}
		select {
		case <-stopped:
			t.Fatalf("server Run() returned before it accepted connections: %v", runErr)
		case <-deadline.C:
			t.Fatalf("server did not accept connections at %s within 5s", addr)
		case <-tick.C:
		}
	}
}
