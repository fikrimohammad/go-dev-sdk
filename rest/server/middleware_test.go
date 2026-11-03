package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol/http1/resp"
	"github.com/fikrimohammad/go-servicekit/errs"
	"github.com/fikrimohammad/go-servicekit/rest/internal/semconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// echoRequestID is a route handler that writes the request ID that
// RequestIDFrom finds in its context.
func echoRequestID(ctx context.Context, c *app.RequestContext) {
	c.String(http.StatusOK, RequestIDFrom(ctx))
}

// requestIDResponse sends in as X-Request-ID to a route that echoes the
// request ID, and returns the response header and the echoed body.
func requestIDResponse(t *testing.T, in string) (header, body string) {
	t.Helper()
	s := newTestServer(t)
	s.Hertz().GET("/id", echoRequestID)
	rec := perform(s, "GET", "/id", nil, ut.Header{Key: "X-Request-ID", Value: in})
	return rec.Header().Get("X-Request-ID"), rec.Body.String()
}

func TestRequestID_keepsValid(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{name: "dotted underscored ID", in: "req-123.abc_XYZ"},
		{name: "nginx 32-hex ID", in: "0123456789abcdef0123456789abcdef"},
		{name: "64-character ID", in: strings.Repeat("a", 64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, body := requestIDResponse(t, tt.in)
			if header != tt.in {
				t.Errorf("X-Request-ID response header for incoming %q = %q, want the incoming ID", tt.in, header)
			}
			if body != tt.in {
				t.Errorf("RequestIDFrom for incoming %q = %q, want the incoming ID", tt.in, body)
			}
		})
	}
}

func TestRequestID_replacesInvalid(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{name: "empty ID", in: ""},
		{name: "65-character ID", in: strings.Repeat("a", 65)},
		{name: "space", in: "a b"},
		{name: "newline", in: "x\n"},
		{name: "non-ASCII letter", in: "café"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header, body := requestIDResponse(t, tt.in)
			if header == tt.in || len(header) != 26 {
				t.Errorf("X-Request-ID response header for incoming %q = %q, want a generated 26-character ID", tt.in, header)
			}
			if body != header {
				t.Errorf("RequestIDFrom for incoming %q = %q, want the response header %q", tt.in, body, header)
			}
		})
	}
}

func TestRecovery_writesProblem(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "string value", value: "boom"},
		{name: "error value", value: errors.New("boom-error")},
		{name: "nil value", value: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			captureLogs(t)
			s := newTestServer(t)
			s.Hertz().GET("/panic", func(context.Context, *app.RequestContext) {
				panic(tt.value)
			})
			rec := perform(s, "GET", "/panic", nil)

			if rec.Code != http.StatusInternalServerError {
				t.Errorf("GET /panic after panic(%v) status = %d, want %d", tt.value, rec.Code, http.StatusInternalServerError)
			}
			if ct := rec.Header().Get("Content-Type"); ct != problemContentType {
				t.Errorf("GET /panic after panic(%v) Content-Type = %q, want %q", tt.value, ct, problemContentType)
			}
			reqID := rec.Header().Get("X-Request-ID")
			if reqID == "" {
				t.Errorf("GET /panic after panic(%v) X-Request-ID is empty, want a request ID", tt.value)
			}
			got := decodeProblem(t, rec)
			want := map[string]any{
				"title":      "Internal Server Error",
				"status":     float64(http.StatusInternalServerError),
				"request_id": reqID,
			}
			if !maps.Equal(got, want) {
				t.Errorf("GET /panic after panic(%v) body = %v, want %v", tt.value, got, want)
			}
		})
	}
}

func TestRecovery_discardsPartialResponse(t *testing.T) {
	captureLogs(t)
	s := newTestServer(t)
	s.Hertz().GET("/partial", func(_ context.Context, c *app.RequestContext) {
		c.String(http.StatusOK, "partial")
		panic("boom")
	})
	rec := perform(s, "GET", "/partial", nil)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("GET /partial status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if ct := rec.Header().Get("Content-Type"); ct != problemContentType {
		t.Errorf("GET /partial Content-Type = %q, want %q", ct, problemContentType)
	}
	if strings.Contains(rec.Body.String(), "partial") {
		t.Errorf("GET /partial body = %q, want no trace of the discarded partial response", rec.Body.String())
	}
	if got := decodeProblem(t, rec)["status"]; got != float64(http.StatusInternalServerError) {
		t.Errorf("GET /partial body status = %v, want %d", got, http.StatusInternalServerError)
	}
}

func TestRecovery_logsPanicOnce(t *testing.T) {
	buf := captureLogs(t)
	s := newTestServer(t)
	s.Hertz().GET("/panic", func(context.Context, *app.RequestContext) {
		panic("boom-value")
	})
	perform(s, "GET", "/panic", nil)

	var records []map[string]any
	for line := range strings.SplitSeq(buf.String(), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		if rec["msg"] == "panic recovered" {
			records = append(records, rec)
		}
	}
	if len(records) != 1 {
		t.Fatalf("panic recovered records = %d, want 1; log output:\n%s", len(records), buf.String())
	}
	rec := records[0]
	checks := []struct {
		key  string
		want any
	}{
		{key: "level", want: "ERROR"},
		{key: "panic", want: "boom-value"},
		{key: "http.request.method", want: "GET"},
		{key: "url.path", want: "/panic"},
	}
	for _, c := range checks {
		if got := rec[c.key]; got != c.want {
			t.Errorf("panic recovered record %s = %v, want %v", c.key, got, c.want)
		}
	}
	if stack, _ := rec["stack"].(string); !strings.Contains(stack, "middleware_test.go") {
		t.Errorf("panic recovered record stack = %q, want the panicking frame in middleware_test.go", stack)
	}
}

// incomingTraceID and incomingSpanID are the IDs in validTraceparent, which
// the W3C Trace Context spec uses as its example.
const (
	incomingTraceID  = "4bf92f3577b34da6a3ce929d0e0e4736"
	incomingSpanID   = "00f067aa0ba902b7"
	validTraceparent = "00-" + incomingTraceID + "-" + incomingSpanID + "-01"
)

// okHandler is a route handler that answers 200 with a fixed body.
func okHandler(_ context.Context, c *app.RequestContext) {
	c.String(http.StatusOK, "ok")
}

// attributesByKey indexes kvs by key.
func attributesByKey(kvs []attribute.KeyValue) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(kvs))
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

// onlySpan returns the one span that rec has ended. It fails the test if
// there is not exactly one.
func onlySpan(t *testing.T, rec *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	return spans[0]
}

func TestTracing_edgeTrust(t *testing.T) {
	tests := []struct {
		name    string
		headers []ut.Header
	}{
		{name: "no traceparent"},
		{
			name:    "malformed traceparent",
			headers: []ut.Header{{Key: "traceparent", Value: "00-zz-" + incomingSpanID + "-01"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, rec := newTracedServer(t)
			s.Hertz().GET("/users/:id", okHandler)
			perform(s, "GET", "/users/42", nil, tt.headers...)

			span := onlySpan(t, rec)
			if span.Parent().IsValid() {
				t.Errorf("span parent = %v, want no parent (a new root)", span.Parent())
			}
			if got := span.SpanContext().TraceID().String(); got == incomingTraceID {
				t.Errorf("span trace ID = %s, want a new trace ID, not the incoming %s", got, incomingTraceID)
			}
			if n := len(span.Links()); n != 0 {
				t.Errorf("span links = %d, want 0: without a valid traceparent there is no link", n)
			}
		})
	}
}

func TestTracing_validTraceparentLinked(t *testing.T) {
	s, rec := newTracedServer(t)
	s.Hertz().GET("/users/:id", okHandler)
	perform(s, "GET", "/users/42", nil, ut.Header{Key: "traceparent", Value: validTraceparent})

	span := onlySpan(t, rec)
	if span.Parent().IsValid() {
		t.Errorf("span parent = %v, want no parent: the incoming span context is a link", span.Parent())
	}
	if got := span.SpanContext().TraceID().String(); got == incomingTraceID {
		t.Errorf("span trace ID = %s, want a new trace ID, not the incoming %s", got, incomingTraceID)
	}
	links := span.Links()
	if len(links) != 1 {
		t.Fatalf("span links = %d, want 1", len(links))
	}
	link := links[0].SpanContext
	if link.TraceID().String() != incomingTraceID || link.SpanID().String() != incomingSpanID {
		t.Errorf("span link = %s/%s, want %s/%s", link.TraceID(), link.SpanID(), incomingTraceID, incomingSpanID)
	}
}

func TestTracing_ignoresBaggage(t *testing.T) {
	s, rec := newTracedServer(t)
	var members int
	s.Hertz().GET("/users/:id", func(ctx context.Context, c *app.RequestContext) {
		members = baggage.FromContext(ctx).Len()
		c.String(http.StatusOK, "ok")
	})
	perform(s, "GET", "/users/42", nil,
		ut.Header{Key: "traceparent", Value: validTraceparent},
		ut.Header{Key: "baggage", Value: "k=v"},
	)

	if members != 0 {
		t.Errorf("baggage.FromContext in the handler has %d members, want 0", members)
	}
	if _, ok := attributesByKey(onlySpan(t, rec).Attributes())["k"]; ok {
		t.Errorf("span has attribute %q from baggage, want none", "k")
	}
}

// checkAttributes compares the attributes of a span with the complete set in
// want. It reports each missing attribute, each attribute that isn't in want,
// and each value whose type or value differs. A want entry with a zero value
// only requires its key to be present.
func checkAttributes(t *testing.T, got []attribute.KeyValue, want []attribute.KeyValue) {
	t.Helper()
	gotByKey := make(map[attribute.Key]attribute.Value, len(got))
	for _, kv := range got {
		gotByKey[kv.Key] = kv.Value
	}
	wantByKey := make(map[attribute.Key]attribute.Value, len(want))
	for _, kv := range want {
		wantByKey[kv.Key] = kv.Value
	}
	for _, kv := range want {
		g, ok := gotByKey[kv.Key]
		switch {
		case !ok:
			t.Errorf("attribute %s is missing, want %s (%v)", kv.Key, kv.Value.String(), kv.Value.Type())
		case kv.Value.Type() == attribute.EMPTY:
			// Present with any value.
		case g.Type() != kv.Value.Type() || g.String() != kv.Value.String():
			t.Errorf("attribute %s = %s (%v), want %s (%v)", kv.Key, g.String(), g.Type(), kv.Value.String(), kv.Value.Type())
		}
	}
	for _, kv := range got {
		if _, ok := wantByKey[kv.Key]; !ok {
			t.Errorf("attribute %s = %s (%v) is unexpected, want no such attribute", kv.Key, kv.Value.String(), kv.Value.Type())
		}
	}
}

func TestTracing_spanContract(t *testing.T) {
	// utPeer requires network.peer.address without checking its value. ut fixes
	// the remote address to 0.0.0.0, which is ut's detail and not part of the
	// contract. The socket row checks the real address exactly.
	utPeer := attribute.KeyValue{Key: otelsemconv.NetworkPeerAddressKey}

	socketAddr := freeAddr(t)
	_, socketPortText, err := net.SplitHostPort(socketAddr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q) error = %v", socketAddr, err)
	}
	socketPort, err := strconv.Atoi(socketPortText)
	if err != nil {
		t.Fatalf("port of %q error = %v", socketAddr, err)
	}

	tests := []struct {
		name       string
		cfg        Config
		setup      func(s *Server)
		send       func(t *testing.T, s *Server) int
		wantName   string
		wantStatus int
		wantCode   codes.Code
		wantAttrs  []attribute.KeyValue
	}{
		{
			name: "matched route",
			setup: func(s *Server) {
				s.Hertz().GET("/users/:id", okHandler)
			},
			send: sendUT("GET", "/users/42",
				ut.Header{Key: "Host", Value: "api.example.com:8443"},
				ut.Header{Key: "User-Agent", Value: "test-agent/1.0"},
				ut.Header{Key: "X-Request-ID", Value: "req-contract"},
			),
			wantName:   "GET /users/:id",
			wantStatus: http.StatusOK,
			wantCode:   codes.Unset,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLPathKey.String("/users/42"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/users/:id"),
				otelsemconv.ServerAddressKey.String("api.example.com"),
				otelsemconv.ServerPortKey.Int(8443),
				utPeer,
				otelsemconv.UserAgentOriginalKey.String("test-agent/1.0"),
				otelsemconv.HTTPRequestHeader("x-request-id", "req-contract"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
			},
		},
		{
			// Without a Host header, the request has no server.address or
			// server.port.
			name:       "unknown path without Host header",
			send:       sendUT("GET", "/nope", ut.Header{Key: "X-Request-ID", Value: "req-404"}),
			wantName:   "GET",
			wantStatus: http.StatusNotFound,
			wantCode:   codes.Unset,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLPathKey.String("/nope"),
				otelsemconv.URLSchemeKey.String("http"),
				utPeer,
				otelsemconv.HTTPRequestHeader("x-request-id", "req-404"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusNotFound),
			},
		},
		{
			name:       "unknown method",
			send:       sendUT("PROPFIND", "/nope", ut.Header{Key: "X-Request-ID", Value: "req-405"}),
			wantName:   "HTTP",
			wantStatus: http.StatusNotFound,
			wantCode:   codes.Unset,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("_OTHER"),
				otelsemconv.HTTPRequestMethodOriginalKey.String("PROPFIND"),
				otelsemconv.URLPathKey.String("/nope"),
				otelsemconv.URLSchemeKey.String("http"),
				utPeer,
				otelsemconv.HTTPRequestHeader("x-request-id", "req-405"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusNotFound),
			},
		},
		{
			name: "400 with errs code",
			setup: func(s *Server) {
				s.Hertz().GET("/bad", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusBadRequest, errs.New(1001, "bad"))
				})
			},
			send:       sendUT("GET", "/bad", ut.Header{Key: "X-Request-ID", Value: "req-400"}),
			wantName:   "GET /bad",
			wantStatus: http.StatusBadRequest,
			wantCode:   codes.Unset,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLPathKey.String("/bad"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/bad"),
				utPeer,
				otelsemconv.HTTPRequestHeader("x-request-id", "req-400"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusBadRequest),
				attribute.Int(semconv.ErrsCodeKey, 1001),
			},
		},
		{
			name: "500 with errs code",
			setup: func(s *Server) {
				s.Hertz().GET("/boom", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusInternalServerError, errs.New(5001, "x"))
				})
			},
			send:       sendUT("GET", "/boom", ut.Header{Key: "X-Request-ID", Value: "req-500"}),
			wantName:   "GET /boom",
			wantStatus: http.StatusInternalServerError,
			wantCode:   codes.Error,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLPathKey.String("/boom"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/boom"),
				utPeer,
				otelsemconv.HTTPRequestHeader("x-request-id", "req-500"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusInternalServerError),
				otelsemconv.ErrorTypeKey.String("500"),
				attribute.Int(semconv.ErrsCodeKey, 5001),
			},
		},
		{
			name: "500 without errs code",
			setup: func(s *Server) {
				s.Hertz().GET("/plain", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusInternalServerError, errors.New("plain"))
				})
			},
			send:       sendUT("GET", "/plain", ut.Header{Key: "X-Request-ID", Value: "req-plain"}),
			wantName:   "GET /plain",
			wantStatus: http.StatusInternalServerError,
			wantCode:   codes.Error,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLPathKey.String("/plain"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/plain"),
				utPeer,
				otelsemconv.HTTPRequestHeader("x-request-id", "req-plain"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusInternalServerError),
				otelsemconv.ErrorTypeKey.String("500"),
			},
		},
		{
			// The only row that listens on a socket. It checks every value,
			// including the peer address and protocol version that ut doesn't
			// set.
			name:  "real socket",
			cfg:   Config{Addr: socketAddr},
			setup: func(s *Server) { s.Hertz().GET("/users/:id", okHandler) },
			send: sendOverSocket("GET", "/users/42",
				ut.Header{Key: "User-Agent", Value: "socket-agent/1.0"},
				ut.Header{Key: "X-Request-ID", Value: "req-socket"},
			),
			wantName:   "GET /users/:id",
			wantStatus: http.StatusOK,
			wantCode:   codes.Unset,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLPathKey.String("/users/42"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/users/:id"),
				otelsemconv.ServerAddressKey.String("127.0.0.1"),
				otelsemconv.ServerPortKey.Int(socketPort),
				otelsemconv.NetworkPeerAddressKey.String("127.0.0.1"),
				otelsemconv.NetworkProtocolVersionKey.String("1.1"),
				otelsemconv.UserAgentOriginalKey.String("socket-agent/1.0"),
				otelsemconv.HTTPRequestHeader("x-request-id", "req-socket"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, rec := newTracedServerCfg(t, tt.cfg)
			if tt.setup != nil {
				tt.setup(s)
			}
			if got := tt.send(t, s); got != tt.wantStatus {
				t.Errorf("%s response status = %d, want %d", tt.name, got, tt.wantStatus)
			}

			span := onlySpan(t, rec)
			if got := span.Name(); got != tt.wantName {
				t.Errorf("span name = %q, want %q", got, tt.wantName)
			}
			if got := span.SpanKind(); got != trace.SpanKindServer {
				t.Errorf("span kind = %v, want %v", got, trace.SpanKindServer)
			}
			if got := span.InstrumentationScope().Name; got != tracerScope {
				t.Errorf("instrumentation scope = %q, want %q", got, tracerScope)
			}
			if got := span.Status(); got.Code != tt.wantCode || got.Description != "" {
				t.Errorf("span status = %v %q, want %v with no description", got.Code, got.Description, tt.wantCode)
			}
			checkAttributes(t, span.Attributes(), tt.wantAttrs)
		})
	}
}

func TestTracing_schemeFromAbsoluteTarget(t *testing.T) {
	s, rec := newTracedServer(t)
	s.Hertz().GET("/users/:id", okHandler)
	perform(s, "GET", "zzz9://evil/users/42", nil)

	got := attributesByKey(onlySpan(t, rec).Attributes())["url.scheme"].AsString()
	if got != "http" {
		t.Errorf("url.scheme for absolute-form target zzz9://evil/users/42 = %q, want %q", got, "http")
	}
}

func TestTracing_requestIDAttribute(t *testing.T) {
	s, rec := newTracedServer(t)
	s.Hertz().GET("/users/:id", okHandler)
	perform(s, "GET", "/users/42", nil, ut.Header{Key: "X-Request-ID", Value: "req-123"})

	got := attributesByKey(onlySpan(t, rec).Attributes())["http.request.header.x-request-id"]
	want := attribute.StringSlice("http.request.header.x-request-id", []string{"req-123"}).Value
	if got.Type() != want.Type() || got.String() != want.String() {
		t.Errorf("attribute http.request.header.x-request-id = %v (%v), want %v (%v)", got.String(), got.Type(), want.String(), want.Type())
	}
}

func TestTracing_requestIDAttributeReplacesInvalid(t *testing.T) {
	s, rec := newTracedServer(t)
	s.Hertz().GET("/users/:id", okHandler)
	recorded := perform(s, "GET", "/users/42", nil, ut.Header{Key: "X-Request-ID", Value: "bad id"})

	header := recorded.Header().Get("X-Request-ID")
	if len(header) != 26 {
		t.Errorf("X-Request-ID response header for incoming %q = %q, want a generated 26-character ID", "bad id", header)
	}
	got := attributesByKey(onlySpan(t, rec).Attributes())["http.request.header.x-request-id"].AsStringSlice()
	if !slices.Equal(got, []string{header}) {
		t.Errorf("attribute http.request.header.x-request-id = %q, want the generated ID %q", got, header)
	}
	if slices.Contains(got, "bad id") {
		t.Errorf("attribute http.request.header.x-request-id = %q, want the invalid incoming ID dropped", got)
	}
}

func TestRecovery_streamedResponseDropsConnection(t *testing.T) {
	buf := captureLogs(t)
	s := newTestServerCfg(t, Config{Addr: freeAddr(t)})
	s.Hertz().GET("/stream", func(_ context.Context, c *app.RequestContext) {
		w := resp.NewChunkedBodyWriter(&c.Response, c.GetWriter())
		c.Response.HijackWriter(w)
		if _, err := w.Write([]byte("chunk-1")); err != nil {
			t.Errorf("write first chunk error = %v", err)
		}
		if err := w.Flush(); err != nil {
			t.Errorf("flush first chunk error = %v", err)
		}
		panic("mid-stream")
	})
	startServer(t, s)

	body := rawExchange(t, s.cfg.Addr, "GET /stream HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
	if !strings.Contains(body, "chunk-1") {
		t.Errorf("streamed response = %q, want the chunk written before the panic", body)
	}
	if n := strings.Count(body, "HTTP/1.1 "); n != 1 {
		t.Errorf("status lines in %q = %d, want 1: a 500 must not follow the streamed 200", body, n)
	}
	if strings.Contains(body, "HTTP/1.1 500") {
		t.Errorf("streamed response = %q, want no second HTTP/1.1 500 status line", body)
	}
	if strings.HasSuffix(body, "0\r\n\r\n") {
		t.Errorf("streamed response = %q, want no terminating chunk, so the client sees a truncated response", body)
	}
	if !strings.Contains(buf.String(), "panic recovered") {
		t.Errorf("log output = %q, want a panic recovered record", buf.String())
	}
}

func TestRecovery_keepsCORSHeaders(t *testing.T) {
	captureLogs(t)
	s := newTestServer(t)
	s.Hertz().GET("/panic", func(context.Context, *app.RequestContext) {
		panic("boom")
	})
	s.Hertz().GET("/cors", func(_ context.Context, c *app.RequestContext) {
		c.Response.Header.Set("Access-Control-Allow-Origin", "https://app.example")
		c.Response.Header.Set("access-control-allow-credentials", "true")
		c.Response.Header.Set("X-Vendor", "dropped")
		panic("boom")
	})
	rec := perform(s, "GET", "/cors", nil)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Errorf("GET /cors Access-Control-Allow-Origin = %q, want %q", got, "https://app.example")
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("GET /cors Access-Control-Allow-Credentials = %q, want %q", got, "true")
	}
	if got := rec.Header().Get("X-Vendor"); got != "" {
		t.Errorf("GET /cors X-Vendor = %q, want the header cleared by recovery", got)
	}
	if got := rec.Header().Get("X-Request-ID"); got == "" {
		t.Errorf("GET /cors X-Request-ID is empty, want the request ID kept")
	}
	if ct := rec.Header().Get("Content-Type"); ct != problemContentType {
		t.Errorf("GET /cors Content-Type = %q, want %q", ct, problemContentType)
	}
}

func TestRequestID_problemIgnoresHandlerHeader(t *testing.T) {
	s := newTestServer(t)
	var ours string
	s.Hertz().GET("/proxy", func(ctx context.Context, c *app.RequestContext) {
		ours = RequestIDFrom(ctx)
		c.Response.Header.Set(requestIDHeader, "vendor-id")
		Error(c, http.StatusBadGateway, errs.New(0, "upstream"))
	})
	rec := perform(s, "GET", "/proxy", nil)

	if ours == "" {
		t.Fatal("RequestIDFrom in the handler is empty, want a request ID")
	}
	if got := decodeProblem(t, rec)["request_id"]; got != ours {
		t.Errorf("GET /proxy problem request_id = %v, want %q", got, ours)
	}
	if got := rec.Header().Get(requestIDHeader); got != ours {
		t.Errorf("GET /proxy X-Request-ID header = %q, want %q", got, ours)
	}
}

func TestRecovery_problemSurvivesHeaderReset(t *testing.T) {
	captureLogs(t)
	s := newTestServer(t)
	var ours string
	s.Hertz().GET("/panic", func(ctx context.Context, c *app.RequestContext) {
		ours = RequestIDFrom(ctx)
		c.Response.Header.Reset()
		panic("boom")
	})
	rec := perform(s, "GET", "/panic", nil)

	if ours == "" {
		t.Fatal("RequestIDFrom in the handler is empty, want a request ID")
	}
	if got := decodeProblem(t, rec)["request_id"]; got != ours {
		t.Errorf("GET /panic problem request_id = %v, want %q", got, ours)
	}
	if got := rec.Header().Get(requestIDHeader); got != ours {
		t.Errorf("GET /panic X-Request-ID header = %q, want %q", got, ours)
	}
}

func TestRecovery_recordsExceptionOnSpan(t *testing.T) {
	captureLogs(t)
	s, rec := newTracedServer(t)
	s.Hertz().GET("/panic", func(context.Context, *app.RequestContext) {
		panic("boom")
	})
	perform(s, "GET", "/panic", nil)

	span := onlySpan(t, rec)
	if got := span.Status(); got.Code != codes.Error || got.Description != "" {
		t.Errorf("span status = %v %q, want %v with no description", got.Code, got.Description, codes.Error)
	}
	if got := attributesByKey(span.Attributes())["error.type"].String(); got != "500" {
		t.Errorf("span error.type = %q, want %q", got, "500")
	}

	var events []sdktrace.Event
	for _, ev := range span.Events() {
		if ev.Name == "exception" {
			events = append(events, ev)
		}
	}
	if len(events) != 1 {
		t.Fatalf("exception events = %d, want 1", len(events))
	}
	ev := attributesByKey(events[0].Attributes)
	if got := ev["exception.type"].String(); got != "string" {
		t.Errorf("exception.type = %q, want %q", got, "string")
	}
	if got := ev["exception.message"].String(); got != "boom" {
		t.Errorf("exception.message = %q, want %q", got, "boom")
	}
	if stack := ev["exception.stacktrace"].String(); !strings.Contains(stack, "middleware_test.go") {
		t.Errorf("exception.stacktrace = %q, want the panicking frame in middleware_test.go", stack)
	}
}

// metricAttributes converts the attributes of one metrics call to key-values in
// key order. Each value keeps its Go type, so checkAttributes can compare types:
// a string gives a string attribute, an int an int attribute, and any other
// type a string that names the type, which can't match the expected value.
func metricAttributes(attrs map[string]any) []attribute.KeyValue {
	kvs := make([]attribute.KeyValue, 0, len(attrs))
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		switch v := attrs[k].(type) {
		case string:
			kvs = append(kvs, attribute.String(k, v))
		case int:
			kvs = append(kvs, attribute.Int(k, v))
		default:
			kvs = append(kvs, attribute.String(k, fmt.Sprintf("%T(%v)", v, v)))
		}
	}
	return kvs
}

// selectAttributes returns the key-values in kvs whose keys are in keys.
func selectAttributes(kvs []attribute.KeyValue, keys ...attribute.Key) []attribute.KeyValue {
	var out []attribute.KeyValue
	for _, kv := range kvs {
		if slices.Contains(keys, kv.Key) {
			out = append(out, kv)
		}
	}
	return out
}

// checkRequestMetrics checks what one request recorded in mc. It expects exactly
// one http.server.request.duration measurement, with the complete attribute set
// wantAttrs and a non-negative value. It also expects http.server.active_requests
// to change by +1 and then by -1, both with only the method and scheme from
// wantAttrs.
func checkRequestMetrics(t *testing.T, mc *recordingMetrics, wantAttrs []attribute.KeyValue) {
	t.Helper()
	records := mc.Records()
	if len(records) != 1 {
		t.Fatalf("duration records = %d, want 1", len(records))
	}
	duration := records[0]
	if duration.instrument != semconv.ServerRequestDuration {
		t.Errorf("duration instrument = %+v, want %+v", duration.instrument, semconv.ServerRequestDuration)
	}
	if duration.value < 0 {
		t.Errorf("duration value = %v, want a non-negative value", duration.value)
	}
	checkAttributes(t, metricAttributes(duration.attrs), wantAttrs)

	ups := mc.UpDowns()
	if len(ups) != 2 {
		t.Fatalf("active-requests changes = %d, want 2 (+1 and -1)", len(ups))
	}
	wantActive := selectAttributes(wantAttrs, otelsemconv.HTTPRequestMethodKey, otelsemconv.URLSchemeKey)
	for i, wantValue := range []float64{1, -1} {
		up := ups[i]
		if up.instrument != semconv.ServerActiveRequests {
			t.Errorf("active-requests change %d instrument = %+v, want %+v", i, up.instrument, semconv.ServerActiveRequests)
		}
		if up.value != wantValue {
			t.Errorf("active-requests change %d value = %v, want %v", i, up.value, wantValue)
		}
		checkAttributes(t, metricAttributes(up.attrs), wantActive)
	}
}

func TestMeasure_contract(t *testing.T) {
	socketAddr := freeAddr(t)

	tests := []struct {
		name       string
		cfg        Config
		setup      func(s *Server)
		send       func(t *testing.T, s *Server) int
		wantStatus int
		wantAttrs  []attribute.KeyValue
	}{
		{
			name: "matched route",
			setup: func(s *Server) {
				s.Hertz().GET("/users/:id", okHandler)
			},
			send:       sendUT("GET", "/users/42"),
			wantStatus: http.StatusOK,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/users/:id"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
			},
		},
		{
			// No route matched, so there is no http.route.
			name:       "unknown path",
			send:       sendUT("GET", "/nope"),
			wantStatus: http.StatusNotFound,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusNotFound),
			},
		},
		{
			name:       "unknown method",
			send:       sendUT("PROPFIND", "/nope"),
			wantStatus: http.StatusNotFound,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("_OTHER"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusNotFound),
			},
		},
		{
			// A 4xx with an errs code has no error.type, but has errs.code.
			name: "400 with errs code",
			setup: func(s *Server) {
				s.Hertz().GET("/bad", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusBadRequest, errs.New(1001, "bad"))
				})
			},
			send:       sendUT("GET", "/bad"),
			wantStatus: http.StatusBadRequest,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/bad"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusBadRequest),
				attribute.Int(semconv.ErrsCodeKey, 1001),
			},
		},
		{
			name: "500 with errs code",
			setup: func(s *Server) {
				s.Hertz().GET("/boom", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusInternalServerError, errs.New(5001, "x"))
				})
			},
			send:       sendUT("GET", "/boom"),
			wantStatus: http.StatusInternalServerError,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/boom"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusInternalServerError),
				otelsemconv.ErrorTypeKey.String("500"),
				attribute.Int(semconv.ErrsCodeKey, 5001),
			},
		},
		{
			name: "500 without errs code",
			setup: func(s *Server) {
				s.Hertz().GET("/plain", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusInternalServerError, errors.New("plain"))
				})
			},
			send:       sendUT("GET", "/plain"),
			wantStatus: http.StatusInternalServerError,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/plain"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusInternalServerError),
				otelsemconv.ErrorTypeKey.String("500"),
			},
		},
		{
			// An absolute-form target can carry any scheme, and it is folded to http.
			name: "absolute-form target",
			setup: func(s *Server) {
				s.Hertz().GET("/x", okHandler)
			},
			send:       sendUT("GET", "zzz9://evil/x"),
			wantStatus: http.StatusOK,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/x"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
			},
		},
		{
			// The only row that listens on a socket. It is the only one where
			// ut's missing protocol version is replaced by the real one.
			name:  "real socket",
			cfg:   Config{Addr: socketAddr},
			setup: func(s *Server) { s.Hertz().GET("/users/:id", okHandler) },
			send: sendOverSocket("GET", "/users/42",
				ut.Header{Key: "User-Agent", Value: "socket-agent/1.0"},
			),
			wantStatus: http.StatusOK,
			wantAttrs: []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/users/:id"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
				otelsemconv.NetworkProtocolVersionKey.String("1.1"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, mc, _ := newTelemetryServer(t, tt.cfg)
			if tt.setup != nil {
				tt.setup(s)
			}
			if got := tt.send(t, s); got != tt.wantStatus {
				t.Errorf("%s response status = %d, want %d", tt.name, got, tt.wantStatus)
			}
			checkRequestMetrics(t, mc, tt.wantAttrs)
		})
	}
}

// logLine is one JSON log record: the raw line, which is needed to count keys,
// and its decoded fields.
type logLine struct {
	raw    string
	fields map[string]any
}

// logLines decodes every line in buf. It fails the test if a line is not JSON.
func logLines(t *testing.T, buf *logBuffer) []logLine {
	t.Helper()
	var lines []logLine
	for raw := range strings.SplitSeq(buf.String(), "\n") {
		if raw == "" {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatalf("log line %q is not JSON: %v", raw, err)
		}
		lines = append(lines, logLine{raw: raw, fields: fields})
	}
	return lines
}

// requestLog returns the one access-log line, the record with msg "request". It
// fails the test if there is not exactly one.
func requestLog(t *testing.T, buf *logBuffer) logLine {
	t.Helper()
	var found []logLine
	for _, l := range logLines(t, buf) {
		if l.fields["msg"] == "request" {
			found = append(found, l)
		}
	}
	if len(found) != 1 {
		t.Fatalf("request log lines = %d, want 1; log output:\n%s", len(found), buf.String())
	}
	return found[0]
}

func TestLogging_levelsAndFields(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(s *Server)
		method     string
		path       string
		wantStatus int
		wantLevel  string
		wantRoute  string
	}{
		{
			name: "200 is INFO",
			setup: func(s *Server) {
				s.Hertz().GET("/users/:id", okHandler)
			},
			method:     "GET",
			path:       "/users/42",
			wantStatus: http.StatusOK,
			wantLevel:  "INFO",
			wantRoute:  "/users/:id",
		},
		{
			name:       "404 is WARN and has no route",
			method:     "GET",
			path:       "/nope",
			wantStatus: http.StatusNotFound,
			wantLevel:  "WARN",
		},
		{
			name: "500 is ERROR",
			setup: func(s *Server) {
				s.Hertz().GET("/boom", func(_ context.Context, c *app.RequestContext) {
					Error(c, http.StatusInternalServerError, errors.New("plain"))
				})
			},
			method:     "GET",
			path:       "/boom",
			wantStatus: http.StatusInternalServerError,
			wantLevel:  "ERROR",
			wantRoute:  "/boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			s, _, rec := newTelemetryServer(t, Config{})
			if tt.setup != nil {
				tt.setup(s)
			}
			recorded := perform(s, tt.method, tt.path, nil, ut.Header{Key: "X-Request-ID", Value: "req-log"})
			if recorded.Code != tt.wantStatus {
				t.Fatalf("%s %s status = %d, want %d", tt.method, tt.path, recorded.Code, tt.wantStatus)
			}

			line := requestLog(t, buf)
			fields := line.fields
			checks := []struct {
				key  string
				want any
			}{
				{key: "level", want: tt.wantLevel},
				{key: "http.request.method", want: tt.method},
				{key: "url.path", want: tt.path},
				{key: "http.response.status_code", want: float64(tt.wantStatus)},
				{key: "request.id", want: "req-log"},
			}
			for _, c := range checks {
				if got := fields[c.key]; got != c.want {
					t.Errorf("request log %s = %v (%T), want %v", c.key, got, got, c.want)
				}
			}
			if _, ok := fields["duration"].(float64); !ok {
				t.Errorf("request log duration = %v (%T), want a number of seconds", fields["duration"], fields["duration"])
			}
			if tt.wantRoute == "" {
				if _, ok := fields["http.route"]; ok {
					t.Errorf("request log has http.route = %v, want none for an unmatched request", fields["http.route"])
				}
			} else if got := fields["http.route"]; got != tt.wantRoute {
				t.Errorf("request log http.route = %v, want %q", got, tt.wantRoute)
			}
			for _, key := range []string{"request_id", "method", "path", "route", "status"} {
				if _, ok := fields[key]; ok {
					t.Errorf("request log has key %q, want the semconv name only", key)
				}
			}

			// logs adds trace_id from the span context. The line must carry it
			// exactly once, and it must be the span's own trace.
			if n := strings.Count(line.raw, `"trace_id":`); n != 1 {
				t.Errorf("request log has %d trace_id keys, want 1: %s", n, line.raw)
			}
			span := onlySpan(t, rec)
			if got := fields["trace_id"]; got != span.SpanContext().TraceID().String() {
				t.Errorf("request log trace_id = %v, want the span trace ID %s", got, span.SpanContext().TraceID())
			}
		})
	}
}

func TestLogging_boundErrorUnderError(t *testing.T) {
	buf := captureLogs(t)
	s, _, _ := newTelemetryServer(t, Config{})
	s.Hertz().GET("/bound", func(_ context.Context, c *app.RequestContext) {
		Error(c, http.StatusBadRequest, errs.New(1001, "bad amount"))
	})
	perform(s, "GET", "/bound", nil)

	fields := requestLog(t, buf).fields
	entry, ok := fields["error"].(map[string]any)
	if !ok {
		t.Fatalf("request log error = %v (%T), want an object; log output:\n%s", fields["error"], fields["error"], buf.String())
	}
	checkErrorCode(t, entry, 1001)
}

func TestLogging_boundErrorsUnderErrors(t *testing.T) {
	buf := captureLogs(t)
	s, _, _ := newTelemetryServer(t, Config{})
	s.Hertz().GET("/bound", func(_ context.Context, c *app.RequestContext) {
		_ = c.Error(errs.New(1001, "first"))
		Error(c, http.StatusBadRequest, errs.New(1002, "second"))
	})
	perform(s, "GET", "/bound", nil)

	fields := requestLog(t, buf).fields
	holder, ok := fields["errors"].(map[string]any)
	if !ok {
		t.Fatalf("request log errors = %v (%T), want an object; log output:\n%s", fields["errors"], fields["errors"], buf.String())
	}
	for i, want := range []float64{1001, 1002} {
		entry, ok := holder[strconv.Itoa(i)].(map[string]any)
		if !ok {
			t.Fatalf("request log errors[%d] = %v (%T), want an object", i, holder[strconv.Itoa(i)], holder[strconv.Itoa(i)])
		}
		checkErrorCode(t, entry, want)
	}
}

// checkErrorCode checks that one logged errs error carries the code want.
func checkErrorCode(t *testing.T, entry map[string]any, want float64) {
	t.Helper()
	if got := entry["code"]; got != want {
		t.Errorf("logged errs code = %v (%T), want %v; entry %v", got, got, want, entry)
	}
}

func TestRecovery_fullStack(t *testing.T) {
	buf := captureLogs(t)
	s, mc, rec := newTelemetryServer(t, Config{})
	s.Hertz().GET("/panic", func(context.Context, *app.RequestContext) {
		panic("boom")
	})
	recorded := perform(s, "GET", "/panic", nil, ut.Header{Key: "X-Request-ID", Value: "req-panic"})

	if recorded.Code != http.StatusInternalServerError {
		t.Fatalf("GET /panic status = %d, want %d", recorded.Code, http.StatusInternalServerError)
	}
	if got := decodeProblem(t, recorded)["request_id"]; got != recorded.Header().Get(requestIDHeader) {
		t.Errorf("GET /panic problem request_id = %v, want the X-Request-ID header %q", got, recorded.Header().Get(requestIDHeader))
	}

	checkRequestMetrics(t, mc, []attribute.KeyValue{
		otelsemconv.HTTPRequestMethodKey.String("GET"),
		otelsemconv.URLSchemeKey.String("http"),
		otelsemconv.HTTPRouteKey.String("/panic"),
		otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusInternalServerError),
		otelsemconv.ErrorTypeKey.String("500"),
	})
	var sum int64
	for _, up := range mc.UpDowns() {
		sum += int64(up.value)
	}
	if sum != 0 {
		t.Errorf("active requests net change after the panic = %d, want 0", sum)
	}

	span := onlySpan(t, rec)
	if got := span.Status(); got.Code != codes.Error {
		t.Errorf("span status = %v, want %v", got.Code, codes.Error)
	}
	exceptions := 0
	for _, ev := range span.Events() {
		if ev.Name == "exception" {
			exceptions++
		}
	}
	if exceptions != 1 {
		t.Errorf("span exception events = %d, want 1", exceptions)
	}

	line := requestLog(t, buf)
	if got := line.fields["level"]; got != "ERROR" {
		t.Errorf("access log level = %v, want ERROR", got)
	}
	if got := line.fields["http.response.status_code"]; got != float64(http.StatusInternalServerError) {
		t.Errorf("access log http.response.status_code = %v, want %d", got, http.StatusInternalServerError)
	}
	panics := 0
	for _, l := range logLines(t, buf) {
		if l.fields["msg"] == "panic recovered" {
			panics++
		}
	}
	if panics != 1 {
		t.Errorf("panic recovered log lines = %d, want 1", panics)
	}
}

func TestHertz_teamMiddlewareTelemetry(t *testing.T) {
	tests := []struct {
		name       string
		team       app.HandlerFunc
		wantStatus int
		wantLevel  string
	}{
		{
			name: "rejects with 401",
			team: func(_ context.Context, c *app.RequestContext) {
				Error(c, http.StatusUnauthorized, nil)
			},
			wantStatus: http.StatusUnauthorized,
			wantLevel:  "WARN",
		},
		{
			name: "panic is recovered as 500",
			team: func(context.Context, *app.RequestContext) {
				panic("team boom")
			},
			wantStatus: http.StatusInternalServerError,
			wantLevel:  "ERROR",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			s, mc, _ := newTelemetryServer(t, Config{})
			s.Hertz().Use(tt.team)
			reached := false
			s.Hertz().GET("/team", func(_ context.Context, c *app.RequestContext) {
				reached = true
				c.String(http.StatusOK, "ok")
			})
			recorded := perform(s, "GET", "/team", nil)

			if recorded.Code != tt.wantStatus {
				t.Errorf("GET /team status = %d, want %d", recorded.Code, tt.wantStatus)
			}
			if reached {
				t.Errorf("GET /team handler ran, want team middleware to stop it")
			}

			wantAttrs := []attribute.KeyValue{
				otelsemconv.HTTPRequestMethodKey.String("GET"),
				otelsemconv.URLSchemeKey.String("http"),
				otelsemconv.HTTPRouteKey.String("/team"),
				otelsemconv.HTTPResponseStatusCodeKey.Int(tt.wantStatus),
			}
			if tt.wantStatus >= http.StatusInternalServerError {
				wantAttrs = append(wantAttrs, otelsemconv.ErrorTypeKey.String(strconv.Itoa(tt.wantStatus)))
			}
			checkRequestMetrics(t, mc, wantAttrs)

			line := requestLog(t, buf)
			if got := line.fields["level"]; got != tt.wantLevel {
				t.Errorf("access log level = %v, want %s", got, tt.wantLevel)
			}
			if got := line.fields["http.response.status_code"]; got != float64(tt.wantStatus) {
				t.Errorf("access log http.response.status_code = %v, want %d", got, tt.wantStatus)
			}
		})
	}
}

func TestBoundErrsCode_firstNonZeroCodeWins(t *testing.T) {
	s, mc, rec := newTelemetryServer(t, Config{})
	s.Hertz().GET("/first", func(_ context.Context, c *app.RequestContext) {
		_ = c.Error(errs.New(1001, "a"))
		Error(c, http.StatusInternalServerError, errs.New(5001, "b"))
	})
	perform(s, "GET", "/first", nil)

	span := onlySpan(t, rec)
	if v, ok := attributesByKey(span.Attributes())[semconv.ErrsCodeKey]; !ok || v.AsInt64() != 1001 {
		t.Errorf("span errs.code = %v (present %t), want 1001: the first bound error with a code wins", v.AsInt64(), ok)
	}
	checkRequestMetrics(t, mc, []attribute.KeyValue{
		otelsemconv.HTTPRequestMethodKey.String("GET"),
		otelsemconv.URLSchemeKey.String("http"),
		otelsemconv.HTTPRouteKey.String("/first"),
		otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusInternalServerError),
		otelsemconv.ErrorTypeKey.String("500"),
		attribute.Int(semconv.ErrsCodeKey, 1001),
	})
}

func TestMeasure_durationInSeconds(t *testing.T) {
	buf := captureLogs(t)
	s, mc, _ := newTelemetryServer(t, Config{})
	const wait = 20 * time.Millisecond
	s.Hertz().GET("/slow", func(ctx context.Context, c *app.RequestContext) {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		c.String(http.StatusOK, "ok")
	})
	perform(s, "GET", "/slow", nil)

	lo, hi := wait.Seconds(), testTimeout.Seconds()
	records := mc.Records()
	if len(records) != 1 {
		t.Fatalf("duration records = %d, want 1", len(records))
	}
	if got := records[0].value; got < lo || got >= hi {
		t.Errorf("http.server.request.duration = %v, want seconds in [%v, %v)", got, lo, hi)
	}
	got, _ := requestLog(t, buf).fields["duration"].(float64)
	if got < lo || got >= hi {
		t.Errorf("request log duration = %v, want seconds in [%v, %v)", got, lo, hi)
	}
}

func TestBoundErrsCode_reportedAtAnyStatus(t *testing.T) {
	buf := captureLogs(t)
	s, mc, rec := newTelemetryServer(t, Config{})
	s.Hertz().GET("/soft", func(_ context.Context, c *app.RequestContext) {
		_ = c.Error(errs.New(1001, "soft"))
		c.String(http.StatusOK, "ok")
	})
	perform(s, "GET", "/soft", nil)

	span := onlySpan(t, rec)
	attrs := attributesByKey(span.Attributes())
	if v, ok := attrs[semconv.ErrsCodeKey]; !ok || v.AsInt64() != 1001 {
		t.Errorf("span errs.code = %v (present %t), want 1001 at status 200", v.AsInt64(), ok)
	}
	if _, ok := attrs[string(otelsemconv.ErrorTypeKey)]; ok {
		t.Errorf("span has error.type at status 200, want none")
	}
	if got := span.Status().Code; got != codes.Unset {
		t.Errorf("span status = %v at status 200, want %v", got, codes.Unset)
	}
	checkRequestMetrics(t, mc, []attribute.KeyValue{
		otelsemconv.HTTPRequestMethodKey.String("GET"),
		otelsemconv.URLSchemeKey.String("http"),
		otelsemconv.HTTPRouteKey.String("/soft"),
		otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
		attribute.Int(semconv.ErrsCodeKey, 1001),
	})
	if _, ok := requestLog(t, buf).fields["error"]; !ok {
		t.Errorf("request log has no error key, want the bound error at status 200; log output:\n%s", buf.String())
	}
}

func TestBoundErrsCode_typedNilIgnored(t *testing.T) {
	buf := captureLogs(t)
	s, mc, rec := newTelemetryServer(t, Config{})
	s.Hertz().GET("/typed", func(_ context.Context, c *app.RequestContext) {
		var e *errs.Error
		_ = c.Error(e)
		c.String(http.StatusOK, "ok")
	})
	recorded := perform(s, "GET", "/typed", nil)

	if recorded.Code != http.StatusOK {
		t.Errorf("GET /typed status = %d, want %d: a typed nil error must not panic", recorded.Code, http.StatusOK)
	}
	if _, ok := attributesByKey(onlySpan(t, rec).Attributes())[semconv.ErrsCodeKey]; ok {
		t.Errorf("span has errs.code for a typed nil error, want none")
	}
	checkRequestMetrics(t, mc, []attribute.KeyValue{
		otelsemconv.HTTPRequestMethodKey.String("GET"),
		otelsemconv.URLSchemeKey.String("http"),
		otelsemconv.HTTPRouteKey.String("/typed"),
		otelsemconv.HTTPResponseStatusCodeKey.Int(http.StatusOK),
	})
	line := requestLog(t, buf)
	if got := line.fields["error"]; got != "nil *errs.Error" {
		t.Errorf("request log error = %v, want %q", got, "nil *errs.Error")
	}
}

func TestRecovery_keepsEveryCORSValue(t *testing.T) {
	captureLogs(t)
	s := newTestServer(t)
	s.Hertz().GET("/cors", func(_ context.Context, c *app.RequestContext) {
		c.Response.Header.Add("Access-Control-Expose-Headers", "X-First")
		c.Response.Header.Add("Access-Control-Expose-Headers", "X-Second")
		panic("boom")
	})
	rec := perform(s, "GET", "/cors", nil)

	var got []string
	for _, v := range rec.Header().PeekAll("Access-Control-Expose-Headers") {
		got = append(got, string(v))
	}
	want := []string{"X-First", "X-Second"}
	if !slices.Equal(got, want) {
		t.Errorf("GET /cors Access-Control-Expose-Headers values = %q, want %q", got, want)
	}
}

func TestRecovery_hijackHandlerCleared(t *testing.T) {
	captureLogs(t)
	addr := freeAddr(t)
	s := newTestServerCfg(t, Config{Addr: addr})
	var ran atomic.Bool
	s.Hertz().GET("/hijack", func(_ context.Context, c *app.RequestContext) {
		c.Hijack(func(conn network.Conn) {
			ran.Store(true)
			_, _ = conn.Write([]byte("HIJACKED"))
		})
		panic("boom")
	})
	startServer(t, s)

	raw := rawExchange(t, addr, "GET /hijack HTTP/1.1\r\nHost: example\r\nConnection: close\r\n\r\n")
	if !strings.HasPrefix(raw, "HTTP/1.1 500 ") {
		t.Errorf("GET /hijack response = %q, want a 500 status line", raw)
	}
	if strings.Contains(raw, "HIJACKED") {
		t.Errorf("GET /hijack response = %q, want no bytes from the hijack handler after the 500", raw)
	}
	if ran.Load() {
		t.Errorf("hijack handler ran after the panic, want it cleared by recovery")
	}
}

func TestRecovery_logsNormalizedMethod(t *testing.T) {
	buf := captureLogs(t)
	s := newTestServer(t)
	s.Hertz().Handle("PROPFIND", "/panic", func(context.Context, *app.RequestContext) {
		panic("boom")
	})
	perform(s, "PROPFIND", "/panic", nil)

	var found []map[string]any
	for _, l := range logLines(t, buf) {
		if l.fields["msg"] == "panic recovered" {
			found = append(found, l.fields)
		}
	}
	if len(found) != 1 {
		t.Fatalf("panic recovered records = %d, want 1; log output:\n%s", len(found), buf.String())
	}
	if got := found[0]["http.request.method"]; got != "_OTHER" {
		t.Errorf("panic recovered http.request.method = %v, want %q: the log uses the normalized method", got, "_OTHER")
	}
}
