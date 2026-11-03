package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fikrimohammad/go-servicekit/rest/internal/semconv"
	otelsemconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// echoed is what the echo vendor reports about the request it received.
type echoed struct {
	Method        string      `json:"method"`
	URI           string      `json:"uri"`
	Host          string      `json:"host"`
	Header        http.Header `json:"header"`
	Body          string      `json:"body"`
	ContentLength int64       `json:"content_length"`
}

// newEchoVendor starts a vendor that echoes the request as JSON. It answers
// 201 Created with two X-R values, so a test can check the response mapping.
func newEchoVendor(t *testing.T) *httptest.Server {
	t.Helper()
	return newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		out, err := json.Marshal(echoed{
			Method:        r.Method,
			URI:           r.URL.RequestURI(),
			Host:          r.Host,
			Header:        r.Header,
			Body:          string(body),
			ContentLength: r.ContentLength,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Add("X-R", "a")
		w.Header().Add("X-R", "b")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(out)
	})
}

// recordingBody is a request body that records whether it was closed.
type recordingBody struct {
	io.Reader
	closed bool
}

// Close records that the body was closed.
func (b *recordingBody) Close() error {
	b.closed = true
	return nil
}

// failingReader is a reader whose every read fails with err.
type failingReader struct{ err error }

// Read returns the error of the reader.
func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestRoundTrip_MapsRequestAndResponse(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		target  string      // path and query, appended to the vendor URL
		headers http.Header // headers set on the request, multi-valued allowed
		host    string      // req.Host, when not empty
		body    string      // request body; empty sends http.NoBody
	}{
		{
			name:   "GET with a query",
			method: http.MethodGet,
			target: "/items?b=2&a=1",
		},
		{
			name:    "POST with a body and Content-Type",
			method:  http.MethodPost,
			target:  "/orders",
			headers: http.Header{"Content-Type": {"application/json"}},
			body:    `{"id":1}`,
		},
		{
			name:    "multi-value headers",
			method:  http.MethodGet,
			target:  "/",
			headers: http.Header{"X-A": {"1", "2"}},
		},
		{
			name:   "Host override reaches the vendor address",
			method: http.MethodGet,
			target: "/",
			host:   "api.vendor.test",
		},
		{
			name:   "empty body",
			method: http.MethodPost,
			target: "/empty",
		},
		{
			// Hertz would add a form Content-Type here. The vendor must not see one.
			name:   "POST without Content-Type",
			method: http.MethodPost,
			target: "/no-type",
			body:   "payload",
		},
		{
			// Hertz keeps a copied Content-Length on a GET, which has no body,
			// so the vendor would wait for bytes that never come. The vendor
			// must see the length of the empty body.
			name:    "GET with a stale Content-Length",
			method:  http.MethodGet,
			target:  "/stale",
			headers: http.Header{"Content-Length": {"5"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newEchoVendor(t)
			c, err := New(Config{})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			req, err := http.NewRequestWithContext(t.Context(), tt.method, vendor.URL+tt.target, strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("NewRequest(%s %s) error = %v", tt.method, tt.target, err)
			}
			req.Host = tt.host
			for k, vs := range tt.headers {
				for _, v := range vs {
					req.Header.Add(k, v)
				}
			}

			resp, err := RoundTripper(c).RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip(%s %s) error = %v, want nil", tt.method, tt.target, err)
			}
			raw, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Fatalf("RoundTrip(%s %s) body read error = %v", tt.method, tt.target, err)
			}

			if resp.StatusCode != http.StatusCreated {
				t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusCreated)
			}
			if resp.Status != "201 Created" {
				t.Errorf("Status = %q, want %q", resp.Status, "201 Created")
			}
			if resp.Proto != "HTTP/1.1" || resp.ProtoMajor != 1 || resp.ProtoMinor != 1 {
				t.Errorf("Proto = %q %d.%d, want HTTP/1.1 1.1", resp.Proto, resp.ProtoMajor, resp.ProtoMinor)
			}
			if got, want := resp.Header.Values("X-R"), []string{"a", "b"}; !slices.Equal(got, want) {
				t.Errorf("X-R response header = %q, want %q", got, want)
			}
			if resp.ContentLength != int64(len(raw)) {
				t.Errorf("ContentLength = %d, want %d", resp.ContentLength, len(raw))
			}
			if resp.Request != req {
				t.Errorf("Request = %p, want the request %p", resp.Request, req)
			}

			var e echoed
			if err := json.Unmarshal(raw, &e); err != nil {
				t.Fatalf("vendor echo %q is not JSON: %v", raw, err)
			}
			if e.Method != tt.method {
				t.Errorf("vendor method = %q, want %q", e.Method, tt.method)
			}
			if e.URI != tt.target {
				t.Errorf("vendor URI = %q, want %q", e.URI, tt.target)
			}
			wantHost := tt.host
			if wantHost == "" {
				wantHost = strings.TrimPrefix(vendor.URL, "http://")
			}
			if e.Host != wantHost {
				t.Errorf("vendor Host = %q, want %q", e.Host, wantHost)
			}
			for k, vs := range tt.headers {
				// Content-Length is computed by Hertz, so the vendor's value is
				// checked through ContentLength below.
				if k == "Content-Length" {
					continue
				}
				if got := e.Header[k]; !slices.Equal(got, vs) {
					t.Errorf("vendor header %s = %q, want %q", k, got, vs)
				}
			}
			if got, want := e.Header.Values("Content-Type"), tt.headers.Values("Content-Type"); !slices.Equal(got, want) {
				t.Errorf("vendor Content-Type = %q, want %q", got, want)
			}
			if e.Body != tt.body {
				t.Errorf("vendor body = %q, want %q", e.Body, tt.body)
			}
			if e.ContentLength != int64(len(tt.body)) {
				t.Errorf("vendor ContentLength = %d, want %d", e.ContentLength, len(tt.body))
			}
		})
	}
}

func TestRoundTrip_SendsPathAsGiven(t *testing.T) {
	for _, tt := range sentPaths {
		t.Run(tt.name, func(t *testing.T) {
			vendor, uris := newRequestURIVendor(t)
			c, tel := newTelemetryClient(t, Config{MaxAttempts: 1})
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, vendor.URL+tt.path, nil)
			if err != nil {
				t.Fatalf("NewRequest(%s) error = %v", tt.path, err)
			}

			resp, err := RoundTripper(c).RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip(GET %s%s) error = %v, want nil", vendor.URL, tt.path, err)
			}
			_ = resp.Body.Close()
			select {
			case got := <-uris:
				if got != tt.path {
					t.Errorf("RoundTrip(GET %s%s) vendor RequestURI = %q, want %q", vendor.URL, tt.path, got, tt.path)
				}
			default:
				t.Fatalf("RoundTrip(GET %s%s): vendor received no request", vendor.URL, tt.path)
			}

			spans := clientSpans(tel.spans)
			if len(spans) != 1 {
				t.Fatalf("client spans = %d, want 1", len(spans))
			}
			if got, want := spanAttributes(spans[0].Attributes())[otelsemconv.URLFullKey], vendor.URL+tt.path; got != want {
				t.Errorf("RoundTrip(GET %s%s) url.full = %v, want %q", vendor.URL, tt.path, got, want)
			}
		})
	}
}

func TestRoundTrip_DropsTraceContextHeaders(t *testing.T) {
	// A global propagator may have set these on the caller's request. None of
	// them may reach the vendor, while the caller's other headers still do.
	received := make(chan http.Header, 1)
	vendor := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- r.Header.Clone():
		default:
		}
		okHandler(w, r)
	})
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, vendor.URL+"/x", nil)
	if err != nil {
		t.Fatalf("NewRequest error = %v", err)
	}
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	req.Header.Set("tracestate", "vendor=1")
	req.Header.Set("Baggage", "tenant=acme")
	req.Header.Set("X-Keep", "yes")

	resp, err := RoundTripper(c).RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip(GET %s/x) error = %v, want nil", vendor.URL, err)
	}
	_ = resp.Body.Close()

	select {
	case h := <-received:
		for _, name := range []string{"Traceparent", "Tracestate", "Baggage"} {
			if got := h.Values(name); len(got) != 0 {
				t.Errorf("RoundTrip(GET %s/x) vendor header %s = %q, want none", vendor.URL, name, got)
			}
		}
		if got := h.Get("X-Keep"); got != "yes" {
			t.Errorf("RoundTrip(GET %s/x) vendor header X-Keep = %q, want %q", vendor.URL, got, "yes")
		}
	default:
		t.Fatalf("RoundTrip(GET %s/x): vendor received no request", vendor.URL)
	}
}

func TestRoundTrip_HeadKeepsContentLengthWithoutBody(t *testing.T) {
	// The vendor reports the length a GET would have had. Hertz must not wait
	// for those bytes, and the caller must see the length with an empty body.
	vendor := newVendor(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1234")
		w.WriteHeader(http.StatusOK)
	})
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, vendor.URL+"/x", nil)
	if err != nil {
		t.Fatalf("NewRequest error = %v", err)
	}

	resp, err := RoundTripper(c).RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip(HEAD %s/x) error = %v, want nil", vendor.URL, err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("RoundTrip(HEAD %s/x) body read error = %v", vendor.URL, err)
	}
	if resp.ContentLength != 1234 {
		t.Errorf("RoundTrip(HEAD %s/x) ContentLength = %d, want 1234", vendor.URL, resp.ContentLength)
	}
	if len(body) != 0 {
		t.Errorf("RoundTrip(HEAD %s/x) body = %q, want empty", vendor.URL, body)
	}
}

func TestRoundTrip_RetriesPOSTWithIdempotencyKey(t *testing.T) {
	vendor := newScriptedVendor(t, vendorStep{status: http.StatusServiceUnavailable}, vendorStep{status: http.StatusOK})
	c, err := New(Config{BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	hc := &http.Client{Transport: RoundTripper(c)}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, vendor.URL+"/pay", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("NewRequest error = %v", err)
	}
	req.Header.Set("Idempotency-Key", "key-1")

	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("POST %s error = %v, want nil", vendor.URL, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST %s status = %d, want %d", vendor.URL, resp.StatusCode, http.StatusOK)
	}
	if got := vendor.hits.Load(); got != 2 {
		t.Errorf("POST %s vendor hits = %d, want 2", vendor.URL, got)
	}
	want := seenRequest{method: http.MethodPost, body: "payload", idempotencyKey: "key-1"}
	for _, got := range vendor.requests() {
		if got != want {
			t.Errorf("vendor received %+v, want %+v", got, want)
		}
	}
}

func TestRoundTrip_ClosesRequestBody(t *testing.T) {
	errRead := errors.New("read failed")
	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		return ctx
	}
	tests := []struct {
		name    string
		ctx     func() context.Context // builds the context of the call
		body    *recordingBody
		wantErr error // nil when the call succeeds
	}{
		{
			name: "success",
			ctx:  t.Context,
			body: &recordingBody{Reader: strings.NewReader("x")},
		},
		{
			name:    "cancelled context",
			ctx:     cancelled,
			body:    &recordingBody{Reader: strings.NewReader("x")},
			wantErr: context.Canceled,
		},
		{
			name:    "body read fails",
			ctx:     t.Context,
			body:    &recordingBody{Reader: failingReader{err: errRead}},
			wantErr: errRead,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vendor := newVendor(t, okHandler)
			c, err := New(Config{})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			req, err := http.NewRequestWithContext(tt.ctx(), http.MethodPost, vendor.URL+"/x", nil)
			if err != nil {
				t.Fatalf("NewRequest error = %v", err)
			}
			req.Body = tt.body

			resp, err := RoundTripper(c).RoundTrip(req)
			if resp != nil {
				_ = resp.Body.Close()
			}
			switch {
			case tt.wantErr == nil && err != nil:
				t.Errorf("RoundTrip() error = %v, want nil", err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Errorf("RoundTrip() error = %v, want errors.Is %v", err, tt.wantErr)
			}
			if !tt.body.closed {
				t.Errorf("request body closed = false, want true")
			}
		})
	}
}

func TestRoundTrip_TimeoutMatchesDeadlineExceeded(t *testing.T) {
	vendor, _ := newSlowVendor(t, 2*time.Second)
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, vendor.URL+"/slow", nil)
	if err != nil {
		t.Fatalf("NewRequest error = %v", err)
	}

	start := time.Now()
	resp, err := (&http.Client{Transport: RoundTripper(c)}).Do(req)
	elapsed := time.Since(start)
	if resp != nil {
		_ = resp.Body.Close()
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("GET %s error = %v, want errors.Is DeadlineExceeded", vendor.URL, err)
	}
	if elapsed >= time.Second {
		t.Errorf("GET %s returned after %v, want under 1s", vendor.URL, elapsed)
	}
}

func TestRoundTrip_TelemetryMatchesDo(t *testing.T) {
	vendor := newVendor(t, okHandler)
	const target = "/items?id=7"

	direct, directTel := newTelemetryClient(t, Config{})
	status, err := send(t.Context(), direct, http.MethodGet, vendor.URL+target, nil)
	if err != nil || status != http.StatusOK {
		t.Fatalf("Do GET error = %v, status = %d, want 200 and no error", err, status)
	}

	viaRoundTrip, rtTel := newTelemetryClient(t, Config{})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, vendor.URL+target, nil)
	if err != nil {
		t.Fatalf("NewRequest error = %v", err)
	}
	resp, err := RoundTripper(viaRoundTrip).RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip GET error = %v, want nil", err)
	}
	_ = resp.Body.Close()

	directSpans := clientSpans(directTel.spans)
	rtSpans := clientSpans(rtTel.spans)
	if len(directSpans) != 1 || len(rtSpans) != 1 {
		t.Fatalf("client spans = %d via Do and %d via RoundTrip, want 1 and 1", len(directSpans), len(rtSpans))
	}
	checkAttributeSet(t, "RoundTrip client span", spanAttributes(rtSpans[0].Attributes()), spanAttributes(directSpans[0].Attributes()))
	if got := spanAttributes(rtSpans[0].Attributes())[otelsemconv.HTTPResponseStatusCodeKey]; got != http.StatusOK {
		t.Errorf("RoundTrip client span status code = %v, want %d", got, http.StatusOK)
	}

	directRecords := directTel.metrics.Records()
	rtRecords := rtTel.metrics.Records()
	if len(directRecords) != 1 || len(rtRecords) != 1 {
		t.Fatalf("duration records = %d via Do and %d via RoundTrip, want 1 and 1", len(directRecords), len(rtRecords))
	}
	if got, want := rtRecords[0].instrument, semconv.ClientRequestDuration; got != want {
		t.Errorf("RoundTrip instrument = %+v, want %+v", got, want)
	}
	checkAttributeSet(t, "RoundTrip duration record", metricAttributes(rtRecords[0].attrs), metricAttributes(directRecords[0].attrs))
}

func TestRoundTrip_ConcurrentRequests(t *testing.T) {
	vendor := newVendor(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	})
	c, err := New(Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	rt := RoundTripper(c)

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			payload := fmt.Sprintf("payload-%d", i)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, vendor.URL+"/echo", strings.NewReader(payload))
			if err != nil {
				t.Errorf("NewRequest(%q) error = %v", payload, err)
				return
			}
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Errorf("RoundTrip(%q) error = %v, want nil", payload, err)
				return
			}
			got, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Errorf("RoundTrip(%q) body read error = %v", payload, err)
				return
			}
			if string(got) != payload {
				t.Errorf("RoundTrip(%q) body = %q, want %q", payload, got, payload)
			}
		})
	}
	wg.Wait()
}
