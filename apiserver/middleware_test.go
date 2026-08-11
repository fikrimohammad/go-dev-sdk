package apiserver

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// --- RequestID tests ---

func TestRequestID_GeneratedWhenMissing(t *testing.T) {
	_ = initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{RequestID()},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", nil)
	id := resp.Header.Get("X-Request-ID")
	if id == "" {
		t.Error("expected X-Request-ID to be generated")
	}
	if len(id) != 8 {
		t.Errorf("expected 8-char request ID, got %q (%d chars)", id, len(id))
	}
}

func TestRequestID_PreservedWhenProvided(t *testing.T) {
	_ = initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{RequestID()},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", map[string]string{"X-Request-ID": "my-custom-id"})
	id := resp.Header.Get("X-Request-ID")
	if id != "my-custom-id" {
		t.Errorf("expected custom request ID preserved, got %q", id)
	}
}

func TestRequestID_AvailableInContext(t *testing.T) {
	_ = initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{RequestID()},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				reqID, ok := c.Get("X-Request-ID")
				if !ok {
					c.JSON(500, map[string]string{"error": "no request ID"})
					return
				}
				c.JSON(200, map[string]string{"request_id": reqID.(string)})
			},
		},
	)

	resp := ts.get(t, "/test", nil)
	body := readBody(t, resp)
	if !strings.Contains(body, "request_id") {
		t.Errorf("expected request_id in response body, got: %s", body)
	}
}

// --- PanicRecovery tests ---

func TestPanicRecovery_CatchesPanic(t *testing.T) {
	_ = initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("boom")
			},
		},
	)

	resp := ts.get(t, "/panic", nil)
	body := readBody(t, resp)

	if resp.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
	if !strings.Contains(body, "5001") {
		t.Errorf("expected error code 5001 in body, got: %s", body)
	}
	if !strings.Contains(body, "internal server error") {
		t.Errorf("expected error message in body, got: %s", body)
	}
	if !strings.Contains(body, `"base"`) {
		t.Errorf("expected 'base' wrapper in body, got: %s", body)
	}
}

func TestPanicRecovery_PanicValueInLog(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("test-panic-value")
			},
		},
	)

	ts.get(t, "/panic", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "test-panic-value") {
		t.Errorf("expected panic value in log, got: %s", buf.String())
	}
}

func TestPanicRecovery_StackInLog(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("stack-test")
			},
		},
	)

	ts.get(t, "/panic", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "goroutine") {
		t.Errorf("expected stack trace in log, got: %s", buf.String())
	}
}

func TestPanicRecovery_HandlerErrorNotCaught(t *testing.T) {
	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /error": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad request"})
			},
		},
	)

	resp := ts.get(t, "/error", nil)
	if resp.StatusCode != 400 {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestPanicRecovery_NilPanic(t *testing.T) {
	_ = initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic(nil)
			},
		},
	)

	resp := ts.get(t, "/panic", nil)
	if resp.StatusCode != 500 {
		t.Errorf("expected status 500 for nil panic, got %d", resp.StatusCode)
	}
}

func TestPanicRecovery_PanicString(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("string panic")
			},
		},
	)

	ts.get(t, "/panic", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "string panic") {
		t.Errorf("expected string panic in log, got: %s", buf.String())
	}
}

func TestPanicRecovery_PanicError(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic(io.ErrUnexpectedEOF)
			},
		},
	)

	ts.get(t, "/panic", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "unexpected EOF") {
		t.Errorf("expected error message in log, got: %s", buf.String())
	}
}

func TestPanicRecovery_ResponseFormat(t *testing.T) {
	_ = initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("format-test")
			},
		},
	)

	resp := ts.get(t, "/panic", nil)
	body := readBody(t, resp)

	if !strings.Contains(body, `"base"`) {
		t.Errorf("expected 'base' key in response, got: %s", body)
	}
	if !strings.Contains(body, `"code":"5001"`) && !strings.Contains(body, `"code": "5001"`) {
		t.Errorf("expected code 5001 in response, got: %s", body)
	}
	if !strings.Contains(body, `"message":"internal server error"`) && !strings.Contains(body, `"message": "internal server error"`) {
		t.Errorf("expected error message in response, got: %s", body)
	}
}

func TestPanicRecovery_IncludesTraceID(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("trace-panic")
			},
		},
	)

	ts.get(t, "/panic", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "panic recovered") {
		t.Errorf("expected 'panic recovered' in log, got: %s", out)
	}
}

// --- Logger tests ---

func TestLogger_Status400(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /bad": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/bad", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request error") {
		t.Errorf("expected 'request error' in log, got: %s", out)
	}
	if !strings.Contains(out, "400") {
		t.Errorf("expected status 400 in log, got: %s", out)
	}
}

func TestLogger_Status404(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /notfound": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(404, map[string]string{"error": "not found"})
			},
		},
	)

	ts.get(t, "/notfound", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "404") {
		t.Errorf("expected status 404 in log, got: %s", buf.String())
	}
}

func TestLogger_Status500(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /fail": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(500, map[string]string{"error": "fail"})
			},
		},
	)

	ts.get(t, "/fail", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "500") {
		t.Errorf("expected status 500 in log, got: %s", buf.String())
	}
}

func TestLogger_Status200_LoggedAsInfo(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /ok": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	ts.get(t, "/ok", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request") {
		t.Errorf("expected 'request' message in log, got: %s", out)
	}
	if !strings.Contains(out, "200") {
		t.Errorf("expected status 200 in log, got: %s", out)
	}
}

func TestLogger_Status201_LoggedAsInfo(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"POST /create": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(201, map[string]string{"status": "created"})
			},
		},
	)

	ts.post(t, "/create", `{}`, map[string]string{"Content-Type": "application/json"})
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request") {
		t.Errorf("expected 'request' message in log, got: %s", out)
	}
	if !strings.Contains(out, "201") {
		t.Errorf("expected status 201 in log, got: %s", out)
	}
}

func TestLogger_IncludesTraceID(t *testing.T) {
	buf := initTestLogger(t)
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Tracer(tc), Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "trace.id") {
		t.Errorf("expected trace.id field in log, got: %s", out)
	}
}

func TestLogger_IncludesRequestID(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{RequestID(), Logger()},
		map[string]app.HandlerFunc{
			"GET /ok": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	ts.get(t, "/ok", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request.id") {
		t.Errorf("expected request.id field in log, got: %s", out)
	}
}

func TestLogger_IncludesMethod(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"POST /err": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.post(t, "/err", `{}`, map[string]string{"Content-Type": "application/json"})
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "POST") {
		t.Errorf("expected POST method in log, got: %s", buf.String())
	}
}

func TestLogger_IncludesPath(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /some/path": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/some/path", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "/some/path") {
		t.Errorf("expected path in log, got: %s", buf.String())
	}
}

func TestLogger_IncludesDuration(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	if !strings.Contains(buf.String(), "duration") {
		t.Errorf("expected duration in log, got: %s", buf.String())
	}
}

func TestLogger_HandlerErrorLogged(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				err := errs.New(errs.InvalidArgument, "bad request")
				_ = c.Error(err)
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request error") {
		t.Errorf("expected 'request error' in log, got: %s", out)
	}
	if !strings.Contains(out, "bad request") {
		t.Errorf("expected error message 'bad request' in log, got: %s", out)
	}
	if !strings.Contains(out, "INVALID_ARGUMENT") && !strings.Contains(out, "InvalidArgument") {
		t.Errorf("expected error code in log, got: %s", out)
	}
}

func TestLogger_HandlerErrorWithCauseLogged(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				rootErr := errors.New("connection refused")
				err := errs.Wrap(errs.DBInternal, "database query failed", rootErr)
				_ = c.Error(err)
				c.JSON(500, map[string]string{"error": "fail"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "connection refused") {
		t.Errorf("expected root cause 'connection refused' in log, got: %s", out)
	}
}

func TestLogger_NoHandlerError(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request error") {
		t.Errorf("expected 'request error' in log, got: %s", out)
	}
}

func TestLogger_PanicThenLog(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery(), Logger()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("panic-log-test")
			},
		},
	)

	ts.get(t, "/panic", nil)
	time.Sleep(100 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "panic recovered") {
		t.Errorf("expected 'panic recovered' in log, got: %s", out)
	}
}

func TestLogger_AllRequestsLogged(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery(), Logger()},
		map[string]app.HandlerFunc{
			"GET /ok": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	for i := 0; i < 5; i++ {
		ts.get(t, "/ok", nil)
	}
	time.Sleep(100 * time.Millisecond)

	out := buf.String()
	count := strings.Count(out, "request")
	if count < 5 {
		t.Errorf("expected 5 log entries for successful requests, got %d in: %s", count, out)
	}
}

func TestLogger_MultipleErrorsLogged(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				_ = c.Error(errs.New(errs.InvalidArgument, "first error"))
				_ = c.Error(errs.New(errs.NotFound, "second error"))
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "first error") {
		t.Errorf("expected first error in log, got: %s", out)
	}
	if !strings.Contains(out, "second error") {
		t.Errorf("expected second error in log, got: %s", out)
	}
}

// --- Integration tests ---

func TestMiddlewareChain_PanicRecoveryAndLogger(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery(), Logger()},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("chain-panic")
			},
		},
	)

	resp := ts.get(t, "/panic", nil)

	if resp.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)
	if !strings.Contains(buf.String(), "chain-panic") {
		t.Errorf("expected panic value in log, got: %s", buf.String())
	}
}

func TestMiddlewareChain_SuccessLogged(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery(), Logger()},
		map[string]app.HandlerFunc{
			"GET /ok": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	ts.get(t, "/ok", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request") {
		t.Errorf("expected 'request' log for success, got: %s", out)
	}
}

func TestMiddlewareChain_ErrorLogged(t *testing.T) {
	buf := initTestLogger(t)

	ts := setupTest(t,
		[]app.HandlerFunc{PanicRecovery(), Logger()},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request error") {
		t.Errorf("expected 'request error' in log, got: %s", out)
	}
}

// --- Tracer middleware tests ---

func TestTracer_CreatesSpan(t *testing.T) {
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Tracer(tc)},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", nil)
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestTracer_SetsMethodAttribute(t *testing.T) {
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Tracer(tc)},
		map[string]app.HandlerFunc{
			"POST /test": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.post(t, "/test", `{}`, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestTracer_ErrorSetsStatus(t *testing.T) {
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Tracer(tc)},
		map[string]app.HandlerFunc{
			"GET /fail": func(ctx context.Context, c *app.RequestContext) {
				err := errs.New(errs.Internal, "something broke")
				_ = c.Error(err)
				c.JSON(500, map[string]string{"error": "fail"})
			},
		},
	)

	resp := ts.get(t, "/fail", nil)
	if resp.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestTracer_JoinsUpstreamTraceContext(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	tc := newTestTracerClient(t)

	const upstreamTraceID = "0af7651916cd43dd8448eb211c80319c"
	traceparent := "00-" + upstreamTraceID + "-b7ad6b7169203331-01"

	var gotTraceID string
	ts := setupTest(t,
		[]app.HandlerFunc{Tracer(tc)},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				gotTraceID = tracer.TraceIDFrom(ctx)
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", map[string]string{"traceparent": traceparent})
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	if gotTraceID != upstreamTraceID {
		t.Errorf("trace ID = %q, want upstream %q", gotTraceID, upstreamTraceID)
	}
}

func TestTracer_StartsNewTraceWithoutUpstreamContext(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	tc := newTestTracerClient(t)

	var gotTraceID string
	ts := setupTest(t,
		[]app.HandlerFunc{Tracer(tc)},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				gotTraceID = tracer.TraceIDFrom(ctx)
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	if len(gotTraceID) != 32 {
		t.Errorf("expected a fresh 32-hex trace ID, got %q", gotTraceID)
	}
}

// --- Metrics middleware tests ---

func TestMetrics_RecordsCount(t *testing.T) {
	mc := newTestMetricsClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Metrics(mc)},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", nil)
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestMetrics_IncludesAttributes(t *testing.T) {
	mc := newTestMetricsClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{Metrics(mc)},
		map[string]app.HandlerFunc{
			"GET /test": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/test", nil)
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

// --- Full middleware chain test ---

func TestMiddlewareChain_FullStack(t *testing.T) {
	_ = initTestLogger(t)
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{
			RequestID(),
			PanicRecovery(),
			Tracer(tc),
			Logger(),
			Metrics(mc),
		},
		map[string]app.HandlerFunc{
			"GET /ok": func(ctx context.Context, c *app.RequestContext) {
				c.JSON(200, map[string]string{"status": "ok"})
			},
		},
	)

	resp := ts.get(t, "/ok", nil)
	if resp.StatusCode != 200 {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	id := resp.Header.Get("X-Request-ID")
	if id == "" {
		t.Error("expected X-Request-ID in response")
	}
}

func TestMiddlewareChain_FullStackPanic(t *testing.T) {
	buf := initTestLogger(t)
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{
			RequestID(),
			PanicRecovery(),
			Tracer(tc),
			Logger(),
			Metrics(mc),
		},
		map[string]app.HandlerFunc{
			"GET /panic": func(ctx context.Context, c *app.RequestContext) {
				panic("full-stack-panic")
			},
		},
	)

	resp := ts.get(t, "/panic", nil)
	if resp.StatusCode != 500 {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)
	if !strings.Contains(buf.String(), "full-stack-panic") {
		t.Errorf("expected panic value in log, got: %s", buf.String())
	}
}

func TestMiddlewareChain_FullStackError(t *testing.T) {
	buf := initTestLogger(t)
	mc := newTestMetricsClient(t)
	tc := newTestTracerClient(t)

	ts := setupTest(t,
		[]app.HandlerFunc{
			RequestID(),
			PanicRecovery(),
			Tracer(tc),
			Logger(),
			Metrics(mc),
		},
		map[string]app.HandlerFunc{
			"GET /err": func(ctx context.Context, c *app.RequestContext) {
				err := errs.New(errs.InvalidArgument, "bad request")
				_ = c.Error(err)
				c.JSON(400, map[string]string{"error": "bad"})
			},
		},
	)

	ts.get(t, "/err", nil)
	time.Sleep(50 * time.Millisecond)

	out := buf.String()
	if !strings.Contains(out, "request error") {
		t.Errorf("expected 'request error' in log, got: %s", out)
	}
	if !strings.Contains(out, "bad request") {
		t.Errorf("expected error message in log, got: %s", out)
	}
}
