package apiserver

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/fikrimohammad/go-dev-sdk/errs"
	"github.com/fikrimohammad/go-dev-sdk/observability/logs"
	"github.com/fikrimohammad/go-dev-sdk/observability/metrics"
	"github.com/fikrimohammad/go-dev-sdk/observability/tracer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RequestIDKey is the context key for the request ID.
type RequestIDKey struct{}

// RequestID returns a middleware that extracts or generates a request ID.
// If the incoming request has an X-Request-ID header, it is used; otherwise
// a short UUID is generated. The ID is set on the response header and stored
// in the request context.
//
//go:noinline
func RequestID() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		id := string(c.GetHeader("X-Request-ID"))
		if id == "" {
			id = shortUUID()
		}
		c.Set("X-Request-ID", id)
		c.Response.Header.Set("X-Request-ID", id)
		c.Next(ctx)
	}
}

// PanicRecovery returns a middleware that recovers from panics, logs the stack
// trace, and writes a 500 JSON response in the standard API format.
//
//go:noinline
func PanicRecovery() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				attrs := []any{
					"panic", r,
					"method", string(c.Request.Method()),
					"path", string(c.Request.Path()),
					"stack", string(stack),
				}
				if id := tracer.TraceIDFrom(ctx); id != "" {
					attrs = append(attrs, "trace_id", id)
				}
				logs.Error(ctx, "panic recovered", attrs...)
				c.JSON(http.StatusInternalServerError, map[string]any{
					"base": map[string]any{
						"code":    "5001",
						"message": "internal server error",
					},
				})
			}
		}()
		c.Next(ctx)
	}
}

// Logger returns a middleware that logs every request as structured slog attributes.
// Successful requests (status < 400) are logged at Info level.
// Failed requests (status >= 400) are logged at Error level with bound errors.
//
//go:noinline
func Logger() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)

		status := c.Response.StatusCode()
		duration := time.Since(start)

		attrs := []slog.Attr{
			slog.String("method", string(c.Request.Method())),
			slog.String("path", string(c.Request.Path())),
			slog.String("route", string(c.FullPath())),
			slog.Int("status", status),
			slog.Duration("duration", duration),
		}

		if id := tracer.TraceIDFrom(ctx); id != "" {
			attrs = append(attrs, slog.String("trace_id", id))
		}
		if reqID, ok := c.Get("X-Request-ID"); ok {
			attrs = append(attrs, slog.String("request_id", reqID.(string)))
		}

		if status >= 400 {
			switch len(c.Errors) {
			case 0:
			case 1:
				attrs = append(attrs, slogAttrToAttr(errs.SlogAttr(c.Errors[0].Err)))
			default:
				groups := make([]slog.Attr, 0, len(c.Errors))
				for _, err := range c.Errors {
					groups = append(groups, slogAttrToAttr(errs.SlogAttr(err.Err)))
				}
				attrs = append(attrs, slog.Any("errors", groups))
			}
			logs.Error(ctx, "request error", attrsToArgs(attrs)...)
		} else {
			logs.Info(ctx, "request", attrsToArgs(attrs)...)
		}
	}
}

// requestAttributes builds the shared OTel HTTP attribute set from the request.
// Used by both Tracer and Metrics middleware for consistent attribution.
func requestAttributes(c *app.RequestContext) map[string]string {
	attrs := map[string]string{
		"http.request.method":       string(c.Request.Method()),
		"http.route":                c.FullPath(),
		"http.response.status_code": strconv.Itoa(c.Response.StatusCode()),
		"url.scheme":                string(c.Request.URI().Scheme()),
		"network.protocol.name":     "",
		"network.protocol.version":  "",
		"error.type":                "",
	}

	splittedProtocol := strings.Split(c.GetRequest().Header.GetProtocol(), "/")
	if len(splittedProtocol) == 2 {
		attrs["network.protocol.name"] = strings.ToLower(splittedProtocol[0])
		attrs["network.protocol.version"] = splittedProtocol[1]
	}

	return attrs
}

// Tracer returns a middleware that creates an OTel span for each request.
// The span is named "{method} {http.route}" per OTel HTTP semantic conventions.
// On completion, it sets all attributes and span status.
//
// The incoming trace context (W3C traceparent header) is extracted before the
// span starts, so requests carrying an upstream trace join it; otherwise a new
// trace is rooted here. The whole request lifecycle stays on a single trace.
//
//go:noinline
func Tracer(tc tracer.Client) app.HandlerFunc {
	propagator := otel.GetTextMapPropagator()
	t := tc.Tracer(tracerScope)
	return func(ctx context.Context, c *app.RequestContext) {
		ctx = propagator.Extract(ctx, requestHeaderCarrier{header: &c.Request.Header})

		spanName := string(c.Request.Method()) + " " + c.FullPath()

		ctx, span := t.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
		)
		defer span.End()

		c.Next(ctx)

		rawAttrs := requestAttributes(c)
		attrs := make(map[string]any, len(rawAttrs))
		for k, v := range rawAttrs {
			attrs[k] = v
		}
		span.SetAttributes(attribute.String("url.path", string(c.Request.Path())))
		span.SetAttributes(tracer.Attrs(attrs)...)

		if len(c.Errors) > 0 {
			err := c.Errors[0].Err
			errorCode := errs.CodeFromError(err)
			span.SetAttributes(attribute.String("error.type", errorCode.String()))
			span.SetStatus(otelcodes.Error, err.Error())
		}
	}
}

// Metrics returns a middleware that records request count and latency
// using the provided metrics.Client. Attributes follow OTel HTTP semantic conventions.
//
//go:noinline
func Metrics(mc metrics.Client) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)

		rawAttrs := requestAttributes(c)
		attrs := make(map[string]any, len(rawAttrs))
		for k, v := range rawAttrs {
			attrs[k] = v
		}

		if len(c.Errors) > 0 && errs.CodeFromError(c.Errors[0].Err) != errs.OK {
			attrs["error.type"] = errs.CodeFromError(c.Errors[0].Err).String()
		}

		_ = mc.Count(ctx, "http.server.request.count", 1, attrs)
		_ = mc.Histogram(ctx, "http.server.request.duration", time.Since(start).Seconds(), attrs)
	}
}

// tracerScope is the instrumentation scope name for the HTTP server tracer.
const tracerScope = "http.server.request"

// requestHeaderCarrier adapts a Hertz request header to the OTel
// propagation.TextMapCarrier interface so the incoming trace context can be
// extracted from it.
type requestHeaderCarrier struct {
	header *protocol.RequestHeader
}

func (c requestHeaderCarrier) Get(key string) string {
	return c.header.Get(key)
}

func (c requestHeaderCarrier) Set(key, value string) {
	c.header.Set(key, value)
}

func (c requestHeaderCarrier) Keys() []string {
	keys := make([]string, 0)
	c.header.VisitAll(func(key, _ []byte) {
		keys = append(keys, string(key))
	})
	return keys
}

// shortUUID generates an 8-character hex string for use as a request ID.
func shortUUID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%x", b)
}

// slogAttrToAttr converts a generic slog.Attr (from errs.SlogAttr) to ensure
// it is properly typed. This is a no-op pass-through for clarity.
func slogAttrToAttr(a slog.Attr) slog.Attr {
	return a
}

// attrsToArgs converts a slice of slog.Attr to []any for use with logs.*Context.
func attrsToArgs(attrs []slog.Attr) []any {
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	return args
}
