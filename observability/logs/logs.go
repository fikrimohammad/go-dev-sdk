// Package logs installs the application logger, routes structured records to
// stdout/stderr by severity, and adds trace correlation fields from a record's
// context.
package logs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

// Logger emits structured log records. All methods accept a context for trace
// correlation and key/value args whose developer-style keys are normalized to
// OTel semantic convention style ("order_id" → "order.id"). Keys that already
// contain a dot and the standard correlation keys (trace_id, span_id, and
// trace_flags) keep their spelling.
//
// Records with a valid span context receive trace_id and span_id attributes
// automatically. If the caller supplies either key explicitly, the explicit
// value wins. A nil context is treated as having no span.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
}

// stdout and stderr are the writers loggers route to. They are variables so
// tests can redirect output by swapping them (e.g. mockey.MockValue).
var (
	stdout io.Writer = os.Stdout
	stderr io.Writer = os.Stderr
)

// SetStreams redirects the package-level stdout/stderr writers. Streams are
// resolved lazily by streamWriter on every Write, so this takes effect for any
// already-installed logger. Pass a writer, or nil to restore the process
// defaults.
//
// Deprecated: pass Config.Writer and Config.ErrorWriter to New instead.
// SetStreams mutates package-level state and is not safe to call concurrently
// with logging; it remains for tests and legacy callers.
func SetStreams(stdoutWriter, stderrWriter io.Writer) {
	if stdoutWriter == nil {
		stdoutWriter = os.Stdout
	}
	if stderrWriter == nil {
		stderrWriter = os.Stderr
	}
	stdout = stdoutWriter
	stderr = stderrWriter
}

// streamWriter dereferences a stream variable on every Write so that output
// follows redirection even when the handler was constructed earlier.
type streamWriter struct{ w *io.Writer }

func (sw streamWriter) Write(p []byte) (int, error) {
	if sw.w == nil || *sw.w == nil {
		return 0, errors.New("logs: writer is nil")
	}
	return (*sw.w).Write(p)
}

var (
	stdoutWriter = streamWriter{w: &stdout}
	stderrWriter = streamWriter{w: &stderr}
)

// streamHandler routes records by severity: debug and info go to stdout; warn
// and error go to stderr.
type streamHandler struct {
	stdout slog.Handler
	stderr slog.Handler
}

func (h *streamHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler(level).Enabled(ctx, level)
}

func (h *streamHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.handler(r.Level).Handle(ctx, r)
}

func (h *streamHandler) handler(level slog.Level) slog.Handler {
	if level >= slog.LevelWarn {
		return h.stderr
	}
	return h.stdout
}

func (h *streamHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &streamHandler{
		stdout: h.stdout.WithAttrs(attrs),
		stderr: h.stderr.WithAttrs(attrs),
	}
}

func (h *streamHandler) WithGroup(name string) slog.Handler {
	return &streamHandler{
		stdout: h.stdout.WithGroup(name),
		stderr: h.stderr.WithGroup(name),
	}
}

// traceHandler adds trace_id and span_id attributes from the record's context.
// Explicit values provided by the caller win: a key is only added when the
// record does not already contain it.
type traceHandler struct {
	next       slog.Handler
	hasTraceID bool
	hasSpanID  bool
}

func (h traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			if !h.hasTraceID && !recordHasKey(r, "trace_id") {
				r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
			}
			if !h.hasSpanID && !recordHasKey(r, "span_id") {
				r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
			}
		}
	}
	return h.next.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.next = h.next.WithAttrs(attrs)
	for _, attr := range attrs {
		switch attr.Key {
		case "trace_id":
			h.hasTraceID = true
		case "span_id":
			h.hasSpanID = true
		}
	}
	return h
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{
		next:       h.next.WithGroup(name),
		hasTraceID: h.hasTraceID,
		hasSpanID:  h.hasSpanID,
	}
}

// recordHasKey reports whether r already contains a top-level attribute with
// the given key.
func recordHasKey(r slog.Record, key string) bool {
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			found = true
			return false
		}
		return true
	})
	return found
}

// slogLogger implements Logger with a slog.Logger.
type slogLogger struct {
	log *slog.Logger
}

func (l *slogLogger) Debug(ctx context.Context, msg string, args ...any) {
	l.log.DebugContext(ctx, msg, normalizeArgs(args)...)
}

func (l *slogLogger) Info(ctx context.Context, msg string, args ...any) {
	l.log.InfoContext(ctx, msg, normalizeArgs(args)...)
}

func (l *slogLogger) Warn(ctx context.Context, msg string, args ...any) {
	l.log.WarnContext(ctx, msg, normalizeArgs(args)...)
}

func (l *slogLogger) Error(ctx context.Context, msg string, args ...any) {
	l.log.ErrorContext(ctx, msg, normalizeArgs(args)...)
}

// New builds a configured logger routing by severity to the configured
// writers. Debug and info records go to Config.Writer (or the package stdout
// stream when nil); warn and error records go to Config.ErrorWriter (or the
// package stderr stream when nil). The returned Logger resolves the package
// streams lazily, so output follows redirection even after construction. New
// returns an error if info has no service name or cfg is invalid.
func New(info appinfo.Info, cfg Config) (Logger, error) {
	if err := attributes.ValidateAppInfo(info); err != nil {
		return nil, fmt.Errorf("logs: %w", err)
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	attrs := logAttrs(cfg.GlobalKV, info)
	opts := &slog.HandlerOptions{Level: level}

	output := cfg.Writer
	if output == nil {
		output = stdoutWriter
	}
	errorOutput := cfg.ErrorWriter
	if errorOutput == nil {
		errorOutput = stderrWriter
	}
	var handler slog.Handler = traceHandler{next: &streamHandler{
		stdout: newHandler(cfg.Format, output, opts),
		stderr: newHandler(cfg.Format, errorOutput, opts),
	}}
	if len(attrs) > 0 {
		handler = handler.WithAttrs(attrs)
	}
	return &slogLogger{log: slog.New(handler)}, nil
}

func newHandler(format string, w io.Writer, opts *slog.HandlerOptions) slog.Handler {
	switch format {
	case FormatJSON:
		return slog.NewJSONHandler(w, opts)
	default:
		return slog.NewTextHandler(w, opts)
	}
}

// normalizeArgs standardizes developer-style attribute keys in a log call's
// args to the OTel semantic convention style (e.g. "order_id" → "order.id").
// Keys that already contain a dot and standard correlation keys keep their
// spelling. It handles both key/value pairs and slog.Attr values, mirroring
// slog's argument parsing so that attributes and pairs can be mixed.
func normalizeArgs(args []any) []any {
	out := make([]any, len(args))
	copy(out, args)
	for i := 0; i < len(out); {
		switch v := out[i].(type) {
		case slog.Attr:
			out[i] = slog.Attr{Key: attributes.NormalizeKey(v.Key), Value: v.Value}
			i++
		case string:
			out[i] = attributes.NormalizeKey(v)
			i += 2
		default:
			i++
		}
	}
	return out
}

// logAttrs merges the configured GlobalKV with the service identity via the
// shared attribute helpers. Identity attributes (service.name, service.version,
// deployment.environment) always win over colliding GlobalKV keys because the
// identity map is passed last.
func logAttrs(kv map[string]any, info appinfo.Info) []slog.Attr {
	kvs := attributes.ConvertMapsToKVs(kv, attributes.ConvertAppInfoToMap(info))
	attrs := make([]slog.Attr, 0, len(kvs))
	for _, kv := range kvs {
		attrs = append(attrs, slog.Any(string(kv.Key), kv.Value.AsInterface()))
	}
	return attrs
}

func parseLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(value)); err != nil {
		return 0, fmt.Errorf("logs: unsupported level %q", value)
	}
	return level, nil
}

var defaultLogger atomic.Pointer[Logger]

func init() {
	SetDefault(newDefaultLogger())
}

// newDefaultLogger returns a debug-level text logger writing to stderr.
func newDefaultLogger() Logger {
	return &slogLogger{
		log: slog.New(traceHandler{next: slog.NewTextHandler(stderrWriter, &slog.HandlerOptions{Level: slog.LevelDebug})}),
	}
}

// Default returns the logger used by the package-level Debug, Info, Warn, and
// Error funcs. It is never nil: before SetDefault is called, and after
// SetDefault(nil), it is a debug-level text logger on stderr.
func Default() Logger {
	return *defaultLogger.Load()
}

// SetDefault makes l the Logger used by the package-level funcs, replacing any
// installed earlier. A nil l restores the default debug-level text logger. It
// is safe to call concurrently with logging calls.
func SetDefault(l Logger) {
	if l == nil {
		l = newDefaultLogger()
	}
	defaultLogger.Store(&l)
}

// Debug emits a record at debug level.
func Debug(ctx context.Context, msg string, args ...any) { Default().Debug(ctx, msg, args...) }

// Info emits a record at info level.
func Info(ctx context.Context, msg string, args ...any) { Default().Info(ctx, msg, args...) }

// Warn emits a record at warn level.
func Warn(ctx context.Context, msg string, args ...any) { Default().Warn(ctx, msg, args...) }

// Error emits a record at error level.
func Error(ctx context.Context, msg string, args ...any) { Default().Error(ctx, msg, args...) }
