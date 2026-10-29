// Package logs installs the application logger and routes structured records
// to stdout/stderr by severity.
package logs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/fikrimohammad/go-dev-sdk/appinfo"
	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

// Logger emits structured log records. All methods accept a context for trace
// correlation and key/value args whose keys are normalized to OTel semantic
// convention style ("order_id" → "order.id").
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
// already-installed logger. It is intended for tests that capture output and
// is not safe to call concurrently with logging. Pass a writer, or nil to
// restore the process defaults.
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

// New builds a configured logger routing by severity to stdout/stderr.
// The returned Logger resolves the streams lazily, so output follows
// redirection even after construction.
func New(info appinfo.Info, cfg Config) (Logger, error) {
	if info.Name == "" || filepath.Base(info.Name) != info.Name {
		return nil, fmt.Errorf("logs: invalid application name %q", info.Name)
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
	return &slogLogger{
		log: slog.New(&streamHandler{
			stdout: newHandler(cfg.Format, stdoutWriter, opts, attrs),
			stderr: newHandler(cfg.Format, stderrWriter, opts, attrs),
		}),
	}, nil
}

func newHandler(format string, w io.Writer, opts *slog.HandlerOptions, attrs []slog.Attr) slog.Handler {
	var h slog.Handler
	switch format {
	case FormatJSON:
		h = slog.NewJSONHandler(w, opts)
	default:
		h = slog.NewTextHandler(w, opts)
	}
	if len(attrs) > 0 {
		h = h.WithAttrs(attrs)
	}
	return h
}

// normalizeArgs standardizes attribute keys in a log call's args to the OTel
// semantic convention style (e.g. "order_id" → "order.id"). It handles both
// key/value pairs and slog.Attr values, mirroring slog's argument parsing so
// that attributes and pairs can be mixed.
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
		log: slog.New(slog.NewTextHandler(stderrWriter, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
}

// Default returns the logger used by the package-level Debug, Info, Warn, and
// Error funcs. It is never nil: before SetDefault is called it is a debug-level
// text logger on stderr.
func Default() Logger {
	return *defaultLogger.Load()
}

// SetDefault makes l the Logger used by the package-level funcs, replacing any
// installed earlier. It is safe to call concurrently with logging calls.
func SetDefault(l Logger) {
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
