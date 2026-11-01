package errs

import (
	"errors"
	"log/slog"
	"maps"
	"strings"
)

// LogValue implements slog.LogValuer, producing structured key-value pairs.
// Fields: code, message, debug (if set), meta (if set), cause (if set), stack (if set).
func (e *Error) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.Int("code", e.code),
		slog.String("message", e.message),
	}

	if debug := e.Debug(); debug != "" {
		attrs = append(attrs, slog.String("debug", debug))
	}

	meta := maps.Clone(e.meta)
	delete(meta, "debug")
	if len(meta) > 0 {
		attrs = append(attrs, slog.Any("meta", meta))
	}

	if e.cause != nil {
		if inner, ok := e.cause.(*Error); ok {
			attrs = append(attrs, slog.Any("cause", inner.LogValue()))
		} else {
			attrs = append(attrs, slog.String("cause", e.cause.Error()))
		}
	}

	if frames := e.StackFrames(); len(frames) > 0 {
		attrs = append(attrs, slog.String("stack", strings.Join(frames, "\n")))
	}

	return slog.GroupValue(attrs...)
}

// SlogAttr returns an slog Attr that logs err using LogValue() for structured output.
// It finds the first *Error in err's chain, so wrapped errors keep the structured
// output. Errors without an *Error in their chain are logged with slog.Any.
// Usage: slog.Error("msg", errs.SlogAttr(err)).
func SlogAttr(err error) slog.Attr {
	if e, ok := errors.AsType[*Error](err); ok {
		return slog.Attr{Key: "err", Value: e.LogValue()}
	}
	return slog.Any("err", err)
}
