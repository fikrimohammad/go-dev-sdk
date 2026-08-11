package errs

import (
	"log/slog"
	"strings"
)

// LogValue implements slog.LogValuer, producing structured key-value pairs.
// Fields: code, message, debug (if set), meta (if set), cause (if set), stack (if set).
func (e *Error) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.String("code", e.code.String()),
		slog.String("message", e.message),
	}

	if debug := e.Debug(); debug != "" {
		attrs = append(attrs, slog.String("debug", debug))
	}

	if len(e.meta) > 0 {
		attrs = append(attrs, slog.Any("meta", e.meta))
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
// slog checks the error interface before LogValuer, so this helper forces the
// structured path. Usage: slog.Error("msg", errs.SlogAttr(err))
func SlogAttr(err error) slog.Attr {
	if lv, ok := err.(slog.LogValuer); ok {
		return slog.Attr{Key: "err", Value: lv.LogValue()}
	}
	return slog.Any("err", err)
}
