package wire

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrorType returns the low-cardinality error.type value for err, which is never
// the message text. In order: a recovered panic gives "panic"; an error with a
// Code gives the code, as errs.Error does; a JetStream API error gives its
// non-zero error code; a deadline or a timeout gives "timeout"; a cancellation
// gives "canceled"; anything else gives "_OTHER". A nil err gives "".
func ErrorType(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrPanic) {
		return "panic"
	}
	if coded, ok := errors.AsType[interface {
		error
		Code() int
	}](err); ok {
		return strconv.Itoa(coded.Code())
	}
	if apiErr, ok := errors.AsType[*jetstream.APIError](err); ok && apiErr.ErrorCode != 0 {
		return strconv.Itoa(int(apiErr.ErrorCode))
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
		return "timeout"
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "_OTHER"
}
