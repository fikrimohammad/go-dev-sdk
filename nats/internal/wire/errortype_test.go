package wire

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/fikrimohammad/go-servicekit/errs"
)

// codedError is a handler error that carries an errs-style code.
type codedError struct{ code int }

func (e codedError) Error() string { return "coded failure" }
func (e codedError) Code() int     { return e.code }

// netTimeoutError is a net.Error whose timeout flag is set by the test.
type netTimeoutError struct{ timeout bool }

func (e netTimeoutError) Error() string   { return "net failure" }
func (e netTimeoutError) Timeout() bool   { return e.timeout }
func (e netTimeoutError) Temporary() bool { return false }

// TestErrorType is a regression test for the error.type classes: each class must
// map to its value, including through wrapping, and the message text is never used.
func TestErrorType(t *testing.T) {
	panicErr := fmt.Errorf("%w: %v", ErrPanic, "boom")
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"recovered panic", panicErr, "panic"},
		{"wrapped recovered panic", fmt.Errorf("consume: %w", panicErr), "panic"},
		{"errs code", errs.New(4001, "bad input"), "4001"},
		{"errs code wrapped", fmt.Errorf("publish: %w", errs.New(4001, "bad input")), "4001"},
		{"custom code", codedError{code: 4002}, "4002"},
		{"code wins over deadline", codedError{code: 4003}, "4003"},
		{"jetstream api error", &jetstream.APIError{ErrorCode: 10071}, "10071"},
		{"jetstream api error wrapped", fmt.Errorf("sync send: %w", &jetstream.APIError{ErrorCode: 10071}), "10071"},
		{"jetstream api error without code", &jetstream.APIError{Description: "x"}, "_OTHER"},
		{"deadline exceeded", context.DeadlineExceeded, "timeout"},
		{"deadline exceeded wrapped", fmt.Errorf("send: %w", context.DeadlineExceeded), "timeout"},
		{"nats timeout", nats.ErrTimeout, "timeout"},
		{"net timeout", netTimeoutError{timeout: true}, "timeout"},
		{"net timeout wrapped", fmt.Errorf("dial: %w", netTimeoutError{timeout: true}), "timeout"},
		{"net error without timeout", netTimeoutError{timeout: false}, "_OTHER"},
		{"canceled", context.Canceled, "canceled"},
		{"canceled wrapped", fmt.Errorf("send: %w", context.Canceled), "canceled"},
		{"other", errors.New("send failed"), "_OTHER"},
		{"nil", nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorType(tc.err); got != tc.want {
				t.Fatalf("ErrorType(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
