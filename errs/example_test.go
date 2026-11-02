package errs_test

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/fikrimohammad/go-dev-sdk/errs"
)

func ExampleNew() {
	err := errs.New(404, "user not found")
	fmt.Println(err.Code(), err.Error())
	// Output: 404 user not found
}

func ExampleWrap() {
	cause := errors.New("connection refused")
	err := errs.Wrap(503, "fetch profile", cause)
	fmt.Println(err.Error())
	fmt.Println(errors.Is(err, cause))
	// Output:
	// fetch profile: connection refused
	// true
}

func ExampleError_WithMeta() {
	base := errs.New(422, "invalid input")
	err := base.WithMeta("field", "email")
	fmt.Println(err.Meta()["field"], base.Meta() == nil)
	// Output: email true
}

func Example_codeMatching() {
	a := errs.New(409, "email taken")
	b := errs.New(409, "username taken")
	fmt.Println(errors.Is(a, b))

	u1 := errs.New(0, "unknown")
	u2 := errs.New(0, "unknown")
	fmt.Println(errors.Is(u1, u2))
	// Output:
	// true
	// false
}

func ExampleSlogAttr() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Drop the timestamp and the stack so the output is deterministic.
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			if a.Key == "stack" {
				return slog.Attr{}
			}
			return a
		},
	}))
	err := errs.Wrap(503, "fetch profile", errors.New("connection refused"))
	logger.Error("request failed", errs.SlogAttr(err))
	// Output: {"level":"ERROR","msg":"request failed","err":{"code":503,"message":"fetch profile","cause":"connection refused"}}
}
