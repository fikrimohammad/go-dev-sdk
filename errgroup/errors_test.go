package errgroup

import (
	"errors"
	"testing"
)

func TestPanicError_Error(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "string value", value: "boom", want: "errgroup: task panicked: boom"},
		{name: "error value", value: errors.New("disk full"), want: "errgroup: task panicked: disk full"},
		{name: "int value", value: 42, want: "errgroup: task panicked: 42"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pe := &PanicError{Value: tt.value}
			if got := pe.Error(); got != tt.want {
				t.Errorf("(&PanicError{Value: %v}).Error() = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestPanicError_Unwrap(t *testing.T) {
	sentinel := errors.New("disk full")
	tests := []struct {
		name  string
		value any
		want  error
	}{
		{name: "error value is unwrapped", value: sentinel, want: sentinel},
		{name: "non-error value unwraps to nil", value: "boom", want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pe := &PanicError{Value: tt.value}
			if got := pe.Unwrap(); got != tt.want {
				t.Errorf("(&PanicError{Value: %v}).Unwrap() = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}
