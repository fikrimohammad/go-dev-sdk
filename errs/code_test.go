package errs

import "testing"

func TestCode_String(t *testing.T) {
	tests := []struct {
		code     Code
		expected string
	}{
		{OK, "OK"},
		{InvalidArgument, "INVALID_ARGUMENT"},
		{NotFound, "NOT_FOUND"},
		{Internal, "INTERNAL"},
		{DBInternal, "DB_INTERNAL"},
		{CacheInternal, "CACHE_INTERNAL"},
		{MQInternal, "MQ_INTERNAL"},
		{S3Internal, "S3_INTERNAL"},
		{Code(9999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.code.String(); got != tt.expected {
				t.Errorf("String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCode_HTTPStatus(t *testing.T) {
	tests := []struct {
		code     Code
		expected int
	}{
		{OK, 200},
		{InvalidArgument, 400},
		{NotFound, 404},
		{Internal, 500},
		{DBInternal, 500},
		{CacheInternal, 500},
		{MQInternal, 500},
		{S3Internal, 500},
		{Code(9999), 500},
	}

	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			if got := tt.code.HTTPStatus(); got != tt.expected {
				t.Errorf("HTTPStatus() = %d, want %d", got, tt.expected)
			}
		})
	}
}
