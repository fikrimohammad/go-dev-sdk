package errs

import "net/http"

// Code represents an error category as a typed int enum.
type Code int

const (
	OK              Code = 0
	InvalidArgument Code = 1001
	NotFound        Code = 4004
	Internal        Code = 5001
	DBInternal      Code = 5002
	CacheInternal   Code = 5003
	MQInternal      Code = 5004
	S3Internal      Code = 5005
)

// String returns the human-readable name of the code.
func (c Code) String() string {
	switch c {
	case OK:
		return "OK"
	case InvalidArgument:
		return "INVALID_ARGUMENT"
	case NotFound:
		return "NOT_FOUND"
	case Internal:
		return "INTERNAL"
	case DBInternal:
		return "DB_INTERNAL"
	case CacheInternal:
		return "CACHE_INTERNAL"
	case MQInternal:
		return "MQ_INTERNAL"
	case S3Internal:
		return "S3_INTERNAL"
	default:
		return "UNKNOWN"
	}
}

// HTTPStatus returns the HTTP status code for the error code.
func (c Code) HTTPStatus() int {
	switch {
	case c == OK:
		return http.StatusOK
	case c >= 1000 && c < 4000:
		return http.StatusBadRequest
	case c >= 4000 && c < 5000:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}
