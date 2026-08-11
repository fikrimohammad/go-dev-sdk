package confloader

import (
	"errors"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
)

var (
	// Connection/config errors.
	ErrInvalidProvider         = errors.New("invalid provider specified")
	ErrInvalidEndpoint         = errors.New("invalid endpoint specified")
	ErrInvalidAuthClientID     = errors.New("invalid auth_client_id specified")
	ErrInvalidNamespace        = errors.New("invalid namespace specified")
	ErrInvalidAuthClientSecret = errors.New("invalid auth_client_secret specified")
	ErrInvalidEnvironment      = errors.New("invalid environment specified (required for provider infisical)")
	ErrUnsupportedProvider     = errors.New("unsupported provider specified")

	// Getter / tag errors.
	ErrInvalidGetterType = errors.New("confloader: a field with a conf tag must be of type Getter[T]")
	ErrMissingTag        = errors.New("confloader: a field of type Getter[T] must have a conf tag")
	ErrInvalidTag        = errors.New("confloader: invalid conf tag (expected key=value pairs separated by commas)")
	ErrTagMissingKey     = errors.New("confloader: conf tag must specify key")
	ErrTagMissingFolder  = errors.New("confloader: conf tag must specify folder")
	ErrRootFolder        = errors.New("confloader: config cannot reside in the root folder (folder must not be empty)")

	// ErrNotFound is returned by Getter.Get when a config key is absent in
	// both the requested folder and the default folder. It is a definitive
	// answer (not retried). Re-exported from the client package so callers
	// depend on the confloader API, not the internal subpackage.
	ErrNotFound = client.ErrNotFound

	// Fetch errors.
	ErrParseFailed     = errors.New("confloader: failed to parse config value")
	ErrProviderFailed  = errors.New("confloader: provider fetch failed")
	ErrProviderConnect = errors.New("confloader: failed to connect to provider")
	// ErrStale is the sentinel used to describe the stale state of an entry:
	// its last background refresh failed with a transient provider error while
	// a last-known-good value is still being served by the cache. It is NOT
	// returned by Getter.Get (Get refreshes stale entries on read and surfaces
	// the source's own error instead); it remains available for callers who
	// inspect Loader.IsStale / Loader.StaleReason for health checks.
	ErrStale = errors.New("confloader: config value is stale (last refresh failed, serving last-known-good)")
)
