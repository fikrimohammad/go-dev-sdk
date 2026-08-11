package confloader

import (
	"time"

	"github.com/fikrimohammad/go-dev-sdk/confloader/client"
)

// Provider identifies the backend that stores the configuration/secrets.
type Provider string

const (
	ProviderEtcd      Provider = "etcd"
	ProviderInfisical Provider = "infisical"
)

// AllowedProviders is the canonical list of supported providers.
var AllowedProviders = []Provider{
	ProviderEtcd,
	ProviderInfisical,
}

// DefaultFolder is the standardized fallback folder. When a key is missing in
// the requested folder, the loader falls back to this folder inside the same
// namespace. Re-exported from the client package for convenience.
const DefaultFolder = client.DefaultFolder

// Config is the standardized connection + polling configuration shared by
// every provider. It describes WHERE the config lives (provider, endpoint,
// credentials, project) and HOW the cache is kept fresh (polling watcher).
type Config struct {
	// Provider is the backend that stores the configs.
	Provider Provider `yaml:"provider" json:"provider"`

	// Endpoint is the host[:port] of the provider instance. For IP-based
	// endpoints the port must be included (e.g. "localhost:2379").
	Endpoint string `yaml:"endpoint" json:"endpoint"`

	// AuthClientID is the username / client ID used to authenticate.
	AuthClientID string `yaml:"auth_client_id" json:"auth_client_id"`

	// AuthClientSecret is the password / client secret used to authenticate.
	AuthClientSecret string `yaml:"auth_client_secret" json:"auth_client_secret"`

	// Namespace is the root namespace for this app.
	//   - etcd:    the root key prefix (a config may never live directly under it).
	//   - infisical: the workspace/project ID the secrets belong to.
	Namespace string `yaml:"namespace" json:"namespace"`

	// Environment is only used by some providers. infisical requires a
	// non-empty environment (e.g. "prod", "staging") to scope secrets; it is
	// ignored by etcd. Leave empty for providers that do not use it.
	Environment string `yaml:"environment" json:"environment"`

	// Watcher configures the polling mechanism that keeps the local cache
	// fresh. Even providers that support push/watch use polling as the
	// canonical refresh path here, so staleness stays predictable.
	Watcher WatcherConfig `yaml:"watcher" json:"watcher"`
}

// WatcherConfig controls the polling loop that refreshes cached entries.
type WatcherConfig struct {
	// PollingInterval is the base period between refresh attempts.
	PollingInterval time.Duration `yaml:"polling_interval" json:"polling_interval"`

	// PollingMaxRetries is how many times a failed refresh is retried before
	// the stale cache value is kept and the next interval is awaited.
	PollingMaxRetries int `yaml:"polling_max_retries" json:"polling_max_retries"`

	// PollingRetryDelay is the base backoff between retries of a failed refresh.
	PollingRetryDelay time.Duration `yaml:"polling_retry_delay" json:"polling_retry_delay"`

	// PollingRetryBackoff is the multiplier applied to the retry delay on each
	// attempt (exponential backoff). A value <= 1 disables growth.
	PollingRetryBackoff float64 `yaml:"polling_retry_backoff" json:"polling_retry_backoff"`
}

// DefaultWatcherConfig returns sane polling defaults: refresh every 30s,
// retry a failed refresh up to 3 times with 1s base backoff growing x2.
func DefaultWatcherConfig() WatcherConfig {
	return WatcherConfig{
		PollingInterval:     30 * time.Second,
		PollingMaxRetries:   3,
		PollingRetryDelay:   1 * time.Second,
		PollingRetryBackoff: 2.0,
	}
}

// applyDefaults fills zero-valued polling fields with DefaultWatcherConfig.
func (c *Config) applyDefaults() {
	def := DefaultWatcherConfig()
	if c.Watcher.PollingInterval <= 0 {
		c.Watcher.PollingInterval = def.PollingInterval
	}
	if c.Watcher.PollingMaxRetries <= 0 {
		c.Watcher.PollingMaxRetries = def.PollingMaxRetries
	}
	if c.Watcher.PollingRetryDelay <= 0 {
		c.Watcher.PollingRetryDelay = def.PollingRetryDelay
	}
	if c.Watcher.PollingRetryBackoff <= 0 {
		c.Watcher.PollingRetryBackoff = def.PollingRetryBackoff
	}
}

// validate ensures the connection config is usable. The polling config is
// defaulted by applyDefaults, so it is not validated for emptiness here.
func (c *Config) validate() error {
	if c.Provider == "" {
		return ErrInvalidProvider
	}

	if c.Provider != ProviderInfisical && c.Endpoint == "" {
		return ErrInvalidEndpoint
	}

	if c.AuthClientID == "" {
		return ErrInvalidAuthClientID
	}

	if c.AuthClientSecret == "" {
		return ErrInvalidAuthClientSecret
	}

	if c.Namespace == "" {
		return ErrInvalidNamespace
	}

	if c.Provider == ProviderInfisical && c.Environment == "" {
		return ErrInvalidEnvironment
	}

	switch c.Provider {
	case ProviderEtcd, ProviderInfisical:
		// supported
	default:
		return ErrUnsupportedProvider
	}

	return nil
}
