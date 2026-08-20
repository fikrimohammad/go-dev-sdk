package redis

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// Mode defines the Redis connection and topology mode.
type Mode string

const (
	// ModeStandalone connects to a single Redis instance.
	ModeStandalone Mode = "standalone"
	// ModeSentinel connects to a Redis Sentinel quorum for automatic master failover.
	ModeSentinel Mode = "sentinel"
	// ModeCluster connects to a sharded Redis Cluster.
	ModeCluster Mode = "cluster"
)

// Config holds the connection, topology, TLS, and pool settings for a redis client.
type Config struct {
	// Mode defines the Redis topology: "standalone" (default), "sentinel", or "cluster".
	// If omitted, Mode defaults to "sentinel" when MasterName is set, "cluster" when
	// len(Addrs) > 1 and MasterName is empty, or "standalone" otherwise.
	Mode Mode `yaml:"mode"`

	// Standalone endpoint (fallback when Addrs is empty).
	Host string `yaml:"host"`
	Port int    `yaml:"port"`

	// Addrs provides a list of host:port endpoints:
	// - Standalone: optional single address or fallback from Host:Port
	// - Sentinel: list of Sentinel node addresses (e.g. ["sentinel.service.consul:26379"])
	// - Cluster: list of cluster seed node addresses (e.g. ["redis-cluster.service.consul:6379"])
	Addrs []string `yaml:"addrs"`

	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`

	// Sentinel-specific settings.
	MasterName       string `yaml:"master_name"`       // Required for Sentinel mode
	SentinelUsername string `yaml:"sentinel_username"` // Optional username for Sentinel auth
	SentinelPassword string `yaml:"sentinel_password"` // Optional password for Sentinel auth

	// Cluster & Replica routing options.
	MaxRedirects   int  `yaml:"max_redirects"`    // Max redirects for cluster (default: 3)
	ReadOnly       bool `yaml:"read_only"`        // Route read-only commands to replicas
	RouteByLatency bool `yaml:"route_by_latency"` // Pick replica with lowest latency
	RouteRandomly  bool `yaml:"route_randomly"`   // Pick replica randomly for read distribution

	// TLS settings for secure connections.
	TLSEnabled            bool   `yaml:"tls_enabled"`
	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"`
	TLSServerName         string `yaml:"tls_server_name"`
	TLSCACert             string `yaml:"tls_ca_cert"` // PEM data or file path
	TLSCert               string `yaml:"tls_cert"`    // Client certificate PEM data or file path
	TLSKey                string `yaml:"tls_key"`     // Client private key PEM data or file path

	DialTimeout    time.Duration `yaml:"dial_timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`

	PoolSize        int           `yaml:"pool_size"`
	MinIdleConns    int           `yaml:"min_idle_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

const (
	defaultPort           = 6379
	defaultSentinelPort   = 26379
	defaultConnectTimeout = 5 * time.Second
	defaultMaxRedirects   = 3
)

// SetDefaults fills the zero-valued connection and timeout settings with
// sensible defaults and returns the updated copy.
func (c Config) SetDefaults() Config {
	c.Mode = Mode(strings.ToLower(strings.TrimSpace(string(c.Mode))))
	if c.Mode == "" {
		if c.MasterName != "" {
			c.Mode = ModeSentinel
		} else if len(c.Addrs) > 1 {
			c.Mode = ModeCluster
		} else {
			c.Mode = ModeStandalone
		}
	}

	if c.Port == 0 && c.Host != "" {
		c.Port = defaultPort
	}
	if len(c.Addrs) == 0 && c.Host != "" {
		port := c.Port
		if port == 0 {
			port = defaultPort
		}
		c.Addrs = []string{net.JoinHostPort(c.Host, strconv.Itoa(port))}
	}

	for i, addr := range c.Addrs {
		addr = strings.TrimSpace(addr)
		if addr != "" && !strings.Contains(addr, ":") {
			if c.Mode == ModeSentinel {
				c.Addrs[i] = net.JoinHostPort(addr, strconv.Itoa(defaultSentinelPort))
			} else {
				port := c.Port
				if port == 0 {
					port = defaultPort
				}
				c.Addrs[i] = net.JoinHostPort(addr, strconv.Itoa(port))
			}
		}
	}

	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	if c.Mode == ModeCluster && c.MaxRedirects <= 0 {
		c.MaxRedirects = defaultMaxRedirects
	}
	return c
}

// Validate reports configuration problems, such as a missing host or invalid
// connection settings. It returns nil when the config is usable.
func (c Config) Validate() error {
	var errs []error

	switch c.Mode {
	case ModeSentinel:
		if c.MasterName == "" {
			errs = append(errs, errors.New("redis: master_name is required in sentinel mode"))
		}
		if len(c.Addrs) == 0 {
			errs = append(errs, errors.New("redis: at least one sentinel address in addrs is required in sentinel mode"))
		}
	case ModeCluster:
		if len(c.Addrs) == 0 {
			errs = append(errs, errors.New("redis: at least one cluster seed address in addrs is required in cluster mode"))
		}
		if c.DB != 0 {
			errs = append(errs, fmt.Errorf("redis: cluster mode only supports DB 0, got %d", c.DB))
		}
	case ModeStandalone:
		if len(c.Addrs) == 0 && c.Host == "" {
			errs = append(errs, errors.New("redis: host or addrs is required"))
		}
		if c.Host != "" && (c.Port < 1 || c.Port > 65535) {
			errs = append(errs, fmt.Errorf("redis: port %d is out of range (1-65535)", c.Port))
		}
	default:
		errs = append(errs, fmt.Errorf("redis: unknown mode %q (must be standalone, sentinel, or cluster)", c.Mode))
	}

	if c.DB < 0 {
		errs = append(errs, fmt.Errorf("redis: DB must not be negative, got %d", c.DB))
	}
	if c.DialTimeout < 0 {
		errs = append(errs, fmt.Errorf("redis: DialTimeout must not be negative, got %v", c.DialTimeout))
	}
	if c.ReadTimeout < 0 {
		errs = append(errs, fmt.Errorf("redis: ReadTimeout must not be negative, got %v", c.ReadTimeout))
	}
	if c.WriteTimeout < 0 {
		errs = append(errs, fmt.Errorf("redis: WriteTimeout must not be negative, got %v", c.WriteTimeout))
	}
	if c.ConnectTimeout < 0 {
		errs = append(errs, fmt.Errorf("redis: ConnectTimeout must not be negative, got %v", c.ConnectTimeout))
	}
	if c.PoolSize < 0 {
		errs = append(errs, fmt.Errorf("redis: PoolSize must not be negative, got %d", c.PoolSize))
	}
	if c.MinIdleConns < 0 {
		errs = append(errs, fmt.Errorf("redis: MinIdleConns must not be negative, got %d", c.MinIdleConns))
	}
	if c.MaxIdleConns < 0 {
		errs = append(errs, fmt.Errorf("redis: MaxIdleConns must not be negative, got %d", c.MaxIdleConns))
	}
	if c.PoolSize > 0 && c.MinIdleConns > c.PoolSize {
		errs = append(errs, fmt.Errorf("redis: MinIdleConns (%d) must not exceed PoolSize (%d)", c.MinIdleConns, c.PoolSize))
	}
	if c.PoolSize > 0 && c.MaxIdleConns > c.PoolSize {
		errs = append(errs, fmt.Errorf("redis: MaxIdleConns (%d) must not exceed PoolSize (%d)", c.MaxIdleConns, c.PoolSize))
	}
	if c.MaxIdleConns > 0 && c.MinIdleConns > c.MaxIdleConns {
		errs = append(errs, fmt.Errorf("redis: MinIdleConns (%d) must not exceed MaxIdleConns (%d)", c.MinIdleConns, c.MaxIdleConns))
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		errs = append(errs, errors.New("redis: TLSCert and TLSKey must both be set or both be empty"))
	}
	if !c.TLSEnabled && (c.TLSCACert != "" || c.TLSCert != "" || c.TLSKey != "") {
		errs = append(errs, errors.New("redis: TLSCACert, TLSCert, and TLSKey require TLSEnabled to be true"))
	}
	if c.ConnMaxIdleTime < 0 {
		errs = append(errs, fmt.Errorf("redis: ConnMaxIdleTime must not be negative, got %v", c.ConnMaxIdleTime))
	}
	if c.ConnMaxLifetime < 0 {
		errs = append(errs, fmt.Errorf("redis: ConnMaxLifetime must not be negative, got %v", c.ConnMaxLifetime))
	}
	return errors.Join(errs...)
}
