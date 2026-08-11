package redis

import (
	"errors"
	"fmt"
	"time"
)

// Config holds the connection and pool settings for a redis client.
type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`

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
	defaultConnectTimeout = 5 * time.Second
)

// SetDefaults fills the zero-valued connection and timeout settings with
// sensible defaults and returns the updated copy.
//
//   - Port defaults to 6379, the standard redis port.
//   - ConnectTimeout defaults to 5s.
//
// Pool settings are left at their zero value, which preserves the go-redis
// defaults; Dial/Read/Write timeouts likewise keep the go-redis defaults when
// unset. Connection settings (Host, Username, Password, DB) are never
// defaulted.
func (c Config) SetDefaults() Config {
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	return c
}

// Validate reports configuration problems, such as a missing host or invalid
// connection settings. It returns nil when the config is usable.
func (c Config) Validate() error {
	var errs []error
	if c.Host == "" {
		errs = append(errs, errors.New("redis: host is required"))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("redis: port %d is out of range (1-65535)", c.Port))
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
	if c.ConnMaxIdleTime < 0 {
		errs = append(errs, fmt.Errorf("redis: ConnMaxIdleTime must not be negative, got %v", c.ConnMaxIdleTime))
	}
	if c.ConnMaxLifetime < 0 {
		errs = append(errs, fmt.Errorf("redis: ConnMaxLifetime must not be negative, got %v", c.ConnMaxLifetime))
	}
	return errors.Join(errs...)
}
