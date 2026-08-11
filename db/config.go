package db

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 25
	defaultConnMaxLifetime = 3 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// Config holds the connection and pool settings for a database.
type Config struct {
	Driver   string `yaml:"driver"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`

	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
}

// SetDefaults fills the zero-valued pool settings with sensible defaults and
// returns the updated copy. An empty Driver also defaults to "mysql".
//
// Defaults, based on the go-sql-driver/mysql recommendations and the common
// "configuring database/sql for performance" guidance:
//   - MaxOpenConns: 25. A connection limit should always be set explicitly; 25
//     stays comfortably below MySQL's default max_connections (151).
//   - MaxIdleConns: equal to MaxOpenConns (default 25), so connections are
//     reused instead of being opened and closed on every query. An explicitly
//     set MaxOpenConns carries MaxIdleConns along with it.
//   - ConnMaxLifetime: 3m. The driver documents this as required and
//     recommends a lifetime shorter than the ~5m idle timeouts applied by
//     common middlewares, so connections are recycled before being cut off.
//   - ConnMaxIdleTime: 5m. Idle connections expire sooner than MySQL's
//     wait_timeout, avoiding stale connections in the pool.
//
// Connection settings (Host, Port, Database, Username, Password) are never
// defaulted.
func (c Config) SetDefaults() Config {
	if c.Driver == "" {
		c.Driver = "mysql"
	}
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = defaultMaxOpenConns
	}
	if c.MaxIdleConns <= 0 {
		if c.MaxOpenConns > 0 {
			c.MaxIdleConns = c.MaxOpenConns
		} else {
			c.MaxIdleConns = defaultMaxIdleConns
		}
	}
	if c.ConnMaxLifetime <= 0 {
		c.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if c.ConnMaxIdleTime <= 0 {
		c.ConnMaxIdleTime = defaultConnMaxIdleTime
	}
	return c
}

// Validate reports configuration problems, such as missing connection
// settings or invalid pool values. It returns nil when the config is usable.
func (c Config) Validate() error {
	var errs []error
	if c.Host == "" {
		errs = append(errs, errors.New("db: host is required"))
	}
	if c.Port < 1 || c.Port > 65535 {
		errs = append(errs, fmt.Errorf("db: port %d is out of range (1-65535)", c.Port))
	}
	if c.Database == "" {
		errs = append(errs, errors.New("db: database is required"))
	}
	if c.Username == "" {
		errs = append(errs, errors.New("db: username is required"))
	}
	if c.MaxOpenConns < 0 {
		errs = append(errs, fmt.Errorf("db: MaxOpenConns must not be negative, got %d", c.MaxOpenConns))
	}
	if c.MaxIdleConns < 0 {
		errs = append(errs, fmt.Errorf("db: MaxIdleConns must not be negative, got %d", c.MaxIdleConns))
	}
	if c.MaxOpenConns > 0 && c.MaxIdleConns > c.MaxOpenConns {
		errs = append(errs, fmt.Errorf("db: MaxIdleConns (%d) must not exceed MaxOpenConns (%d)", c.MaxIdleConns, c.MaxOpenConns))
	}
	if c.ConnMaxLifetime < 0 {
		errs = append(errs, fmt.Errorf("db: ConnMaxLifetime must not be negative, got %v", c.ConnMaxLifetime))
	}
	if c.ConnMaxIdleTime < 0 {
		errs = append(errs, fmt.Errorf("db: ConnMaxIdleTime must not be negative, got %v", c.ConnMaxIdleTime))
	}
	return errors.Join(errs...)
}
