package db

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 25
	defaultConnMaxLifetime = 3 * time.Minute
	defaultConnMaxIdleTime = 5 * time.Minute
)

// Config holds the connection and pool settings for a database. Call
// [Config.SetDefaults] to fill in zero values; [Connect] does so automatically.
type Config struct {
	// Driver selects the database driver and dialect: "mysql" (default), "pgx",
	// "pgx/v5", "postgres", or "postgresql". The pgx, pgx/v5, postgres, and
	// postgresql names all use the bundled pgx driver. Other driver names are
	// accepted when DSN is set and the driver is registered with [database/sql].
	Driver string `yaml:"driver"`

	// DSN, when set, is used verbatim and takes precedence over Host, Port,
	// Database, Username, Password, and SSLMode. It may use any format the
	// driver understands (for PostgreSQL: a postgres:// URL or a keyword=value
	// string; for MySQL: the go-sql-driver/mysql format).
	DSN string `yaml:"dsn"`

	// Host is the database server hostname or IP address. It is required
	// unless DSN is set.
	Host string `yaml:"host"`

	// Port is the database server port. When it is zero and Host is set, it
	// defaults to 3306 for MySQL and 5432 for PostgreSQL. It is ignored when
	// DSN is set.
	Port int `yaml:"port"`

	// Database is the database name, reported as the db.namespace telemetry
	// attribute. It is required unless DSN is set.
	Database string `yaml:"database"`

	// Username is the database user name. It is required unless DSN is set.
	Username string `yaml:"username"`

	// Password is the database user's password. It may be empty.
	Password string `yaml:"password"`

	// SSLMode selects the TLS mode: disable, allow, prefer, require,
	// verify-ca, verify-full, or empty for the driver default. PostgreSQL
	// receives it as the sslmode DSN parameter; MySQL maps it to the driver's
	// tls parameter (allow and prefer both use "preferred"; verify-ca and
	// verify-full both enable full certificate and hostname verification).
	// It is ignored when DSN is set.
	SSLMode string `yaml:"ssl_mode"`

	// MaxOpenConns is the maximum number of open connections to the database.
	// The zero value means the default (25); negative values are invalid.
	MaxOpenConns int `yaml:"max_open_conns"`

	// MaxIdleConns is the maximum number of idle connections in the pool. The
	// zero value means MaxOpenConns, and the value must not exceed
	// MaxOpenConns.
	MaxIdleConns int `yaml:"max_idle_conns"`

	// ConnMaxLifetime is the maximum amount of time a connection may be
	// reused. The zero value means the default (3m); negative values are
	// invalid.
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`

	// ConnMaxIdleTime is the maximum amount of time a connection may remain
	// idle. The zero value means the default (5m); negative values are
	// invalid.
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
}

// SetDefaults fills the zero-valued driver, port, and pool settings with
// sensible defaults and returns the updated copy. Negative pool values are
// deliberately left in place so Validate can report them.
//
// Driver and SSLMode are trimmed and lowercased. An empty Driver defaults to
// "mysql". When Port is zero and Host is set, it defaults to the driver's
// standard port: 3306 for mysql and 5432 for postgres/pgx. An unknown driver
// has no default port. Port is never defaulted when Host is empty or when DSN
// is used.
//
// Pool defaults are shared by MySQL and PostgreSQL:
//   - MaxOpenConns: 25. The default stays below the common server limits (MySQL
//     max_connections 151, PostgreSQL max_connections 100) while leaving
//     headroom for other clients.
//   - MaxIdleConns: equal to MaxOpenConns (default 25), so connections are
//     reused instead of being opened and closed on every query. An explicitly
//     set MaxOpenConns carries MaxIdleConns along with it.
//   - ConnMaxLifetime: 3m. The MySQL driver documents this as required and
//     recommends a lifetime shorter than the ~5m idle timeouts applied by
//     common middlewares, so connections are recycled before being cut off.
//   - ConnMaxIdleTime: 5m. Idle connections expire sooner than the common
//     server idle timeouts (MySQL wait_timeout, PostgreSQL idle_session_timeout
//     when configured), avoiding stale connections in the pool.
//
// Connection settings (Host, Port, Database, Username, Password) are never
// defaulted except for Port as described above.
func (c Config) SetDefaults() Config {
	c.Driver = strings.ToLower(strings.TrimSpace(c.Driver))
	c.SSLMode = strings.ToLower(strings.TrimSpace(c.SSLMode))
	if c.Driver == "" {
		c.Driver = "mysql"
	}
	if c.Port == 0 && c.Host != "" && c.DSN == "" {
		if spec, ok := driverFor(c.Driver); ok {
			c.Port = spec.port
		}
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = defaultMaxOpenConns
	}
	if c.MaxIdleConns == 0 {
		if c.MaxOpenConns > 0 {
			c.MaxIdleConns = c.MaxOpenConns
		} else {
			c.MaxIdleConns = defaultMaxIdleConns
		}
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = defaultConnMaxLifetime
	}
	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = defaultConnMaxIdleTime
	}
	return c
}

// Validate reports configuration problems, such as an unsupported driver,
// missing connection settings, or invalid pool values. It returns nil when the
// config is usable.
//
// When DSN is set, the discrete connection fields are not required because the
// driver consumes the DSN instead. A driver the package does not recognize is
// accepted only with an explicit DSN.
//
// Validate does not apply defaults; call [Config.SetDefaults] first, as
// [Connect] does.
func (c Config) Validate() error {
	var errs []error

	_, known := driverFor(c.Driver)
	if !known && c.DSN == "" {
		errs = append(errs, fmt.Errorf("db: unsupported driver %q: use mysql, pgx, pgx/v5, postgres, or postgresql, or set DSN for a custom driver", c.Driver))
	}
	if c.DSN == "" {
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
	}
	if c.SSLMode != "" {
		switch c.SSLMode {
		case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		default:
			errs = append(errs, fmt.Errorf("db: SSLMode %q is not a valid SSL mode (disable, allow, prefer, require, verify-ca, verify-full)", c.SSLMode))
		}
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
