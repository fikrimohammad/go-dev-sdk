package db

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	// Register pgx's database/sql driver under "pgx" (and "pgx/v5") so the
	// built-in PostgreSQL support works without a blank import in the caller.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// dialect selects the SQL syntax rules used for DSN rendering, placeholder
// conversion, and literal scanning.
type dialect int

const (
	dialectMySQL dialect = iota
	dialectPostgres
	dialectGeneric
)

// bindType is the placeholder syntax a driver expects. It mirrors the bind
// types of sqlx, but is kept package-local so the binding pipeline does not
// depend on sqlx's driver registry for the built-in drivers.
type bindType int

const (
	// bindQuestion keeps the positional '?' syntax (MySQL and other
	// question-mark drivers).
	bindQuestion bindType = iota
	// bindDollar emits PostgreSQL's numbered '$1' placeholders.
	bindDollar
	// bindNamed emits oracle-style ':arg1' placeholders.
	bindNamed
	// bindAt emits SQL Server's '@p1' placeholders.
	bindAt
)

// driverSpec describes one database driver: the name registered with
// database/sql, the OTel system name, default port, and the dialect rules used
// to render DSNs, rebind queries, and classify errors.
type driverSpec struct {
	name       string   // database/sql driver name passed to sql.Open
	systemName string   // OTel db.system.name
	port       int      // default port used by Config.SetDefaults (0 = none)
	dialect    dialect  // SQL dialect
	bindType   bindType // positional placeholder syntax produced by bindQuery
}

var (
	mysqlDriver = driverSpec{
		name:       "mysql",
		systemName: "mysql",
		port:       3306,
		dialect:    dialectMySQL,
		bindType:   bindQuestion,
	}

	postgresDriver = driverSpec{
		name:       "pgx",
		systemName: "postgresql",
		port:       5432,
		dialect:    dialectPostgres,
		bindType:   bindDollar,
	}
)

// driverFor resolves a configured driver name to its spec. "pgx/v5",
// "postgres", and "postgresql" are accepted as aliases for the bundled pgx
// driver, so callers do not need to import github.com/lib/pq. An empty name
// means mysql.
func driverFor(name string) (driverSpec, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "mysql":
		return mysqlDriver, true
	case "pgx", "pgx/v5", "postgres", "postgresql":
		return postgresDriver, true
	default:
		return driverSpec{}, false
	}
}

// genericDriver describes a database/sql driver the package has no built-in
// knowledge of. Such a driver must be registered by the caller and is usable
// only with an explicit Config.DSN; its placeholder syntax is taken from sqlx's
// driver registry, and it is instrumented under its own driver name.
func genericDriver(name string) driverSpec {
	return driverSpec{
		name:       name,
		systemName: name,
		dialect:    dialectGeneric,
		bindType:   sqlxBindType(name),
	}
}

// sqlxBindType maps a driver registered with sqlx to this package's bindType.
func sqlxBindType(name string) bindType {
	switch sqlx.BindType(name) {
	case sqlx.DOLLAR:
		return bindDollar
	case sqlx.NAMED:
		return bindNamed
	case sqlx.AT:
		return bindAt
	default:
		return bindQuestion
	}
}

// dsn renders the data source name for cfg: Config.DSN verbatim when set,
// otherwise a driver-specific DSN. It returns "" for an unknown driver without
// a DSN; Config.Validate rejects that combination before Connect gets here.
func (d driverSpec) dsn(cfg Config) string {
	if cfg.DSN != "" {
		return cfg.DSN
	}
	switch d.dialect {
	case dialectMySQL:
		return mysqlDSN(cfg)
	case dialectPostgres:
		return postgresDSN(cfg)
	default:
		return ""
	}
}

// mysqlDSN renders a go-sql-driver/mysql data source name from cfg. parseTime
// is enabled so DATE/DATETIME/TIMESTAMP columns scan into [time.Time], and
// cfg.SSLMode is mapped to the driver's tls parameter.
func mysqlDSN(cfg Config) string {
	mc := mysql.NewConfig()
	mc.User = cfg.Username
	mc.Passwd = cfg.Password
	mc.Net = "tcp"
	mc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	mc.DBName = cfg.Database
	mc.ParseTime = true
	mc.TLSConfig = mysqlTLSConfig(cfg.SSLMode)
	return mc.FormatDSN()
}

// mysqlTLSConfig maps the shared SSLMode values to go-sql-driver/mysql's tls
// parameter. MySQL has no separate CA-only verification mode, so verify-ca and
// verify-full both enable full certificate and hostname verification; custom
// CAs, client certificates, and other TLS options must be supplied through a
// registered TLS config name in Config.DSN.
func mysqlTLSConfig(sslMode string) string {
	switch sslMode {
	case "disable":
		return "false"
	case "allow", "prefer":
		// go-sql-driver has no exact "allow" mode; preferred is the closest
		// behavior (TLS when available).
		return "preferred"
	case "require":
		return "skip-verify"
	case "verify-ca", "verify-full":
		return "true"
	default:
		return ""
	}
}

// postgresDSN renders a PostgreSQL URL from cfg. Both pgx and lib/pq accept
// this form, and URL escaping keeps usernames, passwords, and database names
// that contain reserved characters intact.
func postgresDSN(cfg Config) string {
	u := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + cfg.Database,
	}
	if cfg.Username != "" {
		if cfg.Password != "" {
			u.User = url.UserPassword(cfg.Username, cfg.Password)
		} else {
			u.User = url.User(cfg.Username)
		}
	}
	if cfg.SSLMode != "" {
		u.RawQuery = url.Values{"sslmode": {cfg.SSLMode}}.Encode()
	}
	return u.String()
}

// connInfo is the connection identity used for telemetry attributes.
type connInfo struct {
	host     string
	port     int
	database string
}

// connInfo derives the host, port, and database from cfg. When Config.DSN is
// set, it is parsed with the driver's own parser so telemetry reflects the
// effective connection rather than the (ignored) discrete fields. Parse
// failures leave the discrete fields untouched: Connect will surface the real
// error when it opens the pool.
func (d driverSpec) connInfo(cfg Config) connInfo {
	info := connInfo{host: cfg.Host, port: cfg.Port, database: cfg.Database}
	if cfg.DSN == "" {
		return info
	}

	switch d.dialect {
	case dialectMySQL:
		mc, err := mysql.ParseDSN(cfg.DSN)
		if err != nil {
			return info
		}
		if mc.DBName != "" {
			info.database = mc.DBName
		}
		switch mc.Net {
		case "tcp", "tcp4", "tcp6":
			if host, port, err := net.SplitHostPort(mc.Addr); err == nil {
				info.host = host
				if p, err := strconv.Atoi(port); err == nil {
					info.port = p
				}
			} else if mc.Addr != "" {
				info.host = mc.Addr
			}
		}
	case dialectPostgres:
		pc, err := pgx.ParseConfig(cfg.DSN)
		if err != nil {
			return info
		}
		info.host = pc.Host
		info.port = int(pc.Port)
		info.database = pc.Database
	}
	return info
}

// errorInfo classifies err into a database response code and a human-readable
// message:
//
//   - MySQL: the driver error number and its message ("1062", "duplicate entry").
//   - PostgreSQL: the five-character SQLSTATE and the server message.
//   - any other error: an empty code and err.Error().
func errorInfo(err error) (code, message string) {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return strconv.Itoa(int(me.Number)), me.Message
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code, pe.Message
	}
	return "", err.Error()
}
