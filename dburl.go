// Package dburl provides a standard, URL style mechanism for of parsing, and
// opening database connection strings (in the form of a URL).
//
// Supported URLs are of the form:
//
//     protocol+transport://user:pass@host/dbname?opt1=a&opt2=b
//
//                   -- OR --
//
//     protocol:/path/to/file
//
// Where:
//
//     protocol   - driver name or alias (see below)
//     transport  - the transport protocol [tcp, udp, unix] (only mysql for now)
//     user       - the username to connect as
//     pass       - the password to use
//     host       - the remote host
//     dbname     - the database or service name to connect to
//     ?opt1=...  - additional database driver options (see the respective SQL driver for available options)
//
// For example, the following are URLs that can be processed using Parse or
// Open:
//
//     postgres://user:pass@localhost/dbname
//     pg://user:pass@localhost/dbname?sslmode=disable
//     mysql://user:pass@localhost/dbname
//     mysql:/var/run/mysqld/mysqld.sock
//     sqlserver://user:pass@remote-host.com/dbname
//     oracle://user:pass@somehost.com/oracledb
//     sap://user:pass@localhost/dbname
//     sqlite:/path/to/file.db
//     file:myfile.sqlite3?loc=auto
//
// Parse relies heavily on the standard net/url/URL type, as such it has the
// same parsing conventions/semantics for any URL that is parsed by the
// standard Go library's net/url/Parse.
//
// SQL Driver aliases for protocols:
//
// The following protocol aliases are available, and any URL passed to Open or
// Parse will be procesesd the same as their respective driver:
//
//     Database (driver)            | Aliases
//     ------------------------------------------------------------------
//     Microsoft SQL Server (mssql) | ms, sqlserver
//     MySQL (mysql)                | my, mariadb, maria, percona, aurora
//     Oracle (ora)                 | or, oracle, oci8, oci
//     PostgreSQL (postgres)        | pg, postgresql, pgsql
//     SAP HANA (hdb)               | sa, saphana, sap, hana
//     SQLite3 (sqlite3)            | sq, sqlite, file
//
// Please note that this package does not import actual SQL drivers, and only
// provides a standard mechanism to parse their respective URL string.
//
// For reference, these are the following "expected" SQL drivers one would need
// to import:
//
//     Database (driver)            | Package
//     ------------------------------------------------------------------
//     Microsoft SQL Server (mssql) | github.com/denisenkom/go-mssqldb
//     MySQL (mysql)                | github.com/go-sql-driver/mysql
//     Oracle (ora)                 | gopkg.in/rana/ora.v4
//     PostgreSQL (postgres)        | github.com/lib/pq
//     SAP HANA (hdb)               | github.com/SAP/go-hdb/driver
//     SQLite3 (sqlite3)            | github.com/mattn/go-sqlite3
//
// This package was written mainly to support xo (https://github.com/knq/xo)
// and usql (https://github.com/knq/usql).
package dburl

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
)

var (
	// ErrInvalidDatabaseScheme is the invalid database scheme error.
	ErrInvalidDatabaseScheme = errors.New("invalid database scheme")

	// ErrInvalidTransportProtocol is the invalid transport protocol error.
	ErrInvalidTransportProtocol = errors.New("invalid transport protocol")

	// ErrUnknownDatabaseType is the unknown database type error.
	ErrUnknownDatabaseType = errors.New("unknown database type")

	// ErrInvalidPort is the invalid port error.
	ErrInvalidPort = errors.New("invalid port")

	// ErrOraMustProvideUsernameAndPassword is the ora (Oracle) must provide
	// username and password error.
	ErrOraMustProvideUsernameAndPassword = errors.New("ora: must provide username and password")
)

// URL wraps the standard net/url.URL type, adding OriginalScheme, Proto,
// Driver, and DSN strings.
type URL struct {
	// URL is the base net/url/URL.
	url.URL

	// OriginalScheme is the original parsed scheme (ie, "sq", "mysql+unix", "sap", etc).
	OriginalScheme string

	// Proto is the specified protocol (ie, "tcp", "udp", "unix"), if provided.
	Proto string

	// Driver is the non-aliased SQL driver name that should be used in a call
	// to sql/Open.
	Driver string

	// DSN is the built connection "data source name" that can be used in a
	// call to sql/Open.
	DSN string
}

// String satisfies the stringer interface.
func (u *URL) String() string {
	p := &url.URL{
		Scheme:   u.OriginalScheme,
		Opaque:   u.Opaque,
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	return p.String()
}

// Short provides a short description of the user, host, and database.
func (u *URL) Short() string {
	s := u.Driver[:2]
	switch s {
	case "po":
		s = "pg"
	case "hd":
		s = "sa"
	}

	if u.Proto != "tcp" {
		s += "+" + u.Proto
	}

	s += ":"

	if u.User != nil {
		if un := u.User.Username(); un != "" {
			s += un + "@"
		}
	}

	if u.Host != "" {
		s += u.Host
	}

	if u.Path != "" && u.Path != "/" {
		s += u.Path
	}

	if u.Opaque != "" {
		s += u.Opaque
	}

	return s
}

// Parse parses a rawurl string and normalizes the scheme.
func Parse(rawurl string) (*URL, error) {
	// parse url
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		return nil, ErrInvalidDatabaseScheme
	}

	// create url
	v := &URL{URL: *u, OriginalScheme: u.Scheme, Proto: "tcp"}
	v.Scheme = strings.ToLower(v.Scheme)

	// check if +protocol is in scheme
	if strings.Contains(v.Scheme, "+") {
		p := strings.SplitN(v.Scheme, "+", 2)
		v.Scheme = p[0]
		v.Proto = p[1]
	}

	// check protocol
	if v.Proto != "tcp" && v.Proto != "udp" && v.Proto != "unix" {
		return nil, ErrInvalidTransportProtocol
	}

	// get loader
	loader, ok := loaders[v.Scheme]
	if !ok {
		return nil, ErrUnknownDatabaseType
	}

	// process
	v.Driver, v.DSN, err = loader(v)
	if err != nil {
		return nil, err
	}

	if v.Driver != "sqlite3" && v.Opaque != "" {
		return Parse(v.OriginalScheme + "://" + v.Opaque)
	}

	return v, nil
}

// Open takes a rawurl like "protocol+transport://user:pass@host/dbname?option1=a&option2=b"
// and creates a standard sql.DB connection.
//
// See Parse for information on formatting URLs to work properly with Open.
func Open(rawurl string) (*sql.DB, error) {
	u, err := Parse(rawurl)
	if err != nil {
		return nil, err
	}

	return sql.Open(u.Driver, u.DSN)
}

// mssqlProcess processes a mssql url and protocol.
func mssqlProcess(u *URL) (string, string, error) {
	var err error

	// build host or domain socket
	host := u.Host
	port := 1433

	// grab dbname
	var dbname string
	if u.Path != "" {
		dbname = u.Path[1:]
	}

	// extract port if present
	pos := strings.Index(host, ":")
	if pos != -1 {
		port, err = strconv.Atoi(host[pos+1:])
		if err != nil {
			return "", "", ErrInvalidPort
		}
		host = host[:pos]
	}

	// format dsn
	dsn := fmt.Sprintf("server=%s;port=%d", host, port)
	if dbname != "" {
		dsn += ";database=" + dbname
	}

	// add user/pass
	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			dsn += ";user id=" + user
		}
		if pass, ok := u.User.Password(); ok {
			dsn += ";password=" + pass
		}
	}

	// add params
	for k, v := range u.Query() {
		dsn += ";" + k + "=" + v[0]
	}

	return "mssql", dsn, nil
}

// mysqlProcess processes a mysql url and protocol.
func mysqlProcess(u *URL) (string, string, error) {
	// build host or domain socket
	host := u.Host
	dbname := u.Path

	if strings.HasPrefix(dbname, "/") {
		dbname = dbname[1:]
	}

	if u.Proto == "unix" {
		if u.Opaque != "" {
			host = path.Dir(u.Opaque)
			dbname = path.Base(u.Opaque)
		} else {
			host = path.Join(u.Host, path.Dir(u.Path))
			dbname = path.Base(u.Path)
		}
		host = host + "/" + dbname
		dbname = ""

		u.Host = host
		u.Path = ""
	} else if !strings.Contains(host, ":") {
		// append default port
		host = host + ":3306"
	}

	// create dsn
	dsn := fmt.Sprintf("%s(%s)", u.Proto, host)

	// build user/pass
	if u.User != nil {
		if un := u.User.Username(); len(un) > 0 {
			if up, ok := u.User.Password(); ok {
				un += ":" + up
			}
			dsn = un + "@" + dsn
		}
	}

	// add database name
	dsn += "/" + dbname

	// add params
	params := u.Query().Encode()
	if len(params) > 0 {
		dsn += "?" + params
	}

	// format
	return "mysql", dsn, nil
}

// oracleProcess processes a ora (Oracle) url and protocol.
func oracleProcess(u *URL) (string, string, error) {
	if u.User == nil {
		return "", "", ErrOraMustProvideUsernameAndPassword
	}

	// build host or domain socket
	host := u.Host
	dbname := u.Path[1:]

	// build user/pass
	userinfo := ""
	if un := u.User.Username(); len(un) > 0 {
		userinfo = un
		if up, ok := u.User.Password(); ok {
			userinfo = userinfo + "/" + up
		}
	}

	// format
	return "ora", fmt.Sprintf(
		"%s@%s/%s",
		userinfo,
		host,
		dbname,
	), nil
}

// postgresProcess processes a postgres url and protocol.
func postgresProcess(u *URL) (string, string, error) {
	p := &url.URL{
		Scheme:   "postgres",
		Opaque:   u.Opaque,
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	return "postgres", p.String(), nil
}

// sapProcess processes a hdb url and protocol.
func sapProcess(u *URL) (string, string, error) {
	p := &url.URL{
		Scheme:   "hdb",
		Opaque:   u.Opaque,
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	return "hdb", p.String(), nil
}

// sqliteProcess processes a sqlite3 url and protocol.
func sqliteProcess(u *URL) (string, string, error) {
	dsn := u.Opaque
	if u.Path != "" {
		dsn = u.Path
	}

	if u.Host != "" && u.Host != "localhost" {
		dsn = path.Join(u.Host, dsn)
	}

	// add params
	params := u.Query().Encode()
	if len(params) > 0 {
		dsn += "?" + params
	}

	return "sqlite3", dsn, nil
}

var loaders = map[string]func(*URL) (string, string, error){
	// mssql
	"mssql":     mssqlProcess,
	"sqlserver": mssqlProcess,
	"ms":        mssqlProcess,

	// mysql
	"mysql":   mysqlProcess,
	"mariadb": mysqlProcess,
	"maria":   mysqlProcess,
	"percona": mysqlProcess,
	"aurora":  mysqlProcess,
	"my":      mysqlProcess,

	// oracle
	"ora":    oracleProcess,
	"oracle": oracleProcess,
	"oci8":   oracleProcess,
	"oci":    oracleProcess,
	"or":     oracleProcess,

	// postgresql
	"postgres":   postgresProcess,
	"postgresql": postgresProcess,
	"pgsql":      postgresProcess,
	"pg":         postgresProcess,

	// sqlite
	"sqlite3": sqliteProcess,
	"sqlite":  sqliteProcess,
	"file":    sqliteProcess,
	"sq":      sqliteProcess,

	// sap hana
	"hdb":     sapProcess,
	"hana":    sapProcess,
	"sap":     sapProcess,
	"saphana": sapProcess,
	"sa":      sapProcess,
}

// AddLoaderAliases copies the existing loader set for name for each of the
// aliases.
func AddLoaderAliases(name string, aliases ...string) error {
	f, ok := loaders[name]
	if !ok {
		return ErrInvalidDatabaseScheme
	}

	for _, alias := range aliases {
		loaders[alias] = f
	}

	return nil
}
