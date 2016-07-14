// Package dburl provides a standardized way of processing database connection
// strings in the form of a URL.
//
// Standard URLs are of the form
// protocol+transport://user:pass@host/dbname?opt1=a&opt2=b
//
// For example, the following are URLs that can be processed using Parse or Open:
//     postgres://user:pass@localhost/mydb
//     mysql://user:pass@localhost/mydb
//     oracle://user:pass@somehost.com/oracledb
//     sqlite:/path/to/file.db
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

// URL wraps the standard net/url.URL type, adding a Proto string.
type URL struct {
	url.URL
	OriginalScheme, Proto, Driver, DSN string
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

// Parse parses a rawurl string and normalizes the scheme.
func Parse(rawurl string) (*URL, error) {
	// parse url
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		return nil, errors.New("invalid database scheme")
	}

	// create url
	v := &URL{URL: *u, OriginalScheme: u.Scheme, Proto: "tcp"}
	v.Scheme = strings.ToLower(v.Scheme)

	// check if +unix or whatever is in the scheme
	if strings.Contains(v.Scheme, "+") {
		p := strings.SplitN(v.Scheme, "+", 2)
		v.Scheme = p[0]
		v.Proto = p[1]
	}

	// check protocol
	if v.Proto != "tcp" && v.Proto != "udp" && v.Proto != "unix" {
		return nil, errors.New("invalid transport protocol")
	}

	// get loader
	loader, ok := loaders[v.Scheme]
	if !ok {
		return nil, errors.New("unknown database type")
	}

	// process
	v.Driver, v.DSN, err = loader(v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// OpenURL opens a sql.DB connection to the provided URL.
func OpenURL(u *URL) (*sql.DB, error) {
	return sql.Open(u.Driver, u.DSN)
}

// Open takes a rawurl like
// "protocol+transport://user:pass@host/dbname?option1=a&option2=b" and creates a
// standard sql.DB connection.
//
// Supports mysql, postgresql, mssql, sqlite, and oracle databases.
func Open(rawurl string) (*sql.DB, error) {
	u, err := Parse(rawurl)
	if err != nil {
		return nil, err
	}

	return OpenURL(u)
}

// mssqlProcess processes a mssql url and protocol.
func mssqlProcess(u *URL) (string, string, error) {
	var err error

	// build host or domain socket
	host := u.Host
	port := 1433
	var dbname string

	// grab dbname
	if u.Path != "" {
		dbname = u.Path[1:]
	}
	if dbname == "" {
		return "", "", errors.New("no database name specified")
	}

	// extract port if present
	pos := strings.Index(host, ":")
	if pos != -1 {
		port, err = strconv.Atoi(host[pos+1:])
		if err != nil {
			return "", "", errors.New("invalid port")
		}
		host = host[:pos]
	}

	// format dsn
	dsn := fmt.Sprintf("server=%s;port=%d;database=%s", host, port, dbname)

	// add user/pass
	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			dsn = dsn + ";user id=" + user
		}
		if pass, ok := u.User.Password(); ok {
			dsn = dsn + ";password=" + pass
		}
	}

	// add params
	for k, v := range u.Query() {
		dsn = dsn + ";" + k + "=" + v[0]
	}

	return "mssql", dsn, nil
}

// mysqlProcess processes a mssql url and protocol.
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
	} else if !strings.Contains(host, ":") {
		// append default port
		host = host + ":3306"
	}

	// create dsn
	dsn := fmt.Sprintf("%s(%s)", u.Proto, host)

	// build user/pass
	userinfo := ""
	if u.User != nil {
		if un := u.User.Username(); len(un) > 0 {
			userinfo = un
			if up, ok := u.User.Password(); ok {
				userinfo = userinfo + ":" + up
			}
		}
	}

	if userinfo != "" {
		dsn = userinfo + "@" + dsn
	}
	if dbname != "" {
		dsn = dsn + "/" + dbname
	}

	// add params
	params := u.Query().Encode()
	if len(params) > 0 {
		dsn = dsn + "?" + params
	}

	// format
	return "mysql", dsn, nil
}

// oracleProcess processes a mssql url and protocol.
func oracleProcess(u *URL) (string, string, error) {
	if u.User == nil {
		return "", "", errors.New("must provide username and password in oracle dsn")
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

// postgresProcess processes a mssql url and protocol.
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

// sqliteProcess processes a mssql url and protocol.
func sqliteProcess(u *URL) (string, string, error) {
	p := u.Opaque
	if u.Path != "" {
		p = u.Path
	}

	if u.Host != "" && u.Host != "localhost" {
		p = path.Join(u.Host, p)
	}

	return "sqlite3", p + u.Query().Encode(), nil
}

var loaders map[string]func(*URL) (string, string, error)

func init() {
	loaders = map[string]func(*URL) (string, string, error){
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
	}
}
