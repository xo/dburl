package dburl

import (
	"errors"
	"net/url"
	"os"
	stdpath "path"
	"sort"
	"strings"
)

// GenScheme returns a func that generates a scheme:// style DSN from the
// passed URL.
func GenScheme(scheme string) func(*URL) (string, error) {
	return func(u *URL) (string, error) {
		z := &url.URL{
			Scheme:   scheme,
			Opaque:   u.Opaque,
			User:     u.User,
			Host:     u.Host,
			Path:     u.Path,
			RawPath:  u.RawPath,
			RawQuery: u.RawQuery,
			Fragment: u.Fragment,
		}

		return z.String(), nil
	}
}

// GenFromURL returns a func that generates a DSN using urlstr as the default
// URL parameters, overriding the values only if when in the passed URL.
func GenFromURL(urlstr string) func(*URL) (string, error) {
	z, err := url.Parse(urlstr)
	if err != nil {
		panic(err)
	}

	return func(u *URL) (string, error) {
		opaque := z.Opaque
		if u.Opaque != "" {
			opaque = u.Opaque
		}

		user := z.User
		if u.User != nil {
			user = u.User
		}

		host, port := z.Hostname(), z.Port()
		if h := u.Hostname(); h != "" {
			host = h
		}
		if p := u.Port(); p != "" {
			port = p
		}
		if port != "" {
			host += ":" + port
		}

		path := z.Path
		if u.Path != "" {
			path = u.Path
		}

		rawPath := z.RawPath
		if u.RawPath != "" {
			rawPath = u.RawPath
		}

		q := z.Query()
		for k, v := range u.Query() {
			q.Set(k, strings.Join(v, " "))
		}

		fragment := z.Fragment
		if u.Fragment != "" {
			fragment = u.Fragment
		}

		y := &url.URL{
			Scheme:   z.Scheme,
			Opaque:   opaque,
			User:     user,
			Host:     host,
			Path:     path,
			RawPath:  rawPath,
			RawQuery: q.Encode(),
			Fragment: fragment,
		}

		return y.String(), nil
	}
}

// GenPostgres generates a postgres DSN from the passed URL.
func GenPostgres(u *URL) (string, error) {
	q := u.Query()

	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")
	if host == "." {
		return "", ErrPostgresDoesNotSupportRelativePath
	}

	// resolve path
	if u.Proto == "unix" {
		if host == "" {
			dbname = "/" + dbname
		}

		host, port, dbname = resolveDir(stdpath.Join(host, dbname))
	}

	q.Set("host", host)
	q.Set("port", port)
	q.Set("dbname", dbname)

	// add user/pass
	if u.User != nil {
		q.Set("user", u.User.Username())
		pass, _ := u.User.Password()
		q.Set("password", pass)
	}

	return genOptions(q, "", "=", " ", ",", true), nil
}

// GenSQLServer generates a mssql DSN from the passed URL.
func GenSQLServer(u *URL) (string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")

	// add instance name to host if present
	if i := strings.Index(dbname, "/"); i != -1 {
		host = host + `\` + dbname[:i]
		dbname = dbname[i+1:]
	}

	q := u.Query()
	q.Set("Server", host)
	q.Set("Port", port)
	q.Set("Database", dbname)

	// add user/pass
	if u.User != nil {
		q.Set("User ID", u.User.Username())
		pass, _ := u.User.Password()
		q.Set("Password", pass)
	}

	return genOptionsODBC(q, true), nil
}

// GenSybase generates a sqlany DSN from the passed URL.
func GenSybase(u *URL) (string, error) {
	// of format "UID=DBA;PWD=sql;Host=demo12;DatabaseName=demo;ServerName=myserver"
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")

	// add instance name to host if present
	if i := strings.Index(dbname, "/"); i != -1 {
		host = host + `\` + dbname[:i]
		dbname = dbname[i+1:]
	}

	q := u.Query()
	q.Set("Host", host)
	if port != "" {
		q.Set("LINKS", "tcpip(PORT="+port+")")
	}
	q.Set("DatabaseName", dbname)

	// add user/pass
	if u.User != nil {
		q.Set("UID", u.User.Username())
		pass, _ := u.User.Password()
		q.Set("PWD", pass)
	}

	return genOptionsODBC(q, true), nil
}

// GenMySQL generates a mysql DSN from the passed URL.
func GenMySQL(u *URL) (string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")

	// create dsn
	dsn := ""

	// build user/pass
	if u.User != nil {
		if un := u.User.Username(); len(un) > 0 {
			if up, ok := u.User.Password(); ok {
				un += ":" + up
			}
			dsn += un + "@"
		}
	}

	// resolve path
	if u.Proto == "unix" {
		if host == "" {
			dbname = "/" + dbname
		}
		host, dbname = resolveSocket(stdpath.Join(host, dbname))
		port = ""
	}

	// if host or proto is not empty
	if u.Proto != "unix" {
		if host == "" {
			host = "127.0.0.1"
		}
		if port == "" {
			port = "3306"
		}
	}
	if port != "" {
		port = ":" + port
	}

	dsn += u.Proto + "(" + host + port + ")"

	// add database name
	dsn += "/" + dbname

	return dsn + genQueryOptions(u.Query()), nil
}

// GenMyMySQL generates a MyMySQL MySQL DSN from the passed URL.
func GenMyMySQL(u *URL) (string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")

	// resolve path
	if u.Proto == "unix" {
		if host == "" {
			dbname = "/" + dbname
		}
		host, dbname = resolveSocket(stdpath.Join(host, dbname))
		port = ""
	}

	// if host or proto is not empty
	if u.Proto != "unix" {
		if host == "" {
			host = "127.0.0.1"
		}
		if port == "" {
			port = "3306"
		}
	}
	if port != "" {
		port = ":" + port
	}

	dsn := u.Proto + ":" + host + port

	// add opts
	dsn += genOptions(
		convertOptions(u.Query(), "true", ""),
		",", "=", ",", " ", false,
	)

	// add asterisk
	if dsn != "" {
		dsn += "*"
	}

	// add database
	dsn += dbname

	// add user/pass
	if u.User != nil {
		if user := u.User.Username(); user != "" {
			dsn += "/" + user
			if pass, ok := u.User.Password(); ok {
				dsn += "/" + pass
			}
		}
	}

	return dsn, nil
}

// GenOracle generates a ora DSN from the passed URL.
func GenOracle(u *URL) (string, error) {
	// create dsn
	dsn := u.Host + u.Path

	// build user/pass
	var un string
	if u.User != nil {
		if un = u.User.Username(); len(un) > 0 {
			if up, ok := u.User.Password(); ok {
				un += "/" + up
			}
		}
	}

	return un + "@" + dsn, nil
}

// GenOpaque generates a opaque file path DSN from the passed URL.
func GenOpaque(u *URL) (string, error) {
	dsn := u.Opaque
	if u.Host != "" {
		dsn = u.Host + u.Path
	}

	// add params
	params := u.Query().Encode()
	if len(params) > 0 {
		dsn += "?" + params
	}

	return dsn, nil
}

// GenFirebird generates a firebirdsql DSN from the passed URL.
func GenFirebird(u *URL) (string, error) {
	z := &url.URL{
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	return z.String(), nil
}

// GenADODB generates a adodb DSN from the passed URL.
func GenADODB(u *URL) (string, error) {
	q := u.Query()
	q.Set("Provider", u.Hostname())
	q.Set("Port", u.Port())

	// grab dbname
	dsname, dbname := strings.TrimPrefix(u.Path, "/"), ""
	if dsname == "" {
		dsname = "."
	}

	// check if data source is not a path on disk
	if mode(dsname) == 0 {
		if i := strings.IndexAny(dsname, `\/`); i != -1 {
			dbname = dsname[i+1:]
			dsname = dsname[:i]
		}
	}

	q.Set("Data Source", dsname)
	q.Set("Database", dbname)

	// add user/pass
	if u.User != nil {
		q.Set("User ID", u.User.Username())
		pass, _ := u.User.Password()
		q.Set("Password", pass)
	}

	return genOptionsODBC(q, true), nil
}

// GenODBC generates a odbc DSN from the passed URL.
func GenODBC(u *URL) (string, error) {
	q := u.Query()

	q.Set("Driver", "{"+strings.Replace(u.Proto, "+", " ", -1)+"}")
	q.Set("Server", u.Hostname())

	port := u.Port()
	if port == "" {
		proto := strings.ToLower(u.Proto)
		switch {
		case strings.Contains(proto, "mysql"):
			port = "3306"
		case strings.Contains(proto, "postgres"):
			port = "5432"

		default:
			port = "1433"
		}
	}
	q.Set("Port", port)
	q.Set("Database", strings.TrimPrefix(u.Path, "/"))

	// add user/pass
	if u.User != nil {
		q.Set("UID", u.User.Username())
		p, _ := u.User.Password()
		q.Set("PWD", p)
	}

	return genOptionsODBC(q, true), nil
}

// GenOLEODBC generates a oleodbc DSN from the passed URL.
func GenOLEODBC(u *URL) (string, error) {
	props, err := GenODBC(u)
	if err != nil {
		return "", nil
	}

	return `Provider=MSDASQL.1;Extended Properties="` + props + `"`, nil
}

// GenClickhouse generates a clickhouse DSN from the passed URL.
func GenClickhouse(u *URL) (string, error) {
	z := &url.URL{
		Scheme:   "tcp",
		Opaque:   u.Opaque,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}

	if z.Port() == "" {
		z.Host += ":9000"
	}

	// add parameters
	q := z.Query()
	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			q.Set("username", user)
		}
		if pass, ok := u.User.Password(); ok {
			q.Set("password", pass)
		}
	}
	z.RawQuery = q.Encode()

	return z.String(), nil
}

// GenYQL generates a YQL DSN from the passed URL.
func GenYQL(u *URL) (string, error) {
	dsn := ""

	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			dsn += user
		}
		if pass, ok := u.User.Password(); ok {
			dsn += "|" + pass
		} else {
			return "", errors.New("missing password")
		}
	}

	if u.Host != "" {
		if dsn == "" {
			dsn = "|"
		}
		dsn += "|store://" + u.Host + u.Path
	}

	return dsn, nil
}

// GenVoltDB generates a VoltDB DSN from the passed URL.
func GenVoltDB(u *URL) (string, error) {
	host, port := "localhost", "21212"
	if h := u.Hostname(); h != "" {
		host = h
	}
	if p := u.Port(); p != "" {
		port = p
	}
	return host + ":" + port, nil
}

// genOptions takes URL values and generates options, joining together with
// joiner, and separated by sep, with any multi URL values joined by valSep,
// ignoring any values with keys in ignore.
//
// For example, to build a "ODBC" style connection string, use like the following:
//     genOptions(u.Query(), "", "=", ";", ",")
func genOptions(q url.Values, joiner, assign, sep, valSep string, skipWhenEmpty bool, ignore ...string) string {
	qlen := len(q)
	if qlen == 0 {
		return ""
	}

	// make ignore map
	ig := make(map[string]bool, len(ignore))
	for _, v := range ignore {
		ig[strings.ToLower(v)] = true
	}

	// sort keys
	s := make([]string, len(q))
	var i int
	for k := range q {
		s[i] = k
		i++
	}
	sort.Strings(s)

	var opts []string
	for _, k := range s {
		if !ig[strings.ToLower(k)] {
			val := strings.Join(q[k], valSep)
			if !skipWhenEmpty || val != "" {
				if val != "" {
					val = assign + val
				}
				opts = append(opts, k+val)
			}
		}
	}

	if len(opts) != 0 {
		return joiner + strings.Join(opts, sep)
	}

	return ""
}

// genOptionsODBC is a util wrapper around genOptions that uses the fixed settings
// for ODBC style connection strings.
func genOptionsODBC(q url.Values, skipWhenEmpty bool, ignore ...string) string {
	return genOptions(q, "", "=", ";", ",", skipWhenEmpty, ignore...)
}

// genQueryOptions generatens standard query options.
func genQueryOptions(q url.Values) string {
	if s := q.Encode(); s != "" {
		return "?" + s
	}
	return ""
}

// convertOptions converts an option value based on name, value pairs.
func convertOptions(q url.Values, pairs ...string) url.Values {
	n := make(url.Values)
	for k, v := range q {
		x := make([]string, len(v))
		for i, z := range v {
			for j := 0; j < len(pairs); j += 2 {
				if pairs[j] == z {
					z = pairs[j+1]
				}
			}
			x[i] = z
		}
		n[k] = x
	}

	return n
}

// mode returns the mode of the path.
func mode(path string) os.FileMode {
	if fi, err := os.Stat(path); err == nil {
		return fi.Mode()
	}

	return 0
}

// resolveSocket tries to resolve a path to a unix domain socket
// based on the actual file path of the form "/path/to/socket/dbname" returning
// either the original path and the empty string, or the components
// "/path/to/socket" and "dbname", when /path/to/socket/dbname is reported by
// os.Stat as a unix socket.
func resolveSocket(path string) (string, string) {
	dir, dbname := path, ""
	for dir != "" && dir != "/" && dir != "." {
		if m := mode(dir); m&os.ModeSocket != 0 {
			return dir, dbname
		}
		dir, dbname = stdpath.Dir(dir), stdpath.Base(dir)
	}

	return path, ""
}

// resolveDir resolves a directory with a :port list.
func resolveDir(path string) (string, string, string) {
	dir := path
	for dir != "" && dir != "/" && dir != "." {
		port := ""
		i, j := strings.LastIndex(dir, ":"), strings.LastIndex(dir, "/")
		if i != -1 && i > j {
			port = dir[i+1:]
			dir = dir[:i]
		}

		if mode(dir)&os.ModeDir != 0 {
			rest := strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(path, dir), ":"+port), "/")
			return dir, port, rest
		}

		if j != -1 {
			dir = dir[:j]
		} else {
			dir = ""
		}
	}

	return path, "", ""
}
