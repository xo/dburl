package dburl

import (
	"errors"
	"net/url"
	"path"
	"strconv"
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

// GenSQLServer generates a mssql DSN from the passed URL.
func GenSQLServer(u *URL) (string, error) {
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
	if i := strings.Index(host, ":"); i != -1 {
		port, err = strconv.Atoi(host[i+1:])
		if err != nil {
			return "", ErrInvalidPort
		}
		host = host[:i]
	}

	// extract instance name
	if i := strings.Index(dbname, "/"); i != -1 {
		host = host + `\` + dbname[:i]
		dbname = dbname[i+1:]
	}

	// format dsn
	dsn := "server=" + host + ";port=" + strconv.Itoa(port)
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

	return dsn, nil
}

// GenSybase generates a sqlany DSN from the passed URL.
func GenSybase(u *URL) (string, error) {
	// of format "UID=DBA;PWD=sql;Host=demo12;DatabaseName=demo;ServerName=myserver"
	var err error

	// build host or domain socket
	host := u.Host
	port := 1234

	// grab dbname
	var dbname string
	if u.Path != "" {
		dbname = u.Path[1:]
	}

	// extract port if present
	if i := strings.Index(host, ":"); i != -1 {
		port, err = strconv.Atoi(host[i+1:])
		if err != nil {
			return "", ErrInvalidPort
		}
		host = host[:i]
	}

	// format dsn
	dsn := "Host=" + host + ";LINKS=tcpip(PORT=" + strconv.Itoa(port) + ")"

	// add user/pass
	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			dsn += ";UID=" + user
		}
		if pass, ok := u.User.Password(); ok {
			dsn += ";PWD=" + pass
		}
	}

	// add database
	if dbname != "" {
		dsn += ";DatabaseName=" + dbname
	}

	// add params
	for k, v := range u.Query() {
		dsn += ";" + k + "=" + v[0]
	}

	return dsn, nil
}

// GenMySQL generates a mysql DSN from the passed URL.
func GenMySQL(u *URL) (string, error) {
	// build host or domain socket
	host := u.Host
	dbname := strings.TrimPrefix(u.Path, "/")

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
	dsn := u.Proto + "(" + host + ")"

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

	return dsn, nil
}

// GenMyMySQL generates a MyMySQL MySQL DSN from the passed URL.
func GenMyMySQL(u *URL) (string, error) {
	var opts []string
	for k, v := range u.Query() {
		val := k
		if v[0] != "true" {
			val += "=" + v[0]
		}

		opts = append(opts, val)
	}

	// build host or domain socket
	host := u.Host
	dbname := strings.TrimPrefix(u.Path, "/")

	if u.Proto == "unix" {
		if u.Opaque != "" {
			host = path.Dir(u.Opaque)
			dbname = path.Base(u.Opaque)
		} else {
			host = path.Join(u.Host, path.Dir(u.Path))
			dbname = path.Base(u.Path)
		}

		if o := strings.Join(opts, ","); o != "" {
			host += "," + o
		}

		host += "*" + dbname
		dbname = ""

		u.Host = host
		u.Path = ""
	} else if !strings.Contains(host, ":") {
		// append default port
		host = host + ":3306"
	}

	dsn := u.Proto + ":" + host

	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			dsn += "/" + user
		}
		if pass, ok := u.User.Password(); ok {
			dsn += "/" + pass
		}
	}

	return "", nil
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

// GenSQLite3 generates a sqlite3 DSN from the passed URL.
func GenSQLite3(u *URL) (string, error) {
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
	// grab dbname
	dbname := strings.TrimPrefix(u.Path, "/")
	if dbname == "" {
		dbname = "."
	}

	// format dsn
	dsn := "Provider=" + u.Host + ";Data Source=" + dbname

	// add params
	for k, v := range u.Query() {
		dsn += ";" + k + "=" + v[0]
	}

	return dsn, nil
}

// GenODBC generates a odbc DSN from the passed URL.
func GenODBC(u *URL) (string, error) {
	dsn := "Driver={" + strings.Replace(u.Proto, "+", " ", -1) + "}"

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

	// format dsn
	dsn += ";Server=" + u.Hostname() + ";Port=" + port
	dbname := strings.TrimPrefix(u.Path, "/")
	if dbname != "" {
		dsn += ";Database=" + dbname
	}

	// add user/pass
	if u.User != nil {
		if user := u.User.Username(); len(user) > 0 {
			dsn += ";UID=" + user
		}
		if pass, ok := u.User.Password(); ok {
			dsn += ";PWD=" + pass
		}
	}

	// add params
	for k, v := range u.Query() {
		dsn += ";" + k + "=" + v[0]
	}

	return dsn, nil
}

// GenOLEODBC generates a oleodbc DSN from the passed URL.
func GenOLEODBC(u *URL) (string, error) {
	return "", nil
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

// GenPGX generates a PGX PostgreSQL DSN from the passed URL.
func GenPGX(u *URL) (string, error) {
	return "", nil
}
