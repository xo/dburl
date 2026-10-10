package dburl

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// OdbcIgnoreQueryPrefixes are the query prefixes to ignore when generating the
// odbc DSN. Used by [GenOdbc].
var OdbcIgnoreQueryPrefixes []string

// GenScheme returns a generator that will generate a scheme based on the
// passed scheme DSN.
func GenScheme(scheme string) func(*URL) (string, string, error) {
	return func(u *URL) (string, string, error) {
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
		if z.Host == "" {
			z.Host = "localhost"
		}
		return z.String(), "", nil
	}
}

// GenSchemeHost returns a generator that rewrites only the scheme, as
// [GenScheme] does, and that returns [ErrMissingHost] when the URL names no
// host. It is for a scheme whose host is not a server, such as the S3 bucket
// of awsathena or the project of bigquery, where localhost is never right
// (D38).
func GenSchemeHost(scheme string) func(*URL) (string, string, error) {
	gen := GenScheme(scheme)
	return func(u *URL) (string, string, error) {
		if u.Host == "" {
			return "", "", ErrMissingHost
		}
		return gen(u)
	}
}

// genRewrite writes u with the scheme of the driver, which a driver that
// refuses any scheme but its own name needs. The user information, the path
// with its escaping and the passed raw query pass through. The host defaults
// to localhost, and the port to the passed port, which is empty for a driver
// that has a default port of its own (D34).
func genRewrite(u *URL, scheme, port, rawQuery string) string {
	host := "localhost"
	if h := u.Hostname(); h != "" {
		host = h
	}
	if p := u.Port(); p != "" {
		port = p
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" {
		host += ":" + port
	}
	z := &url.URL{
		Scheme:   scheme,
		User:     u.User,
		Host:     host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: rawQuery,
	}
	return z.String()
}

// GenFromURL returns a func that generates a DSN based on parameters of the
// passed URL.
//
// The query is rebuilt with [net/url.Values.Encode], which writes a space as
// a +, and a key that appears more than once is joined into one value with
// spaces. Do not use it for a driver that reads a + as itself, such as pgx,
// which has [GenPgxFromURL], or for a driver that takes a repeated key.
func GenFromURL(urlstr string) func(*URL) (string, string, error) {
	z, err := url.Parse(urlstr)
	if err != nil {
		panic(err)
	}
	return func(u *URL) (string, string, error) {
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
		pstr := z.Path
		if u.Path != "" {
			pstr = u.Path
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
			Path:     pstr,
			RawPath:  rawPath,
			RawQuery: q.Encode(),
			Fragment: fragment,
		}
		return strings.TrimPrefix(y.String(), "truncate://"), "", nil
	}
}

// GenOpaque generates a opaque file path DSN from the passed URL.
func GenOpaque(u *URL) (string, string, error) {
	if u.Opaque == "" {
		return "", "", ErrMissingPath
	}
	return u.Opaque + genQueryOptions(u.Query()), "", nil
}

// GenAvatica generates an avatica DSN from the passed URL.
//
// Targets [xo/dbimp/avatica], which reads an avatica:// URL and refuses any
// other scheme, and refuses a path. The user information and the query pass
// through as they were written. It adds no port, because the driver defaults
// to 8765, the port of an Avatica server (D34 and D47). The alias phoenix
// reaches the same driver.
//
// [xo/dbimp/avatica]: https://github.com/xo/dbimp
func GenAvatica(u *URL) (string, string, error) {
	return genRewrite(u, "avatica", "", u.RawQuery), "", nil
}

// GenArangoDB generates an arangodb DSN from the passed URL.
//
// Targets the driver planned in [xo/dbimp/arangodb], which is to read an
// arangodb:// URL and refuse any other scheme. The user information, the path
// and the query pass through as they were written. It adds no port, because
// the driver defaults to 8529 (D34).
//
// The driver has no tag yet, so this generator is provisional (D32).
//
// [xo/dbimp/arangodb]: https://github.com/xo/dbimp
func GenArangoDB(u *URL) (string, string, error) {
	return genRewrite(u, "arangodb", "", u.RawQuery), "", nil
}

// GenGizmoSQL generates a flightsql DSN for GizmoSQL from the passed URL.
//
// GizmoSQL serves Flight SQL, so the DSN is a flightsql:// URL, with the
// default port 31337, which is the port of GizmoSQL. The Flight SQL driver has
// no default port (D34). It returns flightsql as the Go driver.
func GenGizmoSQL(u *URL) (string, string, error) {
	return genRewrite(u, "flightsql", "31337", u.RawQuery), "flightsql", nil
}

// GenOpensearch generates a opensearch DSN from the passed URL.
//
// Targets the driver [xo/dbimp/opensearch], which reads a opensearch:// URL and refuses
// any other scheme. The URL has no path, because OpenSearch has no database to choose. The user information and the query pass through as
// they were written, and the driver refuses a key it does not know. It adds no
// port, because the driver has its own default, which tls=true does not change
// (D34 and D54).
//
// [xo/dbimp/opensearch]: https://github.com/xo/dbimp
func GenOpensearch(u *URL) (string, string, error) {
	return genRewrite(u, "opensearch", "", u.RawQuery), "", nil
}

// GenPinot generates a pinot DSN from the passed URL.
//
// Targets [xo/dbimp/pinot], which reads a pinot:// URL and refuses any other
// scheme, and refuses a path. The user information, the path and the query
// pass through as they were written. It adds no port, because the driver
// defaults to 8099, the port of a Pinot broker (D34 and D43).
//
// [xo/dbimp/pinot]: https://github.com/xo/dbimp
func GenPinot(u *URL) (string, string, error) {
	return genRewrite(u, "pinot", "", u.RawQuery), "", nil
}

// GenRqlite generates a rqlite DSN from the passed URL.
//
// Targets [xo/dbimp/rqlite], which reads a rqlite:// URL and refuses any
// other scheme, and refuses a path. The user information, the path and the
// query pass through as they were written. It adds no port, because the
// driver defaults to 4001, the port of the HTTP API of rqlite (D34 and D44).
//
// [xo/dbimp/rqlite]: https://github.com/xo/dbimp
func GenRqlite(u *URL) (string, string, error) {
	return genRewrite(u, "rqlite", "", u.RawQuery), "", nil
}

// GenCassandra generates a cassandra DSN from the passed URL.
//
// Targets [xo/cassandra], which reads a cassandra:// URL with net/url. The
// scheme is always cassandra, whichever alias was parsed, so cql:// and
// scylla:// reach the driver as cassandra://, which is the only scheme it reads.
// The user information and the path, which is the keyspace, pass through, and
// so does the query, as it was written, because the driver takes a host key
// that can repeat. It adds no port, because gocql defaults to 9042 (D34 and
// D56).
//
// [xo/cassandra]: https://github.com/xo/cassandra
func GenCassandra(u *URL) (string, string, error) {
	return genRewrite(u, "cassandra", "", u.RawQuery), "", nil
}

// GenClickhouse generates a clickhouse DSN from the passed URL.
//
// Targets [xo/dbimp/clickhouse], which reads a clickhouse:// URL with net/url
// and speaks HTTP only. The user information, the path, which is the database,
// and the query pass through as they were written. The driver reads the tls
// key and refuses any other key. It adds no port, because the driver defaults
// to 8123, or 8443 with tls (D34). The transports +http and +https are gone,
// and so is the native protocol on 9000 (D53).
//
// [xo/dbimp/clickhouse]: https://github.com/xo/dbimp
func GenClickhouse(u *URL) (string, string, error) {
	return genRewrite(u, "clickhouse", "", u.RawQuery), "", nil
}

// GenCouchbase generates a couchbase DSN from the passed URL.
//
// Targets [xo/dbimp/couchbase], which reads a couchbase:// URL with net/url
// and refuses any other scheme. The user information, the path and the query
// pass through as they were written, and the driver refuses an unknown or
// repeated key, and a path other than "/". It adds no port, because the
// driver defaults to 8093, or 18093 with tls (D34).
//
// [xo/dbimp/couchbase]: https://github.com/xo/dbimp
func GenCouchbase(u *URL) (string, string, error) {
	return genRewrite(u, "couchbase", "", u.RawQuery), "", nil
}

// GenCockroachDB generates a cockroachdb DSN from the passed URL.
//
// CockroachDB speaks the wire protocol of PostgreSQL, so the DSN is the one
// [GenPgxFromURL] writes, with the default port 26257 and sslmode=disable. It
// returns pgx as the driver. The scheme has a Dialect of its own, because it
// is a product of its own.
func GenCockroachDB(u *URL) (string, string, error) {
	dsn, _, err := cockroachdb(u)
	return dsn, "pgx", err
}

// GenCrateDB generates a cratedb DSN from the passed URL.
//
// CrateDB speaks the wire protocol of PostgreSQL, so the DSN is the one
// [GenPgxFromURL] writes. It adds no port, because pgx defaults to 5432,
// which is the port of the PostgreSQL interface of CrateDB (D34). It returns
// pgx as the Go driver, as [GenCockroachDB] does.
func GenCrateDB(u *URL) (string, string, error) {
	dsn, _, err := cratedb(u)
	return dsn, "pgx", err
}

// Generators for products that speak the wire protocol of PostgreSQL.
var (
	cockroachdb = GenPgxFromURL("postgres://localhost:26257/?sslmode=disable")
	cratedb     = GenPgxFromURL("postgres://localhost/")
	questdb     = GenPgxFromURL("postgres://localhost:8812/")
)

// GenQuestDB generates a questdb DSN from the passed URL.
//
// QuestDB speaks the wire protocol of PostgreSQL, so the DSN is the one
// [GenPgxFromURL] writes, with the default port 8812, which is the port of
// its PostgreSQL interface. pgx would use 5432 (D34). It returns pgx as the
// Go driver, as [GenCrateDB] does.
func GenQuestDB(u *URL) (string, string, error) {
	dsn, _, err := questdb(u)
	return dsn, "pgx", err
}

// GenCosmos generates a cosmos DSN from the passed URL.
func GenCosmos(u *URL) (string, string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")
	if port != "" {
		port = ":" + port
	}
	q := u.Query()
	q.Set("AccountEndpoint", "https://"+host+port)
	// add user/pass
	if u.User == nil {
		return "", "", ErrMissingUser
	}
	q.Set("AccountKey", u.User.Username())
	if dbname != "" {
		q.Set("Db", dbname)
	}
	return genOptionsOdbc(q, true, nil, nil), "gocosmos", nil
}

// GenDrill generates a drill DSN from the passed URL.
//
// Targets the driver [xo/dbimp/drill], which reads a drill:// URL and refuses
// any other scheme. The URL has no path, and the schema is the key schema. The user information and the query pass through as
// they were written, and the driver refuses a key it does not know. It adds no
// port, because the driver has its own default, which tls=true does not change
// (D34 and D54).
//
// [xo/dbimp/drill]: https://github.com/xo/dbimp
func GenDrill(u *URL) (string, string, error) {
	return genRewrite(u, "drill", "", u.RawQuery), "", nil
}

// GenDruid generates a druid DSN from the passed URL.
//
// Targets the driver [xo/dbimp/druid], which reads a druid:// URL, refuses any
// other scheme, and refuses a path, because Druid has no database to choose.
// The user information and the query pass through as they were written. It
// adds no port, because the driver defaults to 8888, the port of the Druid
// Router (D34 and D48).
//
// [xo/dbimp/druid]: https://github.com/xo/dbimp
func GenDruid(u *URL) (string, string, error) {
	return genRewrite(u, "druid", "", u.RawQuery), "", nil
}

// GenDatabend generates a databend DSN from the passed URL.
//
// Targets the driver planned in [xo/dbimp/databend], which replaces
// databend-go and reads a databend:// URL and refuses any other scheme, so
// the scheme is always databend, whichever alias was parsed. The user
// information, the path, which names the database, and the query pass
// through as they were written. It adds no port, because the driver defaults
// to 8000, with TLS or without (D34 and D39).
//
// The driver has no tag yet, so this generator is provisional (D39).
//
// [xo/dbimp/databend]: https://github.com/xo/dbimp
func GenDatabend(u *URL) (string, string, error) {
	return genRewrite(u, "databend", "", u.RawQuery), "", nil
}

// GenDynamo generates a dynamodb DSN from the passed URL.
//
// Targets the driver [xo/dbimp/dynamodb], which reads a dynamodb:// URL. The
// host is the endpoint, such as dynamodb.us-east-1.amazonaws.com, and the
// region is the key region, which the driver requires. The user is the access
// key and the password is the secret key, and the signature needs both, so a
// URL without one returns [ErrMissingUser]. The driver speaks HTTPS unless the
// URL names tls=false, and refuses a path and any other key. The path, the
// user information and the query pass through. It adds no port, because the
// driver uses the port of the scheme (D34 and D55).
//
// The driver has no tag yet, so this generator is provisional (D55).
//
// [xo/dbimp/dynamodb]: https://github.com/xo/dbimp
func GenDynamo(u *URL) (string, string, error) {
	if u.User == nil {
		return "", "", ErrMissingUser
	}
	if p, _ := u.User.Password(); u.User.Username() == "" || p == "" {
		return "", "", ErrMissingUser
	}
	return genRewrite(u, "dynamodb", "", u.RawQuery), "", nil
}

// GenDatabricks generates a databricks DSN from the passed URL.
func GenDatabricks(u *URL) (string, string, error) {
	if u.User == nil {
		return "", "", ErrMissingUser
	}
	user := u.User.Username()
	pass, ok := u.User.Password()
	if !ok || pass == "" {
		return "", "", ErrMissingUser
	}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		return "", "", ErrMissingHost
	}
	if port == "" {
		port = "443"
	}
	s := fmt.Sprintf("token:%s@%s.databricks.com:%s/sql/1.0/endpoints/%s", user, pass, port, host)
	return s + genOptions(u.Query(), "?", "=", "&", ",", true, nil, nil), "", nil
}

// GenElasticsearch generates a elasticsearch DSN from the passed URL.
//
// Targets the driver [xo/dbimp/elasticsearch], which reads a elasticsearch:// URL and refuses
// any other scheme. The URL has no path, because Elasticsearch has no database to choose. The user information and the query pass through as
// they were written, and the driver refuses a key it does not know. It adds no
// port, because the driver has its own default, which tls=true does not change
// (D34 and D54).
//
// [xo/dbimp/elasticsearch]: https://github.com/xo/dbimp
func GenElasticsearch(u *URL) (string, string, error) {
	return genRewrite(u, "elasticsearch", "", u.RawQuery), "", nil
}

// GenExasol generates a exasol DSN from the passed URL.
func GenExasol(u *URL) (string, string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "8563"
	}
	q := u.Query()
	if dbname != "" {
		q.Set("schema", dbname)
	}
	if u.User != nil {
		q.Set("user", u.User.Username())
		pass, _ := u.User.Password()
		q.Set("password", pass)
	}
	return fmt.Sprintf("exa:%s:%s%s", host, port, genOptions(q, ";", "=", ";", ",", true, nil, nil)), "", nil
}

// GenFirebird generates a firebird DSN from the passed URL.
func GenFirebird(u *URL) (string, string, error) {
	z := &url.URL{
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}
	return strings.TrimPrefix(z.String(), "//"), "", nil
}

// GenHive generates a hive DSN from the passed URL.
//
// Targets [beltran/gohive/v2]. Its ParseDSN rejects any string that does not
// start with "hive://", and reads the database name from the path.
//
// The database name is required. An empty path is an error and not a default,
// so an absent name becomes "default".
//
// The driver also cannot start without auth. Its connect path tests the
// accepted values in turn, and anything else reaches panic("Unrecognized
// auth"). The empty string is one of those, so an absent or empty auth
// becomes NONE. Any other value passes through untouched. See D16.
//
// [beltran/gohive/v2]: https://github.com/beltran/gohive
func GenHive(u *URL) (string, string, error) {
	z := &url.URL{
		Scheme:   "hive",
		User:     u.User,
		Host:     u.Host,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}
	// force host
	if z.Host == "" {
		z.Host = "localhost"
	}
	// the database name is required
	dbname := strings.TrimPrefix(u.Path, "/")
	if dbname == "" {
		dbname = "default"
	}
	z.Path = "/" + dbname
	// supply auth only when the caller has not, as the driver panics without it
	if q := z.Query(); q.Get("auth") == "" {
		q.Set("auth", "NONE")
		z.RawQuery = q.Encode()
	}
	return z.String(), "", nil
}

// GenInfluxDB generates an influxdb DSN from the passed URL.
//
// Targets the driver in [xo/dbimp/influxdb], which reads an influxdb:// URL
// and refuses any other scheme. The user information, the path and the query
// pass through as they were written. It adds no driver option, so the driver
// reads sqlmode and version with its own defaults. See [GenInfluxQL].
//
// It adds no port, because the driver defaults to 8086 for version 1 and 2,
// and 8181 otherwise (D34).
//
// [xo/dbimp/influxdb]: https://github.com/xo/dbimp
func GenInfluxDB(u *URL) (string, string, error) {
	return genRewrite(u, "influxdb", "", u.RawQuery), "", nil
}

// GenInfluxQL generates an influxdb DSN for InfluxQL from the passed URL.
//
// It writes the DSN as [GenInfluxDB] does, and adds sqlmode=disable when the
// URL does not name sqlmode, so that the driver speaks InfluxQL. It returns
// influxdb as the Go driver, because one driver serves both schemes.
func GenInfluxQL(u *URL) (string, string, error) {
	q := u.RawQuery
	if !u.Query().Has("sqlmode") {
		if q != "" {
			q += "&"
		}
		q += "sqlmode=disable"
	}
	return genRewrite(u, "influxdb", "", q), "influxdb", nil
}

// GenLibsql generates a libsql DSN from the passed URL.
//
// Targets [xo/dbimp/libsql], which serves libSQL and Turso, reads a libsql://
// URL and refuses any other scheme, and refuses a path. The token is the
// password of the URL. The user information and the query pass through as
// they were written. It adds no port, because the driver defaults to 443 with
// TLS, which is on by default, and needs an explicit port with tls=false
// (D34 and D45).
//
// [xo/dbimp/libsql]: https://github.com/xo/dbimp
func GenLibsql(u *URL) (string, string, error) {
	return genRewrite(u, "libsql", "", u.RawQuery), "", nil
}

// GenMysql generates a mysql DSN from the passed URL.
//
// It adds no port, because go-sql-driver/mysql defaults to 3306 (D34).
func GenMysql(u *URL) (string, string, error) {
	return genMysql(u, "")
}

// GenTiDB generates a tidb DSN from the passed URL.
//
// It is the DSN of [GenMysql], with the default port 4000, which is the port
// of TiDB. go-sql-driver/mysql would use 3306 (D34).
func GenTiDB(u *URL) (string, string, error) {
	return genMysql(u, "4000")
}

// genMysql generates a mysql DSN from the passed URL, with the passed default
// port, which is empty when the driver's own default applies. It names mysql
// as the driver, for mysql and for memsql, tidb and vitess, which speak its
// wire protocol.
func genMysql(u *URL, defaultPort string) (string, string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")
	// build dsn
	var dsn string
	if u.User != nil {
		if n := u.User.Username(); n != "" {
			if p, ok := u.User.Password(); ok {
				n += ":" + p
			}
			dsn += n + "@"
		}
	}
	// resolve path
	if u.Transport == "unix" {
		if host == "" {
			dbname = "/" + dbname
		}
		host, dbname = resolveSocket(path.Join(host, dbname))
		port = ""
	}
	// save host, port, dbname
	if u.hostPortDB == nil {
		u.hostPortDB = []string{host, port, dbname}
	}
	// if host or proto is not empty
	if u.Transport != "unix" {
		if host == "" {
			host = "localhost"
		}
		if port == "" {
			port = defaultPort
		}
	}
	if port != "" {
		port = ":" + port
	}
	// add proto and database
	dsn += u.Transport + "(" + host + port + ")" + "/" + dbname
	return dsn + genQueryOptions(u.Query()), "mysql", nil
}

// GenNeo4j generates a neo4j DSN from the passed URL.
//
// Targets [xo/dbimp/neo4j], which reads a neo4j:// URL with net/url and
// refuses any other scheme. The path names the database, and the driver
// reads an empty path as the database neo4j. The user information, the path
// and the query pass through as they were written. It adds no port, because
// the driver defaults to 7474, or 7473 with tls (D34).
//
// [xo/dbimp/neo4j]: https://github.com/xo/dbimp
func GenNeo4j(u *URL) (string, string, error) {
	return genRewrite(u, "neo4j", "", u.RawQuery), "", nil
}

// GenOdbc generates an odbc DSN from the passed URL.
//
// Targets [xo/odbc], which reads an odbc+<driver>:// URL, so the URL passes
// through with its user information, host, port, path and query. The driver
// writes the connection string from it, and quotes a value that holds a ; or
// a brace (D52). GenOdbc adds no host, no port and no option, because the ODBC
// driver that the user installed owns each default (D34).
//
// A query key that starts with a prefix in [OdbcIgnoreQueryPrefixes] is
// dropped, so a client can keep its own options out of the DSN. A URL with no
// driver, such as odbc://host/db, returns [ErrMissingDriver].
//
// [xo/odbc]: https://github.com/xo/odbc
func GenOdbc(u *URL) (string, string, error) {
	if u.Transport == "" || u.Transport == "tcp" || u.Transport == "unix" {
		return "", "", ErrMissingDriver
	}
	q := u.Query()
	for k := range q {
		if hasPrefix(strings.ToLower(k), OdbcIgnoreQueryPrefixes) {
			delete(q, k)
		}
	}
	z := &url.URL{
		Scheme:   "odbc+" + u.Transport,
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: q.Encode(),
	}
	return z.String(), "", nil
}

// GenPgx generates a pgx DSN from the passed URL.
//
// Targets [jackc/pgx/v5/stdlib], which reads a postgres:// URL with the same
// rules as libpq. Every component is percent-encoded, a space as %20, because
// the driver rejects a raw space and reads a + as itself. A unix socket
// directory cannot be a URL host, so it is passed as the host query option.
//
// GenPgx supplies no default host and no default port, so the driver reads
// PGHOST and then its socket default. See [GenPgxFromURL] for one that does.
//
// See [GenPostgres], which lib/pq uses instead.
//
// [jackc/pgx/v5/stdlib]: https://github.com/jackc/pgx
func GenPgx(u *URL) (string, string, error) {
	return genPgx(u, "", "", nil)
}

// GenPgxFromURL returns a func that generates a pgx DSN, taking the default
// host, port and query options from the passed URL. The parsed URL overrides
// each one it carries, as with [GenFromURL].
//
// Use it in place of GenFromURL for a scheme whose driver is pgx, because
// GenFromURL writes a space in a query option as a +, which pgx reads as
// itself.
func GenPgxFromURL(urlstr string) func(*URL) (string, string, error) {
	z, err := url.Parse(urlstr)
	if err != nil {
		panic(err)
	}
	return func(u *URL) (string, string, error) {
		return genPgx(u, z.Hostname(), z.Port(), z.Query())
	}
}

// genPgx generates a pgx DSN from the passed URL and defaults, and names pgx
// as the driver. See [GenPgx].
func genPgx(u *URL, defHost, defPort string, defQuery url.Values) (string, string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")
	if host == "." {
		return "", "", ErrRelativePathNotSupported
	}
	// resolve path
	if u.Transport == "unix" {
		if host == "" {
			dbname = "/" + dbname
		}
		host, port, dbname = resolveDir(path.Join(host, dbname))
	} else {
		if host == "" {
			host = defHost
		}
		if port == "" {
			port = defPort
		}
	}
	// save host, port, dbname
	if u.hostPortDB == nil {
		u.hostPortDB = []string{host, port, dbname}
	}
	// the parsed URL overrides each default option it carries
	q := make(url.Values)
	for k, v := range defQuery {
		q[k] = v
	}
	for k, v := range u.Query() {
		q[k] = v
	}
	// a socket directory is not a URL host
	if u.Transport == "unix" {
		q.Set("host", host)
		q.Set("port", port)
		host, port = "", ""
	}
	dsn := "postgres://"
	if u.User != nil {
		dsn += escapePgx(u.User.Username())
		if pass, _ := u.User.Password(); pass != "" {
			dsn += ":" + escapePgx(pass)
		}
		dsn += "@"
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	dsn += host
	if port != "" {
		dsn += ":" + port
	}
	if dbname != "" {
		dsn += "/" + escapePgx(dbname)
	}
	keys := make([]string, 0, len(q))
	for k, v := range q {
		if strings.Join(v, ",") != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(dsn)
	for i, k := range keys {
		sep := "&"
		if i == 0 {
			sep = "?"
		}
		b.WriteString(sep + escapePgx(k) + "=" + escapePgx(strings.Join(q[k], ",")))
	}
	return b.String(), "pgx", nil
}

// escapePgx percent-encodes a pgx URL component, writing a space as %20.
func escapePgx(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// GenPq generates a pq DSN from the passed URL.
//
// It is the DSN of [GenPostgres], and it names postgres as the driver, which
// is the name that lib/pq registers.
func GenPq(u *URL) (string, string, error) {
	dsn, _, err := GenPostgres(u)
	return dsn, "postgres", err
}

// GenPostgres generates a postgres DSN from the passed URL.
//
// Targets [lib/pq], which reads the keyword/value form. A value holding a
// space, a quote or a backslash is quoted, as the parser requires.
//
// See [GenPgx], which postgres uses instead, and [GenPq], which names the
// driver for the pq scheme.
//
// [lib/pq]: https://github.com/lib/pq
func GenPostgres(u *URL) (string, string, error) {
	host, port, dbname := u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/")
	if host == "." {
		return "", "", ErrRelativePathNotSupported
	}
	// resolve path
	if u.Transport == "unix" {
		if host == "" {
			dbname = "/" + dbname
		}
		host, port, dbname = resolveDir(path.Join(host, dbname))
	}
	// build q
	q := u.Query()
	q.Set("host", host)
	q.Set("port", port)
	q.Set("dbname", dbname)
	// add user/pass
	if u.User != nil {
		q.Set("user", u.User.Username())
		pass, _ := u.User.Password()
		q.Set("password", pass)
	}
	// save host, port, dbname
	if u.hostPortDB == nil {
		u.hostPortDB = []string{host, port, dbname}
	}
	for k, v := range q {
		q[k] = []string{quotePostgres(strings.Join(v, ","))}
	}
	return genOptions(q, "", "=", " ", ",", true, nil, nil), "", nil
}

// GenSolr generates a solr DSN from the passed URL.
//
// Targets the driver [xo/dbimp/solr], which reads a solr:// URL and refuses
// any other scheme. The path is the name of one collection, and it is optional. The user information and the query pass through as
// they were written, and the driver refuses a key it does not know. It adds no
// port, because the driver has its own default, which tls=true does not change
// (D34 and D54).
//
// [xo/dbimp/solr]: https://github.com/xo/dbimp
func GenSolr(u *URL) (string, string, error) {
	return genRewrite(u, "solr", "", u.RawQuery), "", nil
}

// GenSnowflake generates a snowflake DSN from the passed URL.
//
// Targets [xo/dbimp/snowflake], which reads a snowflake:// URL. The host is
// the full host of the account, such as org-account.snowflakecomputing.com,
// and the driver refuses a host that does not end that way. The user is the
// login user, and the password is the private key of the user, as the base64url
// text of its PKCS8 DER bytes. The key is the only way to sign in, so a URL with
// no host, no user or no password returns [ErrMissingHost] or [ErrMissingUser].
// A host that does not end that way, has no port and is not an IP address is
// an account name, such as org-account, and GenSnowflake adds the suffix (D58).
// The path holds the database and the schema, and the query takes role,
// warehouse, timeout and timezone. Each one passes through as it was written,
// and the driver refuses any other key. It adds no port, because the driver
// uses 443 (D34 and D58).
//
// [xo/dbimp/snowflake]: https://github.com/xo/dbimp
func GenSnowflake(u *URL) (string, string, error) {
	if u.Hostname() == "" {
		return "", "", ErrMissingHost
	}
	if u.User == nil {
		return "", "", ErrMissingUser
	}
	if p, _ := u.User.Password(); u.User.Username() == "" || p == "" {
		return "", "", ErrMissingUser
	}
	z := *u
	if host := u.Hostname(); u.Port() == "" && net.ParseIP(host) == nil && !strings.HasSuffix(strings.ToLower(host), snowflakeSuffix) {
		z.Host = host + snowflakeSuffix
	}
	return genRewrite(&z, "snowflake", "", u.RawQuery), "", nil
}

// snowflakeSuffix ends the host of every Snowflake account.
const snowflakeSuffix = ".snowflakecomputing.com"

// GenSpanner generates a spanner DSN from the passed URL.
//
// Targets [googleapis/go-sql-spanner]. The URL is
// spanner://host:port/project/instance/database?name=value, and the DSN is
// host:port/projects/project/instances/instance/databases/database;name=value.
// An empty host leaves the endpoint to the driver, as spanner:///p/i/d does.
// [Parse] reads a URL with no host and a path as a unix socket, so the scheme
// accepts that transport, and a spanner+unix URL is refused here.
// The path must name all three, and each query option passes through as a
// driver option, such as usePlainText=true for the emulator (D35).
//
// [googleapis/go-sql-spanner]: https://github.com/googleapis/go-sql-spanner
func GenSpanner(u *URL) (string, string, error) {
	if strings.Contains(u.OriginalScheme, "+") {
		return "", "", ErrInvalidTransportProtocol
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", ErrMissingPath
	}
	var b strings.Builder
	if u.Host != "" {
		b.WriteString(u.Host + "/")
	}
	b.WriteString("projects/" + parts[0] + "/instances/" + parts[1] + "/databases/" + parts[2])
	q := u.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(";" + k + "=" + strings.Join(q[k], ","))
	}
	return b.String(), "", nil
}

// GenSqlserver generates a sqlserver DSN from the passed URL.
func GenSqlserver(u *URL) (string, string, error) {
	z := &url.URL{
		Scheme:   "sqlserver",
		Opaque:   u.Opaque,
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}
	if z.Host == "" {
		z.Host = "localhost"
	}
	driver := "sqlserver"
	if strings.Contains(strings.ToLower(u.Scheme), "azuresql") ||
		u.Query().Get("fedauth") != "" {
		driver = "azuresql"
	}
	v := strings.Split(strings.TrimPrefix(z.Path, "/"), "/")
	if n, q := len(v), z.Query(); !q.Has("database") && n != 0 && len(v[0]) != 0 {
		q.Set("database", v[n-1])
		z.Path, z.RawQuery = "/"+strings.Join(v[:n-1], "/"), q.Encode()
	}
	return z.String(), driver, nil
}

// GenSurrealDB generates a surrealdb DSN from the passed URL.
//
// Targets [xo/dbimp/surrealdb], which reads a surrealdb:// URL with net/url
// and refuses any other scheme. The path names the namespace and the
// database, as /namespace/database, and passes through with its escaping. The
// user information and the query pass through as they were written. It adds
// no port, because the driver defaults to 8000 (D34). A path that does not
// name both returns [ErrMissingPath] (D38).
//
// [xo/dbimp/surrealdb]: https://github.com/xo/dbimp
func GenSurrealDB(u *URL) (string, string, error) {
	// the driver needs both the namespace and the database (D38)
	ns, db, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	if ns == "" || db == "" {
		return "", "", ErrMissingPath
	}
	return genRewrite(u, "surrealdb", "", u.RawQuery), "", nil
}

// GenTrino generates a trino DSN from the passed URL.
//
// Targets [xo/dbimp/trino], which serves Trino and Presto, registers the name
// trino, reads a trino:// URL and refuses any other scheme. The path names the
// catalog and the schema, and each is optional. The path and the query pass
// through as they were written. It adds no port, because the driver defaults
// to 8080, and to 8443 with tls (D34).
//
// Two things are written for the scheme (D50):
//
//   - The driver requires a user, and a server that does no authentication
//     accepts any name, so a URL with no user gets the user "user", as it
//     always did. This is an exception to rule 10.
//   - The scheme chooses the flavor: the key flavor is trino unless the URL
//     names it, so the driver does not ask the server, and presto:// against a
//     Trino server fails and does not act as Trino. This is an exception to
//     rule 7, as sqlmode=disable is for influxql (D29).
//
// The driver reads TLS from the tls key and never from the scheme, as every
// dbimp driver does, so no alias of the scheme means HTTPS.
//
// [xo/dbimp/trino]: https://github.com/xo/dbimp
func GenTrino(u *URL) (string, string, error) {
	return genTrino(u, "trino")
}

// GenPresto generates a trino DSN for Presto from the passed URL.
//
// Presto is a flavor of the driver that [GenTrino] targets. The DSN is the one
// GenTrino writes, with the key flavor set to presto, and the Dialect is
// presto. It returns trino as the name of the driver, because the driver
// registers one name (D50).
func GenPresto(u *URL) (string, string, error) {
	dsn, _, err := genTrino(u, "presto")
	return dsn, "trino", err
}

// genTrino writes the DSN for the flavor of the scheme. See [GenTrino].
func genTrino(u *URL, flavor string) (string, string, error) {
	z := *u
	if z.User == nil {
		z.User = url.User("user")
	}
	rawQuery := u.RawQuery
	if !u.Query().Has("flavor") {
		if rawQuery != "" {
			rawQuery += "&"
		}
		rawQuery += "flavor=" + flavor
	}
	return genRewrite(&z, "trino", "", rawQuery), "", nil
}

// GenVoltdb generates a voltdb DSN from the passed URL.
func GenVoltdb(u *URL) (string, string, error) {
	host, port := "localhost", "21212"
	if h := u.Hostname(); h != "" {
		host = h
	}
	if p := u.Port(); p != "" {
		port = p
	}
	return host + ":" + port, "", nil
}

// tlsOption reads the tls option of u, with def as the value when it is not
// named, and returns it with the raw query that has no tls key. The drivers
// that [GenMaxCompute], [GenTableStore] and [GenYDB] target do not know the
// key, and MaxCompute would send it to the server as a hint, so it is removed.
// A value that strconv.ParseBool refuses, and a key given twice, return
// [ErrInvalidQuery]. The dbimp drivers read tls in the same way (D51).
func tlsOption(u *URL, def bool) (bool, string, error) {
	q := u.Query()
	vals, ok := q["tls"]
	if !ok {
		return def, u.RawQuery, nil
	}
	if len(vals) != 1 {
		return false, "", fmt.Errorf("%w: tls is given %d times", ErrInvalidQuery, len(vals))
	}
	tls, err := strconv.ParseBool(vals[0])
	if err != nil {
		return false, "", fmt.Errorf("%w: tls %q", ErrInvalidQuery, vals[0])
	}
	q.Del("tls")
	return tls, q.Encode(), nil
}

// genHTTP writes a URL for a driver that reads an http or https endpoint. The
// scheme is https when the tls option is true, and the option defaults to defTLS.
// The host defaults to localhost, and the path to defPath. The user information
// and the path pass through (D51).
func genHTTP(u *URL, defTLS bool, defPath string) (string, string, error) {
	tls, rawQuery, err := tlsOption(u, defTLS)
	if err != nil {
		return "", "", err
	}
	scheme := "http"
	if tls {
		scheme = "https"
	}
	host := u.Host
	if host == "" {
		host = "localhost"
	}
	path, rawPath := u.Path, u.RawPath
	if path == "" {
		path = defPath
	}
	z := &url.URL{
		Scheme:   scheme,
		User:     u.User,
		Host:     host,
		Path:     path,
		RawPath:  rawPath,
		RawQuery: rawQuery,
	}
	return z.String(), "", nil
}

// GenMaxCompute generates a maxcompute DSN from the passed URL.
//
// Targets [aliyun/aliyun-odps-go-sdk/sqldriver], which takes an http or https
// URL as the endpoint and reads the project from the project query option. The
// endpoint is https, because MaxCompute is a hosted service, and tls=false
// makes it http. The scheme has no +http or +https transport (D51).
//
// [aliyun/aliyun-odps-go-sdk/sqldriver]: https://github.com/aliyun/aliyun-odps-go-sdk
func GenMaxCompute(u *URL) (string, string, error) {
	return genHTTP(u, true, "/")
}

// GenTableStore generates a tablestore DSN from the passed URL.
//
// Targets [aliyun/aliyun-tablestore-go-sql-driver], which reads its endpoint
// from the scheme and the host. The endpoint is https, because Tablestore is a
// hosted service, and tls=false makes it http. The scheme has no +http or
// +https transport (D51).
//
// [aliyun/aliyun-tablestore-go-sql-driver]: https://github.com/aliyun/aliyun-tablestore-go-sql-driver
func GenTableStore(u *URL) (string, string, error) {
	return genHTTP(u, true, "")
}

// GenYDB generates a ydb dsn from the passed URL.
//
// Targets [ydb-platform/ydb-go-sdk/v3], which reads TLS from the scheme: grpcs
// is secure and grpc is not. The tls option chooses it, and is false by
// default. With tls=true the scheme is grpcs and the default port is 2135, and
// otherwise they are grpc and 2136. The aliases yds and ydbs are gone (D51).
//
// [ydb-platform/ydb-go-sdk/v3]: https://github.com/ydb-platform/ydb-go-sdk
func GenYDB(u *URL) (string, string, error) {
	tls, rawQuery, err := tlsOption(u, false)
	if err != nil {
		return "", "", err
	}
	scheme, host, port := "grpc", "localhost", "2136"
	if tls {
		scheme, port = "grpcs", "2135"
	}
	if h := u.Hostname(); h != "" {
		host = h
	}
	if p := u.Port(); p != "" {
		port = p
	}
	var userpass string
	if u.User != nil {
		userpass = u.User.String() + "@"
	}
	q, _ := url.ParseQuery(rawQuery)
	s := scheme + "://" + userpass + host + ":" + port + "/" + strings.TrimPrefix(u.Path, "/")
	return s + genOptions(q, "?", "=", "&", ",", true, nil, nil), "", nil
}

// GenDuckDB generates a duckdb dsn from the passed URL.
func GenDuckDB(u *URL) (string, string, error) {
	// Same as GenOpaque but accepts empty path which refers to in-memory DB
	return u.Opaque + genQueryOptions(u.Query()), "", nil
}

// quotePostgres quotes a keyword/value connection string value that holds a
// space, a quote or a backslash. An empty value is left empty, so that
// genOptions skips it.
func quotePostgres(s string) string {
	if !strings.ContainsAny(s, `'\`) && strings.IndexFunc(s, unicode.IsSpace) == -1 {
		return s
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// genQueryOptions generates standard query options.
func genQueryOptions(q url.Values) string {
	if s := q.Encode(); s != "" {
		return "?" + s
	}
	return ""
}

// genOptionsOdbc is a util wrapper around genOptions that uses the fixed
// settings for ODBC style connection strings.
func genOptionsOdbc(q url.Values, skipWhenEmpty bool, ignore, ignorePrefixes []string) string {
	return genOptions(q, "", "=", ";", ",", skipWhenEmpty, ignore, ignorePrefixes)
}

// genOptions takes URL values and generates options.
//
// Each name and value is joined by assign, each pair is separated by sep, and
// joiner comes before the first pair. A value holding more than one entry is
// joined by valSep. A key listed in ignore, or that starts with a prefix in
// ignorePrefixes, is skipped. When skipWhenEmpty is true, a key with an empty
// value is skipped.
//
// For example, an ODBC style connection string is built like this:
//
//	genOptions(u.Query(), "", "=", ";", ",", true, nil, nil)
//
//nolint:unparam
func genOptions(q url.Values, joiner, assign, sep, valSep string, skipWhenEmpty bool, ignore, ignorePrefixes []string) string {
	if len(q) == 0 {
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
		if s := strings.ToLower(k); !ig[s] && !hasPrefix(s, ignorePrefixes) {
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

// hasPrefix returns true when s begins with any listed prefix.
func hasPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}
