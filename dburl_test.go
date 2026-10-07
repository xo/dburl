package dburl

import (
	"errors"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestNoDependencies checks that the module requires nothing outside the
// standard library.
//
// golangci-lint enforces the same rule through depguard, and this test
// repeats it because `go test` is the one command that runs on every change.
func TestNoDependencies(t *testing.T) {
	buf, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	var block bool
	for i, line := range strings.Split(string(buf), "\n") {
		s := strings.TrimSpace(line)
		switch {
		case s == "", strings.HasPrefix(s, "//"):
		case block && s == ")":
			block = false
		case block, strings.HasPrefix(s, "require "), strings.HasPrefix(s, "replace "):
			t.Errorf("go.mod:%d: expected no dependency, got: %q", i+1, s)
		case s == "require (", s == "replace (":
			block = true
		}
	}
}

// TestSchemeMetadata checks that every registered scheme describes itself.
//
// The metadata exists so that this registry can generate its own driver table
// rather than having usql generate it. A scheme missing a field is a row usql
// cannot render. See D17.
func TestSchemeMetadata(t *testing.T) {
	schemes := BaseSchemes()
	dialects := make(map[string]string, len(schemes))
	for _, scheme := range schemes {
		dialects[scheme.Name] = scheme.Dialect
	}
	for _, scheme := range schemes {
		// file is a pseudo scheme that resolves paths on disk, and has no
		// database and no driver behind it
		if scheme.Name == "file" {
			continue
		}
		if scheme.Desc == "" {
			t.Errorf("%s: expected a Desc, got: %q", scheme.Name, scheme.Desc)
		}
		if scheme.Deployment == 0 {
			t.Errorf("%s: expected a Deployment, got: %d", scheme.Name, scheme.Deployment)
		}
		if scheme.Dialect == "" {
			t.Errorf("%s: expected a Dialect, got: %q", scheme.Name, scheme.Dialect)
		}
		// a Dialect names a registered scheme, and that scheme is canonical
		// for its product, so it names itself
		if _, ok := dialects[scheme.Dialect]; !ok {
			t.Errorf("%s: Dialect %q is not a registered scheme", scheme.Name, scheme.Dialect)
		} else if d := dialects[scheme.Dialect]; d != scheme.Dialect {
			t.Errorf("%s: Dialect %q itself has Dialect %q", scheme.Name, scheme.Dialect, d)
		}
		if scheme.GoPackage == "" {
			t.Errorf("%s: expected a GoPackage, got: %q", scheme.Name, scheme.GoPackage)
		}
		if scheme.DriverURL == "" {
			t.Errorf("%s: expected a DriverURL, got: %q", scheme.Name, scheme.DriverURL)
		}
	}
}

// TestEveryDecisionIsIndexed checks that the table at the top of
// docs/PLAN.md lists every decision written below it, and nothing else.
//
// A decision log rots when an entry is added and the index is not. See D10.
func TestEveryDecisionIsIndexed(t *testing.T) {
	buf, err := os.ReadFile("docs/PLAN.md")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	indexed, written := make(map[string]bool), make(map[string]bool)
	for _, line := range strings.Split(string(buf), "\n") {
		switch {
		case strings.HasPrefix(line, "| [D"):
			indexed[line[3:strings.Index(line, "]")]] = true
		case strings.HasPrefix(line, "### D"):
			written[line[4:strings.Index(line, ".")]] = true
		}
	}
	for d := range written {
		if !indexed[d] {
			t.Errorf("D%s is written but not in the index", d)
		}
	}
	for d := range indexed {
		if !written[d] {
			t.Errorf("D%s is in the index but not written", d)
		}
	}
}

func TestNormalize(t *testing.T) {
	tests := []struct {
		s   string
		cut int
		exp string
	}{
		{`postgres://crate@127.0.0.1:5432/doc`, 3, `postgres:127.0.0.1:5432:doc:crate`},
		{`postgres://127.0.0.1:5432/doc`, 3, `postgres:127.0.0.1:5432:doc`},
		{`postgres://crate@127.0.0.1:5432`, 3, `postgres:127.0.0.1:5432::crate`},
		{`postgres://127.0.0.1`, 3, `postgres:127.0.0.1:`},
		{`postgres://127.0.0.1`, 0, `postgres:127.0.0.1:::`},
		{`mysql://user@host/db`, 0, `mysql:host::db:user`},
	}
	for _, test := range tests {
		t.Run(test.s, func(t *testing.T) {
			u, err := Parse(test.s)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if s := u.Normalize(":", "", test.cut); s != test.exp {
				t.Errorf("expected %q, got: %q", test.exp, s)
			}
		})
	}
}

func TestParseDialect(t *testing.T) {
	// SchemeName is the scheme, Driver is the name for sql.Open, and Dialect
	// is the product (D37)
	tests := []struct {
		s, scheme, driver, dialect string
	}{
		{`postgres://host/db`, "postgres", "pgx", "postgres"},
		{`pgx://host/db`, "pgx", "pgx", "postgres"},
		{`pq://host/db`, "pq", "postgres", "postgres"},
		{`cr://host/db`, "cockroachdb", "pgx", "cockroachdb"},
		{`ct://host/db`, "cratedb", "pgx", "cratedb"},
		{`crate://host/db`, "cratedb", "pgx", "cratedb"},
		{`rs://host/db`, "redshift", "pgx", "redshift"},
		{`mysql://host/db`, "mysql", "mysql", "mysql"},
		{`tidb://host/db`, "tidb", "mysql", "tidb"},
		{`memsql://host/db`, "memsql", "mysql", "memsql"},
		{`vt://host/db`, "vitess", "mysql", "vitess"},
		{`gizmosql://host`, "gizmosql", "flightsql", "gizmosql"},
		{`questdb://host/qdb`, "questdb", "pgx", "questdb"},
		{`qs://host/qdb`, "questdb", "pgx", "questdb"},
		{`pinot://host`, "pinot", "pinot", "pinot"},
		{`rqlite://host`, "rqlite", "rqlite", "rqlite"},
		{`turso://host`, "libsql", "libsql", "libsql"},
		{`phoenix://host`, "avatica", "avatica", "avatica"},
		{`dr://host`, "druid", "druid", "druid"},
		{`dl://host`, "drill", "drill", "drill"},
		{`solr://host`, "solr", "solr", "solr"},
		{`so://host`, "solr", "solr", "solr"},
		{`es://host`, "elasticsearch", "elasticsearch", "elasticsearch"},
		{`os://host`, "opensearch", "opensearch", "opensearch"},
		{`elastic://host`, "elasticsearch", "elasticsearch", "elasticsearch"},
		{`open://host`, "opensearch", "opensearch", "opensearch"},
		{`trino://host`, "trino", "trino", "trino"},
		{`presto://host`, "presto", "trino", "presto"},
		{`moderncsqlite:file.db`, "moderncsqlite", "moderncsqlite", "sqlite3"},
		{`godror://user:pass@host/sid`, "godror", "godror", "oracle"},
		{`oracle://user:pass@host/sid`, "oracle", "oracle", "oracle"},
		{`arango://host/db`, "arangodb", "arangodb", "arangodb"},
		{`influxdb://host/db`, "influxdb", "influxdb", "influxdb"},
		{`influxql://host/db`, "influxql", "influxdb", "influxql"},
		{`cosmos://key@host/db`, "cosmos", "gocosmos", "cosmos"},
		// file: resolves to the scheme of the file, and takes its Dialect
		{`file:fake.sqlite3`, "sqlite3", "sqlite3", "sqlite3"},
		{`file:/var/run/postgresql`, "postgres", "pgx", "postgres"},
	}
	for _, test := range tests {
		t.Run(test.s, func(t *testing.T) {
			u, err := Parse(test.s)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if u.SchemeName != test.scheme {
				t.Errorf("expected scheme %q, got: %q", test.scheme, u.SchemeName)
			}
			if u.Driver != test.driver {
				t.Errorf("expected driver %q, got: %q", test.driver, u.Driver)
			}
			if u.Dialect != test.dialect {
				t.Errorf("expected dialect %q, got: %q", test.dialect, u.Dialect)
			}
		})
	}
}

func TestDialectProtocols(t *testing.T) {
	tests := []struct {
		name string
		exp  []string
	}{
		{"postgres", []string{"libpq", "pg", "pgsql", "pgx", "postgres", "postgresql", "pq", "px"}},
		{"pq", []string{"libpq", "pg", "pgsql", "pgx", "postgres", "postgresql", "pq", "px"}},
		{"redshift", []string{"redshift", "rs"}},
		{"tidb", []string{"ti", "tidb"}},
		{"cockroachdb", []string{"cdb", "cockroach", "cockroachdb", "cr", "crdb"}},
		{"cratedb", []string{"crate", "cratedb", "ct"}},
		{"file", []string{"file", "fi"}},
		{"unknown", nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if v := DialectProtocols(test.name); !slices.Equal(v, test.exp) {
				t.Errorf("expected %v, got: %v", test.exp, v)
			}
		})
	}
	// a product that speaks the wire protocol of mysql has a dialect of its
	// own, so the mysql family holds only mysql (D37)
	v := DialectProtocols("mysql")
	for _, name := range []string{"tidb", "vitess", "memsql"} {
		if slices.Contains(v, name) {
			t.Errorf("expected no %q in %v", name, v)
		}
	}
	if !slices.Contains(v, "mysql") {
		t.Errorf("expected %q in %v", "mysql", v)
	}
}

func TestBadParse(t *testing.T) {
	tests := []struct {
		s   string
		exp error
	}{
		{``, ErrInvalidDatabaseScheme},
		{` `, ErrInvalidDatabaseScheme},
		{`pgsqlx://`, ErrUnknownDatabaseScheme},
		{`m`, ErrInvalidDatabaseScheme},
		{`pg+udp://user:pass@localhost/dbname`, ErrInvalidTransportProtocol},
		{`sqlite+unix://`, ErrInvalidTransportProtocol},
		{`sqlite+tcp://`, ErrInvalidTransportProtocol},
		{`file+tcp://`, ErrInvalidTransportProtocol},
		{`file://`, ErrMissingPath},
		{`mssql+tcp://user:pass@host/dbname`, ErrInvalidTransportProtocol},
		{`mssql+foobar://`, ErrInvalidTransportProtocol},
		{`mssql+unix:/var/run/mssql.sock`, ErrInvalidTransportProtocol},
		{`mssql+udp:localhost:155`, ErrInvalidTransportProtocol},
		{`mysql+foo+bar://host/dbname`, ErrInvalidTransportProtocol},
		{`memsql:/var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`tidb:/var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`spanner://host/p/db`, ErrMissingPath},
		{`surrealdb://`, ErrMissingPath},
		{`surrealdb://host/ns`, ErrMissingPath},
		{`sr://host//db`, ErrMissingPath},
		{`awsathena://`, ErrMissingHost},
		// Parse reads no host with a path as a socket, before the generator
		{`athena:///db?region=us-east-1`, ErrInvalidTransportProtocol},
		{`bigquery://`, ErrMissingHost},
		{`bq:///dataset`, ErrInvalidTransportProtocol},
		{`spanner+unix:///p/i/db`, ErrInvalidTransportProtocol},
		{`spanner+tcp://host/p/i/db`, ErrInvalidTransportProtocol},
		{`spanner://host/p/i/db/x`, ErrMissingPath},
		{`vitess:/var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`memsql+unix:///var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`tidb+unix:///var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`vitess+unix:///var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`cockroach:/var/run/postgresql`, ErrInvalidTransportProtocol},
		{`cratedb:/var/run/postgresql`, ErrInvalidTransportProtocol},
		{`ct+unix:/var/run/postgresql`, ErrInvalidTransportProtocol},
		{`cockroach+unix:/var/run/postgresql`, ErrInvalidTransportProtocol},
		{`cockroach:./path`, ErrInvalidTransportProtocol},
		{`cockroach+unix:./path`, ErrInvalidTransportProtocol},
		{`redshift:/var/run/postgresql`, ErrInvalidTransportProtocol},
		{`redshift+unix:/var/run/postgresql`, ErrInvalidTransportProtocol},
		{`redshift:./path`, ErrInvalidTransportProtocol},
		{`redshift+unix:./path`, ErrInvalidTransportProtocol},
		{`pg:./path/to/socket`, ErrRelativePathNotSupported}, // relative paths are not possible for postgres sockets
		{`pg+unix:./path/to/socket`, ErrRelativePathNotSupported},
		{`snowflake://`, ErrMissingHost},
		{`sf://`, ErrMissingHost},
		{`snowflake://account`, ErrMissingUser},
		{`sf://account`, ErrMissingUser},
		{`mq+unix://`, ErrInvalidTransportProtocol},
		{`mq+tcp://`, ErrInvalidTransportProtocol},
		{`ots+tcp://`, ErrInvalidTransportProtocol},
		{`tablestore+tcp://`, ErrInvalidTransportProtocol},
		{`mc+tcp://id:key@host/api?project=p`, ErrInvalidTransportProtocol},
		{`mc+https://id:key@host/api?project=p`, ErrInvalidTransportProtocol},
		{`maxcompute+http://host/api?project=p`, ErrInvalidTransportProtocol},
		{`ots+https://user:pass@host/instance`, ErrInvalidTransportProtocol},
		{`tablestore+http://user:pass@host/instance`, ErrInvalidTransportProtocol},
		{`yds://host`, ErrUnknownDatabaseScheme},
		{`ydbs://host`, ErrUnknownDatabaseScheme},
		{`mc://host/api?tls=maybe`, ErrInvalidQuery},
		{`ots://host/instance?tls=1&tls=0`, ErrInvalidQuery},
		{`ydb://host?tls=`, ErrInvalidQuery},
		{`maxcompute+unix:/var/run/mc.sock`, ErrInvalidTransportProtocol},
		{`prs://admin@host/catalogname`, ErrUnknownDatabaseScheme},
		{`prestos://admin@host/catalogname`, ErrUnknownDatabaseScheme},
		{`prestodbs://admin:pass@host:9998/catalogname`, ErrUnknownDatabaseScheme},
		{`nzgo://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`nz://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`netezza://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`tdengine://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`td://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`trs://admin@host/catalogname`, ErrUnknownDatabaseScheme},
		{`trinos://admin@host/catalogname`, ErrUnknownDatabaseScheme},
		{`ql:file.db`, ErrUnknownDatabaseScheme},
		{`cznic:file.db`, ErrUnknownDatabaseScheme},
		{`cznicql:file.db`, ErrUnknownDatabaseScheme},
		{`mymysql://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`zm://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`mymy://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`adodb://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ad://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ado://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`oleodbc://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`odbc://user:pass@host/dbname`, ErrMissingDriver},
		{`drill+http://host`, ErrInvalidTransportProtocol},
		{`solr+https://host/c`, ErrInvalidTransportProtocol},
		{`es+http://host`, ErrInvalidTransportProtocol},
		{`opensearch+tcp://host`, ErrInvalidTransportProtocol},
		{`clickhouse+https://host/db`, ErrInvalidTransportProtocol},
		{`clickhouse+http://host/db`, ErrInvalidTransportProtocol},
		{`oo://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ole://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`tds://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ax://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ase://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`sapase://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ignite://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`ig://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`gridgain://user:pass@host/dbname`, ErrUnknownDatabaseScheme},
		{`unknown_file.ext3`, ErrInvalidDatabaseScheme},
		{`file:fake.unknown`, ErrUnknownFileHeader},
		{`file:unknown_file.ext3`, ErrUnknownFileExtension},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			testBadParse(t, test.s, test.exp)
		})
	}
}

func testBadParse(t *testing.T, s string, exp error) {
	t.Helper()
	_, err := Parse(s)
	switch {
	case err == nil:
		t.Errorf("%q expected error nil error, got: %v", s, err)
	case !errors.Is(err, exp):
		t.Errorf("%q expected error %v, got: %v", s, exp, err)
	}
}

func TestIsDuckdbHeader(t *testing.T) {
	tests := []struct {
		name string
		buf  string
		exp  bool
	}{
		{"real", "\x06\xd7\x6f\x27\xc2\xda\xb3\xb9DUCK\x40\x00\x00\x00", true},
		{"multibyte checksum", "\xc2\xda\xc2\xda\xc2\xda\xc2\xdaDUCK\x00\x00\x00\x00", true},
		{"newline checksum", "\x01\x02\x0a\x04\x05\x06\x07\x08DUCK\x00\x00\x00\x00", true},
		{"ascii checksum", "12345678DUCK87654321", true},
		{"exactly magic", "12345678DUCK", true},
		{"short", "12345678DUC", false},
		{"empty", "", false},
		{"magic misaligned", "123456789DUCK7654321", false},
		{"no magic", "1234567887654321....", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if v := isDuckdbHeader([]byte(test.buf)); v != test.exp {
				t.Errorf("expected %t, got: %t", test.exp, v)
			}
		})
	}
}

func TestParse(t *testing.T) {
	OdbcIgnoreQueryPrefixes = []string{"usql_"}
	tests := []struct {
		s    string
		d    string
		exp  string
		path string
	}{
		{
			`pg:`,
			`pgx`,
			`postgres://`,
			``,
		},
		{
			`pg://`,
			`pgx`,
			`postgres://`,
			``,
		},
		{
			`pg:user:pass@localhost/booktest`,
			`pgx`,
			`postgres://user:pass@localhost/booktest`,
			``,
		},
		{
			`pg://user:p%20ss@host/db`,
			`pgx`,
			`postgres://user:p%20ss@host/db`,
			``,
		},
		{
			`pg://user:it%27s@host/db`,
			`pgx`,
			`postgres://user:it%27s@host/db`,
			``,
		},
		{
			`pg://user:a%5Cb@host/db`,
			`pgx`,
			`postgres://user:a%5Cb@host/db`,
			``,
		},
		{
			`pg://user:a%C2%A0b@host/db`,
			`pgx`,
			`postgres://user:a%C2%A0b@host/db`,
			``,
		},
		{
			`pg://user:a=b@host/db`,
			`pgx`,
			`postgres://user:a%3Db@host/db`,
			``,
		},
		{
			`pg://host/my%20db?application_name=my%20app`,
			`pgx`,
			`postgres://host/my%20db?application_name=my%20app`,
			``,
		},
		{
			`pq:`,
			`postgres`,
			``,
			``,
		},
		{
			`pq://user:pass@localhost/booktest`,
			`postgres`,
			`dbname=booktest host=localhost password=pass user=user`,
			``,
		},
		{
			`libpq://user:p%20ss@host:5433/db?sslmode=verify-ca`,
			`postgres`,
			`dbname=db host=host password='p ss' port=5433 sslmode=verify-ca user=user`,
			``,
		},
		{
			`pgsql://user:pass@localhost/booktest`,
			`pgx`,
			`postgres://user:pass@localhost/booktest`,
			``,
		},
		{
			`postgresql://user:pass@localhost/booktest`,
			`pgx`,
			`postgres://user:pass@localhost/booktest`,
			``,
		},
		{
			`pq://user:it%27s@host/db`,
			`postgres`,
			`dbname=db host=host password='it\'s' user=user`,
			``,
		},
		{
			`pq://user:a%5Cb@host/db`,
			`postgres`,
			`dbname=db host=host password='a\\b' user=user`,
			``,
		},
		{
			`pq://user:a%C2%A0b@host/db`,
			`postgres`,
			`dbname=db host=host password='a b' user=user`,
			``,
		},
		{
			`pq://host/my%20db?application_name=my%20app`,
			`postgres`,
			`application_name='my app' dbname='my db' host=host`,
			``,
		},
		{
			`pq:/var/run/postgresql:6666/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql port=6666`,
			``,
		},
		{
			`pg:/var/run/postgresql`,
			`pgx`,
			`postgres://?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`pg:/var/run/postgresql:6666/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql&port=6666`,
			`/var/run/postgresql`,
		},
		{
			`/var/run/postgresql:6666/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql&port=6666`,
			`/var/run/postgresql`,
		},
		{
			`pg:/var/run/postgresql/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`/var/run/postgresql/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`pg:/var/run/postgresql:7777`,
			`pgx`,
			`postgres://?host=%2Fvar%2Frun%2Fpostgresql&port=7777`,
			`/var/run/postgresql`,
		},
		{
			`pg+unix:/var/run/postgresql:4444/booktest`,
			`pgx`,
			`postgres:///booktest?host=%2Fvar%2Frun%2Fpostgresql&port=4444`,
			`/var/run/postgresql`,
		},
		{
			`/var/run/postgresql:7777`,
			`pgx`,
			`postgres://?host=%2Fvar%2Frun%2Fpostgresql&port=7777`,
			`/var/run/postgresql`,
		},
		{
			`pg:user:pass@/var/run/postgresql/mydb`,
			`pgx`,
			`postgres://user:pass@/mydb?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`pg:user:pass@/really/bad/path`,
			`pgx`,
			`postgres://user:pass@?host=%2Freally%2Fbad%2Fpath`,
			``,
		},
		{
			`my:`,
			`mysql`,
			`tcp(localhost)/`,
			``,
		},
		{
			`my://`,
			`mysql`,
			`tcp(localhost)/`,
			``,
		},
		{
			`my:booktest:booktest@localhost/booktest`,
			`mysql`,
			`booktest:booktest@tcp(localhost)/booktest`,
			``,
		},
		{
			`my:/var/run/mysqld/mysqld.sock/mydb?timeout=90`,
			`mysql`,
			`unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`,
			`/var/run/mysqld/mysqld.sock`,
		},
		{
			`/var/run/mysqld/mysqld.sock/mydb?timeout=90`,
			`mysql`,
			`unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`,
			`/var/run/mysqld/mysqld.sock`,
		},
		{
			`my:///var/run/mysqld/mysqld.sock/mydb?timeout=90`,
			`mysql`,
			`unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`,
			`/var/run/mysqld/mysqld.sock`,
		},
		{
			`my+unix:user:pass@mysqld.sock?timeout=90`,
			`mysql`,
			`user:pass@unix(mysqld.sock)/?timeout=90`,
			``,
		},
		{
			`my:./path/to/socket`,
			`mysql`,
			`unix(path/to/socket)/`,
			``,
		},
		{
			`my+unix:./path/to/socket`,
			`mysql`,
			`unix(path/to/socket)/`,
			``,
		},
		{
			`mssql://`,
			`sqlserver`,
			`sqlserver://localhost`,
			``,
		},
		{
			`mssql://user:pass@localhost/dbname`,
			`sqlserver`,
			`sqlserver://user:pass@localhost/?database=dbname`,
			``,
		},
		{
			`mssql://user@localhost/service/dbname`,
			`sqlserver`,
			`sqlserver://user@localhost/service?database=dbname`,
			``,
		},
		{
			`mssql://user:!234%23$@localhost:1580/dbname`,
			`sqlserver`,
			`sqlserver://user:%21234%23$@localhost:1580/?database=dbname`,
			``,
		},
		{
			`mssql://user:!234%23$@localhost:1580/service/dbname?fedauth=true`,
			`azuresql`,
			`sqlserver://user:%21234%23$@localhost:1580/service?database=dbname&fedauth=true`,
			``,
		},
		{
			`azuresql://user:pass@localhost:100/dbname`,
			`azuresql`,
			`sqlserver://user:pass@localhost:100/?database=dbname`,
			``,
		},
		{
			`sqlserver://xxx.database.windows.net?database=xxx&fedauth=ActiveDirectoryMSI`,
			`azuresql`,
			`sqlserver://xxx.database.windows.net?database=xxx&fedauth=ActiveDirectoryMSI`,
			``,
		},
		{
			`azuresql://xxx.database.windows.net/dbname?fedauth=ActiveDirectoryMSI`,
			`azuresql`,
			`sqlserver://xxx.database.windows.net/?database=dbname&fedauth=ActiveDirectoryMSI`,
			``,
		},
		{
			`odbc+Postgres+Unicode://user:pass@host:5432/dbname?not_ignored=1`,
			`odbc`,
			`odbc+Postgres+Unicode://user:pass@host:5432/dbname?not_ignored=1`,
			``,
		},
		{
			`odbc+Postgres+Unicode://user:pass@host:5432/dbname?usql_ignore=1&not_ignored=1`,
			`odbc`,
			`odbc+Postgres+Unicode://user:pass@host:5432/dbname?not_ignored=1`,
			``,
		},
		{
			`odbc+ODBC+Driver+18+for+SQL+Server://sa:p%3Bw@host/inst/dbname`,
			`odbc`,
			`odbc+ODBC+Driver+18+for+SQL+Server://sa:p;w@host/inst/dbname`,
			``,
		},
		{
			`sqlite:///path/to/file.sqlite3`,
			`sqlite3`,
			`/path/to/file.sqlite3`,
			``,
		},
		{
			`sq://path/to/file.sqlite3`,
			`sqlite3`,
			`path/to/file.sqlite3`,
			``,
		},
		{
			`sq:path/to/file.sqlite3`,
			`sqlite3`,
			`path/to/file.sqlite3`,
			``,
		},
		{
			`sq:./path/to/file.sqlite3`,
			`sqlite3`,
			`./path/to/file.sqlite3`,
			``,
		},
		{
			`sq://./path/to/file.sqlite3?loc=auto`,
			`sqlite3`,
			`./path/to/file.sqlite3?loc=auto`,
			``,
		},
		{
			`sq::memory:?loc=auto`,
			`sqlite3`,
			`:memory:?loc=auto`,
			``,
		},
		{
			`sq://:memory:?loc=auto`,
			`sqlite3`,
			`:memory:?loc=auto`,
			``,
		},
		{
			`or://user:pass@localhost:3000/sidname`,
			`oracle`,
			`oracle://user:pass@localhost:3000/sidname`,
			``,
		},
		{
			`or://localhost`,
			`oracle`,
			`oracle://localhost:1521`,
			``,
		},
		{
			`oracle://user:pass@localhost`,
			`oracle`,
			`oracle://user:pass@localhost:1521`,
			``,
		},
		{
			`oracle://user:pass@localhost/service_name/instance_name`,
			`oracle`,
			`oracle://user:pass@localhost:1521/service_name/instance_name`,
			``,
		},
		{
			`oracle://user:pass@localhost:2000/xe.oracle.docker`,
			`oracle`,
			`oracle://user:pass@localhost:2000/xe.oracle.docker`,
			``,
		},
		{
			`or://username:password@host/ORCL`,
			`oracle`,
			`oracle://username:password@host:1521/ORCL`,
			``,
		},
		{
			`odpi://username:password@sales-server:1521/sales.us.acme.com`,
			`oracle`,
			`oracle://username:password@sales-server:1521/sales.us.acme.com`,
			``,
		},
		{
			`oracle://username:password@sales-server.us.acme.com/sales.us.oracle.com`,
			`oracle`,
			`oracle://username:password@sales-server.us.acme.com:1521/sales.us.oracle.com`,
			``,
		},
		{
			`ca://host`,
			`cql`,
			`cql://host`,
			``,
		},
		{
			`cassandra://host:9999`,
			`cql`,
			`cql://host:9999`,
			``,
		},
		{
			`scy://user@host:9999`,
			`cql`,
			`cql://user@host:9999`,
			``,
		},
		{
			`scylla://user@host:9999?timeout=1000`,
			`cql`,
			`cql://user@host:9999?timeout=1000`,
			``,
		},
		{
			`datastax://user:pass@localhost:9999/?timeout=1000`,
			`cql`,
			`cql://user:pass@localhost:9999/?timeout=1000`,
			``,
		},
		{
			`ca://user:pass@localhost:9999/dbname?timeout=1000`,
			`cql`,
			`cql://user:pass@localhost:9999/dbname?timeout=1000`,
			``,
		},
		{
			`sf://user@host:9999/dbname/schema?timeout=1000`,
			`snowflake`,
			`user@host:9999/dbname/schema?timeout=1000`,
			``,
		},
		{
			`sf://user:pass@localhost:9999/dbname/schema?timeout=1000`,
			`snowflake`,
			`user:pass@localhost:9999/dbname/schema?timeout=1000`,
			``,
		},
		{
			`px://user:pass@host/db?application_name=my%20app`,
			`pgx`,
			`postgres://user:pass@host/db?application_name=my%20app`,
			``,
		},
		{
			`px:/var/run/postgresql:6666/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql&port=6666`,
			``,
		},
		{
			`cr://`,
			`pgx`,
			`postgres://localhost:26257?sslmode=disable`,
			``,
		},
		{
			`cockroachdb://user:pass@host/db`,
			`pgx`,
			`postgres://user:pass@host:26257/db?sslmode=disable`,
			``,
		},
		{
			`cr://user:pass@host:1234/db?sslmode=verify-full&application_name=my%20app`,
			`pgx`,
			`postgres://user:pass@host:1234/db?application_name=my%20app&sslmode=verify-full`,
			``,
		},
		{
			`ct://`,
			`pgx`,
			`postgres://localhost`,
			``,
		},
		{
			`crate://crate@127.0.0.1/doc`,
			`pgx`,
			`postgres://crate@127.0.0.1/doc`,
			``,
		},
		{
			`cratedb://user:pass@host:6543/doc?sslmode=require&application_name=my%20app`,
			`pgx`,
			`postgres://user:pass@host:6543/doc?application_name=my%20app&sslmode=require`,
			``,
		},
		{
			`rs://`,
			`pgx`,
			`postgres://localhost:5439`,
			``,
		},
		{
			`redshift://user:pass@host/db?application_name=my%20app`,
			`pgx`,
			`postgres://user:pass@host:5439/db?application_name=my%20app`,
			``,
		},
		{
			`rs://user:pass@amazon.com/dbname`,
			`pgx`,
			`postgres://user:pass@amazon.com:5439/dbname`,
			``,
		},
		{
			`ve://`,
			`vertica`,
			`vertica://localhost:5433/`,
			``,
		},
		{
			`ve://user:pass@vertica-host/dbvertica?tlsmode=server-strict`,
			`vertica`,
			`vertica://user:pass@vertica-host:5433/dbvertica?tlsmode=server-strict`,
			``,
		},
		{
			`vertica://vertica:P4ssw0rd@localhost/vertica`,
			`vertica`,
			`vertica://vertica:P4ssw0rd@localhost:5433/vertica`,
			``,
		},
		{
			`ve://vertica:P4ssw0rd@localhost:5433/vertica`,
			`vertica`,
			`vertica://vertica:P4ssw0rd@localhost:5433/vertica`,
			``,
		},
		{
			`moderncsqlite:///path/to/file.sqlite3`,
			`moderncsqlite`,
			`/path/to/file.sqlite3`,
			``,
		},
		{
			`modernsqlite:///path/to/file.sqlite3`,
			`moderncsqlite`,
			`/path/to/file.sqlite3`,
			``,
		},
		{
			`mq://path/to/file.sqlite3`,
			`moderncsqlite`,
			`path/to/file.sqlite3`,
			``,
		},
		{
			`mq:path/to/file.sqlite3`,
			`moderncsqlite`,
			`path/to/file.sqlite3`,
			``,
		},
		{
			`mq:./path/to/file.sqlite3`,
			`moderncsqlite`,
			`./path/to/file.sqlite3`,
			``,
		},
		{
			`mq://./path/to/file.sqlite3?loc=auto`,
			`moderncsqlite`,
			`./path/to/file.sqlite3?loc=auto`,
			``,
		},
		{
			`mq::memory:?loc=auto`,
			`moderncsqlite`,
			`:memory:?loc=auto`,
			``,
		},
		{
			`mq://:memory:?loc=auto`,
			`moderncsqlite`,
			`:memory:?loc=auto`,
			``,
		},
		{
			`gr://user:pass@localhost:3000/sidname`,
			`godror`,
			`user/pass@//localhost:3000/sidname`,
			``,
		},
		{
			`gr://localhost`,
			`godror`,
			`localhost`,
			``,
		},
		{
			`godror://user:pass@localhost`,
			`godror`,
			`user/pass@//localhost`,
			``,
		},
		{
			`godror://user:pass@localhost/service_name/instance_name`,
			`godror`,
			`user/pass@//localhost/service_name/instance_name`,
			``,
		},
		{
			`godror://user:pass@localhost:2000/xe.oracle.docker`,
			`godror`,
			`user/pass@//localhost:2000/xe.oracle.docker`,
			``,
		},
		{
			`gr://username:password@host/ORCL`,
			`godror`,
			`username/password@//host/ORCL`,
			``,
		},
		{
			`gr://username:password@sales-server:1521/sales.us.acme.com`,
			`godror`,
			`username/password@//sales-server:1521/sales.us.acme.com`,
			``,
		},
		{
			`godror://username:password@sales-server.us.acme.com/sales.us.oracle.com`,
			`godror`,
			`username/password@//sales-server.us.acme.com/sales.us.oracle.com`,
			``,
		},
		{
			`pgx://`,
			`pgx`,
			`postgres://`,
			``,
		},
		{
			`cql://user:p%40ss@[::1]:9042/ks?consistency=localQuorum&host=%5B::2%5D:9042&host=h3`,
			`cql`,
			`cql://user:p%40ss@[::1]:9042/ks?consistency=localQuorum&host=%5B::2%5D:9042&host=h3`,
			``,
		},
		{
			`cql://host/ks?timeout=10s`,
			`cql`,
			`cql://host/ks?timeout=10s`,
			``,
		},
		{
			`couchbase://`,
			`couchbase`,
			`couchbase://localhost`,
			``,
		},
		{
			`n1ql://user:pass@host/`,
			`couchbase`,
			`couchbase://user:pass@host/`,
			``,
		},
		{
			`n1://user:pass@host:9093`,
			`couchbase`,
			`couchbase://user:pass@host:9093`,
			``,
		},
		{
			`couchbase://user:p%40ss@host/?tls=true&query_context=default%3Adbmeta._default`,
			`couchbase`,
			`couchbase://user:p%40ss@host/?tls=true&query_context=default%3Adbmeta._default`,
			``,
		},
		{
			`couchbase://[::1]/?scan_consistency=request_plus&timeout=10s`,
			`couchbase`,
			`couchbase://[::1]/?scan_consistency=request_plus&timeout=10s`,
			``,
		},
		{
			`couchbase://host?tls=1`,
			`couchbase`,
			`couchbase://host?tls=1`,
			``,
		},
		{
			`couchbase://host:9000?tls=true`,
			`couchbase`,
			`couchbase://host:9000?tls=true`,
			``,
		},
		{
			`couchbase://host?tls=false`,
			`couchbase`,
			`couchbase://host?tls=false`,
			``,
		},
		{
			`couchbase://host?tls=bogus`,
			`couchbase`,
			`couchbase://host?tls=bogus`,
			``,
		},
		{
			`surrealdb://root:pw@127.0.0.1/dbmeta/dbmeta`,
			`surrealdb`,
			`surrealdb://root:pw@127.0.0.1/dbmeta/dbmeta`,
			``,
		},
		{
			`sr://user:pass@host:9000/ns/db?tls=true`,
			`surrealdb`,
			`surrealdb://user:pass@host:9000/ns/db?tls=true`,
			``,
		},
		{
			`sur://user:p%40ss@host/my%2Fns/db?auth=database&encoding=json`,
			`surrealdb`,
			`surrealdb://user:p%40ss@host/my%2Fns/db?auth=database&encoding=json`,
			``,
		},
		{
			`surreal://[::1]/ns/db`,
			`surrealdb`,
			`surrealdb://[::1]/ns/db`,
			``,
		},
		{
			`neo4j://`,
			`neo4j`,
			`neo4j://localhost`,
			``,
		},
		{
			`nj://neo4j:pw@127.0.0.1/dbmeta`,
			`neo4j`,
			`neo4j://neo4j:pw@127.0.0.1/dbmeta`,
			``,
		},
		{
			`neo://user:pass@host/db?tls=true&cancel=metadata`,
			`neo4j`,
			`neo4j://user:pass@host/db?tls=true&cancel=metadata`,
			``,
		},
		{
			`n4j://user:p%40ss@host:9000/my%2Fdb?tls=1`,
			`neo4j`,
			`neo4j://user:p%40ss@host:9000/my%2Fdb?tls=1`,
			``,
		},
		{
			`neo4j://[::1]/`,
			`neo4j`,
			`neo4j://[::1]/`,
			``,
		},
		{
			`influxdb://`,
			`influxdb`,
			`influxdb://localhost`,
			``,
		},
		{
			`influx://:apiv3_tok@127.0.0.1/dbmeta`,
			`influxdb`,
			`influxdb://:apiv3_tok@127.0.0.1/dbmeta`,
			``,
		},
		{
			`in://user:tok@host:9999/db?sqlmode=require`,
			`influxdb`,
			`influxdb://user:tok@host:9999/db?sqlmode=require`,
			``,
		},
		{
			`influxdb://host/db?sqlmode=allow&version=2`,
			`influxdb`,
			`influxdb://host/db?sqlmode=allow&version=2`,
			``,
		},
		{
			`influxdb://host/db?version=01`,
			`influxdb`,
			`influxdb://host/db?version=01`,
			``,
		},
		{
			`influxql://`,
			`influxdb`,
			`influxdb://localhost?sqlmode=disable`,
			``,
		},
		{
			`iq://:tok@host/db?version=1`,
			`influxdb`,
			`influxdb://:tok@host/db?version=1&sqlmode=disable`,
			``,
		},
		{
			`influxql://host/db?sqlmode=allow`,
			`influxdb`,
			`influxdb://host/db?sqlmode=allow`,
			``,
		},
		{
			`influxql://[::1]:8086/db`,
			`influxdb`,
			`influxdb://[::1]:8086/db?sqlmode=disable`,
			``,
		},
		{
			`arangodb://`,
			`arangodb`,
			`arangodb://localhost`,
			``,
		},
		{
			`ar://root:pw@127.0.0.1/dbmeta`,
			`arangodb`,
			`arangodb://root:pw@127.0.0.1/dbmeta`,
			``,
		},
		{
			`arango://user:p%40ss@host:9000/my%2Fdb?tls=true`,
			`arangodb`,
			`arangodb://user:p%40ss@host:9000/my%2Fdb?tls=true`,
			``,
		},
		{
			`arangodb://[::1]/_system`,
			`arangodb`,
			`arangodb://[::1]/_system`,
			``,
		},
		{
			`tidb://root@host/db`,
			`mysql`,
			`root@tcp(host:4000)/db`,
			``,
		},
		{
			`ti://root@host:4001/db`,
			`mysql`,
			`root@tcp(host:4001)/db`,
			``,
		},
		{
			`vitess://root@host/db`,
			`mysql`,
			`root@tcp(host)/db`,
			``,
		},
		{
			`memsql://root@host/db`,
			`mysql`,
			`root@tcp(host)/db`,
			``,
		},
		{
			`hdb://user:pass@host`,
			`hdb`,
			`hdb://user:pass@host:30015`,
			``,
		},
		{
			`hdb://user:pass@host:30113`,
			`hdb`,
			`hdb://user:pass@host:30113`,
			``,
		},
		{
			`h2://sa:pw@127.0.0.1/dbmeta?mem=true`,
			`h2`,
			`h2://sa:pw@127.0.0.1/dbmeta?mem=true`,
			``,
		},
		{
			`spanner://localhost:9010/p/i/db?usePlainText=true`,
			`spanner`,
			`localhost:9010/projects/p/instances/i/databases/db;usePlainText=true`,
			``,
		},
		{
			`spanner:///p/i/db`,
			`spanner`,
			`projects/p/instances/i/databases/db`,
			``,
		},
		{
			`sp://host/p/i/db?a=1&b=2`,
			`spanner`,
			`host/projects/p/instances/i/databases/db;a=1;b=2`,
			``,
		},
		{
			`gizmosql://admin:pw@127.0.0.1`,
			`flightsql`,
			`flightsql://admin:pw@127.0.0.1:31337`,
			``,
		},
		{
			`gz://admin:pw@host:9999?timeout=3s`,
			`flightsql`,
			`flightsql://admin:pw@host:9999?timeout=3s`,
			``,
		},
		{
			`gizmo://`,
			`flightsql`,
			`flightsql://localhost:31337`,
			``,
		},
		{
			`questdb://admin:quest@127.0.0.1/qdb`,
			`pgx`,
			`postgres://admin:quest@127.0.0.1:8812/qdb`,
			``,
		},
		{
			`pinot://127.0.0.1`,
			`pinot`,
			`pinot://127.0.0.1`,
			``,
		},
		{
			`rqlite://user:pass@127.0.0.1`,
			`rqlite`,
			`rqlite://user:pass@127.0.0.1`,
			``,
		},
		{
			`databend://`,
			`databend`,
			`databend://localhost`,
			``,
		},
		{
			`dd://root:pw@127.0.0.1:9000/default`,
			`databend`,
			`databend://root:pw@127.0.0.1:9000/default`,
			``,
		},
		{
			`libsql://`,
			`libsql`,
			`libsql://localhost`,
			``,
		},
		{
			`turso://user:tok@db-org.turso.io`,
			`libsql`,
			`libsql://user:tok@db-org.turso.io`,
			``,
		},
		{
			`ls://:tok@127.0.0.1:8080?tls=false`,
			`libsql`,
			`libsql://:tok@127.0.0.1:8080?tls=false`,
			``,
		},
		{
			`avatica://`,
			`avatica`,
			`avatica://localhost`,
			``,
		},
		{
			`av://user:pw@127.0.0.1`,
			`avatica`,
			`avatica://user:pw@127.0.0.1`,
			``,
		},
		{
			`phoenix://admin:p%40s%20s@host:8766?tls=true&auth=basic`,
			`avatica`,
			`avatica://admin:p%40s%20s@host:8766?tls=true&auth=basic`,
			``,
		},
		{
			`avatica://[::1]`,
			`avatica`,
			`avatica://[::1]`,
			``,
		},
		{
			`druid://`,
			`druid`,
			`druid://localhost`,
			``,
		},
		{
			`dr://admin:key@127.0.0.1`,
			`druid`,
			`druid://admin:key@127.0.0.1`,
			``,
		},
		{
			`druid://admin:p%40s%20s@host:8082?tls=true&timezone=UTC&timeout=30s`,
			`druid`,
			`druid://admin:p%40s%20s@host:8082?tls=true&timezone=UTC&timeout=30s`,
			``,
		},
		{
			`druid://[::1]`,
			`druid`,
			`druid://[::1]`,
			``,
		},
		{
			`drill://`,
			`drill`,
			`drill://localhost`,
			``,
		},
		{
			`dl://u:p%40w@host:8048?tls=true&schema=dfs.tmp&autolimit=10`,
			`drill`,
			`drill://u:p%40w@host:8048?tls=true&schema=dfs.tmp&autolimit=10`,
			``,
		},
		{
			`solr://host/coll?mode=facet`,
			`solr`,
			`solr://host/coll?mode=facet`,
			``,
		},
		{
			`solr://u:p@host:8984`,
			`solr`,
			`solr://u:p@host:8984`,
			``,
		},
		{
			`es://elastic:key@host?auth=apikey&fetch_size=500&tls=true`,
			`elasticsearch`,
			`elasticsearch://elastic:key@host?auth=apikey&fetch_size=500&tls=true`,
			``,
		},
		{
			`elasticsearch://`,
			`elasticsearch`,
			`elasticsearch://localhost`,
			``,
		},
		{
			`os://admin:pw@host:9201?tls=true&fetch_size=100`,
			`opensearch`,
			`opensearch://admin:pw@host:9201?tls=true&fetch_size=100`,
			``,
		},
		{
			`opensearch://[::1]`,
			`opensearch`,
			`opensearch://[::1]`,
			``,
		},
		{
			`trino://host`,
			`trino`,
			`trino://user@host?flavor=trino`,
			``,
		},
		{
			`trino://admin:pw@host:8001/catalogname/schemaname?source=dburl`,
			`trino`,
			`trino://admin:pw@host:8001/catalogname/schemaname?source=dburl&flavor=trino`,
			``,
		},
		{
			`tr://host`,
			`trino`,
			`trino://user@host?flavor=trino`,
			``,
		},
		{
			`presto://host/catalogname/schemaname`,
			`trino`,
			`trino://user@host/catalogname/schemaname?flavor=presto`,
			``,
		},
		{
			`prestodb://admin:pass@host:9998/catalogname?tls=true`,
			`trino`,
			`trino://admin:pass@host:9998/catalogname?tls=true&flavor=presto`,
			``,
		},
		{
			`trino://admin@host/cat?flavor=presto`,
			`trino`,
			`trino://admin@host/cat?flavor=presto`,
			``,
		},
		{
			`presto://admin@host/cat?flavor=trino&source=x`,
			`trino`,
			`trino://admin@host/cat?flavor=trino&source=x`,
			``,
		},
		{
			`mc://id:key@host:8443/api?project=p&tunnelEndpoint=t`,
			`maxcompute`,
			`https://id:key@host:8443/api?project=p&tunnelEndpoint=t`,
			``,
		},
		{
			`mc://id:key@host/api?project=p&tls=false`,
			`maxcompute`,
			`http://id:key@host/api?project=p`,
			``,
		},
		{
			`maxcompute://host/api?tls=true&project=p`,
			`maxcompute`,
			`https://host/api?project=p`,
			``,
		},
		{
			`ots://user:pass@localhost/instance_name?tls=false`,
			`ots`,
			`http://user:pass@localhost/instance_name`,
			``,
		},
		{
			`tablestore://user:pass@localhost/instance_name?tls=true&requestTimeout=5s`,
			`ots`,
			`https://user:pass@localhost/instance_name?requestTimeout=5s`,
			``,
		},
		{
			`ydb://?tls=true`,
			`ydb`,
			`grpcs://localhost:2135/`,
			``,
		},
		{
			`ydb://user:pass@localhost:8888/?opt1=a&tls=true&opt2=b`,
			`ydb`,
			`grpcs://user:pass@localhost:8888/?opt1=a&opt2=b`,
			``,
		},
		{
			`ydb://host/local?tls=false&go_balancer=disable`,
			`ydb`,
			`grpc://host:2136/local?go_balancer=disable`,
			``,
		},
		{
			`ca://`,
			`cql`,
			`cql://localhost`,
			``,
		},
		{
			`exa://`,
			`exasol`,
			`exa:localhost:8563`,
			``,
		},
		{
			`exa://user:pass@host:1883/dbname?autocommit=1`,
			`exasol`,
			`exa:host:1883;autocommit=1;password=pass;schema=dbname;user=user`,
			``,
		},
		{
			`mc://`,
			`maxcompute`,
			`https://localhost/`,
			``,
		},
		{
			`mc://id:key@service.cn-hangzhou.maxcompute.aliyun.com/api?project=p`,
			`maxcompute`,
			`https://id:key@service.cn-hangzhou.maxcompute.aliyun.com/api?project=p`,
			``,
		},
		{
			`maxcompute://host/api?project=p`,
			`maxcompute`,
			`https://host/api?project=p`,
			``,
		},
		{
			`ots://user:pass@localhost/instance_name`,
			`ots`,
			`https://user:pass@localhost/instance_name`,
			``,
		},
		{
			`tablestore://user:pass@localhost/instance_name`,
			`ots`,
			`https://user:pass@localhost/instance_name`,
			``,
		},
		{
			`bend://user:pass@localhost/instance_name?sslmode=disabled&warehouse=wh`,
			`databend`,
			`databend://user:pass@localhost/instance_name?sslmode=disabled&warehouse=wh`,
			``,
		},
		{
			`databend://user:pass@localhost/instance_name?tenant=tn&warehouse=wh`,
			`databend`,
			`databend://user:pass@localhost/instance_name?tenant=tn&warehouse=wh`,
			``,
		},
		{
			`flightsql://user:pass@localhost?timeout=3s&token=foobar&tls=enabled`,
			`flightsql`,
			`flightsql://user:pass@localhost?timeout=3s&token=foobar&tls=enabled`,
			``,
		},
		{
			`duckdb:/path/to/foo.db?access_mode=read_only&threads=4`,
			`duckdb`,
			`/path/to/foo.db?access_mode=read_only&threads=4`,
			``,
		},
		{
			`dk:///path/to/foo.db?access_mode=read_only&threads=4`,
			`duckdb`,
			`/path/to/foo.db?access_mode=read_only&threads=4`,
			``,
		},
		{
			`duckdb://`,
			`duckdb`,
			``,
			``,
		},
		{
			`duckdb://:memory:`,
			`duckdb`,
			`:memory:`,
			``,
		},
		{
			`duckdb:?threads=4`,
			`duckdb`,
			`?threads=4`,
			``,
		},
		{
			`file:./testdata/test.sqlite3?a=b`,
			`sqlite3`,
			`./testdata/test.sqlite3?a=b`,
			``,
		},
		{
			`file:./testdata/test.duckdb?a=b`,
			`duckdb`,
			`./testdata/test.duckdb?a=b`,
			``,
		},
		{
			`file:__nonexistent__.db`,
			`sqlite3`,
			`__nonexistent__.db`,
			``,
		},
		{
			`file:__nonexistent__.sqlite3`,
			`sqlite3`,
			`__nonexistent__.sqlite3`,
			``,
		},
		{
			`file:__nonexistent__.duckdb`,
			`duckdb`,
			`__nonexistent__.duckdb`,
			``,
		},
		{
			`__nonexistent__.db`,
			`sqlite3`,
			`__nonexistent__.db`,
			``,
		},
		{
			`__nonexistent__.sqlite3`,
			`sqlite3`,
			`__nonexistent__.sqlite3`,
			``,
		},
		{
			`__nonexistent__.duckdb`,
			`duckdb`,
			`__nonexistent__.duckdb`,
			``,
		},
		{
			`file:fake.sqlite3?a=b`,
			`sqlite3`,
			`fake.sqlite3?a=b`,
			``,
		},
		{
			`fake.sq`,
			`sqlite3`,
			`fake.sq`,
			``,
		},
		{
			`file:fake.duckdb?a=b`,
			`duckdb`,
			`fake.duckdb?a=b`,
			``,
		},
		{
			`fake.dk`,
			`duckdb`,
			`fake.dk`,
			``,
		},
		{
			`file:/var/run/mysqld/mysqld.sock/mydb?timeout=90`,
			`mysql`,
			`unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`,
			`/var/run/mysqld/mysqld.sock`,
		},
		{
			`file:/var/run/postgresql`,
			`pgx`,
			`postgres://?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`file:/var/run/postgresql:6666/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql&port=6666`,
			`/var/run/postgresql`,
		},
		{
			`file:/var/run/postgresql/mydb`,
			`pgx`,
			`postgres:///mydb?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`file:/var/run/postgresql:7777`,
			`pgx`,
			`postgres://?host=%2Fvar%2Frun%2Fpostgresql&port=7777`,
			`/var/run/postgresql`,
		},
		{
			`file://user:pass@/var/run/postgresql/mydb`,
			`pgx`,
			`postgres://user:pass@/mydb?host=%2Fvar%2Frun%2Fpostgresql`,
			`/var/run/postgresql`,
		},
		{
			`hive://myhost/mydb`,
			`hive`,
			`hive://myhost/mydb?auth=NONE`,
			``,
		},
		{
			`hive://myhost`,
			`hive`,
			`hive://myhost/default?auth=NONE`,
			``,
		},
		{
			`hive://`,
			`hive`,
			`hive://localhost/default?auth=NONE`,
			``,
		},
		{
			`hi://myhost:9999/mydb?auth=NONE`,
			`hive`,
			`hive://myhost:9999/mydb?auth=NONE`,
			``,
		},
		{
			`hive2://user:pass@myhost:9999/mydb?auth=NONE`,
			`hive`,
			`hive://user:pass@myhost:9999/mydb?auth=NONE`,
			``,
		},
		{
			`hive://myhost/mydb?sslcert=%2Fc.pem&sslkey=%2Fk.pem`,
			`hive`,
			`hive://myhost/mydb?auth=NONE&sslcert=%2Fc.pem&sslkey=%2Fk.pem`,
			``,
		},
		{
			`hive://myhost/mydb?auth=KERBEROS`,
			`hive`,
			`hive://myhost/mydb?auth=KERBEROS`,
			``,
		},
		{
			`hive://myhost/mydb?auth=NOSASL&transport=binary`,
			`hive`,
			`hive://myhost/mydb?auth=NOSASL&transport=binary`,
			``,
		},
		{
			`hive://myhost/mydb?auth=`,
			`hive`,
			`hive://myhost/mydb?auth=NONE`,
			``,
		},
		{
			`hive://myhost/mydb?auth`,
			`hive`,
			`hive://myhost/mydb?auth=NONE`,
			``,
		},
		{
			`hive://myhost/mydb?transport=http&service=hive2`,
			`hive`,
			`hive://myhost/mydb?auth=NONE&service=hive2&transport=http`,
			``,
		},
		{
			`dy://user:pass@myhost:9999?TimeoutMs=1000`,
			`godynamo`,
			`Region=myhost;AkId=user;Secret_Key=pass;TimeoutMs=1000`,
			``,
		},
		{
			`br://user:pass@dbname`,
			`databricks`,
			`token:user@pass.databricks.com:443/sql/1.0/endpoints/dbname`,
			``,
		},
		{
			`brick://user:pass@dbname?timeout=1000&maxRows=1000`,
			`databricks`,
			`token:user@pass.databricks.com:443/sql/1.0/endpoints/dbname?maxRows=1000&timeout=1000`,
			``,
		},
		{
			`ydb://`,
			`ydb`,
			`grpc://localhost:2136/`,
			``,
		},
		{
			`clickhouse://user:pass@localhost/db?tls=true`,
			`clickhouse`,
			`clickhouse://user:pass@localhost/db?tls=true`,
			``,
		},
		{
			`ch://user:pass@host:8443/`,
			`clickhouse`,
			`clickhouse://user:pass@host:8443/`,
			``,
		},
		{
			`clickhouse://`,
			`clickhouse`,
			`clickhouse://localhost`,
			``,
		},
	}
	m := make(map[string]bool)
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if _, ok := m[test.s]; ok {
				t.Fatalf("%s is already tested", test.s)
			}
			m[test.s] = true
			testParse(t, test.s, test.d, test.exp, test.path)
		})
	}
}

func testParse(t *testing.T, s, d, exp, path string) {
	t.Helper()
	u, err := Parse(s)
	switch {
	case err != nil:
		t.Errorf("%q expected no error, got: %v", s, err)
	case u.Driver != d:
		t.Errorf("%q expected driver %q, got: %q", s, d, u.Driver)
	case u.DSN != exp:
		_, err := Stat(path)
		if path != "" && err != nil && os.IsNotExist(err) {
			t.Logf("%q expected dsn %q, got: %q -- ignoring because `%s` does not exist", s, exp, u.DSN, path)
		} else {
			t.Errorf("%q expected:\n%q\ngot:\n%q", s, exp, u.DSN)
		}
	}
}

func TestBuildURL(t *testing.T) {
	tests := []struct {
		m   map[string]any
		exp string
		err error
	}{
		{nil, "", ErrInvalidDatabaseScheme},
		{
			map[string]any{
				"proto":     "mysql",
				"transport": "tcp",
				"host":      "localhost",
				"port":      999,
				"q": map[string]any{
					"foo":  "bar",
					"opt1": "b",
				},
			},
			"mysql+tcp://localhost:999?foo=bar&opt1=b", nil,
		},
		{
			map[string]any{
				"proto":    "sqlserver",
				"host":     "localhost",
				"port":     "5555",
				"instance": "instance",
				"database": "dbname",
				"q": map[string]any{
					"foo":  "bar",
					"opt1": "b",
				},
			},
			"sqlserver://localhost:5555/instance/dbname?foo=bar&opt1=b", nil,
		},
		{
			map[string]any{
				"proto":    "pg",
				"host":     "host name",
				"user":     "user name",
				"password": "P!!!@@@@ 👀",
				"database": "my awesome db",
				"q": map[string]any{
					"foo":  "bar is cool",
					"opt1": "b zzzz@@@:/",
				},
			},
			"pg://user+name:P%21%21%21%40%40%40%40+%F0%9F%91%80@host+name/my%20awesome%20db?foo=bar+is+cool&opt1=b+zzzz%40%40%40%3A%2F", nil,
		},
		{
			map[string]any{
				"file": "fake.sqlite3",
				"q": map[string]any{
					"foo":  "bar",
					"opt1": "b",
				},
			},
			"file:fake.sqlite3?foo=bar&opt1=b", nil,
		},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			switch s, err := BuildURL(test.m); {
			case err != nil && !errors.Is(err, test.err):
				t.Fatalf("expected error %v, got: %v", test.err, err)
			case err != nil && test.err == nil:
				t.Fatalf("expected no error, got: %v", err)
			case s != test.exp:
				t.Errorf("expected %q, got: %q", test.exp, s)
			default:
				t.Logf("dsn: %q", s)
			}
			switch u, err := FromMap(test.m); {
			case err != nil:
				t.Logf("parse error: %v", err)
			default:
				t.Logf("url: %q", u.String())
			}
		})
	}
}

func init() {
	statFile, openFile := Stat, OpenFile
	Stat = func(name string) (fs.FileInfo, error) {
		if s, ok := newStat(name); ok {
			return s, nil
		}
		return statFile(name)
	}
	OpenFile = func(name string) (fs.File, error) {
		if s, ok := newStat(name); ok {
			return s, nil
		}
		return openFile(name)
	}
}

type stat struct {
	name    string
	mode    fs.FileMode
	content string
}

func newStat(name string) (stat, bool) {
	const (
		sqlite3Header = "SQLite format 3\000.........."
		// a checksum as duckdb writes it. It holds the valid UTF-8 sequence
		// \xc2\xda and a \n. A rune based matcher steps over neither one
		// correctly.
		duckdbHeader = "\x06\xd7\x6f\x0a\xc2\xda\xb3\xb9DUCK\x40\x00\x00\x00......"
	)
	files := map[string]string{
		"fake.sqlite3": sqlite3Header,
		"fake.sq":      sqlite3Header,
		"fake.duckdb":  duckdbHeader,
		"fake.dk":      duckdbHeader,
		"fake.unknown": "not a database header......................",
	}
	switch name {
	case "/var/run/postgresql":
		return stat{name, fs.ModeDir, ""}, true
	case "/var/run/mysqld/mysqld.sock":
		return stat{name, fs.ModeSocket, ""}, true
	case "fake.sqlite3", "fake.sq", "fake.duckdb", "fake.dk", "fake.unknown":
		return stat{name, 0, files[name]}, true
	}
	return stat{}, false
}

func (s stat) Name() string       { return s.name }
func (s stat) Size() int64        { return int64(len(s.content)) }
func (s stat) Mode() fs.FileMode  { return s.mode }
func (s stat) ModTime() time.Time { return time.Now() }
func (s stat) IsDir() bool        { return s.mode&fs.ModeDir != 0 }
func (s stat) Sys() any           { return nil }
func (s stat) Close() error       { return nil }

func (s stat) Stat() (fs.FileInfo, error) {
	return s, nil
}

func (s stat) Read(b []byte) (int, error) {
	v := []byte(s.content)
	copy(b, v)
	return len(v), nil
}
