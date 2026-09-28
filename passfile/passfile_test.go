package passfile

import (
	"database/sql"
	"database/sql/driver"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xo/dburl"
)

func TestParse(t *testing.T) {
	entries, err := Parse(strings.NewReader(passfile))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(entries) != 10 {
		t.Fatalf("entries should have exactly 10 entries, got: %d", len(entries))
	}
	exp := []Entry{
		{"postgres", "*", "*", "*", "postgres", "P4ssw0rd"},
		{"cql", "*", "*", "*", "cassandra", "cassandra"},
		{"godror", "*", "*", "*", "system", "P4ssw0rd"},
		{"ignite", "*", "*", "*", "ignite", "ignite"},
		{"mymysql", "*", "*", "*", "root", "P4ssw0rd"},
		{"mysql", "*", "*", "*", "root", "P4ssw0rd"},
		{"oracle", "*", "*", "*", "system", "P4ssw0rd"},
		{"pgx", "*", "*", "*", "postgres", "P4ssw0rd"},
		{"sqlserver", "*", "*", "*", "sa", "Adm1nP@ssw0rd"},
		{"vertica", "*", "*", "*", "dbadmin", "P4ssw0rd"},
	}
	if !reflect.DeepEqual(entries, exp) {
		t.Errorf("entries does not equal expected:\nexp:%#v\n---\ngot:%#v", exp, entries)
	}
}

func TestMatch(t *testing.T) {
	file := filepath.Join(t.TempDir(), "pass")
	const entries = `cockroachdb:crhost:*:*:cruser:crpass
postgres:*:*:*:pguser:pgpass
mysql:*:*:*:myuser:mypass
`
	if err := os.WriteFile(file, []byte(entries), 0o600); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	t.Setenv("TESTPASS", file)
	tests := []struct {
		s   string
		exp *url.Userinfo
	}{
		// a postgres: entry matches every scheme that speaks PostgreSQL,
		// whichever driver the scheme opens
		{`postgres://host/db`, url.UserPassword("pguser", "pgpass")},
		{`pg://host/db`, url.UserPassword("pguser", "pgpass")},
		{`pgx://host/db`, url.UserPassword("pguser", "pgpass")},
		{`pq://host/db`, url.UserPassword("pguser", "pgpass")},
		{`libpq://host/db`, url.UserPassword("pguser", "pgpass")},
		{`rs://host/db`, url.UserPassword("pguser", "pgpass")},
		// CockroachDB has a dialect of its own, so a cockroachdb: entry
		// matches only its own URLs, and a postgres: entry does not reach them
		{`cr://crhost/db`, url.UserPassword("cruser", "crpass")},
		{`postgres://crhost/db`, url.UserPassword("pguser", "pgpass")},
		{`cr://host/db`, nil},
		{`tidb://host/db`, url.UserPassword("myuser", "mypass")},
		{`oracle://host/db`, nil},
		// a URL that carries a password is left alone
		{`postgres://u:p@host/db`, nil},
	}
	for _, test := range tests {
		t.Run(test.s, func(t *testing.T) {
			u, err := dburl.Parse(test.s)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			user, err := Match(u, "", "testpass")
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if !reflect.DeepEqual(user, test.exp) {
				t.Errorf("expected %v, got: %v", test.exp, user)
			}
		})
	}
}

func TestMatchUserAndDatabase(t *testing.T) {
	entries := []Entry{
		{"postgres", "*", "*", "reports", "*", "reportspass"},
		{"postgres", "*", "*", "*", "postgres", "pgpass"},
		{"postgres", "*", "*", "*", "crate", "cratepass"},
	}
	tests := []struct {
		s   string
		exp *url.Userinfo
	}{
		// a user that the URL names is never replaced with another one
		{`postgres://crate@127.0.0.1:5432/doc?sslmode=disable`, url.UserPassword("crate", "cratepass")},
		{`postgres://other@127.0.0.1:5432/doc`, nil},
		// an entry supplies the user when the URL names none
		{`postgres://127.0.0.1:5432/doc`, url.UserPassword("postgres", "pgpass")},
		// an entry for one database matches only that database, and its
		// user * keeps the user that the URL names
		{`postgres://crate@127.0.0.1:5432/reports`, url.UserPassword("crate", "reportspass")},
		// an empty password is a password, so the file is not read
		{`postgres://crate:@127.0.0.1:5432/doc`, nil},
	}
	for _, test := range tests {
		t.Run(test.s, func(t *testing.T) {
			u, err := dburl.Parse(test.s)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			user, err := MatchEntries(u, entries, dburl.DialectProtocols(u.UnaliasedDriver)...)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if !reflect.DeepEqual(user, test.exp) {
				t.Errorf("expected %v, got: %v", test.exp, user)
			}
		})
	}
}

// stubDriver is a database/sql driver that is never connected. OpenURL only
// has to reach it by name.
type stubDriver struct{}

func (stubDriver) Open(string) (driver.Conn, error) {
	return nil, driver.ErrSkip
}

func init() {
	// pgx is not linked here, so the name is free for the stub.
	sql.Register("pgx", stubDriver{})
}

func TestOpenURLUsesGoDriver(t *testing.T) {
	// cockroachdb returns the Driver cockroachdb and the GoDriver pgx (D30),
	// so OpenURL must open pgx. No driver is named cockroachdb.
	u, err := dburl.Parse("cockroachdb://user:pass@host/db")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	db, err := OpenURL(u, t.TempDir(), "testpass")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	defer db.Close()
	if _, ok := db.Driver().(stubDriver); !ok {
		t.Errorf("expected the pgx driver, got: %T", db.Driver())
	}
}

const passfile = `# sample ~/.usqlpass file
# 
# format is:
# protocol:host:port:dbname:user:pass
postgres:*:*:*:postgres:P4ssw0rd

cql:*:*:*:cassandra:cassandra
godror:*:*:*:system:P4ssw0rd
ignite:*:*:*:ignite:ignite
mymysql:*:*:*:root:P4ssw0rd
mysql:*:*:*:root:P4ssw0rd
oracle:*:*:*:system:P4ssw0rd
pgx:*:*:*:postgres:P4ssw0rd
sqlserver:*:*:*:sa:Adm1nP@ssw0rd
vertica:*:*:*:dbadmin:P4ssw0rd
`
