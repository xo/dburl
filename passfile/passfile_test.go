package passfile

import (
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
		// and a cockroachdb: entry matches a PostgreSQL URL on its host
		{`cr://crhost/db`, url.UserPassword("cruser", "crpass")},
		{`postgres://crhost/db`, url.UserPassword("cruser", "crpass")},
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
