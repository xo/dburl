package passfile

import (
	"reflect"
	"strings"
	"testing"

	"github.com/xo/dburl"
)

func expectedUserPassword(t *testing.T, url string, user, pass string) {
	entries, _ := Parse(strings.NewReader(matchdata))
	parsedURL, _ := dburl.Parse(url)
	ui, _ := MatchEntries(parsedURL, entries, "postgres")

	if ui.Username() != user {
		t.Fatalf("expected user %s, got %s", user, ui.Username())
	}

	url_pass, ok := ui.Password()

	if !ok {
		url_pass = ""
	}

	if url_pass != pass {
		t.Fatalf("expected pass %s, got %s", pass, url_pass)
	}
}

func TestMatching(t *testing.T) {
	expectedUserPassword(t, "postgres://user@host:1/db", "user", "pass1")
	expectedUserPassword(t, "postgres://user2@host:1/db", "user2", "pass2")
	expectedUserPassword(t, "postgres://user@host:2/db", "user", "pass3")
	expectedUserPassword(t, "postgres://user2@host:2/db", "user2", "pass4")
	expectedUserPassword(t, "postgres://user@host2:1/db", "user", "pass5")
	expectedUserPassword(t, "postgres://user2@host2:1/db", "user2", "pass6")
	expectedUserPassword(t, "postgres://user@host:1/db2", "user", "pass7")
	expectedUserPassword(t, "postgres://user2@host:1/db2", "user2", "pass8")
	expectedUserPassword(t, "postgres://user@host:2/db2", "user", "pass7")
	expectedUserPassword(t, "postgres://user2@host:2/db2", "user2", "pass10")
}

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

const matchdata = `
#All fields entered
postgres:host:1:db:user:pass1
postgres:host:1:db:user2:pass2

#Any port
postgres:host:*:db:user:pass3
postgres:host:*:db:user2:pass4

#Any host/port
postgres:*:*:db:user:pass5
postgres:*:*:db:user2:pass6

#Order matters (will get here)
postgres:host:2:db2:user2:pass10

#Any database
postgres:*:*:*:user:pass7
postgres:*:*:*:user2:pass8

#Order matters (won't get here)
postgres:host:2:db2:user:pass9
`
