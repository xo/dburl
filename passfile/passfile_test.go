package passfile

import (
	"reflect"
	"strings"
	"testing"
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
