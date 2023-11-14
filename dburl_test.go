package dburl

import (
	"errors"
	"io/fs"
	"os"
	"strconv"
	"testing"
	"time"
)

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
		{`ql://`, ErrMissingPath},
		{`duckdb://`, ErrMissingPath},
		{`mssql+tcp://user:pass@host/dbname`, ErrInvalidTransportProtocol},
		{`mssql+foobar://`, ErrInvalidTransportProtocol},
		{`mssql+unix:/var/run/mssql.sock`, ErrInvalidTransportProtocol},
		{`mssql+udp:localhost:155`, ErrInvalidTransportProtocol},
		{`adodb+foo+bar://provider/database`, ErrInvalidTransportProtocol},
		{`memsql:/var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`tidb:/var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`vitess:/var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`memsql+unix:///var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`tidb+unix:///var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`vitess+unix:///var/run/mysqld/mysqld.sock`, ErrInvalidTransportProtocol},
		{`cockroach:/var/run/postgresql`, ErrInvalidTransportProtocol},
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
		{`bend://`, ErrMissingHost},
		{`databend://`, ErrMissingHost},
		{`unknown_file.ext3`, ErrInvalidDatabaseScheme},
	}
	for i, tt := range tests {
		test := tt
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

func TestParse(t *testing.T) {
	tests := []struct {
		s    string
		d    string
		exp  string
		path string
	}{
		{
			`pg:`,
			`postgres`,
			``,
			``,
		},
		{
			`pg://`,
			`postgres`,
			``,
			``,
		},
		{
			`pg:user:pass@localhost/booktest`,
			`postgres`,
			`dbname=booktest host=localhost password=pass user=user`,
			``,
		},
		{
			`pg:/var/run/postgresql`,
			`postgres`,
			`host=/var/run/postgresql`,
			`/var/run/postgresql`,
		},
		{
			`pg:/var/run/postgresql:6666/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql port=6666`,
			`/var/run/postgresql`,
		},
		{
			`/var/run/postgresql:6666/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql port=6666`,
			`/var/run/postgresql`,
		},
		{
			`pg:/var/run/postgresql/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql`,
			`/var/run/postgresql`,
		},
		{
			`/var/run/postgresql/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql`,
			`/var/run/postgresql`,
		},
		{
			`pg:/var/run/postgresql:7777`,
			`postgres`,
			`host=/var/run/postgresql port=7777`,
			`/var/run/postgresql`,
		},
		{
			`pg+unix:/var/run/postgresql:4444/booktest`,
			`postgres`,
			`dbname=booktest host=/var/run/postgresql port=4444`,
			`/var/run/postgresql`,
		},
		{
			`/var/run/postgresql:7777`,
			`postgres`,
			`host=/var/run/postgresql port=7777`,
			`/var/run/postgresql`,
		},
		{
			`pg:user:pass@/var/run/postgresql/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql password=pass user=user`,
			`/var/run/postgresql`,
		},
		{
			`pg:user:pass@/really/bad/path`,
			`postgres`,
			`host=/really/bad/path password=pass user=user`,
			``,
		},
		{
			`my:`,
			`mysql`,
			`tcp(localhost:3306)/`,
			``,
		},
		{
			`my://`,
			`mysql`,
			`tcp(localhost:3306)/`,
			``,
		},
		{
			`my:booktest:booktest@localhost/booktest`,
			`mysql`,
			`booktest:booktest@tcp(localhost:3306)/booktest`,
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
			`mymy:`,
			`mymysql`,
			`tcp:localhost:3306*//`,
			``,
		},
		{
			`mymy://`,
			`mymysql`,
			`tcp:localhost:3306*//`,
			``,
		},
		{
			`mymy:user:pass@localhost/booktest`,
			`mymysql`,
			`tcp:localhost:3306*booktest/user/pass`,
			``,
		},
		{
			`mymy:/var/run/mysqld/mysqld.sock/mydb?timeout=90&test=true`,
			`mymysql`,
			`unix:/var/run/mysqld/mysqld.sock,test,timeout=90*mydb`,
			`/var/run/mysqld/mysqld.sock`,
		},
		{
			`mymy:///var/run/mysqld/mysqld.sock/mydb?timeout=90`,
			`mymysql`,
			`unix:/var/run/mysqld/mysqld.sock,timeout=90*mydb`,
			`/var/run/mysqld/mysqld.sock`,
		},
		{
			`mymy+unix:user:pass@mysqld.sock?timeout=90`,
			`mymysql`,
			`unix:mysqld.sock,timeout=90*/user/pass`,
			``,
		},
		{
			`mymy:./path/to/socket`,
			`mymysql`,
			`unix:path/to/socket*//`,
			``,
		},
		{
			`mymy+unix:./path/to/socket`,
			`mymysql`,
			`unix:path/to/socket*//`,
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
			`adodb://Microsoft.ACE.OLEDB.12.0?Extended+Properties=%22Text%3BHDR%3DNO%3BFMT%3DDelimited%22`,
			`adodb`,
			`Data Source=.;Extended Properties="Text;HDR=NO;FMT=Delimited";Provider=Microsoft.ACE.OLEDB.12.0`,
			``,
		},
		{
			`adodb://user:pass@Provider.Name:1542/Oracle8i/dbname`,
			`adodb`,
			`Data Source=Oracle8i;Database=dbname;Password=pass;Port=1542;Provider=Provider.Name;User ID=user`,
			``,
		},
		{
			`oo+Postgres+Unicode://user:pass@host:5432/dbname`,
			`adodb`,
			`Provider=MSDASQL.1;Extended Properties="Database=dbname;Driver={Postgres Unicode};PWD=pass;Port=5432;Server=host;UID=user"`,
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
			`presto://host:8001/`,
			`presto`,
			`http://user@host:8001?catalog=default`,
			``,
		},
		{
			`presto://host/catalogname/schemaname`,
			`presto`,
			`http://user@host:8080?catalog=catalogname&schema=schemaname`,
			``,
		},
		{
			`prs://admin@host/catalogname`,
			`presto`,
			`https://admin@host:8443?catalog=catalogname`,
			``,
		},
		{
			`prestodbs://admin:pass@host:9998/catalogname`,
			`presto`,
			`https://admin:pass@host:9998?catalog=catalogname`,
			``,
		},
		{
			`ca://host`,
			`cql`,
			`host:9042`,
			``,
		},
		{
			`cassandra://host:9999`,
			`cql`,
			`host:9999`,
			``,
		},
		{
			`scy://user@host:9999`,
			`cql`,
			`host:9999?username=user`,
			``,
		},
		{
			`scylla://user@host:9999?timeout=1000`,
			`cql`,
			`host:9999?timeout=1000&username=user`,
			``,
		},
		{
			`datastax://user:pass@localhost:9999/?timeout=1000`,
			`cql`,
			`localhost:9999?password=pass&timeout=1000&username=user`,
			``,
		},
		{
			`ca://user:pass@localhost:9999/dbname?timeout=1000`,
			`cql`,
			`localhost:9999?keyspace=dbname&password=pass&timeout=1000&username=user`,
			``,
		},
		{
			`ig://host`,
			`ignite`,
			`tcp://host:10800`,
			``,
		},
		{
			`ignite://host:9999`,
			`ignite`,
			`tcp://host:9999`,
			``,
		},
		{
			`gridgain://user@host:9999`,
			`ignite`,
			`tcp://host:9999?username=user`,
			``,
		},
		{
			`ig://user@host:9999?timeout=1000`,
			`ignite`,
			`tcp://host:9999?timeout=1000&username=user`,
			``,
		},
		{
			`ig://user:pass@localhost:9999/?timeout=1000`,
			`ignite`,
			`tcp://localhost:9999?password=pass&timeout=1000&username=user`,
			``,
		},
		{
			`ig://user:pass@localhost:9999/dbname?timeout=1000`,
			`ignite`,
			`tcp://localhost:9999/dbname?password=pass&timeout=1000&username=user`,
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
			`rs://user:pass@amazon.com/dbname`,
			`postgres`,
			`postgres://user:pass@amazon.com:5439/dbname`,
			``,
		},
		{
			`ve://user:pass@vertica-host/dbvertica?tlsmode=server-strict`,
			`vertica`,
			`vertica://user:pass@vertica-host:5433/dbvertica?tlsmode=server-strict`,
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
			`trino://host:8001/`,
			`trino`,
			`http://user@host:8001?catalog=default`,
			``,
		},
		{
			`trino://host/catalogname/schemaname`,
			`trino`,
			`http://user@host:8080?catalog=catalogname&schema=schemaname`,
			``,
		},
		{
			`trs://admin@host/catalogname`,
			`trino`,
			`https://admin@host:8443?catalog=catalogname`,
			``,
		},
		{
			`pgx://`,
			`pgx`,
			`postgres://localhost:5432/`,
			``,
		},
		{
			`ca://`,
			`cql`,
			`localhost:9042`,
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
			`ots://user:pass@localhost/instance_name`,
			`ots`,
			`https://user:pass@localhost/instance_name`,
			``,
		},
		{
			`ots+https://user:pass@localhost/instance_name`,
			`ots`,
			`https://user:pass@localhost/instance_name`,
			``,
		},
		{
			`ots+http://user:pass@localhost/instance_name`,
			`ots`,
			`http://user:pass@localhost/instance_name`,
			``,
		},
		{
			`tablestore://user:pass@localhost/instance_name`,
			`ots`,
			`https://user:pass@localhost/instance_name`,
			``,
		},
		{
			`tablestore+https://user:pass@localhost/instance_name`,
			`ots`,
			`https://user:pass@localhost/instance_name`,
			``,
		},
		{
			`tablestore+http://user:pass@localhost/instance_name`,
			`ots`,
			`http://user:pass@localhost/instance_name`,
			``,
		},
		{
			`bend://user:pass@localhost/instance_name?sslmode=disabled&warehouse=wh`,
			`databend`,
			`bend://user:pass@localhost/instance_name?sslmode=disabled&warehouse=wh`,
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
			`postgres`,
			`host=/var/run/postgresql`,
			`/var/run/postgresql`,
		},
		{
			`file:/var/run/postgresql:6666/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql port=6666`,
			`/var/run/postgresql`,
		},
		{
			`file:/var/run/postgresql/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql`,
			`/var/run/postgresql`,
		},
		{
			`file:/var/run/postgresql:7777`,
			`postgres`,
			`host=/var/run/postgresql port=7777`,
			`/var/run/postgresql`,
		},
		{
			`file://user:pass@/var/run/postgresql/mydb`,
			`postgres`,
			`dbname=mydb host=/var/run/postgresql password=pass user=user`,
			`/var/run/postgresql`,
		},
	}
	m := make(map[string]bool)
	for i, tt := range tests {
		test := tt
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
	case u.GoDriver != "" && u.GoDriver != d:
		t.Errorf("%q expected go driver %q, got: %q", s, d, u.GoDriver)
	case u.GoDriver == "" && u.Driver != d:
		t.Errorf("%q expected driver %q, got: %q", s, d, u.Driver)
	case u.DSN != exp:
		_, err := os.Stat(path)
		if path != "" && err != nil && os.IsNotExist(err) {
			t.Logf("%q expected dsn %q, got: %q -- ignoring because `%s` does not exist", s, exp, u.DSN, path)
		} else {
			t.Errorf("%q expected:\n%q\ngot:\n%q", s, exp, u.DSN)
		}
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
		duckdbHeader  = "12345678DUCK87654321.............."
	)
	files := map[string]string{
		"fake.sqlite3": sqlite3Header,
		"fake.sq":      sqlite3Header,
		"fake.duckdb":  duckdbHeader,
		"fake.dk":      duckdbHeader,
	}
	switch name {
	case "/var/run/postgresql":
		return stat{name, fs.ModeDir, ""}, true
	case "/var/run/mysqld/mysqld.sock":
		return stat{name, fs.ModeSocket, ""}, true
	case "fake.sqlite3", "fake.sq", "fake.duckdb", "fake.dk":
		return stat{name, 0, files[name]}, true
	}
	return stat{}, false
}

func (s stat) Name() string       { return s.name }
func (s stat) Size() int64        { return int64(len(s.content)) }
func (s stat) Mode() fs.FileMode  { return s.mode }
func (s stat) ModTime() time.Time { return time.Now() }
func (s stat) IsDir() bool        { return s.mode&fs.ModeDir != 0 }
func (s stat) Sys() interface{}   { return nil }
func (s stat) Close() error       { return nil }

func (s stat) Stat() (fs.FileInfo, error) {
	return s, nil
}

func (s stat) Read(b []byte) (int, error) {
	v := []byte(s.content)
	copy(b, v)
	return len(v), nil
}
