package dburl

import (
	"io/fs"
	"os"
	"testing"
	"time"
)

type stat fs.FileMode

func (mode stat) Name() string       { return "" }
func (mode stat) Size() int64        { return 1 }
func (mode stat) Mode() fs.FileMode  { return fs.FileMode(mode) }
func (mode stat) ModTime() time.Time { return time.Now() }
func (mode stat) IsDir() bool        { return fs.FileMode(mode)&fs.ModeDir != 0 }
func (mode stat) Sys() interface{}   { return nil }

func init() {
	Stat = func(name string) (fs.FileInfo, error) {
		switch name {
		case "/var/run/postgresql":
			return stat(fs.ModeDir), nil
		case "/var/run/mysqld/mysqld.sock":
			return stat(fs.ModeSocket), nil
		}
		return nil, fs.ErrNotExist
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
	}
	for i, test := range tests {
		_, err := Parse(test.s)
		if err == nil {
			t.Errorf("test %d expected error parsing %q", i, test.s)
			continue
		}
		if err != test.exp {
			t.Errorf("test %d expected error parsing %q: expected: %v got: %v", i, test.s, test.exp, err)
		}
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		s    string
		d    string
		exp  string
		path string
	}{
		{`pg:`, `postgres`, ``, ``},
		{`pg://`, `postgres`, ``, ``},
		{`pg:user:pass@localhost/booktest`, `postgres`, `dbname=booktest host=localhost password=pass user=user`, ``},
		{`pg:/var/run/postgresql`, `postgres`, `host=/var/run/postgresql`, `/var/run/postgresql`},
		{`pg:/var/run/postgresql:6666/mydb`, `postgres`, `dbname=mydb host=/var/run/postgresql port=6666`, `/var/run/postgresql`},
		{`pg:/var/run/postgresql/mydb`, `postgres`, `dbname=mydb host=/var/run/postgresql`, `/var/run/postgresql`},
		{`pg:/var/run/postgresql:7777`, `postgres`, `host=/var/run/postgresql port=7777`, `/var/run/postgresql`},
		{`pg+unix:/var/run/postgresql:4444/booktest`, `postgres`, `dbname=booktest host=/var/run/postgresql port=4444`, `/var/run/postgresql`},
		{`pg:user:pass@/var/run/postgresql/mydb`, `postgres`, `dbname=mydb host=/var/run/postgresql password=pass user=user`, `/var/run/postgresql`},
		{`pg:user:pass@/really/bad/path`, `postgres`, `host=/really/bad/path password=pass user=user`, ``},
		{`my:`, `mysql`, `tcp(localhost:3306)/`, ``}, // 10
		{`my://`, `mysql`, `tcp(localhost:3306)/`, ``},
		{`my:booktest:booktest@localhost/booktest`, `mysql`, `booktest:booktest@tcp(localhost:3306)/booktest`, ``},
		{`my:/var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mysql`, `unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`, `/var/run/mysqld/mysqld.sock`},
		{`my:///var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mysql`, `unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`, `/var/run/mysqld/mysqld.sock`},
		{`my+unix:user:pass@mysqld.sock?timeout=90`, `mysql`, `user:pass@unix(mysqld.sock)/?timeout=90`, ``},
		{`my:./path/to/socket`, `mysql`, `unix(path/to/socket)/`, ``},
		{`my+unix:./path/to/socket`, `mysql`, `unix(path/to/socket)/`, ``},
		{`mymy:`, `mymysql`, `tcp:localhost:3306*//`, ``}, // 18
		{`mymy://`, `mymysql`, `tcp:localhost:3306*//`, ``},
		{`mymy:user:pass@localhost/booktest`, `mymysql`, `tcp:localhost:3306*booktest/user/pass`, ``},
		{`mymy:/var/run/mysqld/mysqld.sock/mydb?timeout=90&test=true`, `mymysql`, `unix:/var/run/mysqld/mysqld.sock,test,timeout=90*mydb`, `/var/run/mysqld/mysqld.sock`},
		{`mymy:///var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mymysql`, `unix:/var/run/mysqld/mysqld.sock,timeout=90*mydb`, `/var/run/mysqld/mysqld.sock`},
		{`mymy+unix:user:pass@mysqld.sock?timeout=90`, `mymysql`, `unix:mysqld.sock,timeout=90*/user/pass`, ``},
		{`mymy:./path/to/socket`, `mymysql`, `unix:path/to/socket*//`, ``},
		{`mymy+unix:./path/to/socket`, `mymysql`, `unix:path/to/socket*//`, ``},
		{`mssql://`, `sqlserver`, `sqlserver://localhost`, ``}, // 26
		{`mssql://user:pass@localhost/dbname`, `sqlserver`, `sqlserver://user:pass@localhost/?database=dbname`, ``},
		{`mssql://user@localhost/service/dbname`, `sqlserver`, `sqlserver://user@localhost/service?database=dbname`, ``},
		{`mssql://user:!234%23$@localhost:1580/dbname`, `sqlserver`, `sqlserver://user:%21234%23$@localhost:1580/?database=dbname`, ``},
		{`mssql://user:!234%23$@localhost:1580/service/dbname?fedauth=true`, `azuresql`, `sqlserver://user:%21234%23$@localhost:1580/service?database=dbname&fedauth=true`, ``},
		{`azuresql://user:pass@localhost:100/dbname`, `azuresql`, `sqlserver://user:pass@localhost:100/?database=dbname`, ``},
		{`sqlserver://xxx.database.windows.net?database=xxx&fedauth=ActiveDirectoryMSI`, `azuresql`, `sqlserver://xxx.database.windows.net?database=xxx&fedauth=ActiveDirectoryMSI`, ``},
		{`azuresql://xxx.database.windows.net/dbname?fedauth=ActiveDirectoryMSI`, `azuresql`, `sqlserver://xxx.database.windows.net/?database=dbname&fedauth=ActiveDirectoryMSI`, ``},
		{
			`adodb://Microsoft.ACE.OLEDB.12.0?Extended+Properties=%22Text%3BHDR%3DNO%3BFMT%3DDelimited%22`, `adodb`, // 30
			`Data Source=.;Extended Properties="Text;HDR=NO;FMT=Delimited";Provider=Microsoft.ACE.OLEDB.12.0`, ``,
		},
		{
			`adodb://user:pass@Provider.Name:1542/Oracle8i/dbname`, `adodb`,
			`Data Source=Oracle8i;Database=dbname;Password=pass;Port=1542;Provider=Provider.Name;User ID=user`, ``,
		},
		{
			`oo+Postgres+Unicode://user:pass@host:5432/dbname`, `adodb`,
			`Provider=MSDASQL.1;Extended Properties="Database=dbname;Driver={Postgres Unicode};PWD=pass;Port=5432;Server=host;UID=user"`, ``,
		},
		{`sqlite:///path/to/file.sqlite3`, `sqlite3`, `/path/to/file.sqlite3`, ``},
		{`sq://path/to/file.sqlite3`, `sqlite3`, `path/to/file.sqlite3`, ``},
		{`sq:path/to/file.sqlite3`, `sqlite3`, `path/to/file.sqlite3`, ``},
		{`sq:./path/to/file.sqlite3`, `sqlite3`, `./path/to/file.sqlite3`, ``},
		{`sq://./path/to/file.sqlite3?loc=auto`, `sqlite3`, `./path/to/file.sqlite3?loc=auto`, ``},
		{`sq::memory:?loc=auto`, `sqlite3`, `:memory:?loc=auto`, ``},
		{`sq://:memory:?loc=auto`, `sqlite3`, `:memory:?loc=auto`, ``},
		{`or://user:pass@localhost:3000/sidname`, `oracle`, `oracle://user:pass@localhost:3000/sidname`, ``}, // 41
		{`or://localhost`, `oracle`, `oracle://localhost:1521`, ``},
		{`oracle://user:pass@localhost`, `oracle`, `oracle://user:pass@localhost:1521`, ``},
		{`oracle://user:pass@localhost/service_name/instance_name`, `oracle`, `oracle://user:pass@localhost:1521/service_name/instance_name`, ``},
		{`oracle://user:pass@localhost:2000/xe.oracle.docker`, `oracle`, `oracle://user:pass@localhost:2000/xe.oracle.docker`, ``},
		{`or://username:password@host/ORCL`, `oracle`, `oracle://username:password@host:1521/ORCL`, ``},
		{`odpi://username:password@sales-server:1521/sales.us.acme.com`, `oracle`, `oracle://username:password@sales-server:1521/sales.us.acme.com`, ``},
		{`oracle://username:password@sales-server.us.acme.com/sales.us.oracle.com`, `oracle`, `oracle://username:password@sales-server.us.acme.com:1521/sales.us.oracle.com`, ``},
		{`presto://host:8001/`, `presto`, `http://user@host:8001?catalog=default`, ``}, // 49
		{`presto://host/catalogname/schemaname`, `presto`, `http://user@host:8080?catalog=catalogname&schema=schemaname`, ``},
		{`prs://admin@host/catalogname`, `presto`, `https://admin@host:8443?catalog=catalogname`, ``},
		{`prestodbs://admin:pass@host:9998/catalogname`, `presto`, `https://admin:pass@host:9998?catalog=catalogname`, ``},
		{`ca://host`, `cql`, `host:9042`, ``}, // 53
		{`cassandra://host:9999`, `cql`, `host:9999`, ``},
		{`scy://user@host:9999`, `cql`, `host:9999?username=user`, ``},
		{`scylla://user@host:9999?timeout=1000`, `cql`, `host:9999?timeout=1000&username=user`, ``},
		{`datastax://user:pass@localhost:9999/?timeout=1000`, `cql`, `localhost:9999?password=pass&timeout=1000&username=user`, ``},
		{`ca://user:pass@localhost:9999/dbname?timeout=1000`, `cql`, `localhost:9999?keyspace=dbname&password=pass&timeout=1000&username=user`, ``},
		{`ig://host`, `ignite`, `tcp://host:10800`, ``}, // 59
		{`ignite://host:9999`, `ignite`, `tcp://host:9999`, ``},
		{`gridgain://user@host:9999`, `ignite`, `tcp://host:9999?username=user`, ``},
		{`ig://user@host:9999?timeout=1000`, `ignite`, `tcp://host:9999?timeout=1000&username=user`, ``},
		{`ig://user:pass@localhost:9999/?timeout=1000`, `ignite`, `tcp://localhost:9999?password=pass&timeout=1000&username=user`, ``},
		{`ig://user:pass@localhost:9999/dbname?timeout=1000`, `ignite`, `tcp://localhost:9999/dbname?password=pass&timeout=1000&username=user`, ``},
		{`sf://user@host:9999/dbname/schema?timeout=1000`, `snowflake`, `user@host:9999/dbname/schema?timeout=1000`, ``},
		{`sf://user:pass@localhost:9999/dbname/schema?timeout=1000`, `snowflake`, `user:pass@localhost:9999/dbname/schema?timeout=1000`, ``},
		{`rs://user:pass@amazon.com/dbname`, `postgres`, `postgres://user:pass@amazon.com:5439/dbname`, ``},                                                     // 67
		{`ve://user:pass@vertica-host/dbvertica?tlsmode=server-strict`, `vertica`, `vertica://user:pass@vertica-host:5433/dbvertica?tlsmode=server-strict`, ``}, // 68
		{`moderncsqlite:///path/to/file.sqlite3`, `moderncsqlite`, `/path/to/file.sqlite3`, ``},                                                                 // 69
		{`modernsqlite:///path/to/file.sqlite3`, `moderncsqlite`, `/path/to/file.sqlite3`, ``},
		{`mq://path/to/file.sqlite3`, `moderncsqlite`, `path/to/file.sqlite3`, ``},
		{`mq:path/to/file.sqlite3`, `moderncsqlite`, `path/to/file.sqlite3`, ``},
		{`mq:./path/to/file.sqlite3`, `moderncsqlite`, `./path/to/file.sqlite3`, ``},
		{`mq://./path/to/file.sqlite3?loc=auto`, `moderncsqlite`, `./path/to/file.sqlite3?loc=auto`, ``},
		{`mq::memory:?loc=auto`, `moderncsqlite`, `:memory:?loc=auto`, ``},
		{`mq://:memory:?loc=auto`, `moderncsqlite`, `:memory:?loc=auto`, ``},
		{`gr://user:pass@localhost:3000/sidname`, `godror`, `user/pass@//localhost:3000/sidname`, ``}, // 77
		{`gr://localhost`, `godror`, `localhost`, ``},
		{`godror://user:pass@localhost`, `godror`, `user/pass@//localhost`, ``},
		{`godror://user:pass@localhost/service_name/instance_name`, `godror`, `user/pass@//localhost/service_name/instance_name`, ``},
		{`godror://user:pass@localhost:2000/xe.oracle.docker`, `godror`, `user/pass@//localhost:2000/xe.oracle.docker`, ``},
		{`gr://username:password@host/ORCL`, `godror`, `username/password@//host/ORCL`, ``},
		{`gr://username:password@sales-server:1521/sales.us.acme.com`, `godror`, `username/password@//sales-server:1521/sales.us.acme.com`, ``},
		{`godror://username:password@sales-server.us.acme.com/sales.us.oracle.com`, `godror`, `username/password@//sales-server.us.acme.com/sales.us.oracle.com`, ``},
		{`trino://host:8001/`, `trino`, `http://user@host:8001?catalog=default`, ``}, // 85
		{`trino://host/catalogname/schemaname`, `trino`, `http://user@host:8080?catalog=catalogname&schema=schemaname`, ``},
		{`trs://admin@host/catalogname`, `trino`, `https://admin@host:8443?catalog=catalogname`, ``},
		{`pgx://`, `pgx`, `postgres://localhost:5432/`, ``},
		{`ca://`, `cql`, `localhost:9042`, ``},
		{`exa://`, `exasol`, `exa:localhost:8563`, ``},
		{`exa://user:pass@host:1883/dbname?autocommit=1`, `exasol`, `exa:host:1883;autocommit=1;password=pass;schema=dbname;user=user`, ``}, // 91
		{`ots://user:pass@localhost/instance_name`, `ots`, `https://user:pass@localhost/instance_name`, ``},
		{`ots+https://user:pass@localhost/instance_name`, `ots`, `https://user:pass@localhost/instance_name`, ``},
		{`ots+http://user:pass@localhost/instance_name`, `ots`, `http://user:pass@localhost/instance_name`, ``},
		{`tablestore://user:pass@localhost/instance_name`, `ots`, `https://user:pass@localhost/instance_name`, ``},
		{`tablestore+https://user:pass@localhost/instance_name`, `ots`, `https://user:pass@localhost/instance_name`, ``},
		{`tablestore+http://user:pass@localhost/instance_name`, `ots`, `http://user:pass@localhost/instance_name`, ``},
		{`bend://user:pass@localhost/instance_name?sslmode=disabled&warehouse=wh`, `databend`, `bend://user:pass@localhost/instance_name?sslmode=disabled&warehouse=wh`, ``},
		{`databend://user:pass@localhost/instance_name?tenant=tn&warehouse=wh`, `databend`, `databend://user:pass@localhost/instance_name?tenant=tn&warehouse=wh`, ``},
		{`flightsql://user:pass@localhost?timeout=3s&token=foobar&tls=enabled`, `flightsql`, `flightsql://user:pass@localhost?timeout=3s&token=foobar&tls=enabled`, ``},
		{`duckdb:/path/to/foo.db?access_mode=read_only&threads=4`, `duckdb`, `/path/to/foo.db?access_mode=read_only&threads=4`, ``},
		{`dk:///path/to/foo.db?access_mode=read_only&threads=4`, `duckdb`, `/path/to/foo.db?access_mode=read_only&threads=4`, ``},
		{`file:./testdata/test.sqlite3?a=b`, `sqlite3`, `./testdata/test.sqlite3?a=b`, ``},
		{`file:./testdata/test.duckdb?a=b`, `duckdb`, `./testdata/test.duckdb?a=b`, ``},
	}
	for i, test := range tests {
		u, err := Parse(test.s)
		switch {
		case err != nil:
			t.Fatalf("test %d expected no error, got: %v", i, err)
		case u.GoDriver != "" && u.GoDriver != test.d:
			t.Errorf("test %d expected go driver %q, got: %q", i, test.d, u.GoDriver)
		case u.GoDriver == "" && u.Driver != test.d:
			t.Errorf("test %d expected driver %q, got: %q", i, test.d, u.Driver)
		case u.DSN != test.exp:
			_, err := os.Stat(test.path)
			if test.path != "" && err != nil && os.IsNotExist(err) {
				t.Logf("test %d expected dsn %q, got: %q -- ignoring because `%s` does not exist", i, test.exp, u.DSN, test.path)
			} else {
				t.Errorf("test %d expected:\n%q\ngot:\n%q", i, test.exp, u.DSN)
			}
		}
	}
}
