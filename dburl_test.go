package dburl

import (
	"testing"
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
		{`mssql+tcp://user:pass@host/dbname`, ErrInvalidTransportProtocol},
		{`mssql+aoeu://`, ErrInvalidTransportProtocol},
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
		{`snowflake://account`, ErrMissingPath},
		{`sf://account`, ErrMissingPath},
	}

	for i, test := range tests {
		_, err := Parse(test.s)
		if err == nil {
			t.Errorf("test %d expected error parsing `%s`, got: nil", i, test.s)
			continue
		}
		if err != test.exp {
			t.Errorf("test %d expected error parsing `%s`: `%v`, got: `%v`", i, test.s, test.exp, err)
		}
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		s   string
		d   string
		exp string
	}{
		{`pg:`, `postgres`, ``},
		{`pg://`, `postgres`, ``},
		{`pg:user:pass@localhost/booktest`, `postgres`, `dbname=booktest host=localhost password=pass user=user`},
		{`pg:/var/run/postgresql`, `postgres`, `host=/var/run/postgresql`},
		{`pg:/var/run/postgresql:6666/mydb`, `postgres`, `dbname=mydb host=/var/run/postgresql port=6666`},
		{`pg:/var/run/postgresql/mydb`, `postgres`, `dbname=mydb host=/var/run/postgresql`},
		{`pg:/var/run/postgresql:7777`, `postgres`, `host=/var/run/postgresql port=7777`},
		{`pg+unix:/var/run/postgresql:4444/booktest`, `postgres`, `dbname=booktest host=/var/run/postgresql port=4444`},
		{`pg:user:pass@/var/run/postgresql/mydb`, `postgres`, `dbname=mydb host=/var/run/postgresql password=pass user=user`},
		{`pg:user:pass@/really/bad/path`, `postgres`, `host=/really/bad/path password=pass user=user`},

		{`my:`, `mysql`, `tcp(127.0.0.1:3306)/`}, // 10
		{`my://`, `mysql`, `tcp(127.0.0.1:3306)/`},
		{`my:booktest:booktest@localhost/booktest`, `mysql`, `booktest:booktest@tcp(localhost:3306)/booktest`},
		{`my:/var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mysql`, `unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`},
		{`my:///var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mysql`, `unix(/var/run/mysqld/mysqld.sock)/mydb?timeout=90`},
		{`my+unix:user:pass@mysqld.sock?timeout=90`, `mysql`, `user:pass@unix(mysqld.sock)/?timeout=90`},
		{`my:./path/to/socket`, `mysql`, `unix(path/to/socket)/`},
		{`my+unix:./path/to/socket`, `mysql`, `unix(path/to/socket)/`},

		{`mymy:`, `mymysql`, `tcp:127.0.0.1:3306*//`}, // 18
		{`mymy://`, `mymysql`, `tcp:127.0.0.1:3306*//`},
		{`mymy:user:pass@localhost/booktest`, `mymysql`, `tcp:localhost:3306*booktest/user/pass`},
		{`mymy:/var/run/mysqld/mysqld.sock/mydb?timeout=90&test=true`, `mymysql`, `unix:/var/run/mysqld/mysqld.sock,test,timeout=90*mydb`},
		{`mymy:///var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mymysql`, `unix:/var/run/mysqld/mysqld.sock,timeout=90*mydb`},
		{`mymy+unix:user:pass@mysqld.sock?timeout=90`, `mymysql`, `unix:mysqld.sock,timeout=90*/user/pass`},
		{`mymy:./path/to/socket`, `mymysql`, `unix:path/to/socket*//`},
		{`mymy+unix:./path/to/socket`, `mymysql`, `unix:path/to/socket*//`},

		{`mssql://`, `mssql`, ``}, // 26
		{`mssql://user:pass@localhost/dbname`, `mssql`, `Database=dbname;Password=pass;Server=localhost;User ID=user`},
		{`mssql://user@localhost/service/dbname`, `mssql`, `Database=dbname;Server=localhost\service;User ID=user`},
		{`mssql://user:!234%23$@localhost:1580/dbname`, `mssql`, `Database=dbname;Password=!234#$;Port=1580;Server=localhost;User ID=user`},

		{`adodb://Microsoft.ACE.OLEDB.12.0?Extended+Properties=%22Text%3BHDR%3DNO%3BFMT%3DDelimited%22`, `adodb`, // 30
			`Data Source=.;Extended Properties="Text;HDR=NO;FMT=Delimited";Provider=Microsoft.ACE.OLEDB.12.0`},
		{`adodb://user:pass@Provider.Name:1542/Oracle8i/dbname`, `adodb`,
			`Data Source=Oracle8i;Database=dbname;Password=pass;Port=1542;Provider=Provider.Name;User ID=user`},
		{`oo+Postgres+Unicode://user:pass@host:5432/dbname`, `adodb`,
			`Provider=MSDASQL.1;Extended Properties="Database=dbname;Driver={Postgres Unicode};PWD=pass;Port=5432;Server=host;UID=user"`},

		{`file:/path/to/file.sqlite3`, `sqlite3`, `/path/to/file.sqlite3`}, // 33
		{`sqlite:///path/to/file.sqlite3`, `sqlite3`, `/path/to/file.sqlite3`},
		{`sq://path/to/file.sqlite3`, `sqlite3`, `path/to/file.sqlite3`},
		{`sq:path/to/file.sqlite3`, `sqlite3`, `path/to/file.sqlite3`},
		{`sq:./path/to/file.sqlite3`, `sqlite3`, `./path/to/file.sqlite3`},
		{`sq://./path/to/file.sqlite3?loc=auto`, `sqlite3`, `./path/to/file.sqlite3?loc=auto`},
		{`sq::memory:?loc=auto`, `sqlite3`, `:memory:?loc=auto`},
		{`sq://:memory:?loc=auto`, `sqlite3`, `:memory:?loc=auto`},

		{`oracle://user:pass@localhost/xe.oracle.docker`, `ora`, `user/pass@localhost/xe.oracle.docker`}, // 41
		{`goracle://user:pass@localhost/xe.oracle.docker`, `goracle`, `user/pass@localhost/xe.oracle.docker`},

		{`presto://host:8001/`, `presto`, `http://user@host:8001?catalog=default`}, // 43
		{`presto://host/catalogname/schemaname`, `presto`, `http://user@host:8080?catalog=catalogname&schema=schemaname`},
		{`prs://admin@host/catalogname`, `presto`, `https://admin@host:8443?catalog=catalogname`},
		{`prestodbs://admin:pass@host:9998/catalogname`, `presto`, `https://admin:pass@host:9998?catalog=catalogname`},

		{`ca://host`, `cql`, `host:9042`}, // 47
		{`cassandra://host:9999`, `cql`, `host:9999`},
		{`scy://user@host:9999`, `cql`, `host:9999?username=user`},
		{`scylla://user@host:9999?timeout=1000`, `cql`, `host:9999?timeout=1000&username=user`},
		{`datastax://user:pass@localhost:9999/?timeout=1000`, `cql`, `localhost:9999?password=pass&timeout=1000&username=user`},
		{`ca://user:pass@localhost:9999/dbname?timeout=1000`, `cql`, `localhost:9999?keyspace=dbname&password=pass&timeout=1000&username=user`},

		{`ig://host`, `ignite`, `tcp://host:10800`}, // 53
		{`ignite://host:9999`, `ignite`, `tcp://host:9999`},
		{`gridgain://user@host:9999`, `ignite`, `tcp://host:9999?username=user`},
		{`ig://user@host:9999?timeout=1000`, `ignite`, `tcp://host:9999?timeout=1000&username=user`},
		{`ig://user:pass@localhost:9999/?timeout=1000`, `ignite`, `tcp://localhost:9999?password=pass&timeout=1000&username=user`},
		{`ig://user:pass@localhost:9999/dbname?timeout=1000`, `ignite`, `tcp://localhost:9999/dbname?password=pass&timeout=1000&username=user`},

		{`snowflake://host/dbname/schema`, `snowflake`, `host/dbname/schema`}, // 58
		{`sf://user@host:9999/dbname/schema?timeout=1000`, `snowflake`, `user@host:9999/dbname/schema?timeout=1000`},
		{`sf://user:pass@localhost:9999/dbname/schema?timeout=1000`, `snowflake`, `user:pass@localhost:9999/dbname/schema?timeout=1000`},

		{`rs://user:pass@amazon.com/dbname`, `postgres`, `postgres://user:pass@amazon.com:5439/dbname`}, // 62
	}

	for i, test := range tests {
		u, err := Parse(test.s)
		if err != nil {
			t.Errorf("test %d expected no error, got: %v", i, err)
			continue
		}

		if u.Driver != test.d {
			t.Errorf("test %d expected driver `%s`, got: `%s`", i, test.d, u.Driver)
		}

		if u.DSN != test.exp {
			t.Errorf("test %d expected DSN `%s`, got: `%s`", i, test.exp, u.DSN)
		}
	}
}
