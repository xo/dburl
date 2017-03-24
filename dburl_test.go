package dburl

import "testing"

func TestBadParse(t *testing.T) {
	tests := []struct {
		s string
	}{
		{``},
		{`pgsqlx://`},
		{`m`},
		{`pg+udp://user:pass@localhost/dbname`},
		{`sqlite+unix://`},
		{`sqlite+tcp://`},
		{`file+tcp://`},
		{`mssql+tcp://user:pass@host/dbname`},
		{`mssql+aoeu://`},
		{`mssql+unix:/var/run/mssql.sock`},
		{`mssql+udp:localhost:155`},
		{`adodb+foo+bar://provider/database`},
		{`memsql:/var/run/mysqld/mysqld.sock`},
		{`tidb:/var/run/mysqld/mysqld.sock`},
		{`vitess:/var/run/mysqld/mysqld.sock`},
		{`memsql+unix:///var/run/mysqld/mysqld.sock`},
		{`tidb+unix:///var/run/mysqld/mysqld.sock`},
		{`vitess+unix:///var/run/mysqld/mysqld.sock`},
		{`cockroach:/var/run/postgresql`},
		{`cockroach+unix:/var/run/postgresql`},
		{`pg:./path/to/socket`}, // relative paths are not possible for postgres sockets
		{`pg+unix:./path/to/socket`},
	}

	for i, test := range tests {
		_, err := Parse(test.s)
		if err == nil {
			t.Errorf("test %d expected error parsing `%s`, got: nil", i, test.s)
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

		{`mymy:`, `mymysql`, `tcp:127.0.0.1:3306*`}, // 18
		{`mymy://`, `mymysql`, `tcp:127.0.0.1:3306*`},
		{`mymy:user:pass@localhost/booktest`, `mymysql`, `tcp:localhost:3306*booktest/user/pass`},
		{`mymy:/var/run/mysqld/mysqld.sock/mydb?timeout=90&test=true`, `mymysql`, `unix:/var/run/mysqld/mysqld.sock,test,timeout=90*mydb`},
		{`mymy:///var/run/mysqld/mysqld.sock/mydb?timeout=90`, `mymysql`, `unix:/var/run/mysqld/mysqld.sock,timeout=90*mydb`},
		{`mymy+unix:user:pass@mysqld.sock?timeout=90`, `mymysql`, `unix:mysqld.sock,timeout=90*/user/pass`},
		{`mymy:./path/to/socket`, `mymysql`, `unix:path/to/socket*`},
		{`mymy+unix:./path/to/socket`, `mymysql`, `unix:path/to/socket*`},

		{`mssql://`, `mssql`, ``}, // 26
		{`mssql://user:pass@localhost/dbname`, `mssql`, `Database=dbname;Password=pass;Server=localhost;User ID=user`},
		{`mssql://user@localhost/service/dbname`, `mssql`, `Database=dbname;Server=localhost\service;User ID=user`},
		{`mssql://user:!234%23$@localhost:1580/dbname`, `mssql`, `Database=dbname;Password=!234#$;Port=1580;Server=localhost;User ID=user`},

		{`adodb://Microsoft.ACE.OLEDB.12.0?Extended+Properties=%22Text%3BHDR%3DNO%3BFMT%3DDelimited%22`, `adodb`,
			`Data Source=.;Extended Properties="Text;HDR=NO;FMT=Delimited";Provider=Microsoft.ACE.OLEDB.12.0`}, // 30
		{`adodb://user:pass@Provider.Name:1542/dbname`, `adodb`, `Database=dbname;Password=pass;Port=1542;Provider=Provider.Name;User ID=user`},

		{`oo+Postgres+Unicode://user:pass@host:5432/dbname`, `adodb`,
			`Provider=MSDASQL.1;Extended Properties="Database=dbname;Driver={Postgres Unicode};PWD=pass;Port=5432;Server=host;UID=user"`}, // 31

		{`file:/path/to/file.sqlite3`, `sqlite3`, `/path/to/file.sqlite3`}, // 33
		{`sqlite:///path/to/file.sqlite3`, `sqlite3`, `/path/to/file.sqlite3`},
		{`sq://path/to/file.sqlite3`, `sqlite3`, `path/to/file.sqlite3`},
		{`sq:path/to/file.sqlite3`, `sqlite3`, `path/to/file.sqlite3`},
		{`sq:./path/to/file.sqlite3`, `sqlite3`, `./path/to/file.sqlite3`},
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
