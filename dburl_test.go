package dburl

import "testing"

func TestParse(t *testing.T) {
	tests := []struct {
		s   string
		d   string
		exp string
	}{
		{`pg:booktest:booktest@localhost/booktest`, `postgres`, `postgres://booktest:booktest@localhost/booktest`},
		{`mssql://user:!234%23$@localhost:1580/dbname`, `mssql`, `server=localhost;port=1580;database=dbname;user id=user;password=!234#$`},
		{`adodb://Microsoft.ACE.OLEDB.12.0?Extended+Properties="Text;HDR=NO;FMT=Delimited"`, `adodb`, `Provider=Microsoft.ACE.OLEDB.12.0;Data Source=.;Extended Properties="Text;HDR=NO;FMT=Delimited"`},
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
