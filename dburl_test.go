package dburl

import "testing"

func TestParse(t *testing.T) {
	l := "mssql://user:!234%23$@localhost:1580/dbname"

	_, err := Parse(l)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}
