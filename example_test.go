package dburl_test

import (
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/xo/dburl"
)

func ExampleParse() {
	u, err := dburl.Parse("pg://user:pass@host:1234/dbname")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(
		"driver:", u.Driver,
		"user:", u.User.Username(),
		"host:", u.Host,
		"db:", u.Path,
	)
	// Output:
	// driver: postgres user: user host: host:1234 db: /dbname
}
