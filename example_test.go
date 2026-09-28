package dburl_test

import (
	"database/sql"
	"log"

	"github.com/xo/dburl"
)

func Example() {
	db, err := dburl.Open("my://user:pass@host:1234/dbname")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	res, err := db.Query("SELECT ...")
	if err != nil {
		log.Fatal(err)
	}
	for res.Next() {
		/* ... */
	}
	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}

func Example_parse() {
	u, err := dburl.Parse("pg://user:pass@host:1234/dbname")
	if err != nil {
		log.Fatal(err)
	}
	// GoDriver names the driver to open when it differs from Driver, as
	// Open does it.
	driver := u.Driver
	if u.GoDriver != "" {
		driver = u.GoDriver
	}
	db, err := sql.Open(driver, u.DSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	res, err := db.Query("SELECT ...")
	if err != nil {
		log.Fatal(err)
	}
	for res.Next() {
		/* ... */
	}
	if err := res.Err(); err != nil {
		log.Fatal(err)
	}
}
