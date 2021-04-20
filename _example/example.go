// _example/example.go
package main

import (
	"fmt"
	"log"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/xo/dburl"
)

func main() {
	db, err := dburl.Open("sqlserver://user:pass@localhost/dbname")
	if err != nil {
		log.Fatal(err)
	}
	var name string
	if err := db.QueryRow(`SELECT name FROM mytable WHERE id=10`).Scan(&name); err != nil {
		log.Fatal(err)
	}
	fmt.Println("name:", name)
}
