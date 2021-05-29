package passfile_test

import (
	"log"
	"os/user"

	"github.com/xo/dburl"
	"github.com/xo/dburl/passfile"
)

func Example_entries() {
	u, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	// read ~/.usqlpass or $ENV{USQLPASS}
	entries, err := passfile.Entries(u, "usqlpass")
	if err != nil {
		log.Fatal(err)
	}
	for i, entry := range entries {
		log.Printf("%d: %v", i, entry)
	}
}

func Example_match() {
	u, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	v, err := dburl.Parse("pg://")
	if err != nil {
		log.Fatal(err)
	}
	// read ~/.usqlpass or $ENV{USQLPASS}
	user, err := passfile.Match(u, v, "usqlpass")
	if err == nil {
		v.User = user
	}
	log.Println("url:", v.String())
}
