# About dburl

Package dburl provides a standard, URL style mechanism for parsing and
opening SQL database connection strings for [Go](https://golang.org/project),
supporting standard URLs for PostgreSQL, MySQL, SQLite3, Oracle, Microsoft SQL
Server, and SAP HANA databases.

## Database URL Connection Strings

Supported database URLs are of the form:

```
   protocol+transport://user:pass@host/dbname?opt1=a&opt2=b
   protocol:/path/to/file
```

Where:

| Component          | Description                                                                          |
|--------------------|--------------------------------------------------------------------------------------|
| protocol           | driver name or alias (see below)                                                     |
| user               | the username to connect as                                                           |
| pass               | the password to use                                                                  |
| host               | the remote host                                                                      |
| dbname<sup>*</sup> | the database, instance, or service name/ID to connect to                             |
| ?opt1=...          | additional database driver options (see respective SQL driver for available options) |

<i><sup><b>*</b></sup> for Microsoft SQL Server, the syntax to supply an
instance and database name is `/instance/dbname`, where `/instance` is
optional. For Oracle databases, `/dbname` is the unique database ID (SID).
Please see below for examples.</i>

## Quickstart

Database connection URLs (as described below) can be parsed with `Parse` as such:

```go
u, err := dburl.Parse("postgresql://user:pass@localhost/mydatabase/?sslmode=disable")
if err != nil { /* ... */ }
```

Additionally, a simple helper func `Open`, is available to simply parse,
open, and return the SQL database connection:

```go
db, err := dburl.Open("sqlite:mydatabase.sqlite3?loc=auto")
if err != nil { /* ... */ }
```

## Example URLs ##

 The following are URLs that can be handled with a call to `Open` or `Parse`:

```
   postgres://user:pass@localhost/dbname
   pg://user:pass@localhost/dbname?sslmode=disable
   mysql://user:pass@localhost/dbname
   mysql:/var/run/mysqld/mysqld.sock
   sqlserver://user:pass@remote-host.com/dbname
   mssql://user:pass@remote-host.com/instance/dbname
   ms://user:pass@remote-host.com:port/instance/dbname?keepAlive=10
   oracle://user:pass@somehost.com/oracledb
   sap://user:pass@localhost/dbname
   sqlite:/path/to/file.db
   file:myfile.sqlite3?loc=auto
```

## Driver Aliases

The following protocol aliases are available, and any URL passed to `Parse` or
`Open` will be handled the same as their respective driver:

| Database (driver)            | Aliases                             |
|------------------------------|-------------------------------------|
| Microsoft SQL Server (mssql) | ms, sqlserver                       |
| MySQL (mysql)                | my, mariadb, maria, percona, aurora |
| Oracle (ora)                 | or, oracle, oci8, oci               |
| PostgreSQL (postgres)        | pg, postgresql, pgsql               |
| SAP HANA (hdb)               | sa, saphana, sap, hana              |
| SQLite3 (sqlite3)            | sq, sqlite, file                    |

## Installation

Install in the usual Go fashion:

```sh
go get -u github.com/knq/dburl
```

## Usage

Please note that the dburl package does not import actual SQL drivers, and only
provides a standard way to parse/open respective database connection URLs.

For reference, these are the following "expected" SQL drivers that would need
to be imported:

| Database (driver)            | Package                                                                      |
|------------------------------|------------------------------------------------------------------------------|
| Microsoft SQL Server (mssql) | [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb) |
| MySQL (mysql)                | [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)     |
| Oracle (ora)                 | [gopkg.in/rana/ora.v4](https://gopkg.in/rana/ora.v4)                         |
| PostgreSQL (postgres)        | [github.com/lib/pq](https://github.com/lib/pq)                               |
| SAP HANA (hdb)               | [github.com/SAP/go-hdb/driver](https://github.com/SAP/go-hdb/driver)         |
| SQLite3 (sqlite3)            | [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3)           |

Please see [the GoDoc API page](http://godoc.org/github.com/knq/dburl) for a
full API listing.

### URL Parsing Rules

`Parse` and `Open` rely heavily on the standard net/url/URL type, as such
parsing rules have the same conventions/semantics as any URL parsed by the
standard library's `net/url.Parse`.


## Full Example

A full example for reference:

```go
// example/example.go
package main

import (
    "fmt"
    "log"

    _ "github.com/denisenkom/go-mssqldb"
    "github.com/knq/dburl"
)

func main() {
    db, err := dburl.Open("sqlserver://user:pass@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }

    var name string
    err = db.QueryRow(`SELECT name FROM mytable WHERE id=10`).Scan(&name)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println(">> got: %s\n", name)
}
```

## Related Projects

The dburl package was built primarily to support these projects:

* [usql](https://github.com/knq/usql) - a universal command-line interface for SQL databases
* [xo](https://github.com/knq/xo) - a command-line tool to generate Go code from a database schema
