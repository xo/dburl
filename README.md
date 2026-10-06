# About dburl

Package `dburl` [parses][goref-parse] and [opens][goref-open] SQL database
connection strings for [Go][go-project], in a standard URL style. It handles
the URL formats of PostgreSQL, MySQL, SQLite3, Oracle Database and Microsoft
SQL Server. It also handles most other SQL databases that have a public Go
driver.

[Overview][] | [Quickstart][] | [Examples][] | [Schemes][] | [Installing][] | [Using][] | [About][]

[Overview]: #database-connection-url-overview "Database Connection URL Overview"
[Quickstart]: #quickstart "Quickstart"
[Examples]: #example-urls "Example URLs"
[Schemes]: #database-schemes-aliases-and-drivers "Database Schemes, Aliases, and Drivers"
[Installing]: #installing "Installing"
[Using]: #using "Using"
[About]: #about "About"

[![Unit Tests][dburl-ci-status]][dburl-ci]
[![Go Reference][goref-dburl-status]][goref-dburl]
[![Discord Discussion][discord-status]][discord]

[dburl-ci]: https://github.com/xo/dburl/actions/workflows/test.yml
[dburl-ci-status]: https://github.com/xo/dburl/actions/workflows/test.yml/badge.svg
[goref-dburl]: https://pkg.go.dev/github.com/xo/dburl
[goref-dburl-status]: https://pkg.go.dev/badge/github.com/xo/dburl.svg
[discord]: https://discord.gg/yJKEzc7prt "Discord Discussion"
[discord-status]: https://img.shields.io/discord/829150509658013727.svg?label=Discord&logo=Discord&colorB=7289da&style=flat-square "Discord Discussion"

## Database Connection URL Overview

Supported database connection URLs are of the form:

```text
protocol+transport://user:pass@host/dbname?opt1=a&opt2=b
protocol:/path/to/file
```

Where:

| Component           | Description                                                                          |
| ------------------- | ------------------------------------------------------------------------------------ |
| protocol            | driver name or alias (see below)                                                     |
| transport           | "tcp", "udp", "unix", "http", "https", or a driver name for odbc                     |
| user                | username                                                                             |
| pass                | password                                                                             |
| host                | host                                                                                 |
| dbname<sup>\*</sup> | database, instance, or service name/ID to connect to                                 |
| ?opt1=...           | additional database driver options (see respective SQL driver for available options) |

<i><sup><b>\*</b></sup> For Microsoft SQL Server, `/dbname` can be
`/instance/dbname`, where `/instance` is optional. For Oracle Database,
`/dbname` takes the form `/service/dbname`. Here `/service` is the service
name or SID, and `/dbname` is optional. See the examples below.</i>

## Quickstart

The [`dburl.Parse` func][goref-parse] parses a database connection URL in the
format above:

```go
import (
    "github.com/xo/dburl"
)

u, err := dburl.Parse("postgresql://user:pass@localhost/mydatabase/?sslmode=disable")
if err != nil { /* ... */ }
```

[`dburl.Open`][goref-open] parses the URL and returns an open
[standard `sql.DB` database][goref-sql-db] connection:

```go
import (
    "github.com/xo/dburl"
)

db, err := dburl.Open("sqlite:mydatabase.sqlite3?loc=auto")
if err != nil { /* ... */ }
```

## Example URLs

[`dburl.Parse`][goref-parse] and [`dburl.Open`][goref-open] handle database
connection URLs such as these:

```text
postgres://user:pass@localhost/dbname
pg://user:pass@localhost/dbname?sslmode=disable
pq://user:pass@localhost/dbname
mysql://user:pass@localhost/dbname
mysql:/var/run/mysqld/mysqld.sock
sqlserver://user:pass@remote-host.com/dbname
mssql://user:pass@remote-host.com/instance/dbname
ms://user:pass@remote-host.com:port/instance/dbname?keepAlive=10
oracle://user:pass@somehost.com/sid
sap://user:pass@localhost/dbname
cassandra://user:pass@localhost/keyspace?consistency=localQuorum
sqlite:/path/to/file.db
file:myfile.sqlite3?loc=auto
odbc+postgres://user:pass@localhost:port/dbname?option1=
```

## Database Schemes, Aliases, and Drivers

The table lists every supported `dburl` protocol scheme, with its aliases and
its Go driver. A parsed URL names the scheme in `URL.SchemeName` and the name
to pass to `sql.Open` in `URL.Driver`. The two differ when a scheme opens a
driver that another product also uses: `tidb` opens `mysql`, `postgres`,
`cockroachdb` and `redshift` open `pgx`, and `pq` opens `postgres`, which is
the name that `github.com/lib/pq` registers. `dburl.Open` passes `URL.Driver`
to `sql.Open`:

<!-- DRIVER DETAILS START -->

| Database             | Scheme / Tag    | Scheme Aliases                                 | Driver Package / Notes                                                                      |
|----------------------|-----------------|------------------------------------------------|---------------------------------------------------------------------------------------------|
| PostgreSQL           | `postgres`      | `pg`, `pgsql`, `postgresql`                    | [github.com/jackc/pgx/v5/stdlib][d-postgres]                                                |
| MySQL                | `mysql`         | `my`, `maria`, `aurora`, `mariadb`, `percona`  | [github.com/go-sql-driver/mysql][d-mysql]                                                   |
| Microsoft SQL Server | `sqlserver`     | `ms`, `mssql`, `azuresql`                      | [github.com/microsoft/go-mssqldb][d-sqlserver]                                              |
| Oracle Database      | `oracle`        | `or`, `ora`, `oci`, `oci8`, `odpi`, `odpi-c`   | [github.com/sijms/go-ora/v3][d-oracle]                                                      |
| SQLite3              | `sqlite3`       | `sq`, `sqlite`, `file`                         | [github.com/mattn/go-sqlite3][d-sqlite3] <sup>[†][f-cgo]</sup> <sup>[§][f-embedded]</sup>   |
| DuckDB               | `duckdb`        | `dk`, `ddb`, `duck`, `file`                    | [github.com/duckdb/duckdb-go/v2][d-duckdb] <sup>[†][f-cgo]</sup> <sup>[§][f-embedded]</sup> |
| ClickHouse           | `clickhouse`    | `ch`                                           | [github.com/ClickHouse/clickhouse-go/v2][d-clickhouse]                                      |
| CSVQ                 | `csvq`          | `cs`, `csv`, `tsv`, `json`                     | [github.com/mithrandie/csvq-driver][d-csvq] <sup>[§][f-embedded]</sup>                      |
|                      |                 |                                                |                                                                                             |
| Alibaba MaxCompute   | `maxcompute`    | `mc`                                           | [github.com/aliyun/aliyun-odps-go-sdk/sqldriver][d-maxcompute] <sup>[¶][f-hosted]</sup>     |
| Alibaba Tablestore   | `ots`           | `ot`, `tablestore`                             | [github.com/aliyun/aliyun-tablestore-go-sql-driver][d-ots] <sup>[¶][f-hosted]</sup>         |
| Amazon Redshift      | `redshift`      | `rs`                                           | [github.com/jackc/pgx/v5/stdlib][d-redshift] <sup>[¶][f-hosted]</sup>                       |
| Apache Avatica       | `avatica`       | `av`, `phoenix`                                | [github.com/xo/dbimp/avatica][d-avatica]                                                    |
| Apache Druid         | `druid`         | `dr`                                           | [github.com/xo/dbimp/druid][d-druid]                                                        |
| Apache H2            | `h2`            |                                                | [github.com/jmrobles/h2go][d-h2]                                                            |
| Apache Hive          | `hive`          | `hi`, `hive2`                                  | [github.com/beltran/gohive/v2][d-hive]                                                      |
| Apache Impala        | `impala`        | `im`                                           | [github.com/sclgo/impala-go][d-impala]                                                      |
| Apache Pinot         | `pinot`         | `pi`                                           | [github.com/xo/dbimp/pinot][d-pinot]                                                        |
| ArangoDB             | `arangodb`      | `ar`, `arango`                                 | [github.com/xo/dbimp/arangodb][d-arangodb]                                                  |
| AWS Athena           | `awsathena`     | `s3`, `aws`, `athena`                          | [github.com/uber/athenadriver/go][d-awsathena] <sup>[¶][f-hosted]</sup>                     |
| Azure CosmosDB       | `cosmos`        | `cm`, `gocosmos`                               | [github.com/btnguyen2k/gocosmos][d-cosmos] <sup>[¶][f-hosted]</sup>                         |
| Cassandra            | `cql`           | `ca`, `scy`, `scylla`, `datastax`, `cassandra` | [github.com/xo/cql][d-cql]                                                                  |
| ChaiSQL              | `chai`          | `ci`, `genji`, `chaisql`                       | [github.com/chaisql/chai][d-chai] <sup>[§][f-embedded]</sup>                                |
| CockroachDB          | `cockroachdb`   | `cr`, `cdb`, `crdb`, `cockroach`               | [github.com/jackc/pgx/v5/stdlib][d-cockroachdb]                                             |
| Couchbase            | `couchbase`     | `n1`, `n1ql`                                   | [github.com/xo/dbimp/couchbase][d-couchbase]                                                |
| CrateDB              | `cratedb`       | `ct`, `crate`                                  | [github.com/jackc/pgx/v5/stdlib][d-cratedb]                                                 |
| Databend             | `databend`      | `dd`, `bend`                                   | [github.com/xo/dbimp/databend][d-databend]                                                  |
| Databricks           | `databricks`    | `br`, `brick`, `bricks`, `databrick`           | [github.com/databricks/databricks-sql-go][d-databricks] <sup>[¶][f-hosted]</sup>            |
| DynamoDb             | `godynamo`      | `dy`, `dyn`, `dynamo`, `dynamodb`              | [github.com/btnguyen2k/godynamo][d-godynamo] <sup>[¶][f-hosted]</sup>                       |
| Exasol               | `exasol`        | `ex`, `exa`                                    | [github.com/exasol/exasol-driver-go][d-exasol]                                              |
| Firebird             | `firebirdsql`   | `fb`, `firebird`                               | [github.com/nakagami/firebirdsql][d-firebirdsql]                                            |
| FlightSQL            | `flightsql`     | `fl`, `flight`                                 | [github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver][d-flightsql]                 |
| GizmoSQL             | `gizmosql`      | `gz`, `gizmo`                                  | [github.com/apache/arrow-go/v18/arrow/flight/flightsql/driver][d-gizmosql]                  |
| GO DRiver for ORacle | `godror`        | `gr`                                           | [github.com/godror/godror][d-godror] <sup>[†][f-cgo]</sup>                                  |
| Google BigQuery      | `bigquery`      | `bq`                                           | [gorm.io/driver/bigquery/driver][d-bigquery] <sup>[¶][f-hosted]</sup>                       |
| Google Spanner       | `spanner`       | `sp`                                           | [github.com/googleapis/go-sql-spanner][d-spanner] <sup>[¶][f-hosted]</sup>                  |
| InfluxDB             | `influxdb`      | `in`, `influx`                                 | [github.com/xo/dbimp/influxdb][d-influxdb]                                                  |
| InfluxDB InfluxQL    | `influxql`      | `iq`                                           | [github.com/xo/dbimp/influxdb][d-influxql]                                                  |
| libSQL               | `libsql`        | `ls`, `turso`                                  | [github.com/xo/dbimp/libsql][d-libsql]                                                      |
| ModernC SQLite3      | `moderncsqlite` | `mq`, `modernsqlite`                           | [modernc.org/sqlite][d-moderncsqlite] <sup>[§][f-embedded]</sup>                            |
| Neo4j                | `neo4j`         | `nj`, `neo`, `n4j`                             | [github.com/xo/dbimp/neo4j][d-neo4j]                                                        |
| ODBC                 | `odbc`          | `od`                                           | [github.com/alexbrainman/odbc][d-odbc] <sup>[†][f-cgo]</sup>                                |
| PostgreSQL lib/pq    | `pq`            | `libpq`                                        | [github.com/lib/pq][d-pq]                                                                   |
| PostgreSQL PGX       | `pgx`           | `px`                                           | [github.com/jackc/pgx/v5/stdlib][d-pgx]                                                     |
| Presto               | `presto`        | `pr`, `prestodb`                               | [github.com/prestodb/presto-go-client/v2][d-presto]                                         |
| QuestDB              | `questdb`       | `qs`                                           | [github.com/jackc/pgx/v5/stdlib][d-questdb]                                                 |
| rqlite               | `rqlite`        | `rq`                                           | [github.com/xo/dbimp/rqlite][d-rqlite]                                                      |
| SAP HANA             | `hdb`           | `sa`, `sap`, `hana`, `saphana`                 | [github.com/SAP/go-hdb/driver][d-hdb]                                                       |
| SingleStore MemSQL   | `memsql`        | `me`                                           | [github.com/go-sql-driver/mysql][d-memsql]                                                  |
| Snowflake            | `snowflake`     | `sf`                                           | [github.com/snowflakedb/gosnowflake/v2][d-snowflake] <sup>[¶][f-hosted]</sup>               |
| SurrealDB            | `surrealdb`     | `sr`, `sur`, `surreal`                         | [github.com/xo/dbimp/surrealdb][d-surrealdb]                                                |
| TiDB                 | `tidb`          | `ti`                                           | [github.com/go-sql-driver/mysql][d-tidb]                                                    |
| Trino                | `trino`         | `tr`, `trs`, `trinos`                          | [github.com/trinodb/trino-go-client/trino][d-trino]                                         |
| Vertica              | `vertica`       | `ve`                                           | [github.com/vertica/vertica-sql-go][d-vertica]                                              |
| Vitess Database      | `vitess`        | `vt`                                           | [github.com/go-sql-driver/mysql][d-vitess]                                                  |
| VoltDB               | `voltdb`        | `vo`, `vdb`, `volt`                            | [github.com/VoltDB/voltdb-client-go/voltdbclient][d-voltdb]                                 |
| YDB                  | `ydb`           | `yd`, `yds`, `ydbs`                            | [github.com/ydb-platform/ydb-go-sdk/v3][d-ydb]                                              |

[d-arangodb]: https://github.com/xo/dbimp
[d-avatica]: https://github.com/xo/dbimp
[d-awsathena]: https://github.com/uber/athenadriver
[d-bigquery]: https://github.com/go-gorm/bigquery
[d-chai]: https://github.com/chaisql/chai
[d-clickhouse]: https://github.com/ClickHouse/clickhouse-go
[d-cockroachdb]: https://github.com/jackc/pgx
[d-cosmos]: https://github.com/btnguyen2k/gocosmos
[d-couchbase]: https://github.com/xo/dbimp
[d-cql]: https://github.com/xo/cql
[d-cratedb]: https://github.com/jackc/pgx
[d-csvq]: https://github.com/mithrandie/csvq-driver
[d-databend]: https://github.com/xo/dbimp
[d-databricks]: https://github.com/databricks/databricks-sql-go
[d-druid]: https://github.com/xo/dbimp
[d-duckdb]: https://github.com/duckdb/duckdb-go
[d-exasol]: https://github.com/exasol/exasol-driver-go
[d-firebirdsql]: https://github.com/nakagami/firebirdsql
[d-flightsql]: https://github.com/apache/arrow-go/tree/main/arrow/flight/flightsql/driver
[d-gizmosql]: https://github.com/apache/arrow-go/tree/main/arrow/flight/flightsql/driver
[d-godror]: https://github.com/godror/godror
[d-godynamo]: https://github.com/btnguyen2k/godynamo
[d-h2]: https://github.com/jmrobles/h2go
[d-hdb]: https://github.com/SAP/go-hdb
[d-hive]: https://github.com/beltran/gohive
[d-impala]: https://github.com/sclgo/impala-go
[d-influxdb]: https://github.com/xo/dbimp
[d-influxql]: https://github.com/xo/dbimp
[d-libsql]: https://github.com/xo/dbimp
[d-maxcompute]: https://github.com/aliyun/aliyun-odps-go-sdk
[d-memsql]: https://github.com/go-sql-driver/mysql
[d-moderncsqlite]: https://gitlab.com/cznic/sqlite
[d-mysql]: https://github.com/go-sql-driver/mysql
[d-neo4j]: https://github.com/xo/dbimp
[d-odbc]: https://github.com/alexbrainman/odbc
[d-oracle]: https://github.com/sijms/go-ora
[d-ots]: https://github.com/aliyun/aliyun-tablestore-go-sql-driver
[d-pgx]: https://github.com/jackc/pgx
[d-pinot]: https://github.com/xo/dbimp
[d-postgres]: https://github.com/jackc/pgx
[d-pq]: https://github.com/lib/pq
[d-presto]: https://github.com/prestodb/presto-go-client
[d-questdb]: https://github.com/jackc/pgx
[d-redshift]: https://github.com/jackc/pgx
[d-rqlite]: https://github.com/xo/dbimp
[d-snowflake]: https://github.com/snowflakedb/gosnowflake
[d-spanner]: https://github.com/googleapis/go-sql-spanner
[d-sqlite3]: https://github.com/mattn/go-sqlite3
[d-sqlserver]: https://github.com/microsoft/go-mssqldb
[d-surrealdb]: https://github.com/xo/dbimp
[d-tidb]: https://github.com/go-sql-driver/mysql
[d-trino]: https://github.com/trinodb/trino-go-client
[d-vertica]: https://github.com/vertica/vertica-sql-go
[d-vitess]: https://github.com/go-sql-driver/mysql
[d-voltdb]: https://github.com/VoltDB/voltdb-client-go
[d-ydb]: https://github.com/ydb-platform/ydb-go-sdk

<!-- DRIVER DETAILS END -->

[f-cgo]: #f-cgo "Requires CGO"
[f-embedded]: #f-embedded "Embedded"
[f-hosted]: #f-hosted "Hosted service"

<p>
  <i>
    <a id="f-cgo"><sup>†</sup> Requires CGO</a><br>
    <a id="f-embedded"><sup>§</sup> Embedded, with no server to run</a><br>
    <a id="f-hosted"><sup>¶</sup> Hosted service, with no server you can run</a>
  </i>
</p>

You can write any alias as `alias://` in place of `protocol://`.
[`dburl.Parse`][goref-parse] and [`dburl.Open`][goref-open] treat the two the
same.

## Installing

Install `dburl` with `go get`:

```sh
$ go get github.com/xo/dburl@latest
```

## Using

`dburl` does not import any Go SQL driver. It only [parses][goref-parse] and
[opens][goref-open] database connection URLs, so you must `import` the SQL
driver yourself:

```go
import (
    // import Microsoft SQL Server driver
    _ "github.com/microsoft/go-mssqldb"
)
```

See the [database schemes table][Schemes] above for the Go driver that each
scheme expects.

[The `dburl` package documentation][goref-dburl] has more examples and the API
details.

### URL Parsing Rules

[`dburl.Parse`][goref-parse] and [`dburl.Open`][goref-open] build on Go's
standard [`net/url.URL`][goref-net-url] type. The same rules and conventions
apply as for [Go's `net/url.Parse` func][goref-net-url-parse].

## Example

A [full example](_example/example.go):

```go
// _example/example.go
package main

import (
	"fmt"
	"log"

	_ "github.com/microsoft/go-mssqldb"
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
```

## Scheme Resolution

On systems other than Windows, `dburl` resolves a path on disk, or a URL with
a `file:` scheme, to a database driver:

1. A directory resolves as a `postgres:` URL.
2. A Unix socket resolves as a `mysql:` URL.
3. For a file that exists, `dburl` reads the file header and resolves it as a
   `sqlite3:` or a `duckdb:` URL.
4. For a file that does not exist, `dburl` matches the file extension against
   the known `sqlite3:` and `duckdb:` extensions.

To turn this off, set [`dburl.ResolveSchemeType`][goref-variables] to false.
You can also supply your own [`dburl.Stat`][goref-variables] and
[`dburl.OpenFile`][goref-variables] funcs instead:

```go
import "github.com/xo/dburl"

func init() {
    dburl.ResolveSchemeType = false
}
```

## About

`dburl` exists to support these projects:

- [usql][usql] - a universal command-line interface for SQL databases
- [dbtpl][dbtpl] - a command-line tool to generate code for SQL databases
- [dbmeta][dbmeta] - a Go package that reads metadata from SQL databases

<br/>

<div align="center">
  <a href="https://github.com/xo/usql" title="A command line client for many databases">usql</a> |
  <a href="https://github.com/xo/dburl" title="Database connection URLs, this project">dburl</a> |
  <a href="https://github.com/xo/dbmeta" title="Database metadata">dbmeta</a> |
  <a href="https://github.com/xo/dbimp" title="Database drivers in pure Go">dbimp</a> |
  <a href="https://github.com/xo/cql" title="A database/sql driver for Cassandra">cql</a> |
  <a href="https://github.com/xo/dbtpl" title="Go code generated from a database">dbtpl</a> |
  <a href="https://github.com/xo/tblfmt" title="Tables of database results">tblfmt</a> |
  <a href="https://github.com/xo/rline" title="The line editor of usql">rline</a> |
  <a href="https://github.com/xo/transit" title="tree-sitter in pure Go">transit</a>
</div>

[go-project]: https://go.dev/project
[goref-open]: https://pkg.go.dev/github.com/xo/dburl#Open
[goref-variables]: https://pkg.go.dev/github.com/xo/dburl#pkg-variables
[goref-parse]: https://pkg.go.dev/github.com/xo/dburl#Parse
[goref-sql-db]: https://pkg.go.dev/database/sql#DB
[goref-net-url]: https://pkg.go.dev/net/url#URL
[goref-net-url-parse]: https://pkg.go.dev/net/url#URL.Parse
[usql]: https://github.com/xo/usql
[dbtpl]: https://github.com/xo/dbtpl
[dbmeta]: https://github.com/xo/dbmeta
