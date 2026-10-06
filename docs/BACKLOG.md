# Backlog

This document lists work that is known and not done. Each item names the
decision or the fact that found it.

A decision is not a backlog item. It goes in [`PLAN.md`](PLAN.md). When an
item here is done, delete it, and record in `PLAN.md` anything that was
decided on the way (D27).

## Schemes

### Add the schemes for the next dbimp drivers

dbimp D73 set the order of the drivers after Neo4j, and each of them is done:
InfluxDB (D29), CrateDB on pgx (D30), ArangoDB (D32), Databend (D39), Pinot
(D43), rqlite (D44), libSQL with Turso (D45), Avatica (D47) and Druid (D48).
TDengine got no driver, and its scheme is removed (D41). No scheme here is
provisional. When dbimp starts another driver, add its scheme when the driver
has `ParseDSN` at a tag, under rule 3.

### Check the odbc scheme against a tagged xo/odbc

D49 moved `odbc` to `github.com/xo/odbc` before the driver has a commit or a
tag. When it is tagged, run `GenOdbc` through its `ParseDSN`, and record the
check in D49. At the same time, decide with Ken whether `GenOdbc` keeps adding
a default `Port`, such as 1433 for a driver it does not know. The new driver
adds none, and passes the connection string on.

### Check the trino and presto schemes against a tagged driver

D50 moved `trino` and `presto` to the dbimp driver `github.com/xo/dbimp/trino`
before the driver exists, so both generators are provisional. dbimp started the
driver on 2026-10-07 (its W31), and the section of its `TRINO.md` on the DSN
is not written. When dbimp tags the driver, run both generators through its
`ParseDSN`, fix each place where they disagree, and record the check in D50.

### Find a driver for gizmosql that keeps a session

The `usql` session reported on 2026-09-29 that `gizmosql://` connects to
GizmoSQL 1.39.0 and that every query then fails with `No session ID in
request context`. The same error comes through `flightsql://`, so it is in
the Flight SQL driver, `github.com/apache/arrow-go/v18` v18.8.0, and not in
the DSN (D36). That driver sends the same `authorization` header with every
request (`driver/utils.go`), and it opens its client with no middleware
(`driver/driver.go`), so it never keeps a token or a cookie that the server
returns. It sends an unknown query key as gRPC metadata, but a session ID
comes from the server, so no key in the URL can supply it. Under D5 dburl does
not work around it. The fix is a driver that keeps the session: a change
upstream in arrow-go, or a dbimp driver. Ken decides which.

### Revisit clickhouse when dbimp writes its driver

Ken decided on 2026-09-29 to leave the ports and the TLS handling of
`clickhouse` until dbimp writes a driver for it (D34). dbimp's drivers take a
`tls` key, read with `strconv.ParseBool`, that selects HTTPS. The driver that
`usql` uses today does not know it, and clickhouse-go uses `secure=true`. When
the dbimp driver exists, read its `ParseDSN` and decide its ports and `tls`
with Ken. Presto and Trino have their own item below.

### Decide clickhouse+https without secure=true

The port audit of D34 found that `clickhouse+https://` fails in clickhouse-go
unless the URL also names `secure=true`. D38 fixed the other two defaults it
found, for `surrealdb`, `awsathena` and `bigquery`. This one waits for the
dbimp ClickHouse driver, with the rest of its TLS handling.

### Decide the generator that fills a missing field

D38 says a generator returns an error for a required field that the URL
lacks. One generator fills one in instead:

- `GenHive` makes the database `default` when the path is empty, which D16
  decided.

`GenPresto` and `GenTrino` made the catalog `default` until D50. Ken decides
whether hive keeps its default under D16.
