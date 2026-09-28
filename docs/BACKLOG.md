# Backlog

This document lists work that is known and not done. Each item names the
decision or the fact that found it.

A decision is not a backlog item. It goes in [`PLAN.md`](PLAN.md). When an
item here is done, delete it, and record in `PLAN.md` anything that was
decided on the way (D27).

## Schemes

### Add the schemes for the next dbimp drivers

dbimp D73 sets the order of the drivers after Neo4j: InfluxDB, CrateDB,
ArangoDB, Databend, TDengine, Apache Pinot, rqlite, then libSQL and Turso.
InfluxDB is done (D29). CrateDB is done on pgx and gets no dbimp driver (D30
and dbimp D88). ArangoDB is provisional (D32), and so are TDengine, Pinot and rqlite (D36).
D38 lets a release carry them before their drivers are tagged.
dbimp settles the name and the URL form of each one in its step 9, and the
name is the database as one lower case word (dbimp D26, D28 and D30). Add
each scheme when its driver has `ParseDSN` at a tag, under rule 3.

ArangoDB has a provisional scheme, `arangodb`, under D32. When the dbimp
driver has `ParseDSN` at a tag, run `GenArangoDB` through it, and confirm the
name and the aliases with Ken, before this library is tagged.

Ken decided these names on 2026-09-28, in dbimp D76:

- libSQL and Turso are one product with one driver, `libsql`, in the package
  `github.com/xo/dbimp/libsql`. `turso` is an alias here.
- Databend moves to `github.com/xo/dbimp/databend`, which registers
  `databend` and replaces `github.com/datafuselabs/databend-go` here and in
  `usql` (dbimp D24). The two are never linked together.

When the `databend` scheme moves, `GenDatabend` must write the scheme
`databend` itself. Today it passes the URL through, so `bend://` reaches the
driver with the scheme `bend`. `databend-go` v0.9.4 reads only whether the
scheme ends in `http`, so that works today. The dbimp driver refuses every
scheme but its own name (dbimp D35).

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

### Revisit presto, trino and clickhouse when dbimp writes their drivers

Ken decided on 2026-09-29 to leave the ports and the TLS handling of these
three until dbimp writes a driver for each (D34). dbimp's drivers take a
`tls` key, read with `strconv.ParseBool`, that selects HTTPS. The drivers
that `usql` uses today do not know it: the Presto driver sends an unknown key
to the server as a session property, and switches to HTTPS only with an
`ssl_*` key, and clickhouse-go uses `secure=true`. When each dbimp driver
exists, read its `ParseDSN` and decide its ports and `tls` with Ken.

### Decide clickhouse+https without secure=true

The port audit of D34 found that `clickhouse+https://` fails in clickhouse-go
unless the URL also names `secure=true`. D38 fixed the other two defaults it
found, for `surrealdb`, `awsathena` and `bigquery`. This one waits for the
dbimp ClickHouse driver, with the rest of its TLS handling.

### Decide the three generators that fill a missing field

D38 says a generator returns an error for a required field that the URL
lacks. Three generators fill one in instead:

- `GenHive` makes the database `default` when the path is empty, which D16
  decided.
- `GenPresto` makes the catalog `default`.
- `GenTrino` makes the catalog `default`.

Presto and Trino wait for their dbimp drivers. Ken decides whether hive keeps
its default under D16.
