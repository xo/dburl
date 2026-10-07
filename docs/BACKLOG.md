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

### Tell usql when the drivers of v0.43.0 exist

`usql` stays on v0.42.0, because v0.43.0 carries D49 and D50, which named two
drivers that did not exist yet. D50 broke its two live drivers, because the DSN
of `trino://` and the driver of `presto://` changed, and D49 would make its
README name a driver it does not import. dbimp tagged its Trino driver in
v0.12.0, and the generators are checked against it (D50), but a release of
them is not cut. `usql` takes the next release in one step, with the move to
dbimp's Trino driver (its W40) and the change for D51 (its W41). Both drivers
of the release are tagged now, `github.com/xo/odbc` `v0.1.0` and dbimp
`v0.12.0`, and the generators are checked against them (D49 and D50). D52
changes the `odbc` DSN from a connection string to an `odbc+<driver>://` URL,
and drops the `ServiceName` option and the default port. D53 moves `clickhouse` to the dbimp driver, which drops clickhouse-go, its
options, the native port and `+http` and `+https`. Tell the `usql` session
when the release is cut.

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

### Decide the generators that fill a missing field

D38 says a generator returns an error for a required field that the URL
lacks. Three generators fill one in instead:

- `GenHive` makes the database `default` when the path is empty, which D16
  decided.
- `GenTrino` and `GenPresto` write the user `user` when the URL has none, which
  D50 decided, as the old generators did.

`GenPresto` and `GenTrino` made the catalog `default` until D50. Ken decides
whether hive keeps its default under D16.
