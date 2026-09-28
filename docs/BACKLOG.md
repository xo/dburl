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

### Revisit presto, trino and clickhouse when dbimp writes their drivers

Ken decided on 2026-09-29 to leave the ports and the TLS handling of these
three until dbimp writes a driver for each (D34). dbimp's drivers take a
`tls` key, read with `strconv.ParseBool`, that selects HTTPS. The drivers
that `usql` uses today do not know it: the Presto driver sends an unknown key
to the server as a session property, and switches to HTTPS only with an
`ssl_*` key, and clickhouse-go uses `secure=true`. When each dbimp driver
exists, read its `ParseDSN` and decide its ports and `tls` with Ken.

### Fix the defaults that the port audit found broken

The port audit of D34 found these, and none of them is a port:

- `surrealdb://` with no path fails in the driver, which needs
  `/namespace/database`.
- `clickhouse+https://` fails in clickhouse-go unless the URL also names
  `secure=true`.
- For `awsathena`, the default host `localhost` becomes the S3 bucket, and
  for `bigquery` it becomes the project, so a URL with no host reaches a
  bucket or a project named localhost.
