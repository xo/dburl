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
and dbimp D88). ArangoDB is provisional (D32).
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

## Tooling

### Clear the lint findings in gen.go

`gen.go` carries `//go:build ignore`, so `golangci-lint run ./...` does not
read it. `golangci-lint run gen.go` reports five findings: two `gosec` G306
findings for the file mode `0o644` of `os.WriteFile`, two `modernize` findings
and one `perfsprint` finding for `+=` on a string in a loop. None of them is a
fault in the output. Decide whether to fix each one or to disable it with a
reason in `.golangci.yml`.
