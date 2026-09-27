# Backlog

This document lists work that is known and not done. Each item names the
decision or the fact that found it.

A decision is not a backlog item. It goes in [`PLAN.md`](PLAN.md). When an
item here is done, delete it, and record in `PLAN.md` anything that was
decided on the way (D27).

## Schemes

### Release neo4j when dbimp tags its driver

D28 added the `neo4j` scheme against dbimp commit `b475894`, which no tag
holds. When dbimp tags a release that holds the driver, run the check of D28
again against the tag, then tag this library.

### Add the schemes for the next dbimp drivers

dbimp D73 sets the order of the drivers after Neo4j: InfluxDB, CrateDB,
ArangoDB, Databend, TDengine, Apache Pinot, rqlite, then libSQL and Turso.
dbimp settles the name and the URL form of each one in its step 9, and the
name is the database as one lower case word (dbimp D26, D28 and D30). Add
each scheme when its driver has `ParseDSN` at a tag, under rule 3.

Two questions are with Ken:

- `databend` is already a scheme here, for `github.com/datafuselabs/databend-go`,
  which registers `databend`. A dbimp driver that registers the same name
  cannot be linked beside it.
- libSQL and Turso can be one driver with one name, or two.

## Tooling

### Clear the lint findings in gen.go

`gen.go` carries `//go:build ignore`, so `golangci-lint run ./...` does not
read it. `golangci-lint run gen.go` reports five findings: two `gosec` G306
findings for the file mode `0o644` of `os.WriteFile`, two `modernize` findings
and one `perfsprint` finding for `+=` on a string in a loop. None of them is a
fault in the output. Decide whether to fix each one or to disable it with a
reason in `.golangci.yml`.
