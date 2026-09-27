# Backlog

This document lists work that is known and not done. Each item names the
decision or the fact that found it.

A decision is not a backlog item. It goes in [`PLAN.md`](PLAN.md). When an
item here is done, delete it, and record in `PLAN.md` anything that was
decided on the way (D27).

## Schemes

### Add a neo4j scheme when dbimp tags its driver

Ken decided on 2026-09-27 to add a scheme for Neo4j, with the aliases `nj`,
`neo` and `n4j`. `nj` is a listed two letter alias, so the automatic `ne` is
not registered. He first chose `4j`, which cannot work: a URL scheme must
start with a letter, so `net/url` refuses `4j://`, and D14 forbids an alias
that cannot work.

The driver is to be `github.com/xo/dbimp/neo4j`, and dbimp confirmed that it
registers exactly `neo4j`, so `Driver` and `Dialect` are both `neo4j`. The
driver does not exist yet. The URL form, the default port and the query keys
are decided in dbimp step 9. The HTTP API of Neo4j answers on port 7474, but
dbimp did not decide the default port yet.

When dbimp tags the driver, follow [SCHEME.md](SCHEME.md). Read `ParseDSN` at
the tag under rule 3, and inject the default port under rule 7, as D26 did
for surrealdb.

## Tooling

### Clear the lint findings in gen.go

`gen.go` carries `//go:build ignore`, so `golangci-lint run ./...` does not
read it. `golangci-lint run gen.go` reports five findings: two `gosec` G306
findings for the file mode `0o644` of `os.WriteFile`, two `modernize` findings
and one `perfsprint` finding for `+=` on a string in a loop. None of them is a
fault in the output. Decide whether to fix each one or to disable it with a
reason in `.golangci.yml`.
