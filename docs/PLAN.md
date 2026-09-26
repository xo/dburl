# Decisions

Every decision that shapes this repository, in the order it was made. Entries
are append only. A decision is never edited to change its conclusion. When a
later decision replaces one, the older entry keeps its text and gains a line
at the top saying what replaced it.

Each heading carries a status. An amendment must be visible from both sides:
if D11 amends D4, then D4 says so too.

| Decision | Status |
| --- | --- |
| [D1](#d1-the-module-depends-on-the-standard-library-and-nothing-else) | Decided |
| [D2](#d2-dburl-never-imports-a-driver-and-never-opens-a-connection) | Decided |
| [D3](#d3-a-file-header-is-matched-by-comparing-bytes-never-by-a-regexp) | Decided |
| [D4](#d4-a-generator-is-written-against-the-driver-version-pinned-in-usql) | Decided |
| [D5](#d5-dburl-does-not-compensate-for-a-broken-driver-parser) | Decided |
| [D6](#d6-each-database-gets-its-own-generator) | Decided |
| [D7](#d7-defaults-cover-the-host-and-the-port-and-not-driver-options) | Decided |
| [D8](#d8-a-scheme-is-added-only-when-the-driver-is-expected-in-usql) | Decided |
| [D9](#d9-golangci-lint-runs-in-ci-at-a-pinned-version) | Decided |
| [D10](#d10-a-rule-that-has-no-test-is-not-a-rule) | Decided |
| [D11](#d11-netezza-keeps-sharing-genpostgres) | Decided |
| [D12](#d12-a-scheme-follows-its-driver-out-of-usql) | Decided |
| [D13](#d13-this-registry-writes-two-published-driver-tables) | Decided |

### D1. The module depends on the standard library and nothing else. Decided.

`go.mod` has held no `require` line since 2016. This is the property that lets
every `xo` project take `dburl` without inheriting anything.

Two things enforce it. `depguard` runs in `golangci-lint` with
`list-mode: strict` and allows only `$gostd`, and it covers test files.
`TestNoDependencies` reads `go.mod` and fails on any `require` or `replace`.
The test exists because `go test` is the one command that runs on every
change. See D9 and D10.

### D2. dburl never imports a driver and never opens a connection. Decided.

`dburl` writes a string. Another program hands that string to a driver. This
is what keeps D1 possible, and it is also the source of most of the defects in
this log: the repository encodes assumptions about code it cannot run.

Everything in D3, D4 and D5 follows from this one fact.

### D3. A file header is matched by comparing bytes, never by a regexp. Decided.

DuckDB detection used `^.{8}DUCK.{8}`. In Go, `.` matches a rune and not a
byte, so a checksum holding a valid multi-byte UTF-8 sequence, or a newline,
pushed the match off the magic. The file was then not recognised.

The test fixture was pure ASCII, where one rune is one byte, so the suite
passed for the whole life of the matcher. A committed real file,
`testdata/test.duckdb`, also passed, because its checksum happens to be one of
the safe ones.

Compare bytes with `bytes.Equal` at a fixed offset. A fixture must contain a
valid multi-byte UTF-8 sequence, or a newline. Those are the only two cases a
rune based matcher gets wrong. A single invalid byte decodes as one rune and
does not reproduce the defect, which is why "not ASCII" is too loose a rule.

The constant is fixed per storage version, not per database, so detection
worked or failed for every file written by a given duckdb build.

### D4. A generator is written against the driver version pinned in usql. Decided.

`prestodb/presto-go-client` version 1 accepted any scheme. Version 2 rejects
everything except `presto` and `trino`, and reads the catalog and the schema
from the path rather than the query. `dburl` kept emitting the version 1 form
and every Presto user broke. Nothing here can detect it.

Before you write or change a generator, read the DSN parser in the exact
driver version that [usql](https://github.com/xo/usql) pins in its `go.mod`.
Documentation and memory are not evidence. The parser is.

### D5. dburl does not compensate for a broken driver parser. Decided.

Dameng support was 130 lines against a median of 20 for a generator, and
almost all of it validated options that the driver's hand written parser
mishandled. None of it can be tested here, because `dburl` does not import
the driver. It took four fix commits in seven weeks while the rest of the
library was stable, and it was removed in v0.26.0.

Emit the DSN and let the driver reject what it rejects. If a driver needs a
wrapper this large, the answer is to not support it.

### D6. A generator is written for a DSN format, not for a database. Decided.

The `Gen*` funcs are helpers. Reusing one across databases is normal and says
nothing about wire compatibility. `GenOpaque` serves every database whose DSN
is a path on disk, `GenMysql` and `GenPostgres` each serve several, and that
is the design working.

What a shared generator does carry is a claim: that every driver behind it
accepts the same DSN. That claim is what needs checking, and D4 is how you
check it.

The `trino` scheme was registered against `GenPresto`, and the claim went
stale. The two products split six years ago and their drivers now want
incompatible DSNs, so the shared generator was right for Trino and wrong for
Presto. It was split in v0.27.0.

Sharing was not the defect. Nobody rereading the drivers was. Split a
generator when the drivers behind it stop agreeing, and not before.

### D7. Defaults cover the host and the port, and not driver options. Decided.

A `dburl` URL supplies the settings that identify the server, and the passed
URL overrides each one it names. That is the whole point of the translation.

It stops there. An option that changes how the driver or the database behaves
belongs to the client that opens the connection, and `usql` and `dbtpl` inject
their own.

The exception is an option that forces a standards compliant mode a using
package needs, such as UTF-8 or connection retries. Two exist:
`sslmode=disable` in the CockroachDB template, and `ServiceName` in the DB2
path of `GenOdbc`.

### D8. A scheme is added only when the driver is expected in usql. Decided.

`usql` is the authoritative list of the drivers that `xo` projects support,
and `dburl` is the authoritative source of connection strings for every `xo`
project that reaches a database. A scheme with no driver behind it is a
promise this repository cannot keep.

Add a scheme when the matching driver is in `usql`, or when you expect it
there soon. D12 covers the other direction.

### D9. golangci-lint runs in CI at a pinned version. Decided.

`.golangci.yml` sets `default: all` and lists what to turn off. That choice
broke once without a word. `exhaustruct` and `wsl` were deprecated and renamed
to `exhaustruct_v5` and `wsl_v5`, the disable list still named the old ones,
and both linters ran again and produced 41 findings.

CI installs an exact version rather than the newest one, so a release upstream
cannot turn on a linter nobody chose. Upgrading is a deliberate commit that
carries whatever new findings come with it.

### D10. A rule that has no test is not a rule. Decided.

Prose decays without anyone noticing. Every rule here that can be checked is
checked:

- `TestNoDependencies` reads `go.mod`. See D1.
- `TestEveryDecisionIsIndexed` matches the headings here against the table.
- `TestIsDuckdbHeader` covers the byte cases in D3.

Most rules here have no test. D4, D5, D6 and D8 all rest on evidence that
lives in another repository or in a person's judgement, and a test that cannot
reach the evidence is worse than none.

A test was written for D6 and then deleted, because it enforced a rule that
was wrong. It read every shared `Gen*` func as a defect and would have made
each legitimate reuse look like one. Prefer no test to a test that encodes a
rule nobody agreed to.

The README driver table has no test either, because `usql` generates it. See
step 9 of [SCHEME.md](SCHEME.md).

When you write a rule in this file, write the test in the same commit.

### D11. Netezza keeps sharing GenPostgres. Decided.

Raised because `nzgo` is registered against `GenPostgres`, which `postgres`
also uses, and because the README does not mark Netezza wire compatible.

It stays. A shared `Gen*` func is a helper and is not a claim of wire
compatibility, so the README marker and the generator answer different
questions. This entry exists because an earlier draft of D6 read the sharing
as a defect, and it was not one.

Recorded so the same reading does not happen twice.

### D12. A scheme follows its driver out of usql. Decided.

When `usql` removes a driver, remove the scheme here. `dburl` exists to give
`xo` projects connection strings for drivers they carry, so a scheme whose
driver was dropped points at nothing.

Removal and demotion are different, and only one of them applies.

`usql` demotes a badly behaved driver to the `bad` build tag first. That takes
it out of the default, `most` and `all` builds, and leaves it reachable with
`-tags bad`. A demoted driver is still in `usql`, so its scheme stays here.
`CONTRIBUTING.md` in `usql` lists what earns a demotion, and every item is
detectable rather than a matter of taste: mutating process global state from
`init`, writing to standard output uninvited, doing work in `init` that must
not happen at import, exiting the process instead of returning an error,
registering a `database/sql` name another driver already uses, and module
hygiene that breaks consumers.

`usql` removes a driver when the fault cannot be contained by not linking it,
or when upstream has stopped. The signals it names are an archived repository,
no release or commit for eighteen months, an unanswered security report, a
module that no longer resolves, and nobody replying.

Neither step is permanent. `impala` sat in `bad` from 2022 to 2025 and came
back. `hive` went in, came out, and went straight back the same day. `avatica`
and `snowflake` were both removed and both returned. A removal judges the
driver as it stands and not the database, so a scheme removed under this
decision can return the same way.

`ramsql` was removed from `usql` in September 2026, because its `engine/log`
called `slog.SetDefault` from `init` with a handler on `os.Stdout`. That
silenced every `log` and `slog` call in any build carrying it, and would have
put driver output into the query results. The driver offered no way to turn it
off, and its last release was July 2024. The scheme was removed here for the
same reason. `dameng` went the same month, under D5.

Read `CONTRIBUTING.md` in `usql` before deciding this one. It holds the rules,
and it was written to be read by coding agents.

### D13. This registry writes two published driver tables. Decided.

`usql` generates the driver table in its own README and the one in this
README from a single builder. `gen.go` calls `writeReadme` twice, once for
`usql` and once for here behind `-dburl-gen`.

One of the four columns reads this registry. `buildAliases` calls
`dburl.SchemeDriverAndAliases` for the alias column, and `dburl.FileTypes` to
decide which rows carry a `file` alias. The other three are `usql`'s own data:
the description and the driver package come from the driver's doc comment, and
the `Scheme / Tag` column is `v.Tag`, which is the name of the directory the
driver lives in.

That last point explains a row that looks wrong and is not. DynamoDB shows
`godynamo` in this registry and `dynamodb` in the table, because the column is
`usql`'s build tag rather than the registered scheme. The tag is also a `dburl`
alias, so both work.

So an alias changed here rewrites `usql`'s front page the next time anyone
runs `go generate`, with nobody editing `usql`. A scheme removed here drops
out of both tables. Neither is a change you make in the repository where it
shows up.

Treat an alias as published, because it is. The three Presto aliases that
v0.27.0 turned into errors, `prs`, `prestos` and `prestodbs`, are listed in
`usql`'s README table today. No issue or pull request in `usql` references
any of them, so there is no evidence of use, but they are advertised as
supported, which is a higher bar than an undocumented alias changing quietly.
Say so in the release note.

The coupling runs one way in code. Nothing in `usql` calls a `Gen*` func. The
only links are `Parse` and the two generator calls in `gen.go`.