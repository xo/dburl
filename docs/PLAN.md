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
| [D6](#d6-a-generator-is-written-for-a-dsn-format-not-for-a-database) | Decided |
| [D7](#d7-defaults-cover-the-host-and-the-port-and-not-driver-options) | Amended by D16, D29, D34, D50 and D52 |
| [D8](#d8-a-scheme-is-added-only-when-the-driver-is-expected-in-usql) | Decided |
| [D9](#d9-golangci-lint-runs-in-ci-at-a-pinned-version) | Decided |
| [D10](#d10-a-rule-that-has-no-test-is-not-a-rule) | Amended by D17 |
| [D11](#d11-netezza-keeps-sharing-genpostgres) | Replaced by D40 |
| [D12](#d12-a-scheme-follows-its-driver-out-of-usql) | Decided |
| [D13](#d13-this-registry-writes-two-published-driver-tables) | Amended by D17 |
| [D14](#d14-an-alias-that-cannot-work-is-removed-not-left-failing) | Decided |
| [D15](#d15-dburl-does-not-validate-driver-option-values) | Decided |
| [D16](#d16-a-required-option-with-no-valid-empty-value-gets-a-default) | Decided |
| [D17](#d17-a-scheme-describes-its-own-database-and-driver) | Amended by D22, D30, D37 and D46 |
| [D18](#d18-a-scheme-records-how-the-database-is-deployed) | Decided |
| [D19](#d19-a-scheme-names-the-dialect-of-its-product) | Amended by D22, D30 and D37 |
| [D20](#d20-the-schemes-of-four-removed-drivers-leave-in-one-release) | Decided |
| [D21](#d21-the-maxcompute-endpoint-protocol-comes-from-the-transport) | Amended by D51 |
| [D22](#d22-postgres-opens-pgx-and-pq-opens-libpq) | Amended by D30 and D37 |
| [D23](#d23-cql-opens-xocql-and-gets-a-url) | Amended by D56 |
| [D24](#d24-a-parsed-url-carries-its-dialect) | Amended by D30 and D37 |
| [D25](#d25-couchbase-opens-xodbimpcouchbase) | Amended by D34 |
| [D26](#d26-surrealdb-opens-xodbimpsurrealdb) | Amended by D34 |
| [D27](#d27-dburl-is-set-up-for-coding-agents-as-every-xo-repository-is) | Decided |
| [D28](#d28-neo4j-opens-xodbimpneo4j) | Amended by D34 |
| [D29](#d29-influxql-is-a-scheme-of-its-own-on-the-influxdb-driver) | Amended by D34 and D37 |
| [D30](#d30-cockroachdb-and-cratedb-each-have-a-dialect-of-their-own) | Amended by D34 and D37 |
| [D31](#d31-a-password-file-entry-matches-the-database-and-the-user) | Decided |
| [D32](#d32-arangodb-is-a-provisional-scheme-for-the-dbimp-driver) | Amended by D34 and D38 |
| [D33](#d33-passfile-opens-the-go-driver-that-dburlopen-opens) | Amended by D37 |
| [D34](#d34-a-default-port-is-added-only-where-the-driver-has-none) | Amended by D40, D50 and D53 |
| [D35](#d35-a-spanner-url-names-its-host-first) | Decided |
| [D36](#d36-gizmosql-questdb-and-three-provisional-schemes) | Amended by D38, D41, D43 and D44 |
| [D37](#d37-a-scheme-names-its-driver-and-override-is-gone) | Decided |
| [D38](#d38-a-missing-required-field-is-an-error-and-provisional-schemes-can-ship) | Amended by D41, D43 and D50 |
| [D39](#d39-databend-moves-to-the-dbimp-driver) | Decided |
| [D40](#d40-the-nzgo-scheme-is-removed) | Decided |
| [D41](#d41-the-tdengine-scheme-is-removed) | Decided |
| [D42](#d42-the-ql-scheme-is-removed) | Decided |
| [D43](#d43-pinot-is-checked-against-dbimp-v070) | Decided |
| [D44](#d44-rqlite-is-checked-against-dbimp-v080) | Decided |
| [D45](#d45-libsql-opens-xodbimplibsql) | Decided |
| [D46](#d46-dbmetas-tests-choose-their-driver-from-gopackage) | Decided |
| [D47](#d47-avatica-opens-xodbimpavatica) | Decided |
| [D48](#d48-druid-is-a-scheme-for-the-dbimp-driver) | Decided |
| [D49](#d49-odbc-opens-xoodbc) | Amended by D52 |
| [D50](#d50-trino-and-presto-move-to-one-dbimp-driver) | Decided |
| [D51](#d51-maxcompute-tablestore-and-ydb-choose-tls-by-the-tls-option) | Amended by D53 |
| [D52](#d52-odbc-sends-the-url-to-xoodbc) | Decided |
| [D53](#d53-clickhouse-opens-the-dbimp-driver) | Decided |
| [D54](#d54-drill-solr-elasticsearch-and-opensearch-are-schemes-for-the-dbimp-drivers) | Decided |
| [D55](#d55-dynamodb-opens-the-dbimp-driver) | Decided |
| [D56](#d56-cql-is-renamed-cassandra) | Decided |

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

### D7. Defaults cover the host and the port, and not driver options. Amended by D16, D29, D34 and D50.

Amended by D50: `flavor` in `GenTrino` and `GenPresto` is a fourth exception to
this rule.

Amended by D34: a default port is added only when the driver has no
default port of its own, or the wrong one for the product.

Amended by D29: `sqlmode=disable` in `GenInfluxQL` is a third exception to
this rule, because it selects the query language of the scheme.

A `dburl` URL supplies the settings that identify the server, and the passed
URL overrides each one it names. That is the whole point of the translation.

It stops there. An option that changes how the driver or the database behaves
belongs to the client that opens the connection, and `usql` and `dbtpl` inject
their own.

The exception is an option that forces a standards compliant mode a using
package needs, such as UTF-8 or connection retries. Two exist:
`sslmode=disable` in the CockroachDB template, and `ServiceName` in the DB2
path of `GenOdbc`.

D16 adds a second exception, for an option the driver requires and has no
valid empty value for. `auth=NONE` on `hive` is the only one.

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

### D10. A rule that has no test is not a rule. Amended by D17.

Amended by D17: this repository writes the README driver table itself, with
`go run gen.go`. Step 10 of SCHEME.md holds that step, and step 9 holds the
tests.

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

### D11. Netezza keeps sharing GenPostgres. Replaced by D40.

Replaced by D40: the `nzgo` scheme is removed, so no Netezza scheme shares
`GenPostgres`.

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

### D13. This registry writes two published driver tables. Amended by D17.

Amended by D17: this repository now writes its own table with `go run gen.go`,
from this registry. `usql` writes only its own table, and reads this registry
at the version it pins. The rest of this entry describes `usql`'s builder and
still holds for `usql`'s table.

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

Treat an alias as published, because it is. Making an alias fail does not
unpublish it, because the table is built from the registry and a failing
alias is still registered. That is what D14 had to fix.

The coupling runs one way in code. Nothing in `usql` calls a `Gen*` func. The
only links are `Parse` and the two generator calls in `gen.go`.

### D14. An alias that cannot work is removed, not left failing. Decided.

v0.27.0 made `prs`, `prestos` and `prestodbs` return an error unless a TLS
option was present, because the v2 driver selects TLS from `ssl_ca`,
`ssl_cert`, `ssl_key` and `ssl_skip_verify` and never from the scheme, so a
trailing `s` had nothing to map onto.

That was half a fix. The aliases stayed in the registry, so `buildAliases`
kept emitting them and `usql`'s published table still advertised all five.
Regeneration could not correct it, because nothing in the registry had
changed. The result was a front page offering three aliases that cannot
connect.

They are removed. `presto` keeps `pr` and `prestodb`, and TLS is reached by
setting one of the four options on the plain scheme, which also moves the
default port to 8443.

Gemini and DeepSeek were both asked and both said keep the aliases and add
scheme metadata so `usql` could render a footnote. Ken chose removal. The
reasoning against is worth keeping: removal costs an error message that named
the exact option to set, and it breaks the same three names in two
consecutive releases.

The general rule: an alias that cannot work under any input does not belong in
the registry. Making it fail leaves it advertised.

### D15. dburl does not validate driver option values. Decided.

`beltran/gohive/v2`, which `hive` moved to, does not return an error for an
option value it does not know. It panics. `hive.go:272` and `hive.go:299` both
call `panic("Unrecognized auth")`, and `hive.go:307` panics on an unknown
`transport`.

The accepted values, read from the connect path rather than from the
documentation: `auth` takes `NONE`, `LDAP`, `CUSTOM`, `KERBEROS`, `NOSASL` or
`DIGEST-MD5`, and `transport` takes `http` or `binary`. `PLAIN` is not an
`auth` value, although the driver does use `PLAIN` internally as the SASL
mechanism for `NONE`, `LDAP` and `CUSTOM`.

`dburl` still does not check a value the caller supplies. A panic is worse
than an error, and the argument for catching it here is real, but the code
that caught it would be a copy of a list that lives in another module and goes
stale the moment that module changes. That is D5, and Dameng is what it looks
like when it is written anyway.

Supplying a default for a missing option is a different thing from checking a
value that is present, and D16 covers it.

Two things follow instead. The client that injects options owns their values,
under D7. And no example, test case or document here uses a value that panics,
because an example is a recommendation. The hive test cases use `auth=NONE`.

### D16. A required option with no valid empty value gets a default. Amends D7.

D7 says defaults cover the host and the port and not driver options. That is
still the rule. This adds one case it did not cover.

`beltran/gohive/v2` does not treat a missing `auth` as unspecified. The
connect path is an if/else chain over the accepted values, and an empty string
reaches `panic("Unrecognized auth")` at `hive.go:297`. So `hive://host/mydb`,
which is what this library emitted in v0.28.0, takes the caller's process
down. dbmeta measured it against Apache Hive 4.2.1, and the chain was then
confirmed in the pinned source.

`GenHive` supplies `auth=NONE` when the caller has not. Any other value is
passed through untouched.

An empty value counts as not supplied. `?auth=` and a bare `?auth` both parse
to an empty string, which is the value that panics, so treating them as an
override would hand the defect straight back. `GenFromURL` cannot express
that, because it overrides per key regardless of value, which is why `hive`
has its own generator rather than a template.

The line D7 draws is between an option that tunes a connection and a field the
driver requires to build one. `auth` is the second. The same driver requires a
database name and returns `database name is required` for an empty path, and
this library already answers that with `default`. `GenPresto` does the same
for an empty catalog. A required field with no valid empty value is structural
even when it is spelled as a query option.

`NONE` is the safe choice and not merely the common one. `NONE`, `LDAP` and
`CUSTOM` all build a SASL `PLAIN` transport carrying the username and
password. Against a server expecting Kerberos, that fails negotiation rather
than connecting without authentication, so the default cannot downgrade a
secured server. It is a protocol selection, not a decision to skip auth.

Gemini and DeepSeek split on this. DeepSeek said emit nothing, because `auth`
names an authentication mode and the panic is the driver's bug to fix. That
reasoning is sound and is the reason this is an amendment with a stated scope
rather than a loosening of D7. Ken chose the default.

The scope is narrow. This covers an option the driver cannot start without. It
does not cover an option that changes behaviour, which stays with the calling
client.

`transport` is the case that shows where the line falls, because it looks like
`auth` and does not qualify. It is a mode selector, and an unknown value
panics it too, at `hive.go:307`. It still gets no default here, because the
driver has a working one and `auth` does not. That difference is visible in a
single struct literal in `ParseDSN`:

    parsedDSN := &DSN{
        Database:      strings.TrimPrefix(u.Path, "/"),
        TransportMode: "binary", // Default transport mode
        Service:       "hive",   // Default service
        ...
    }

`TransportMode` and `Service` are filled in. `Auth` is not, and is only
assigned when the query carries a non-empty value, so it reaches the connect
path as the empty string that panics.

dbmeta measured all four `transport` forms against Apache Hive 4.2.1. Absent
connects, `binary` connects, `http` fails cleanly with an EOF against a
binary-mode server rather than panicking, and an unknown value panics. So the
way to never emit a wrong `transport` is to not emit one, which is what this
library does.

The general test, for the next option that looks like this one: ask what the
driver does with nothing, not only what it does with something wrong.

### D17. A scheme describes its own database and driver. Amends D10 and D13. Amended by D22, D30, D37 and D46.

Amended by D46: dbmeta's tests also read `GoPackage`, to choose the driver
they import.

Amended by D37: `Override` is gone. Every scheme except `file` documents its
own `GoPackage` and `DriverURL`, and `Scheme.Driver` is now `Scheme.Name`.

Amended by D30: a wire compatible product with a `Dialect` of its own, such
as `cockroachdb` and `cratedb`, has no `Override`. Its generator returns the
Go driver name, and it documents its own `GoPackage` and `DriverURL`.

D22 changes how `TestSchemeMetadata` treats a scheme whose `Override` names
a driver that no scheme documents. The rest of this entry stands.

`usql` generated the driver table in both projects' READMEs, from metadata it
parsed out of doc comments in its own driver packages. So this registry, which
is the authority on what schemes exist, could not describe them, and any other
consumer had to read `usql`'s source to learn the same facts.

`Scheme` gains five fields: `Desc`, `Home`, `DriverURL`, `GoPackage` and
`RequiresCGO`.

Three fields were proposed first. Five is the number that actually cuts the
cycle, and all three reviewers reached it separately. The table has four
columns. This registry already owned the scheme and alias columns. `GoPackage`
with `RequiresCGO` gives the driver column, and `Desc` gives the first. With
three, `usql` would still have had to write this README.

`usql` keeps `Tag`, `Group` and `Build`, which are its build system and mean
nothing here. It stops parsing `Pkg`, `URL`, `Desc` and `CGO`, and stops
deriving `Driver`, `Aliases` and `Wire`, which were already available from
here.

WHAT WAS REJECTED

An `Info` struct held as `*Info`, with nil meaning wire compatible. `usql`
supplied the counterexample: `file` is a registered scheme with a blank
`Override` and no driver at all, so nil would have meant wire compatible for
`cockroachdb`, pseudo scheme for `file`, and not yet filled in for anything
new. `Override` already states wire compatibility and a reader can test it.

`[]Info` and `GoPackage []string`, for a scheme served by several drivers.
Both outside models pushed for this. It matches nothing here: `oracle` and
`godror` are separate schemes, and so are `postgres` and `pgx`, and `sqlite3`
and `moderncsqlite`. Two drivers for one protocol is already modelled as two
schemes plus `Override`, and a second mechanism for it would be a spare part.

The fields are flat rather than nested because the registry is now written
with named fields. `Scheme` has six string fields, and positional literals
with six same-typed slots make a transposition a silent bug rather than a
compile error. Converting the 51 entries was the change that made the rest
safe. The conversion was proved behaviour preserving by dumping every
scheme's six original fields and a live DSN, before and after, and diffing.

THE TWO FIELDS THAT ARE NOT FREE

`GoPackage` buys a release lockstep. Go encodes the major version in the
import path, and eleven of `usql`'s forty four carry one, so a driver major
version bump needs a release here before `usql` can regenerate. The hive swap
in v0.28.0 is the worked example. It is taken deliberately, because it is the
field that lets other consumers stop reading `usql`'s source, and because the
coupling it replaces was worse: `usql` wrote this README through a filesystem
path that read the working tree rather than a tagged version.

`Home` is the only field that is not a relocation. Neither project records
that PostgreSQL lives at postgresql.org, so it is about fifty facts somebody
had to author.

They were filled in by asking gemini and deepseek the same list separately and
comparing. Thirty nine matched once a trailing slash and a locale segment were
normalised away. Eight differed and were resolved toward the canonical project
page rather than a product sub page, and `chai` and `odbc` were taken from the
one model that answered them. Three are still blank, `adodb`, `oleodbc` and
`ql`, because neither model was confident and none of the three is a database
with a home page to point at.

The values were not machine verified. Several of the hosts, including
`hive.apache.org` and `cassandra.apache.org`, do not answer from the network
this was written on, and others return 403 to a script while serving a browser,
so a reachability check here measures local network policy rather than the
URLs. They are model recall, cross checked between two models, and they want a
human pass before anyone treats them as authoritative.

`DriverURL` has neither problem. None of the forty four driver URLs carries a
version, and for every versioned driver the two diverge correctly:
`github.com/sijms/go-ora` against `github.com/sijms/go-ora/v3`.

`TestSchemeMetadata` enforces it: every scheme except `file` has a `Desc`,
every scheme with a blank `Override` has a `GoPackage` and a `DriverURL`, and
every scheme with an `Override` has neither.

### D18. A scheme records how the database is deployed. Decided.

`Scheme` gains `Deployment`, a bitmask of `DeploymentEmbedded`,
`DeploymentServer` and `DeploymentHosted`. A database can hold more than one:
CockroachDB is a server anyone can run and also a service, so it is
`DeploymentServer|DeploymentHosted`.

It exists for one thing. `gen.go` marks a row with a footnote when a database
has no server to start, so a reader scanning fifty rows can see that
`bigquery`, `athena`, `redshift`, `snowflake` and `spanner` are not things
they can run. The marker is only added when `DeploymentHosted` is set and
`DeploymentServer` is not, because a database that can be run locally has
something to start even when a hosted product also exists.

THE TEST THAT DECIDED THE SCOPE

dbmeta proposed it and it is the reason this field is narrow: a field earns its
place if it would still be true if every container registry vanished.

BigQuery has no edition anyone can install and Athena is a query layer over
S3. Those are facts about what the products are, and they will read the same
in five years. dbmeta's own two blocked databases fail the same test. Vertica
is unavailable because Rocket Software took it over this year, and Exasol
fails on overlayfs, which is a gap in tooling. Either would have been wrong a
week later while looking authoritative.

So emulator availability is out. An emulator is software a vendor ships and
can stop shipping, which is the Vertica failure mode exactly. `DeploymentServer`
means an edition exists that anyone can run, not that an image exists today.

WHO ASKED FOR IT, AND WHO CANNOT USE IT

The question started from dbmeta, which runs a container per database. dbmeta
cannot import `dburl` and never will, by its own hard rule, so it will never
read this field and keeps its own `Info.Embedded` instead. That left `usql` as
the only consumer, and it was asked directly.

`usql` said yes for exactly one footnote, and said the justification was thin
on its own. What changed the arithmetic was that D17's five fields were
landing anyway, so its generator is already reading a metadata block and a
sixth field costs nothing incremental. This repository generates its own table
from the same registry, so there are two readers rather than one.

Gemini argued against the field outright: deployment describes a vendor's
commercial arrangement rather than the database. That is true of the
mechanism, and the bitmask is the answer to it rather than a reason to drop
the field, because CockroachDB being both is accurate rather than a fudge.

The known weakness is a product moving to hosted only, which does happen and
would go stale silently. It is the same weakness as `RequiresCGO`, which is
version scoped while a scheme row has no version, and it is accepted on the
same terms: low rate of change, and the field earns its place otherwise.

`TestSchemeMetadata` requires a non-zero `Deployment` on every scheme.

### D19. A scheme names the dialect of its product. Amended by D22, D30 and D37.

Amended by D37: a product that speaks another's wire protocol has a
`Dialect` of its own. `memsql`, `tidb`, `vitess` and `redshift` are now
their own dialects.

Amended by D30: a wire compatible scheme takes the `Dialect` of what it
speaks only when it has an `Override`. `cockroachdb` and `cratedb` speak
PostgreSQL, and each has a `Dialect` of its own.

D22 changes how `TestSchemeMetadata` treats a scheme whose `Override` names
a driver that no scheme documents. The rest of this entry stands.

`Scheme` gains `Dialect`, the `Driver` of the scheme that is canonical for
the database product. A canonical scheme names itself.

A product reached by more than one Go driver has a scheme per driver, because
each `Driver` is a name a caller passes to `sql.Open` and neither should
override the other. Four pairs exist: `pgx` with `postgres`, `moderncsqlite`
with `sqlite3`, `godror` with `oracle`, and `mymysql` with `mysql`. A consumer
that maps `URL.Driver` to a product finds nothing for the first of each pair,
because `pgx`, `moderncsqlite`, `godror` and `mymysql` are driver names and
not product names.

This is not the wire compatible relation, which `Override` already carries.
`cockroachdb` and `redshift` are different products that speak PostgreSQL, and
`Override` makes `URL.Driver` say so. `pgx` and `postgres` are the same
product reached two ways. A wire compatible scheme takes the `Dialect` of what
it speaks, so its `Dialect` and its `Override` agree.

WHY IT IS NOT DERIVED

`Desc` almost carries it and cannot be keyed on. Matching by prefix finds
`pgx` from "PostgreSQL PGX" and `mymysql` from "MySQL MyMySQL", and misses
`moderncsqlite` and `godror`, whose descriptions are "ModernC SQLite3" and
"GO DRiver for ORacle" and contain neither product name.

`Home` does group all four pairs correctly today, which is how the fourth pair
was found. It is not a sound key: it is blank on three schemes, it is model
recall rather than measured, and two unrelated products can share a vendor
page.

EVERY SCHEME CARRIES ONE

A blank `Dialect` meaning "this scheme is its own dialect" would overload
absence with a meaning, which is what D17 rejected when it killed `*Info` with
nil meaning wire compatible. So a canonical scheme names itself, a consumer
reads the field without a conditional, and a test can require it.

`file` is the exception and has none, as it has no `Desc` either. It resolves
a path on disk and has no product behind it.

`TestSchemeMetadata` requires a non-empty `Dialect`, that it names a
registered scheme, that the named scheme is canonical, and that a wire
compatible scheme agrees with its `Override`.

WHO ASKED

dbmeta, which cannot import `dburl` and will not read the field at runtime.
Its hard rule 1 forbids the dependency and its D19 removed it after it had
been decided once. It reads this registry at authoring time instead, which its
D80 records. The runtime readers are `usql` and `dbtpl`.

`mymysql` is the pair dbmeta did not name, because it has no model that passes
on that driver. That is a fact about dbmeta's coverage and not about the
product, so the pair is recorded here regardless.

### D20. The schemes of four removed drivers leave in one release. Decided.

`usql` dropped four drivers, and D12 removes their five schemes here. Ken decided
this on 2026-09-27. The schemes and the aliases that go are:

- `mymysql`, with `zm` and `mymy`. `go-sql-driver/mysql` serves the same
  databases through `mysql`.
- `adodb`, with `ad` and `ado`, and `oleodbc`, with `oo` and `ole`. `oleodbc`
  was an `Override` onto `adodb`, so it could not stay without it. `odbc`
  serves both.
- `tds`, with `ax`, `ase` and `sapase`. This was SAP ASE.
- `ignite`, with `ig` and `gridgain`.

This log does not record why `usql` dropped `tds` and `ignite`. That reason
is in `usql`.

Every removed name is a published alias, so a URL that worked in v0.30.0
returns `ErrUnknownDatabaseScheme` in v0.31.0. All five go out together, in
one release with a note, so that callers take the break once. D14 set the same rule for aliases: a name that cannot work is removed,
and it is not left failing.

`GenAdodb`, `GenOleodbc`, `GenIgnite` and `GenMymysql` are removed, together
with `convertOptions`, which only `GenMymysql` called. `URL.Short` compared
the scheme to `oleodbc`. That branch could no longer run, so it is removed.

`TestBadParse` has one case for each removed name. Each case expects
`ErrUnknownDatabaseScheme`, so a scheme that comes back must come back on
purpose. D12 says that a removal can be undone, and the same way applies.

D19 counted four pairs of schemes for one product. With `mymysql` gone there
are three. D19 is not amended, because its rule does not change.

### D21. The maxcompute endpoint protocol comes from the transport. Amended by D51.

Amended by D51: the protocol comes from the `tls` option, and `mc+http` and
`mc+https` are removed.

`usql` changed the `maxcompute` driver from `sqlflow.org/gomaxcompute` to
`github.com/aliyun/aliyun-odps-go-sdk/sqldriver`, at v0.4.26. The DSN changed
with it.

The old driver took `access_id:access_key@host/api` and had no scheme. That
is why the generator was `GenFromURL("truncate://localhost/")`, which built a
URL and then cut the scheme off. `sqldriver.ParseDSN` takes a full URL, and
the scheme of that URL becomes the scheme of the endpoint. A DSN with no
scheme gives an endpoint the driver cannot reach.

`GenMaxCompute` follows `ots`, which is the other Alibaba scheme here, and
`clickhouse`. `mc://` emits `https://`. `mc+https://` and `mc+http://` choose
the protocol. Any other transport returns `ErrInvalidTransportProtocol`,
including an explicit `mc+tcp`, which `ots` also rejects.

The default of `https` is not a driver option in the sense of D7. It is a part
of the address, as the host and the port are, and D7 lets this library supply
those. The public MaxCompute endpoints are `https`.

`project` gets no default, although the driver requires it. D16 gives a
default only to a required option that has a safe value. No project name is
safe, because any default names a project that belongs to someone else, or
that does not exist. D16 also asks what the driver does with nothing. Here it
returns `project name is not set`, which is a clean error, and it does not
panic. So the caller supplies `project`, and a URL without it fails in the
driver, under D5.

The old options `curr_project` and `scheme` are not translated. `sqldriver`
sends any query option it does not know to the server as an SQL hint, so these
two now reach the server. This library does not rewrite options that were
written for a driver `usql` no longer carries. The release note tells callers
to change them.

`Driver` stays `maxcompute`, and the alias stays `mc`. The package registers
itself with `database/sql` as `odps`, so `usql` also registers it as
`maxcompute`. That was `usql`'s proposal, and it keeps every existing URL
reaching the same scheme.

The emitted DSNs were run through `sqldriver.ParseDSN` at v0.4.26 in a scratch
module outside this repository, because D2 forbids the import here. The
endpoint, the project, the tunnel endpoint and the hints all came out as
intended. `TestParse` covers the default, both transports and the alias.
`TestBadParse` covers `+tcp` and `+unix`.

### D22. postgres opens pgx, and pq opens lib/pq. Amends D17 and D19. Amended by D30 and D37.

Amended by D37: `postgres` and `pq` no longer use `Override`. `postgres://`
gives `URL.SchemeName` `postgres` and `URL.Driver` `pgx`, and `pq://` gives
`pq` and `postgres`. `passfile` matches the dialect family of the scheme
only.

Amended by D30: `cockroachdb` has no `Override` now. It returns the Driver
`cockroachdb`, the GoDriver `pgx` and the Dialect `cockroachdb`. `redshift`
is unchanged. The rest of this entry stands.

Ken decided on 2026-09-27 that `github.com/jackc/pgx/v5/stdlib` is the
primary PostgreSQL driver, and that `github.com/lib/pq`, which is still
maintained, stays supported as a second driver. It amends D17 and D19.

THE NAMES

`usql` and every other caller open a connection with
`sql.Open(u.Driver, u.DSN)`. lib/pq registers itself as `postgres`, always,
in `init`, at `conn.go:65` in v1.12.3. pgx registers `pgx` and `pgx/v5`, in
`stdlib/sql.go` in v5.11.0. The two sets do not overlap, so both drivers can
be linked into one program. What must change is which scheme returns which
name:

| Scheme | Aliases | `Override` | `u.Driver` | Opens |
| --- | --- | --- | --- | --- |
| `postgres` | `pg`, `pgsql`, `postgresql` | `pgx` | `pgx` | pgx |
| `cockroachdb`, `redshift` | unchanged | `pgx` | `pgx` | pgx |
| `pgx` | `px` | | `pgx` | pgx |
| `pq` | `libpq` | `postgres` | `postgres` | lib/pq |

This is what `Override` is for: a scheme registers the name a user types, and
the driver it returns is a different one. No driver is registered under a
name it did not choose, so nothing is registered twice. `usql` keeps its
lib/pq code under `postgres` and its pgx code under `pgx`, which is where
both already are. A plan to register pgx as `postgres` was dropped, because
it panics in `database/sql` in any build that also links lib/pq.

`cockroachdb` and `redshift` had `Override: "postgres"`. Left alone, they
would have moved to lib/pq without anyone choosing it.

Both drivers were linked into one scratch program, outside this repository
because of D2. Every scheme in the table was opened with `dburl.Open`, and
each one reached the driver in its last column.

THE AMENDMENTS

The name `postgres` now means two things. As a scheme it means pgx. As a
driver it means lib/pq. D17 and D19 assumed that the scheme registered under
an `Override` name is the scheme that opens that name. `pq` breaks that
assumption, so two rules in `TestSchemeMetadata` change.

D17 said a scheme with an `Override` has no `GoPackage` and no `DriverURL`,
because they belong to the scheme it points at. That still holds when the
target opens its own name. When the target overrides its own name, as
`postgres` does, no scheme documents the driver that registered that name.
The overriding scheme then documents the driver itself. `pq` carries
`github.com/lib/pq`.

D19 said a wire compatible scheme's `Dialect` equals its `Override`. It now
equals the `Dialect` of the scheme it overrides. For every scheme that
existed before, the two rules give the same answer. For `postgres`, the
`Override` is `pgx`, and the `Dialect` of `pgx` is `postgres`. So every
PostgreSQL scheme keeps `Dialect: "postgres"`, and a consumer that matches on
it sees no change.

`gen.go` follows the same rule. A scheme with its own `GoPackage` documents
itself, and only a scheme that borrows its driver is marked wire compatible.
The `postgres` row carries that mark, because it reaches pgx through
`Override`. `gen.go` also stops repeating a scheme's own name in its alias
column. Before `pq`, only a two letter scheme with no `Override` could
repeat itself, and `SchemeDriverAndAliases` already removed that one.

WHO THIS BREAKS

A program that parses `postgres://` and opens `u.Driver` now needs pgx
linked. Without it, it gets `sql: unknown driver "pgx"`. A program that wants
lib/pq writes `pq://`. That is a break, and it needs a release note.

PASSWORD FILES

`passfile.Match`, which `usql` uses for `~/.usqlpass`, matched an entry when
its protocol was a name of `u.Driver`. Once `postgres://` returned `pgx`, a
`postgres:` or `pg:` entry stopped matching `postgres://` URLs, and matched
`pq://` URLs instead, because `pq` returns `postgres`. Every existing
PostgreSQL entry in a `usql` password file would have gone to the wrong
driver's URLs.

`Match` now accepts any name of any scheme that shares the dialect of the
scheme the user typed, and still accepts every name of `u.Driver`, as before.
So a `postgres:` entry matches `postgres://`, `pgx://`, `pq://`,
`cockroachdb://` and `redshift://` URLs. Every match that worked before still
works, including for a scheme that a caller registers with no `Dialect`.

Two narrower rules were rejected. Matching only the names of the typed scheme
restores `postgres://`, but `cockroachdb://` and `redshift://` then lose the
`postgres:` entries they have always matched, because they returned
`postgres`. Matching the typed scheme and `u.Driver` together has the same
loss. The cost of the dialect rule is that it is broader: a `cockroachdb:`
entry can now supply a password to a `postgres://` URL. The host and the port
must still match. Ken chose the dialect rule.

`Register` now keeps `Dialect` in the registry, and `DialectProtocols`
returns the family. `TestDialectProtocols` covers it. `TestMatch` in
`passfile` covers each scheme above, and six of its cases fail against the
old rule.

EACH DRIVER GETS ITS OWN FORM

`postgres` now uses `GenPgx`, which emits a URL, as
`postgres://user:pass@host:5432/db?opt=v`. pgx reads a URL natively.
`pq` and `nzgo` keep `GenPostgres`, which emits the keyword/value form, as
`host=h dbname=d user=u`, which is the form lib/pq and nzgo have always
been given. Ken chose this on 2026-09-27. Each driver is given the form it
documents first.

`GenPgx` computes the host, the port and the database exactly as
`GenPostgres` does, including the unix socket directory and its `:port`
suffix, and records them for `Normalize`. It supplies no default host and no
default port, as `GenPostgres` did not. A driver given no host reads
`PGHOST` and then its socket default, and a default here would override the
environment.

pgx parses a URL with the rules of libpq and not with those of `net/url`.
Three of them shape `GenPgx`:

- A raw space is rejected, and a `+` is read as itself and not as a space.
  `url.Values.Encode` writes a space as `+`, so it cannot be used. Every
  component is escaped with `url.QueryEscape`, and each `+` it writes is
  replaced with `%20`. A literal `+` is already `%2B` at that point.
- A unix socket directory cannot be the URL host, so it goes in the `host`
  query option, and its port in `port`, as
  `postgres:///db?host=%2Fvar%2Frun%2Fpostgresql&port=6666`.
- An IPv6 host is written in brackets.

The output was run through `pgconn.ParseConfig` at v5.11.0. A user with a
space, a password holding `/`, `@`, `:`, a space, a quote, a backslash, `+`
and `%`, a database name holding `?`, an option value holding a space and a
`+`, an IPv6 host and `sslmode=verify-ca` all came back exactly.

Options that only lib/pq understood, such as `binary_parameters`, reach the
server as runtime parameters under pgx, and the server rejects them. Under D5
this library passes them through. A caller who needs one writes `pq://`.

`GenFromURL` had the same `+` defect for pgx. It builds the query with
`url.Values.Encode`, and the `pgx`, `cockroachdb` and `redshift` schemes used
it, so `application_name=my app` reached pgx as `my+app`. That was confirmed
against pgx. `GenFromURL` is not changed, because it serves other schemes
whose drivers read a `+` as a space. The three schemes move off it instead.

`pgx` now uses `GenPgx`, the same generator as `postgres`, because the two
open the same driver. It no longer supplies `localhost:5432`, so `pgx://`
emits `postgres://`, and the driver reads `PGHOST`. It also gains the unix
socket handling that `GenFromURL` never had.

`cockroachdb` and `redshift` use `GenPgxFromURL`, which takes its defaults
from a template URL, as `GenFromURL` does. Their templates are unchanged:
`localhost:26257` with `sslmode=disable`, and `localhost:5439`. The parsed URL
overrides each default it carries, so a caller's `sslmode` replaces the
CockroachDB default. The one visible change beside the escaping is that an
empty database no longer leaves a trailing `/`. pgx reads both forms the
same.

Before this change, no test covered `cockroachdb` at all. `TestParse` now
covers the defaults, an override and an option holding a space for all three
schemes, and the output was run through `pgconn.ParseConfig`.

THE QUOTING DEFECT

`GenPostgres` wrote every value as it was. In the keyword/value form, a space
ends a value, and a backslash escapes the next character. So the password
`p ss` gave `password=p ss`, which lib/pq and pgx both reject with
`missing "=" after "ss"`. The password `a\b` was worse. It parsed without an
error as `ab`, and the server refused the login with nothing pointing here.

`quotePostgres` now wraps a value in single quotes when it holds a quote, a
backslash or a space, and escapes the quotes and backslashes inside it. A
value that needs none of this is written as before. An empty value stays
empty, so `genOptions` still skips it.

A space means any Unicode space and not only an ASCII one, because nzgo ends
an unquoted value at any rune that `unicode.IsSpace` accepts. A no-break space
would split the value.

D6 applies, because `GenPostgres` serves lib/pq and nzgo. nzgo's `parseOpts`
was read, and it accepts the same quoting and escaping as lib/pq. The quoted
output was run through lib/pq v1.12.3 and nzgo v12.0.13 for a space, a quote,
a backslash, a no-break space and all of them at once, in the user, the
password, the database name and an option. nzgo read every value back
exactly, and lib/pq accepted every string.

THE TESTS

`TestParse` covers `GenPgx` for each case above, and `GenPostgres` through
`pq`, `libpq` and `nzgo`. Every PostgreSQL case that returned `postgres` now
returns `pgx`, and the `postgres` cases now expect a URL.

`testParse` skipped a unix socket case whose directory did not exist, and it
checked with `os.Stat` and not the `Stat` that the tests stub. The stubbed
`/var/run/postgresql` never exists on a real machine, so every socket case was
skipped, and a wrong DSN passed. It now checks with `Stat`, and no socket case
is skipped.

### D23. cql opens xo/cql, and gets a URL. Amended by D56.

Amended by D56: the scheme, the driver and the package are `cassandra`.

Ken is rewriting the Cassandra driver as `github.com/xo/cql`, in place of
`github.com/MichaelS11/go-cql-driver`. The `cql` scheme follows it. It still
registers as `cql`, so `Driver` does not change, and only `GoPackage` and
`DriverURL` do. The plan is W14 in that repository, and the DSN is its D24.

The new driver still reads the old DSN, `host:port?keyspace=ks&username=u`.
It also reads a URL that starts with `cql://` or `cassandra://`, and it
parses that URL with `net/url`. `GenCassandra` now emits the URL, as
`cql://user:pass@host:9042/keyspace?opt=v`:

- The scheme is always `cql`, whichever alias was parsed, so the driver
  never repeats the alias list.
- The user information and the path pass through. The path is the keyspace.
- The query passes through as it was written, and it is not rebuilt. The
  driver takes a `host` key that can repeat, to add more hosts. `GenFromURL`
  joins a repeated key into one value with spaces, so it is not used.
- An IPv6 host is written in brackets.
- The default stays `localhost:9042`, under rule 7. The `cql` session was
  asked and agreed: the driver falls back to `127.0.0.1` only when a DSN
  names no host, and gocql's own default port is also 9042.

The driver refuses an unknown key, a key given twice, a keyspace in both the
path and the query, and credentials in both the user information and the
query. Under D5 this library passes each of them through and lets the driver
reject it. The new driver reads `timeout` as a duration, such as `10s`, so an
old URL that wrote a number of milliseconds now fails in the driver.

RULE 3 WITHOUT A PINNED VERSION

`usql` does not pin `github.com/xo/cql` yet, because it has no tag. Its first
tag is `v0.1.0`, under its D19. The generator was written against `dsn.go` in
the working tree of the rewrite, which the `cql` session says is settled and
which Ken is reviewing. The output was run through that `ParseDSN`: the
hosts, including an IPv6 host and two more from repeated `host` keys, the
keyspace, the consistency, the timeouts and a password holding `@` and a
space all came back exactly.

Ken chose to commit this before the driver is tagged. Until the tag exists,
`main` names a driver that cannot be installed at a version, so a release of
this library that carries D23 waits for that tag. When the tag exists, the
same check is to be run against it before this library is tagged.

`github.com/xo/cql` `v0.1.0` was tagged on 2026-09-27, at commit `3416515`.
The check was run again against that tag, fetched from `proxy.golang.org`,
and every result matched the check against the working tree. The condition
above is met.

### D24. A parsed URL carries its Dialect. Amended by D30 and D37.

Amended by D37: `URL.UnaliasedDriver` is gone. `URL.SchemeName` names the
scheme, and `URL.Driver` is always the name for `sql.Open`.

Amended by D30: `cockroachdb://` now returns the Driver `cockroachdb` and the
Dialect `cockroachdb`, not `pgx` and `postgres`. The rest of this entry
stands.

Ken decided on 2026-09-27 that `Parse` sets `URL.Dialect` to the `Dialect` of
the scheme it parsed. It ships in v0.32.0.

D19 put `Dialect` on `Scheme`, and a consumer holding a `*URL` could not reach
it. dburl exported no lookup by scheme name, so a consumer had to find the
`Scheme` in `BaseSchemes()` whose `Driver` was `u.UnaliasedDriver`, and that
missed a scheme registered at run time.

The gap became urgent with D22. dbmeta mapped `URL.Driver` to its own
dialect, and after D22 `postgres://`, `cockroachdb://` and `redshift://` all
return `pgx`, and `pq://` returns `postgres`. So `Driver` names the Go driver
and not the product, and mapping it gives the wrong answer for PostgreSQL.
`Dialect` is `postgres` for all of them.

dbmeta asked first for a new field that names the product. `Scheme.Dialect`
already was that field, and dbmeta withdrew the request.

An exported lookup, `SchemeDialect(name)`, was rejected. It is smaller, but
every caller must pass `UnaliasedDriver`, and passing `Driver` is the obvious
mistake. A field on the URL has no wrong argument.

A `file:` URL resolves to the scheme of the file and parses again, so it
takes that scheme's `Dialect`. `Register` keeps `Dialect` in the registry
since D22, so a scheme registered at run time carries it too.
`TestParseDialect` covers each PostgreSQL scheme, the MySQL, SQLite and Oracle
pairs, `nzgo`, and two `file:` URLs.

### D25. couchbase opens xo/dbimp/couchbase. Amended by D34.

Amended by D34: `GenCouchbase` adds no port now, because the driver
defaults to 8093, or 18093 with `tls`, by the same rule.

The Couchbase scheme moves from `github.com/couchbase/go_n1ql` to
`github.com/xo/dbimp/couchbase`, at `github.com/xo/dbimp` `v0.1.0`. The dbimp
session asked for it, under dbimp D30 and D35, and Ken approved it on
2026-09-27.

The new driver registers only the name `couchbase`, so `Driver` becomes
`couchbase`. `n1ql` stays as an alias, and so does `n1`. `n1` was the two
letter alias taken from `n1ql`, and the rename would have replaced it with
`co`. `n1` is published, so it is listed by name, under D14. A listed two
letter alias turns off the automatic one, so there is no `co`.

`Dialect` becomes `couchbase`, and Ken confirmed it. D19 requires a
`Dialect` to name the `Driver` of a registered scheme, and after the rename
no scheme has the `Driver` `n1ql`. `usql`, dbtpl and dbmeta change any match
on `n1ql` in the same release. `passfile` entries written as `n1ql:` keep
matching, because `n1ql` is still a name in the dialect family.

`GenCouchbase` emits `couchbase://user:pass@host:port/?key=value`. The driver
refuses every other scheme, so the scheme is always `couchbase`, whichever
alias was parsed. The user information and the query pass through as they
were written, and the driver refuses an unknown or a repeated key. An empty
path becomes `/`, and any other path passes through for the driver to refuse.

THE DEFAULT PORT FOLLOWS TLS

Rule 7 supplies a default port, and Ken confirmed that it applies here. The
driver serves the query service on 8093, or on 18093 when `tls` is true. So
the default is 8093, or 18093 when `tls` reads as true. The first draft left
the port out for the driver to choose, and Ken rejected it: a database DSN
carries its default port.

The exception in D22 stays, and Ken confirmed it. `GenPostgres` and `GenPgx`
supply no default host and no default port, so that lib/pq, pgx and nzgo read
`PGHOST` and `PGPORT`. No such variable applies to Couchbase.

`tls` is read with `strconv.ParseBool`, as the driver reads it, so the two
agree on `1`, `t` and `TRUE` as well as `true`. A value that `ParseBool`
rejects gives 8093, and the driver then rejects the value itself, under D5.
An explicit port always wins.

The output was run through `couchbase.ParseDSN` at `v0.1.0`, fetched from
`proxy.golang.org`. The default ports, both TLS spellings, an explicit port,
a password holding `@` and a space, every key the driver takes and an IPv6
host all came back as intended. A path and an unknown key failed in the
driver. `TestParse` covers each case.

### D26. surrealdb opens xo/dbimp/surrealdb. Amended by D34.

Amended by D34: `GenSurrealDB` adds no port now, because the driver
defaults to 8000.

Ken decided on 2026-09-27 to add a scheme for SurrealDB, for the driver
`github.com/xo/dbimp/surrealdb`. The driver registers only `surrealdb`, under
dbimp D26 and D30, so `Driver` and `Dialect` are both `surrealdb`. Ken chose
the aliases `sr`, `sur` and `surreal`. `sr` is a listed two letter alias, so
the automatic `su` is not registered.

The URL form is dbimp D48: `surrealdb://user:pass@host:port/namespace/database`.
The driver refuses every other scheme, and a path that is not exactly two
segments. It reads the escaped path, so a name that holds a slash is written
`%2F`. The query takes `tls`, `auth` and `encoding`, under dbimp D48, D49 and
D51, and the driver refuses any other key and a repeated one.

`GenSurrealDB` writes the scheme `surrealdb` whichever alias was parsed, and
passes the user information, the path with its escaping and the query through
as they were written. The default host is `localhost`, and the default port is
8000 under rule 7. TLS does not change the port, because the driver speaks
HTTPS on the same one.

THE VERSION

`usql` has no SurrealDB driver yet. The generator was written against
`ParseDSN` in dbimp commit `bd0f165`, before the driver was tagged, and the
output was run through it from a scratch module outside this repository. The
driver was then tagged in dbimp `v0.2.0`, at commit `82c1902`, which changes
only dbimp's documentation after `bd0f165`. The check was run again against
`v0.2.0` from `proxy.golang.org`, and every result matched.

The default port, both TLS spellings of the port, a namespace holding `%2F`, a
password holding `@` and a space, and every key came back as intended. An
empty path, a path of one segment and an unknown key failed in the driver.

### D27. dburl is set up for coding agents as every xo repository is. Decided.

Ken decided on 2026-09-27 that every repository in the `xo` namespace is set
up for coding agents the same way. The standard is D110 in dbmeta, and dbmeta
sent it to this session. This entry records what dburl adopted.

THE FILES

- `AGENTS.md` holds the rules, because Codex and the other agents read that
  file. It holds what `CLAUDE.md` held. Git records it as a new file, because
  `CLAUDE.md` still exists, so read the history of the rules before this
  entry in `git log -- CLAUDE.md`.
- `CLAUDE.md` holds one line, `@AGENTS.md`, which Claude Code reads as an
  import. It is an ordinary file and not a symbolic link.
- `CONTRIBUTING.md` is new. It has an Agent skills section with the command
  that installs each skill.
- `.gitignore` ignores `.claude/settings.local.json`, the Claude Code
  permissions of one person.
- `.gitattributes` holds `* text=auto eol=lf`.
- `docs/BACKLOG.md` is new, for work that is known and not done.

dburl is a small library, so it keeps its decisions in `docs/PLAN.md`, with
the index at the top, and does not split them into one file each. dbmeta D111
draws that line.

THE STANDING RULES

`AGENTS.md` now opens with three rules that are the same in every `xo`
repository:

1. Stage changes for review. Commit and push only when Ken says so.
2. Load `simple-english` before writing any text that a person reads.
3. Load `go-pedantry` before writing or reviewing Go code. A rule of the
   project wins where the two conflict.

`CLAUDE.md` named neither skill before. The first rule is how Ken already
worked in this session, and an agent new to the repository did not know it.

THE SKILLS

`.claude/skills/go-pedantry` and `.claude/skills/simple-english` were
symbolic links to `.agents/skills`. A Windows checkout writes a link as a
small text file, so Claude Code loaded no skill there and reported nothing.
They are now ordinary folders, identical to the ones in `.agents/skills`.

The copies were already in the working tree before this session made any of
the other changes, and they matched `.agents/skills` byte for byte. So the
`npx skills@1.7.0 add ... --copy` command was not run. `skills-lock.json` did
not change.

`TestSkillsAreCopies` fails on a link, on a missing copy, on a skill that
`skills-lock.json` does not name, and on two copies that differ.
`TestClaudeImportsAgents` fails unless `CLAUDE.md` is an ordinary file that
holds exactly `@AGENTS.md`. Both tests use only the standard library, as D1
requires.

### D28. neo4j opens xo/dbimp/neo4j. Amended by D34.

Amended by D34: `GenNeo4j` adds no port now, because the driver defaults
to 7474, or 7473 with `tls`, by the same rule.

Ken decided on 2026-09-27 to add a scheme for Neo4j, for the driver
`github.com/xo/dbimp/neo4j`. The driver registers only `neo4j`, under dbimp
D26 and D30, so `Driver` and `Dialect` are both `neo4j`, which matches
dbmeta D109.

Ken chose the aliases `nj`, `neo` and `n4j`. He first chose `4j` for the two
letter alias, and it was dropped. A URL scheme must start with a letter, so
`net/url` refuses `4j://` before dburl sees it, and D14 forbids an alias that
cannot work under any input. `nj` is a listed two letter alias, so the
automatic `ne` is not registered.

The URL form is dbimp D60, D61 and D67:
`neo4j://user:pass@host:port/database`. The path names one database, and an
empty path means the database `neo4j`. The query takes `tls` and `cancel`,
and the driver refuses any other key and a repeated one.

`GenNeo4j` writes the scheme `neo4j` whichever alias was parsed, and passes
the user information, the path with its escaping and the query through as
they were written. The default port follows `tls`, as for couchbase in D25:
7474, or 7473 when `tls` reads as true with `strconv.ParseBool`.

The driver speaks the HTTP interface of Neo4j and not Bolt. The tools of
Neo4j write `neo4j://host:7687` for Bolt, and a URL copied from them reaches
this driver on the Bolt port and fails. dburl does not rewrite that port,
under D5.

THE VERSION

The generator was written against `ParseDSN` in dbimp commit `b475894`, and
the output was run through it from a scratch module outside this repository.
The default ports, both TLS spellings, an explicit port, a database name
holding `%2F`, a password holding `@` and a space, and both keys came back as
intended. A path of two segments and an unknown key failed in the driver.

No dbimp tag holds the driver yet. A release of this library that carries D28
waits for one, and the check is to be run again against it, as D23 set for
cql.

dbimp `v0.3.0` was tagged on 2026-09-28, at commit `b475894`, which is the
commit the generator was written against. The check was run again against
`v0.3.0` from `proxy.golang.org`, and every result matched. The condition
above is met.

### D29. influxql is a scheme of its own on the influxdb driver. Amends D7. Amended by D34 and D37.

Amended by D37: `URL.GoDriver` is gone. `influxql://` gives `URL.SchemeName`
`influxql` and `URL.Driver` `influxdb`.

Amended by D34: `GenInfluxDB` and `GenInfluxQL` add no port now, because
the driver defaults to 8086 for `version` 1 and 2, and 8181 otherwise.

Ken decided on 2026-09-28 how dburl holds the design of dbimp D78. dbimp has
one InfluxDB driver, `github.com/xo/dbimp/influxdb`, which registers
`influxdb` and speaks two languages: SQL on InfluxDB 3 and later, and
InfluxQL on InfluxDB 1, 2 and 3. The driver key `sqlmode` selects between
them, and `sqlmode=disable` gives InfluxQL.

dburl gets two schemes, each with its own generator:

| Scheme | Generator | `URL.Driver` | `URL.GoDriver` | `URL.Dialect` | Sets |
| --- | --- | --- | --- | --- | --- |
| `influxdb` | `GenInfluxDB` | `influxdb` | | `influxdb` | nothing |
| `influxql` | `GenInfluxQL` | `influxql` | `influxdb` | `influxql` | `sqlmode=disable` |

`GenInfluxQL` returns a DSN that starts with `influxdb://`, because the
driver refuses every scheme but its own name (dbimp D35), and it returns
`influxdb` as the name of the Go driver. `Open` and `usql` pass `GoDriver`
to `sql.Open` when it is set, so the one driver serves both schemes. This is
how `cosmos` returns `gocosmos` and how `azuresql` reaches `sqlserver`.

`influxql` is an ordinary scheme, with no `Override`, and it is canonical for
its own dialect. So neither D19 nor D22 changes, and `TestSchemeMetadata`
needs no change. Like any scheme with no `Override`, it carries its own
`GoPackage` and `DriverURL`, which name the same driver as `influxdb`.

Two designs were rejected. dbimp asked first for `influxql` as an alias of
`influxdb` that sets `sqlmode=disable`. An alias is only another name for its
scheme, and a scheme has one `Dialect`, so an alias could not tell a consumer
such as dbmeta that the URL is InfluxQL. A scheme that reaches the driver
through `Override` was then proposed, and it needed an amendment to D19 and
D22. The Go driver name that a generator returns needs none.

THE MODE

`GenInfluxQL` sets `sqlmode=disable`, which is an option of the driver, so it
is an exception to rule 7. It selects the query language of the connection,
as `clickhouse+http` selects the protocol, and it does not tune the
connection. It is the whole meaning of the scheme. A URL that names
`sqlmode` itself keeps its own value, under D5, and the driver rejects what
it rejects.

THE DRIVER'S OTHER DEFAULTS

dbimp D78 also asked the generators to fill in `sqlmode=prefer` and
`version=3` when a URL does not name them. Ken decided against it on
2026-09-28. `GenInfluxDB` adds neither, and `GenInfluxQL` adds only
`sqlmode=disable`. Neither generator adds `version`. Both values are the
defaults of the driver already, and rule 7 says that the client owns the
options, so a default here would only fix today's driver default in place.

THE PROVISIONAL PARTS

Ken asked for both generators before the driver exists, to see them work,
and approved them on 2026-09-28. dbimp has no InfluxDB driver and no
`ParseDSN`, so rule 3 had no evidence, and three parts rest on facts from
dbimp `docs/INFLUXDB.md` and on Ken's approval:

- The default port follows the `version` key: 8086 for `version=1` and
  `version=2`, which is the port of InfluxDB 1 and 2, and 8181 otherwise,
  which is the port of InfluxDB 3.
- The aliases are `in` and `influx` on `influxdb`, and `iq` on `influxql`.
  Both schemes start with `in`, so one of them had to list its two letter
  alias, or the two automatic aliases would collide.
- The path and the user information pass through. dbimp has not decided
  whether the URL names the database in the path or in a key. A token goes
  in the password, which dbimp's facts say the server reads with any user
  name.

A release of this library that carries D29 waits for a dbimp tag with the
driver. The output is then run through its `ParseDSN`, and each generator
changes where the two disagree, before this library is tagged.

dbimp `v0.4.0` was released on 2026-09-29 with the driver. Its `ParseDSN`,
under dbimp D82, matches all three provisional parts. The scheme is only
`influxdb`, the path names at most one database, the default port is 8086 for
`version` 1 and 2 and 8181 otherwise, and a token is the password. The query
takes `sqlmode`, `version`, `describe`, `chunked`, `rp` and `tls`.

One change followed. The driver reads `version` with `strconv.Atoi`, so
`version=01` is 1 and takes port 8086, and the generator compared the text.
The generator now reads it with `strconv.Atoi` too. The output of both
generators was run through `ParseDSN` at `v0.4.0` from `proxy.golang.org`,
and every result matched. The condition above is met.

### D30. cockroachdb and cratedb each have a dialect of their own. Amends D17, D19, D22 and D24. Amended by D34 and D37.

Amended by D37: `cockroachdb://` gives `URL.SchemeName` `cockroachdb` and
`URL.Driver` `pgx`, where it gave `URL.Driver` `cockroachdb` and
`URL.GoDriver` `pgx`.

Amended by D34: `GenCrateDB` adds no port now, because pgx defaults to
5432, which is the port of CrateDB. `GenCockroachDB` keeps 26257.

Ken decided on 2026-09-29 to add a scheme for CrateDB, and to give CrateDB and
CockroachDB each a dialect of its own. dbmeta is writing a model for each at
the same time, and dbmeta chooses its model by `URL.Dialect`, as D24 says.

Both products speak the wire protocol of PostgreSQL, and pgx opens both.
Neither is PostgreSQL, so a `Dialect` of `postgres` would give dbmeta the
wrong model. Under D19 and D22, a scheme with an `Override` takes the
`Dialect` of the scheme it overrides. So neither scheme has an `Override`.
Each generator returns `pgx` as the Go driver instead, as `GenInfluxQL` does
in D29 and `GenCosmos` does for `gocosmos`:

| Scheme | Aliases | `URL.Driver` | `URL.GoDriver` | `URL.Dialect` | Default |
| --- | --- | --- | --- | --- | --- |
| `cockroachdb` | `cr`, `cdb`, `crdb`, `cockroach` | `cockroachdb` | `pgx` | `cockroachdb` | `localhost:26257`, `sslmode=disable` |
| `cratedb` | `ct`, `crate` | `cratedb` | `pgx` | `cratedb` | `localhost:5432` |

`GenCockroachDB` and `GenCrateDB` write the DSN with `GenPgxFromURL`, from the
templates in the last column. Each scheme carries its own `GoPackage` and
`DriverURL`, which name pgx, because it has no `Override`, and the README row
of each has no ‡.

Ken chose `ct` and `crate` for CrateDB. The automatic two letter alias of
`cratedb` is `cr`, which `cockroachdb` holds, so `cratedb` lists `ct`. The
default port 5432 is the port of the PostgreSQL interface of CrateDB, which
is not the port of its HTTP interface, 4200.

WHAT CHANGES FOR COCKROACHDB

Until now `cockroachdb` had `Override: "pgx"` and the `Dialect` `postgres`.
That changes three things that a caller can see:

- `URL.Driver` is `cockroachdb`, not `pgx`. `usql` looks up its driver by
  `URL.Driver`, so it needs an entry named `cockroachdb`.
- `URL.Dialect` is `cockroachdb`, not `postgres`.
- `cockroachdb` leaves the PostgreSQL family of `DialectProtocols`, so a
  `postgres:` entry in a password file no longer matches a `cockroachdb://`
  URL, and a `cockroachdb:` entry no longer matches a `postgres://` URL. D22
  chose the dialect rule for `passfile`, and it now works as written: a
  family is a product.

The DSN of `cockroachdb` does not change, and `dburl.Open` still opens pgx,
through `GoDriver`.

Both DSNs were run through `pgconn.ParseConfig` at pgx v5.11.0. The host, the
port, the database, the user, a password holding `@`, an option holding a
space and the `sslmode` of each came back as intended.

dbimp D76 had given `cratedb` to a future dbimp driver for the HTTP
interface of CrateDB, in `github.com/xo/dbimp/cratedb`. Ken decided on
2026-09-29, in dbimp D88, that dbimp writes no CrateDB driver. `usql` reaches
CrateDB with pgx, as this scheme does, so the scheme stays on pgx.

### D31. A password file entry matches the database and the user. Decided.

The dbmeta session reported on 2026-09-29 that `usql` connected as the user
`postgres` for `postgres://crate@127.0.0.1:5432/doc`, which names the user
`crate`. The same URL written as `crate:@`, with an empty password, connected
as `crate`. The fault was in this library, in two places, and not in `usql`.

`passfile.MatchEntries` reads the password file only when the URL carries no
password, so `crate:@` never read it. For `crate@` it took the first entry
that matched the protocol, the host and the port, and returned the user of
that entry, `postgres`, in place of `crate`. `Entry.Equals` never compared the
database or the user, although the format is
`protocol:host:port:dbname:user:pass`, as in a `.pgpass` file.

`Equals` now compares every field. A field of the entry matches when it is
`*` or equal to the field of the URL. The user has one exception: an entry
matches a URL that names no user, so that the entry can supply the user. An
entry never replaces a user that the URL names with a different one.

The second fault made the first one invisible to a fix. `URL.Normalize`
builds the fields that `MatchEntries` compares. With `cut`, it trims the
empty fields at the end, and it cut one field too many: the last field that
was not empty. So `postgres://crate@host:5432/doc` gave
`postgres:host:5432:doc` with no user, and a URL that named a database and no
user lost the database. `Normalize` now keeps the last field that is not
empty, and it still keeps at least `cut` fields, which is what the old code
gave when every field at the end was empty.

`Normalize` is in `dburl.go` and is exported, so a change there reaches every
caller. `passfile` is the only caller in this repository, and `usql`, dbtpl
and dbmeta do not call it. `TestNormalize` is new, because nothing tested it.
`TestMatchUserAndDatabase` covers the case that dbmeta reported, a URL user
with no entry for it, an entry that supplies the user, an entry for one
database, and an empty password. It failed on the old `Normalize`.

### D32. arangodb is a provisional scheme for the dbimp driver. Amended by D34 and D38.

Amended by D38: a release may carry this scheme before the driver is
tagged. The check against `ParseDSN` still comes when the tag exists.

Amended by D34: `GenArangoDB` adds no port now, because the driver in
dbimp's working tree defaults to 8529.

Ken asked on 2026-09-29 for an ArangoDB scheme for the dbimp driver, before
the driver exists, as he did for InfluxDB in D29. ArangoDB is next in the
order of dbimp D73. dburl had no ArangoDB scheme, and `usql` has no ArangoDB
driver.

The scheme is `arangodb`, with the alias `arango` and the automatic two
letter alias `ar`. `Driver` and `Dialect` are both `arangodb`. `GoPackage` is
`github.com/xo/dbimp/arangodb`.

`GenArangoDB` writes `arangodb://user:pass@host:port/path?query`, whichever
alias was parsed, and passes the user information, the path with its
escaping and the query through as they were written. The default host is
`localhost`, and the default port is 8529 under rule 7.

THE PROVISIONAL PARTS

dbimp has no ArangoDB driver and no `ParseDSN`, so rule 3 had no evidence.
Every part of this scheme rests on the facts in dbimp `docs/ARANGODB.md`, on
the rule of dbimp D26, D28 and D30 that a driver registers the name of the
database as one lower case word, and on Ken's request:

- The name `arangodb`, which dbimp has not confirmed.
- The default port 8529, which is the port of the HTTP interface.
- The path, which passes through. The HTTP interface names the database in
  its path as `/_db/<database>/`, and dbimp step 9 decides how the URL names
  it.
- The alias `arango`, which Ken has not chosen.

A release of this library that carries D32 waits for a dbimp tag with the
driver. The output is then run through its `ParseDSN`, and the generator and
the aliases change where the two disagree, before this library is tagged.

dbimp decided the URL on 2026-09-29, in dbimp D93, which is staged there, and
it matches this scheme. The name is `arangodb`. The DSN is
`arangodb://user:password@host:port/database`, where the path names the
database, and no path means `_system`. The default port is 8529. The query
takes `tls`, `cancel`, `batch` and `auth`, and the driver refuses any other
key, so passing the query through is right. Under dbimp D94, the secret is
always the password of the URL, and `auth=bearer` sends it as a bearer token.
The driver still has no `ParseDSN` at a tag, so the condition above still
holds.

dbimp `v0.5.0` was released on 2026-09-29 with the driver, and Ken approved
it. `GenArangoDB`, as v0.36.0 of this library ships it, was run through
`arangodb.ParseDSN` at `v0.5.0` from `proxy.golang.org`. With no port the
driver used 8529, with no path it used `_system`, and a database holding
`%2F`, `tls`, `cancel`, `batch` and `auth=bearer` all came back as intended.
A path of two segments, an unknown key and `batch=0` failed in the driver.
The generator needs no change, and the scheme is no longer provisional.

### D33. passfile opens the Go driver that dburl.Open opens. Amended by D37.

Amended by D37: `URL.Driver` is now always the name for `sql.Open`, so
`passfile` and `dburl.Open` both call `sql.Open(u.Driver, u.DSN)`.

A documentation sweep on 2026-09-29 found that `passfile.OpenURL`, and so
`passfile.Open`, called `sql.Open(u.Driver, u.DSN)` and ignored
`URL.GoDriver`. `dburl.Open` has used `GoDriver` when it is set since before
this log began. So `passfile.Open` with a `cockroachdb://`, `cratedb://`,
`influxql://`, `cosmos://` or `azuresql://` URL asked `database/sql` for a
driver that nothing registers, and failed with `sql: unknown driver`.

D29 and D30 made the gap wider, because both give a scheme a Go driver whose
name differs from the scheme. `OpenURL` now opens the URL as `dburl.Open`
does. `TestOpenURLUsesGoDriver` registers a stub driver named `pgx` and
opens a `cockroachdb://` URL, and it failed with `unknown driver
"cockroachdb"` on the old code. `Example_parse`, which calls `sql.Open`
itself, now shows how to choose between `GoDriver` and `Driver`.

### D34. A default port is added only where the driver has none. Amends D7, D25, D26, D28, D29, D30 and D32. Amended by D40, D50 and D53.

Amended by D50: `presto` and `trino` no longer add the ports 8080 and 8443,
because they move to a dbimp driver.

Amended by D40: the `nzgo` scheme and its default port 5480 are removed.

Ken decided on 2026-09-29 that a generator adds a default port only when the
driver that reads the DSN has no default port of its own. Whether it has one
is found by reading the driver's source, under rule 3, and not from its
documentation. A driver whose default is the wrong port for the product, as
pgx's 5432 is for CockroachDB, counts as having none. The default host stays
in every generator that had one, because each dbimp driver refuses an empty
host.

Two agents read the parser of every driver that the registry names, at the
version `usql` pins, or at the dbimp tag. Every claim below cites the source,
and a probe that calls only parse functions confirmed each one it could.

THE PORTS REMOVED

The driver defaults to the same port that dburl added, so dburl adds none:

| Scheme | Port | Where the driver sets it |
| --- | --- | --- |
| `mysql`, `memsql`, `vitess` | 3306 | `go-sql-driver/mysql` `ensureHavePort` |
| `cratedb` | 5432 | pgx `pgconn` |
| `h2` | 9092 | `h2go` `driver.go` |
| `hive` | 10000 | `gohive` `dsn.go` |
| `cql` | 9042 | gocql `NewCluster` |
| `couchbase` | 8093, 18093 with `tls` | dbimp `couchbase/dsn.go` |
| `neo4j` | 7474, 7473 with `tls` | dbimp `neo4j/dsn.go` |
| `surrealdb` | 8000 | dbimp `surrealdb/dsn.go` |
| `influxdb`, `influxql` | 8086 for `version` 1 and 2, else 8181 | dbimp `influxdb/dsn.go` |
| `arangodb` | 8529 | dbimp `arangodb/dsn.go`, in the working tree |

Vitess has no single standard port, so it takes the driver's 3306. Without a
port, pgx reads `PGPORT` for `cratedb`, and a `passfile` entry that names
port 5432 no longer matches a `cratedb` URL with no port, so the entry needs
port `*`.

THE PORTS KEPT OR ADDED

The driver has no default, or the wrong one:

- `oracle` 1521, `exasol` 8563 and native `clickhouse` 9000, because the
  driver fails to parse or to dial without a port.
- `avatica` 8765 and `trino` 8080, because net/http would use 80 or 443.
- `vertica` 5433, `ydb` 2136, `databricks` 443 and `voltdb` 21212, because
  the driver needs a port, or, for voltdb, loses its host without one.
- `cockroachdb` 26257 and `redshift` 5439, where pgx would use 5432.
- `tidb` 4000, which is new. `GenTiDB` shares `genMysql` with `GenMysql`.
- `nzgo` 5480, which is new. nzgo defaults to 5432, and Netezza listens on
  5480. `GenNzgo` adds it only when the URL names no port and no socket.
- `hdb` 30015, which is new. go-hdb has no default and fails to dial. HANA's
  port depends on the instance number, and 30015 is the SQL port of instance
  00.
- `clickhouse+http` 8123 and `clickhouse+https` 8443, which are new.
  clickhouse-go has no default for an HTTP DSN, so net/http used 80 or 443.
- `gizmosql` 31337 and `questdb` 8812, which are new schemes (D36).

`flightsql` adds no port, because Flight SQL has no standard port. Every
scheme that added no port before still adds none. For each, the driver has
a default, or the DSN has no port.

WHAT WAITS

Presto, Trino and ClickHouse will each get a dbimp driver. Ken decided to
revisit their ports and the `tls` key then. dbimp's drivers read `tls` with
`strconv.ParseBool`, and none of the three drivers `usql` uses today knows
that key. Until then, `presto` keeps 8080 and 8443 with a TLS option, and
`trino` keeps 8080 and 8443, although the Presto driver itself keeps 8080
over HTTPS.

The dbimp generators now share `genRewrite`, which writes the scheme of the
driver, the default host and a default port that is empty for all of them.
`TestParse` holds the new output of every scheme above. A second run of
`go run gen.go` changed no row that existed before, and the `gen.go` lint
findings are fixed in the same change.

### D35. A spanner URL names its host first. Decided.

Ken decided on 2026-09-29 that a spanner URL is
`spanner://host:port/project/instance/database?name=value`. Before, the host
of the URL was the project, and `GenSpanner` dropped the query, so a URL
could not name the emulator or pass any driver option. dbmeta reached the
emulator only through `SPANNER_EMULATOR_HOST`.

`go-sql-spanner` v1.26.0, which `usql` pins, reads an optional `host:port/`
before `projects/`, and options after `;`. So `GenSpanner` writes
`host:port/projects/project/instances/instance/databases/database`, and each
query option as `;name=value`, in order by name. An empty host, as in
`spanner:///p/i/db`, leaves the endpoint to the driver. `Parse` reads a URL
with no host and a path as a unix socket, so the scheme accepts that
transport, and `GenSpanner` refuses an explicit `spanner+unix` or
`spanner+tcp` URL.

The path must name all three parts, or `GenSpanner` returns
`ErrMissingPath`. This breaks every `spanner://project/instance/database`
URL: it now reads the project as the host and finds two parts in the path, so
it fails. Ken chose the break over reading the path by its length.

The output was run through `ExtractConnectorConfig` at v1.26.0. The host, the
project, the instance, the database and `usePlainText` came back as intended,
with a host and without one.

### D36. gizmosql, questdb, and three provisional schemes. Amended by D38, D41, D43 and D44.

Amended by D44: `rqlite` is checked against dbimp `v0.8.0`, and adds no
port.

Amended by D43: `pinot` is checked against dbimp `v0.7.0`, and adds no port.

Amended by D41: the `tdengine` scheme is removed, because dbimp writes no
TDengine driver.

Amended by D38: a release may carry the provisional schemes before their
drivers are tagged. The aliases are `qs`, `td`, `pi` and `rq`, and Pinot keeps
port 8000.

Ken decided on 2026-09-29 to add these schemes for the containers that
dbmeta lists as Staged:

| Scheme | Aliases | `URL.Driver` | `URL.GoDriver` | `URL.Dialect` | Default port |
| --- | --- | --- | --- | --- | --- |
| `gizmosql` | `gz`, `gizmo` | `gizmosql` | `flightsql` | `gizmosql` | 31337 |
| `questdb` | `qu` | `questdb` | `pgx` | `questdb` | 8812 |
| `tdengine` | `td` | `tdengine` | | `tdengine` | 6041 |
| `pinot` | `pi` | `pinot` | | `pinot` | 8000 |
| `rqlite` | `rq` | `rqlite` | | `rqlite` | 4001 |

GizmoSQL serves Flight SQL, and runs DuckDB behind it. `GenGizmoSQL` writes a
`flightsql://` DSN and returns the Flight SQL driver, as D30 does for pgx, and
the scheme has a `Dialect` of its own. The Flight SQL driver has no default
port, so the scheme adds 31337, which is GizmoSQL's port. The DSN was run
through `NewDriverConfigFromDSN` at arrow-go v18.8.0.

QuestDB speaks the wire protocol of PostgreSQL, as CrateDB does, and dbimp
plans no QuestDB driver. `GenQuestDB` writes the DSN with `GenPgxFromURL` and
returns pgx. pgx would use 5432, so the scheme adds 8812, which is the
PostgreSQL port of QuestDB. dbmeta's container publishes only the HTTP port,
9000, so dbrun does not hand out a URL for this port yet.

`tdengine`, `pinot` and `rqlite` are provisional, as `arangodb` was in D32.
dbimp orders each of them after ArangoDB (dbimp D73), and has decided neither
the name, the URL nor the driver of any of them. The names follow dbimp's
rule, the product as one lower case word. The ports come from dbmeta's
container files: 6041 is the REST port of TDengine, 8000 is the port of the
broker in dbmeta's Pinot container, and 4001 is the HTTP port of rqlite. The
standalone Pinot broker listens on 8099, so the Pinot port is the first thing
to check. Each generator passes the user information, the path and the query
through. A release that carries these waits for a dbimp tag with each driver,
and each is checked against its `ParseDSN` first. Ken has not chosen aliases
for `questdb`, `tdengine`, `pinot` or `rqlite`, so each has only its
automatic two letter alias.

### D37. A scheme names its driver, and Override is gone. Amends D17, D19, D22, D24, D29, D30 and D33.

Ken decided on 2026-09-29 to drop `Scheme.Override`, the construct that made
a scheme "wire compatible" with another. The question went to the `usql`,
dbmeta and dbimp sessions, and to Gemini and DeepSeek. All five recommended
dropping it, in one breaking release.

WHY

`Override` made `URL.Driver` mean two things. For most schemes it named the
product, and for a scheme with an `Override` it named a registered Go
driver. So `tidb://` looked the same as `mysql://` once it was parsed, and a
user of `usql` saw `mysql` or `pgx` where they expected the product.
`Override` also forced the target's `Dialect`, which D29 and D30 worked around
with a Go driver name returned by the generator. dbimp added the case that
`Override` cannot express at all: its Avatica driver will serve Phoenix and
Druid, and its Trino driver will serve Presto, each flavor with a `Dialect`
of its own on one driver (dbimp D98).

THE FIELDS

| Before | After |
| --- | --- |
| `Scheme.Driver` | `Scheme.Name` |
| `Scheme.Override` | gone |
| `URL.Driver`, the scheme or the `Override` | `URL.SchemeName`, always the scheme |
| `URL.UnaliasedDriver` | gone, because it equalled `URL.SchemeName` |
| `URL.GoDriver`, set only when it differed | `URL.Driver`, always the name for `sql.Open` |

`net/url.URL`, which `URL` embeds, already has a `Scheme` field, which
`Parse` sets to the scheme with no transport. So the new field is
`SchemeName`, and it does not shadow `Scheme`. A generator still returns the
name for `sql.Open` as its second value, and an empty value means the scheme's
`Name`. `Parse` always sets `URL.Driver`, so `sql.Open(u.Driver, u.DSN)`
always works, and `Open` and `passfile` call exactly that.

Ken chose one breaking release in v0, with the renames, and not a new `/v2`
module. Gemini proposed `/v2`. DeepSeek and `usql` proposed a v0 release,
because xo projects make no promise of backward compatibility before v1, and
`usql` pins `dburl` and moves with each release. The renames turn most of the
break into compile errors, so a caller cannot keep reading a field whose
meaning changed.

THE SCHEMES THAT MOVED

| Scheme | `URL.SchemeName` | `URL.Driver` | `URL.Dialect` |
| --- | --- | --- | --- |
| `postgres` | `postgres` | `pgx` | `postgres` |
| `pq` | `pq` | `postgres` | `postgres` |
| `redshift` | `redshift` | `pgx` | `redshift` |
| `memsql` | `memsql` | `mysql` | `memsql` |
| `tidb` | `tidb` | `mysql` | `tidb` |
| `vitess` | `vitess` | `mysql` | `vitess` |

Each now documents its own `GoPackage` and `DriverURL`, which name the driver
it opens. `genMysql` names `mysql`, `genPgx` names `pgx`, and the new `GenPq`
names `postgres`.

Ken chose a `Dialect` of its own for `memsql`, `tidb`, `vitess` and `redshift`.
DeepSeek argued for the family until a product model exists, because a
dialect that no model reads is not tested. dbmeta answered that it prefers a
model per product that shares statements, as its CockroachDB model does, to a
fallback, and that it has no model for any of the four yet. So `usql` reads no
metadata for them until dbmeta writes each model.

WHAT CHANGES FOR A CALLER

- A caller that read `URL.GoDriver` or `URL.UnaliasedDriver` does not
  compile, and reads `URL.Driver` or `URL.SchemeName`.
- A caller that read `URL.Driver` to learn the scheme reads `URL.SchemeName`.
  This one does not fail to compile, because `URL.Driver` keeps its name and
  changes its meaning. A new name for the `sql.Open` field, such as
  `SQLDriver`, would have made every old read fail to build. Ken chose to keep
  `Driver`, so each reader of `URL.Driver` must be checked by hand. `usql`
  has 58, and it was told which.
- `usql` keys its registry by the scheme, so it needs an entry under each
  moved name. Its `libpq` driver moves from `postgres` to `pq`, and its pgx
  driver takes `postgres`.
- `passfile.Match` matches the dialect family of the scheme, and no longer
  the names of the driver. So a `mysql:` entry no longer supplies a
  password to `tidb://`, and a `postgres:` entry no longer supplies one to
  `redshift://`, as D30 did for `cockroachdb://`.
- The README marks no row with ‡, because no row borrows another row's
  driver. `gen.go` loses the code that resolved `Override`.

`TestSchemeMetadata` no longer has rules for `Override`, and every scheme but
`file` must document its package. `TestParseDialect` checks `SchemeName`,
`Driver` and `Dialect` for every kind of scheme.

### D38. A missing required field is an error, and provisional schemes can ship. Amends D32 and D36. Amended by D41, D43 and D50.

Amended by D50: `GenPresto` and `GenTrino` no longer make the catalog
`default`, so only `GenHive` fills a missing field.

Amended by D43: Pinot no longer keeps port 8000, because its driver
defaults to 8099.

Amended by D41: the alias `td` goes with the `tdengine` scheme.

Ken decided these on 2026-09-29.

A MISSING REQUIRED FIELD IS AN ERROR

When dburl knows that the driver needs a field that the URL lacks, such as
the host, the port or the path, the generator returns an error that names the
field. It does not fill the field in. Ken asked for the rule to be checked
against the existing schemes before it was written down. It already held in
seven places: `GenOpaque` and `file:` return `ErrMissingPath`, `GenSpanner`
returns `ErrMissingPath`, `GenDatabend` returns `ErrMissingHost`,
`GenCosmos` returns `ErrMissingUser`, and `GenDatabricks` and `GenSnowflake`
return `ErrMissingHost` and `ErrMissingUser`.

A default host or port under rule 7 is not a filled field, because it names a
real server that the caller did not have to write.

The port audit of D34 found three schemes that broke the rule, and two are
fixed:

- `GenSurrealDB` returns `ErrMissingPath` when the path does not name both
  the namespace and the database, which the driver needs. Before, it passed
  the URL through and the driver refused it.
- awsathena and bigquery use the new `GenSchemeHost`, which returns
  `ErrMissingHost` for a URL with no host. The host of awsathena is the S3
  bucket, and the host of bigquery is the project, so the `localhost` that
  `GenScheme` supplied named a bucket or a project that does not exist. A URL
  such as `athena:///db` still fails first in `Parse`, as an invalid
  transport, because `Parse` reads no host with a path as a unix socket.

`clickhouse+https` without `secure=true` waits for the dbimp ClickHouse
driver. Three generators still fill a field: `GenHive` makes the database
`default`, which D16 decided, and `GenPresto` and `GenTrino` make the catalog
`default`. The backlog holds them for Ken.

RELEASE, ALIASES AND A RENAME

- A release carries every scheme on `main` at once, including the
  provisional ones of D32 and D36, before their drivers are tagged. Each is
  still checked against its `ParseDSN` when the tag exists.
- The aliases of the new schemes are `qs` for questdb, `td` for tdengine,
  `pi` for pinot and `rq` for rqlite. `qs` replaces the automatic `qu`.
- Pinot keeps port 8000, the port of the broker in dbmeta's all-in-one
  container.
- `SchemeDriverAndAliases` is now `SchemeNameAndAliases`, because since D37
  it returns the scheme's `Name` and not a driver.

`TestBadParse` covers the new errors for surrealdb, awsathena and bigquery,
and `TestParseDialect` covers `qs`.

### D39. databend moves to the dbimp driver. Decided.

Ken decided in dbimp D24 and D76 that `github.com/xo/dbimp/databend`
replaces `github.com/datafuselabs/databend-go` in `usql` and here, and asked
on 2026-09-29 for the scheme to move now. Ken decided the URL the same day,
in dbimp D117. No dbimp tag holds the driver yet, so the generator is
provisional, as D32 was for ArangoDB and D36 is for three others.

The URL is `databend://user:password@host:port/database?key=value`. The path
names the database, and no path means `default`. The driver refuses a path of
more than one segment. It has its own default port, 8000, for HTTP and for
`tls=true` alike. Only `tls=true` chooses TLS, never the scheme. The keys are
`tls`, `auth`, `cancel` and `timezone`, and the driver refuses any other key.

`GenDatabend` now uses `genRewrite`. It writes the scheme `databend`
whichever alias was parsed, because a dbimp driver refuses any scheme but its
own name (dbimp D35). Before, it passed the URL through as it was typed, so
`bend://` reached the driver with the scheme `bend`. The user information,
the path and the query pass through. It adds no port, because the driver
defaults to 8000 (D34).

The default host is `localhost`. Before, `GenDatabend` returned
`ErrMissingHost` for a URL with no host, because databend-go would dial
`:443`. A dbimp driver refuses an empty host, so dburl supplies one under
rule 7, as it does for every dbimp driver.

`GoPackage` is `github.com/xo/dbimp/databend`, and `DriverURL` is
`https://github.com/xo/dbimp`. The name `databend`, the aliases `dd` and
`bend`, and the `Dialect` `databend` do not change.

databend-go read keys such as `sslmode`, `tenant` and `warehouse`, and chose
HTTPS unless `sslmode=disable`. The dbimp driver refuses those keys, and it
speaks HTTP unless `tls=true`. dbimp offered two choices: drop those keys, or
refuse a URL that holds them. `GenDatabend` does neither. It passes them
through, and the driver refuses them, under D5, as D21 did for the old
MaxCompute keys. A URL written for databend-go must be rewritten, and a
release note says so.

When a dbimp tag holds the driver, `GenDatabend` is run through its
`ParseDSN`, and it changes where the two disagree. Until `usql` imports the
dbimp driver, a release that carries D39 names a package that `usql` does
not link.

dbimp `v0.6.0` was released on 2026-09-29 with the driver. `GenDatabend` was
run through `databend.ParseDSN` at `v0.6.0` from `proxy.golang.org`. With no
port the driver used 8000, and with no path it used `default`. `dd://` and
`bend://` reached it as `databend://`, and a database holding `%2F`, `tls`,
`cancel`, `timezone` and `auth=bearer` came back as intended. A path of two
segments failed in the driver, and a URL that held `sslmode` and `warehouse`
failed with `"sslmode": unknown key`, as the pass-through choice above
intends. The generator needs no change, and the scheme is no longer
provisional.

### D40. The nzgo scheme is removed. Replaces D11. Amends D34.

Ken decided on 2026-09-29 to remove Netezza. He found no sign that the
`nzgo` scheme was ever tested or used, and he cannot get a copy of Netezza to
test against. dbmeta has no Netezza container and no model.

The scheme `nzgo`, its aliases `nz` and `netezza`, and `GenNzgo` are
removed. `GenPostgres` now serves only lib/pq, through `GenPq`, so D11, which
kept Netezza on `GenPostgres`, no longer applies. D34's default port of 5480
for Netezza goes with it. `GenPostgres` keeps its quoting of any Unicode
space, which nzgo needed and lib/pq accepts.

D12 removes a scheme when `usql` removes its driver. Here both go at once, by
Ken's decision. `usql` staged the removal of its driver on 2026-09-29, and the
two can land in either order: `usql` without the driver reports that no
driver is available for a `netezza://` URL, whichever dburl it pins. Every removed name now returns `ErrUnknownDatabaseScheme`, and
`TestBadParse` has a case for each, as D20 set for the schemes it removed.

### D41. The tdengine scheme is removed. Amends D36 and D38.

dbimp D127, which Ken decided on 2026-09-29, says that dbimp writes no
TDengine driver, and moves TDengine to P3. The REST interface of TDengine
ends a failed result as valid JSON with code 0 and a partial row count, so a
driver cannot tell a cut result from a whole one. The WebSocket interface
reports the failure, but it needs a binary encoding and a transport that is
not HTTP.

The provisional `tdengine` scheme of D36 therefore has no driver coming, and
rule 8 allows a scheme only for a driver that `usql` carries or expects soon.
Ken decided to remove it. The scheme, its alias `td` from D38 and
`GenTDengine` are removed, and both names return `ErrUnknownDatabaseScheme`.
The scheme shipped in v0.36.0 and v0.37.0, so the removal is breaking, but no
driver could open a `tdengine://` URL.

A scheme can return, as D12 says, if a driver for TDengine appears.

### D42. The ql scheme is removed. Decided.

Ken decided on 2026-09-30 to remove support for the ql driver,
`modernc.org/ql`. The `usql` session reported that ql was an unfinished
experiment, and that a rarely used database must not force logic into `usql`.
Its handling of a batch as a transaction was the example. `usql` removes its
driver in the same release.

Under D12, the scheme follows its driver out of `usql`. The `ql` scheme and
its aliases `cznic` and `cznicql` are removed, and each name now returns
`ErrUnknownDatabaseScheme`. `TestBadParse` has a case for each, as D20 set.
The scheme was opaque and used `GenOpaque`, which stays, because every other
database held in a file uses it.

### D43. pinot is checked against dbimp v0.7.0. Amends D36 and D38.

dbimp `v0.7.0` was released on 2026-09-30 with the Pinot driver,
`github.com/xo/dbimp/pinot`, and dbimp D129 settles its URL:
`pinot://user:password@host:port`, with no path, because Pinot has no
databases. The driver refuses a path, and takes the keys `tls`, `auth`,
`cancel` and `engine`.

With no port, the driver uses 8099, the port of a Pinot broker. D38 kept port
8000 for the provisional scheme, which was the port of the broker in dbmeta's
all-in-one container. Now the driver has a default of its own, so under D34
`GenPinot` adds no port, and 8099 applies.

`GenPinot` was run through `pinot.ParseDSN` at `v0.7.0` from
`proxy.golang.org`. With no port the driver used 8099, and an explicit port, a
password holding `@` and a space, `tls`, `cancel`, `engine` and `auth=bearer`
came back as intended. A path and an unknown key failed in the driver, under
D5. The scheme is no longer provisional.

### D44. rqlite is checked against dbimp v0.8.0. Amends D36.

dbimp `v0.8.0` was released on 2026-10-01 with the rqlite driver,
`github.com/xo/dbimp/rqlite`, and dbimp D141 settles its URL:
`rqlite://user:password@host:port`, with no path, because rqlite has one
database. The driver refuses a path, and refuses an `http` or `https` scheme.
It takes the keys `tls`, `level` and `freshness`, and a URL with no user
sends no credentials.

With no port, the driver uses 4001, the port of the HTTP API of rqlite. D36
had the provisional scheme add 4001 itself. Now the driver has that default
of its own, so under D34 `GenRqlite` adds no port.

`GenRqlite` was run through `rqlite.ParseDSN` at `v0.8.0` from
`proxy.golang.org`. With no port the driver used 4001, and an explicit port,
a password holding `@` and a space, `tls`, `level=strong` and
`freshness=5s` came back as intended. A path, an unknown key and an invalid
`level` failed in the driver, under D5. `GoPackage` was already the dbimp
package, and the scheme is no longer provisional.

### D45. libsql opens xo/dbimp/libsql. Decided.

dbimp `v0.9.0` was released on 2026-10-01 with one driver for libSQL and
Turso, `github.com/xo/dbimp/libsql`. Ken decided in dbimp D76 that the two are
one product with one driver, `libsql`, and that `turso` is an alias that dburl
owns. dbimp D148 settles the URL: `libsql://user:token@host:port`, with no
path. The driver refuses a path.

The scheme is `libsql`, with the aliases `ls` and `turso`. Ken chose `ls` as
the two letter alias, so the automatic `li` is not registered. `Driver` and `Dialect` are both `libsql`. It needs no cgo. Its
`Deployment` is `DeploymentServer|DeploymentHosted`, because the libSQL
server `sqld` runs anywhere, and Turso is a hosted service.

`GenLibsql` writes the scheme `libsql` whichever alias was parsed, and passes
the user information and the query through. The token is the password of the
URL, and the driver sends it as a bearer token, so the user name is only a
label. `auth=basic` sends basic authentication instead. The driver takes the
keys `tls`, `auth` and `namespace`, and refuses any other, such as the
`authToken` of the old Go client, under D5.

TLS is on by default. With TLS and no port, the driver uses 443, and with
`tls=false` it needs an explicit port and refuses a URL without one. So under
D34 `GenLibsql` adds no port, and a URL with `tls=false` must name one.

`GenLibsql` was run through `libsql.ParseDSN` at `v0.9.0` from
`proxy.golang.org`. With no port the driver used 443, `tls=false` with a port
turned TLS off, and the token, `auth=basic`, `namespace` and a password
holding `@` and a space came back as intended. `tls=false` with no port, a
path and `authToken` failed in the driver.

### D46. dbmeta's tests choose their driver from GoPackage. Amends D17.

Ken approved dbmeta D154 on 2026-10-01. dbmeta's tests now import the driver
that `Scheme.GoPackage` names, in place of the one that `usql` imports.
dbmeta chose this because dburl is upstream of both `usql` and dbmeta, so the
registry is the one place that names each driver.

D17 added `GoPackage` so that `usql` could build its README table from the
registry. Now the field has a second reader, and that reader runs code. A
change to `GoPackage` changes which driver dbmeta tests against, with no
change in the dbmeta repository. The Databend move of D39 and the Pinot and
rqlite settlements of D43 and D44 would each have reached dbmeta this way.

dbmeta takes only the import path from `GoPackage`, with its major version,
such as `/v2` or `/v3`, and pins its own minor and patch release in its test
module. So `GoPackage` must name the import path that registers the driver,
with the major version that `usql` pins, which D17 already required. dbmeta
does not follow the rest of `usql`'s pin. Oracle is the one exception:
`GoPackage` names `go-ora/v3`, and dbmeta tests both v2 and v3 at a fixed
commit (dbmeta D59 and D136).

A change to `GoPackage` is reported to the dbmeta session, as a change to a
scheme or an alias is reported to `usql` under D13. Step 6 of SCHEME.md says
so.

### D47. avatica opens xo/dbimp/avatica. Decided.

dbimp `v0.10.0` was released on 2026-10-01 with the Avatica driver,
`github.com/xo/dbimp/avatica`, and dbimp D156 settles its URL:
`avatica://user:password@host:port`, with no path. The driver refuses a path,
and takes the keys `tls` (false by default) and `auth` (`none` or `basic`),
and refuses any other key. It serves the standalone Avatica server and the
Phoenix Query Server, over JSON only (dbimp D153).

The `avatica` scheme moves from `github.com/apache/calcite-avatica-go/v5` to
`github.com/xo/dbimp/avatica`. `GenAvatica` replaces the template
`GenFromURL("http://localhost:8765/")`. It writes the scheme `avatica`
whichever alias was parsed, and passes the user information and the query
through. It adds no port, because the driver defaults to 8765, the port of an
Avatica server (D34). The old DSN was an `http://` URL, which the new driver
refuses.

The driver registers one name, `avatica`, so the alias `phoenix` stays in
dburl and reaches the same driver, as dbimp asked. `phoenix` does not become a
scheme of its own, and the `Dialect` stays `avatica`. The backlog held that
question for Ken. A scheme of its own can be added if a Phoenix dialect is
ever needed, under dbimp D98.

`GenAvatica` was run through `avatica.ParseDSN` at `v0.10.1`, the newest tag,
from `proxy.golang.org`. With no port the driver used 8765, `phoenix://` and
`av://` reached it as `avatica://`, and an explicit port, a password holding
`@` and a space, `tls=true`, `auth=basic` and an IPv6 host came back as
intended. A path, an unknown key and an invalid `auth` failed in the driver.

`GoPackage` changed, so under D46 the dbmeta session is told, because its
tests import the package that `GoPackage` names.

### D48. druid is a scheme for the dbimp driver. Decided.

dbimp D154 decides that Druid gets a driver of its own on its SQL API, and not
a flavor of Avatica, and dbimp D164 names the driver `github.com/xo/dbimp/druid`,
which registers `druid`. Ken asked on 2026-10-07, through the dbmeta session,
for a `druid` scheme now. dbmeta needs it so that its `druid` dialect can name
a package that dburl names (D46). It does not need a release, because it
builds against the driver directly. Ken decided to add the scheme before the
driver is tagged, as D32 did for ArangoDB, and to give it no alias.

The scheme is `druid`, with the automatic two letter alias `dr`. `Driver` and
`Dialect` are both `druid`. `GoPackage` is `github.com/xo/dbimp/druid`, and the
driver needs no cgo. `GenDruid` writes the scheme `druid` whichever alias was
parsed, and passes the user information and the query through. It adds no
port, because the driver defaults to 8888, the port of the Druid Router
(D34).

THE PROVISIONAL PARTS

No dbimp tag holds the driver. The newest tag, `v0.10.2`, has no `druid`
folder, and the driver is in dbimp's working tree. So rule 3 had no evidence at
a tag. The generator was written against `druid.ParseDSN` in that working tree,
and run through it by a replace in a scratch module outside this repository.
The URL is `druid://user:key@host:port`, with no path, because Druid has no
database to choose. The driver takes the keys `tls`, `timezone` and
`timeout`, and refuses any other. With no port the driver used 8888, and an
explicit port, a password holding `@` and a space, `tls`, `timezone`,
`timeout` and an IPv6 host came back as intended. A path and an unknown key
failed in the driver.

A release of this library that carries D48 may ship before the driver is
tagged, under D38. When dbimp tags a release that holds the driver,
`GenDruid` is run through its `ParseDSN` again, and it changes where the two
disagree. Until then the generator rests on a working tree that can change.

dbimp `v0.11.0` was tagged on 2026-10-07 with the driver, and dbimp confirmed
that its `ParseDSN` did not change from the working tree. `GenDruid`, as
`v0.42.0` of this library ships it, was run through `druid.ParseDSN` at
`v0.11.0` from `proxy.golang.org`, and every result matched. With no port the
driver used 8888. `dr://` reached it as `druid://`, and an explicit port, a
password holding `@` and a space, `tls`, `timezone`, `timeout` and an IPv6
host came back as intended. A path and an unknown key failed in the driver.
The generator needs no change, and the scheme is no longer provisional.

dbimp's CI jobs for Druid had not finished when it tagged, so the driver is
backed by the author's local runs against Druid 36.0.0 and 37.0.0, and not yet
by CI. That is a fact about the driver and not about this generator.

### D49. odbc opens xo/odbc. Amended by D52.

Amended by D52: `GenOdbc` writes the `odbc+<driver>://` URL and no longer
builds a connection string, so the `;` defect and the default port below are
gone.

Ken decided on 2026-10-07 that the `odbc` scheme moves from
`github.com/alexbrainman/odbc` to `github.com/xo/odbc`. The new driver is
written in pure Go. It loads the ODBC driver manager of the system at run time
with `purego`, so it needs no cgo and no C compiler, and it runs the same way
on Windows, macOS and Linux. The driver registers the same name, `odbc`.

`GoPackage` is now `github.com/xo/odbc`, `DriverURL` is
`https://github.com/xo/odbc`, and `RequiresCGO` is false, so the README row
loses its cgo marker. Nothing else in the scheme changes: not the name, the
alias `od`, the transport, the `Dialect` or the generator.

The generator needs no change, because of how `odbc.ParseDSN` reads a DSN. A
string that holds no `://` is taken to be an ODBC connection string, "such as
the one dburl builds", and is passed to the driver manager as it is. That is
the form that `GenOdbc` writes, as `Driver={Postgres Unicode};Server=host;...`.
The driver also reads a URL of the form `odbc+<driver>://`, which `GenOdbc`
does not write. `GenOdbc` was run through `odbc.ParseDSN` from the working
tree, for PostgreSQL, SQL Server with an instance, and SQLite, and each string
came back as the same connection string.

THE CHECK

`github.com/xo/odbc` was tagged `v0.1.0` on 2026-10-07. `GenOdbc` was run
through `odbc.ParseDSN` at `v0.1.0` from `proxy.golang.org`, for PostgreSQL,
SQL Server with an instance, SQLite and MySQL, and each connection string came
back identical. The driver registers `odbc`, and needs only `purego`. So
`GoPackage` names a module that can be installed. Before the tag, the
generator was checked against the working tree.

The check found a defect that was already there: a password or any value that
holds `;` is written without a quote, so `PWD=p;w;` ends the password at the
semicolon. The new driver quotes a value in its own `odbc+<driver>://` form,
but it passes a connection string on as it is. The backlog holds it.

`GenOdbc` adds a default `Port`, 1433 for a driver it does not know. The
driver adds none, because it passes the connection string on, so any default
port belongs to the ODBC driver that the user installed. D34 listed `odbc` as
a driver that cannot be checked from Go source, and that is now possible. This
entry does not change the port. The backlog holds the question.

### D50. trino and presto move to one dbimp driver. Amends D34 and D38.

Ken decided on 2026-10-07 that `trino` and `presto` move to dbimp. dbimp D173
makes one driver serve both: the package is `github.com/xo/dbimp/trino`, it
registers `trino`, and Presto is a flavor. The driver tells the flavors apart
from what the server answers, and never from the DSN alone. dbimp D98 says
that a second scheme for one driver has its own `Name` and `Dialect`, and
returns the registered name of the driver.

So `trino` and `presto` are each a scheme with their own `Dialect`, and both
name `github.com/xo/dbimp/trino`. `GenTrino` writes `trino://`, and returns no
driver name, so `URL.Driver` is `trino`. `GenPresto` writes the same URL and
returns `trino`, so `presto://` opens the same driver with the `Dialect`
`presto`. The two Go clients that `usql` uses today, `trinodb/trino-go-client`
and `prestodb/presto-go-client`, disagreed about the scheme and about where
the catalog and the schema go (D6). The one driver ends that split.

THE DSN

dbimp `v0.12.0` was tagged on 2026-10-07 with the driver, and dbimp D175 settles
its DSN: `trino://user@host:port/catalog/schema?key=value`. The path names the
catalog and the schema, and each is optional. The driver defaults to port 8080,
and to 8443 with `tls=true`. It takes the keys `tls`, `source`, `timezone`,
`timeout`, `session.<name>` and `flavor`, and refuses any other. It requires a
user, and refuses a URL without one.

So the guesses that D50 first made held for the path and the ports, and
changed for two things:

- The generators add no port, no catalog and no TLS, and pass the path and the
  query through. The old `ssl_*` keys of the Presto client are refused.
- A URL with no user gets the user `user`, as `v0.42.0` did. The driver
  requires a user and refuses a URL without one, and a server that does no
  authentication accepts any name. Rule 10 says a generator returns
  `ErrMissingUser` and does not fill the field, and Ken decided on 2026-10-07
  that Trino and Presto are the exception, because `usql trino://host` worked
  before. The `usql` session found that this mattered. dbimp keeps its driver
  strict, and changes nothing.
- The scheme chooses the flavor. The driver takes `flavor=trino` or
  `flavor=presto`, and otherwise asks the server with `GET /v1/info`. Without
  the key, `presto://` against a Trino server would act as Trino while the
  `Dialect` says `presto`. So `GenTrino` writes `flavor=trino` and `GenPresto`
  writes `flavor=presto`, unless the URL names `flavor` itself. Ken decided this
  on 2026-10-07. It is a fourth exception to rule 7, as `sqlmode=disable` is
  for `influxql` (D29).
- TLS is the `tls` key, and never the scheme, as in every dbimp driver. The
  aliases `trs` and `trinos` meant HTTPS, and no alias can mean that now. `trs`
  or `trinos` with no TLS would be a quiet downgrade, and a `tls=true` added by
  the alias would be a rule 7 exception that no other dbimp scheme has. Ken
  decided on 2026-10-07 to drop both aliases, under D14, because they are not
  congruent with the other schemes. `trs://` and `trinos://` return
  `ErrUnknownDatabaseScheme`, and `TestBadParse` has a case for each. The
  scheme keeps the automatic alias `tr`.

Both generators were run through `trino.ParseDSN` at `v0.12.0` from
`proxy.golang.org`. With no port the driver used 8080, with `tls=true` it used
8443, and an escaped catalog, a schema, `source`, `timezone`, `timeout`,
`session.query_max_run_time` and an IPv6 host came back as intended. `presto://`
reached the driver with the flavor `presto`, and `trino://host` parsed with the
default user. A path of three segments, `ssl_ca` and an unknown key failed in
the driver, under D5. Both schemes are no longer provisional.

This also settles the default catalog of D38. The old generators made the
catalog `default`, and now neither generator fills it.

`GoPackage` changed for both schemes, and under D46 the dbmeta tests import the
package that it names, so the dbmeta session is told. The `usql` drivers for
the two old clients move in the same change that takes the release.

### D51. maxcompute, tablestore and ydb choose TLS by the tls option. Amends D21. Amended by D53.

Amended by D53: `clickhouse` also chooses TLS by `tls`, and no scheme keeps
`+http` or `+https`.

Ken decided on 2026-10-07 that no scheme chooses TLS by its scheme or its
transport, because the dbimp drivers read the `tls` key and never the scheme
(D50), and the schemes should agree. Four schemes still chose it by form:
`maxcompute` and `ots` by a `+http` or `+https` transport, `ydb` by the aliases
`yds` and `ydbs`, and `clickhouse` by a `+http` or `+https` transport. The first
three move to `tls`. `clickhouse` keeps its transports until dbimp writes a
ClickHouse driver, which settles its ports and its `tls` with the rest (D34).

None of the three drivers has a `tls` key. MaxCompute's driver reads an http or
https endpoint, Tablestore's driver reads the scheme and the host of its
endpoint, and the YDB driver reads `grpcs` as secure and `grpc` as not. So dburl
reads the option and writes each driver's own form, as `influxql` writes
`sqlmode=disable` (D29). It removes `tls` from the query, because MaxCompute's
driver would send an unknown key to the server as a hint. The value is read with
`strconv.ParseBool`, as the dbimp drivers read it. A value that it refuses, and
a key given twice, return `ErrInvalidQuery`. This is a check of an option that
dburl owns, so it is not the validation of a driver's parser that rule 5 forbids.

THE DEFAULTS

The default of each scheme is the one it had, so a URL with no `tls` keeps its
meaning:

| Scheme | Default | `tls=true` | `tls=false` |
| --- | --- | --- | --- |
| `maxcompute` | `https` | `https` | `http` |
| `ots`, `tablestore` | `https` | `https` | `http` |
| `ydb` | `grpc`, port 2136 | `grpcs`, port 2135 | `grpc`, port 2136 |

MaxCompute and Tablestore are hosted services, so TLS is on by default and
`tls=false` turns it off, as for `libsql` (D45). YDB runs anywhere, so TLS is off
by default, as for every dbimp driver. An explicit port always wins.

WHAT IS REMOVED

- The transports `+http` and `+https` for `maxcompute` and `ots`. Both schemes
  have no transport now, so `mc+https://` and `tablestore+http://` return
  `ErrInvalidTransportProtocol`, as any transport does for a scheme with none.
- The aliases `yds` and `ydbs`, which meant TLS. They return
  `ErrUnknownDatabaseScheme`, as D14 does for an alias that cannot say what it
  meant. `yd` stays.

A URL that used one of them must be rewritten: `ydbs://host` is
`ydb://host?tls=true`, `mc+http://host/api` is `mc://host/api?tls=false`, and
`ots+http://host/i` is `ots://host/i?tls=false`.

`ots://` with no host wrote `https:` and now writes `https://localhost`, as
`maxcompute` always did. The scheme is a hosted service, so a host is required in
practice, and rule 10 may ask for an error in its place. This entry does not
decide that.

The MaxCompute output was run through `sqldriver.ParseDSN` at v0.4.26. The
endpoint and the project came back as intended, with `https` by default and
`http` for `tls=false`, and the hints stayed empty, so `tls` did not reach the
server. `TestParse` covers each scheme and each value, and `TestBadParse` covers
the removed forms and the invalid values.

### D52. odbc sends the URL to xo/odbc. Amends D7 and D49.

Ken decided on 2026-10-07 that `GenOdbc` sends the URL to `github.com/xo/odbc`
as the DSN, because `odbc.ParseDSN` reads a URL of the form
`odbc+<driver>://user:pass@host:port/path?key=value`. dburl no longer builds an
ODBC connection string.

`GenOdbc` writes the URL back with the same user information, host, port, path
and query. It drops a query key that starts with a prefix in
`OdbcIgnoreQueryPrefixes`, which `usql` sets to `usql_`, and compares the
lower case key. It adds nothing else.

WHAT THIS REMOVES

- The default `Port`. The driver adds none, so the ODBC driver that the user
  installed owns it. This answers the question that D49 left open.
- `ServiceName=50000` for DB2. It was an exception to D7, and now the
  exceptions are `sslmode=disable`, `sqlmode=disable` and `flavor`.
- The defect in D49. The driver braces a value that holds `;`, `{`, `}`, `=` or
  a space, and doubles a `}`, so a password `p;w` comes through as `PWD={p;w}`.
- The default host. `GenOdbc` adds no host, because a driver or a DSN on the
  machine may need none, and the driver omits an empty `Server`.

WHAT CHANGES FOR THE CALLER

The DSN is a URL and not a connection string. `usql` and any other caller must
pass it to `sql.Open("odbc", dsn)`, which is what they did. A caller that
read the string for `Driver=` must stop.

`odbc://host/db`, with no driver, returns `ErrMissingDriver`. It wrote
`Driver={tcp}` before, which no ODBC driver has. Rule 10 asks for an error when
a required field is missing, and the driver name is one.

`GenOdbc` was run through `odbc.ParseDSN` at `v0.1.0`. `PWD` came back as
`{p;w}`, and SQL Server with an instance and a port came back as
`Server=host\inst,1433`. `TestParse` covers both, and `TestBadParse` covers the
missing driver.

### D53. clickhouse opens the dbimp driver. Amends D34 and D51.

Ken decided on 2026-10-07 that `clickhouse` moves to `github.com/xo/dbimp/clickhouse`
and that `github.com/ClickHouse/clickhouse-go` is dropped. The dbimp driver
registers the name `clickhouse`, as clickhouse-go does, so a program can link
only one of them, and dburl names the dbimp one.

`GenClickhouse` writes the URL back as `clickhouse://`, with the user
information, the path and the query as they were written. The driver reads
`tls` and refuses any other key. It speaks HTTP only, and its default port is
8123, or 8443 with `tls=true`, so `GenClickhouse` adds no port, as D34 asks.
The host defaults to `localhost`.

WHAT IS REMOVED

- The transports `+http` and `+https`. `clickhouse+https://host/db` returns
  `ErrInvalidTransportProtocol`. Write `clickhouse://host/db?tls=true`.
- The native protocol and its port 9000. The driver cannot speak it. A URL that
  names port 9000 still passes through, and the driver sends HTTP to it.
- Every clickhouse-go option, such as `secure=true`, which the driver refuses
  as an unknown key. This closes the two backlog items about clickhouse.

`GoPackage` is `github.com/xo/dbimp/clickhouse`, and the transport is none. The
alias `ch` stays. dbmeta reads `GoPackage`, so it must be told (D46).

`GenClickhouse` was run through `clickhouse.ParseDSN` at dbimp v0.13.0, and the
host, port, tls, user, password and database came back as written. A password
with `@` came back whole, `clickhouse://` with no host came back as
`localhost:8123`, and `secure=true` came back as `unknown key`. `TestParse`
covers each form, and `TestBadParse` covers the removed transports.

### D54. drill, solr, elasticsearch and opensearch are schemes for the dbimp drivers. Decided.

Ken asked on 2026-10-07 for schemes for OpenSearch, Elasticsearch, Solr and
Drill. dbimp wrote one driver for each, in `github.com/xo/dbimp/drill`,
`/solr`, `/elasticsearch` and `/opensearch`. Each registers a name equal to its
package. Elasticsearch and OpenSearch share no code, so they are two schemes.
The schemes have no transport.

| Scheme | Alias | URL | Keys besides `tls` |
| --- | --- | --- | --- |
| `drill` | `dl` | `drill://user:pass@host:8047`, no path | `schema`, `autolimit` |
| `solr` | `so` | `solr://user:pass@host:8983/collection`, the path optional | `mode` |
| `elasticsearch` | `es`, `elastic` | `elasticsearch://user:pass@host:9200`, no path | `auth`, `fetch_size`, `time_zone`, `field_multi_value_leniency`, `catalog` |
| `opensearch` | `os`, `open` | `opensearch://user:pass@host:9200`, no path | `fetch_size` |

`drill` takes `dl` because the automatic alias `dr` belongs to `druid`, and
registering it would panic. `elasticsearch` takes the explicit aliases `es`
and `elastic`, and `opensearch` takes `os` and `open`.

Each generator is `genRewrite` with the scheme name. The user information, the
path and the query pass through, the host defaults to `localhost`, and no port
is added, because each driver has its own default and `tls=true` does not move
it (D34). A path on `drill`, `elasticsearch` or `opensearch`, and a key the
driver does not know, are left for the driver to refuse (rule 5).
`GoPackage` is the package of the driver. Elasticsearch and OpenSearch are
`DeploymentServer | DeploymentHosted`, and the other two are `DeploymentServer`.

THE PROVISIONAL PARTS

dbimp has tagged none of the four drivers, and they are in its working tree,
for the order drill, solr, elasticsearch and opensearch. So rule 3 had no
evidence at a tag, as in D48. Each generator was run through `ParseDSN` in the
working tree by a replace in a scratch module outside this repository. The
default port, an explicit port, a password holding `@`, `tls`, the keys above,
and an IPv6 host came back as intended, and a path or an unknown key failed in
the driver. dbimp says two answers of Ken's can still change a DSN: whether
`solr` needs a collection in the path, and `fetch_size=0` for OpenSearch 2.19.6,
which the DSN refuses today. When dbimp tags each driver, run its generator
through the tagged `ParseDSN` again, and change it where the two disagree. A
release that carries D54 may ship before the tags, under D38.

### D55. dynamodb opens the dbimp driver. Decided.

Ken decided on 2026-10-07 that DynamoDB moves from
`github.com/btnguyen2k/godynamo` to `github.com/xo/dbimp/dynamodb`, and that
the URLs change with it. The scheme is now `dynamodb`, where it was `godynamo`
with `dynamodb` as an alias, and its `Dialect` is `dynamodb`. The aliases are
`dy`, `dyn` and `dynamo`. The name `godynamo` is dropped, as `trs` was in D50,
and returns `ErrUnknownDatabaseScheme`. `GoPackage` is
`github.com/xo/dbimp/dynamodb`, and the scheme is `DeploymentServer |
DeploymentHosted`, because DynamoDB Local runs anywhere.

THE URL

The old URL was `dynamodb://key:secret@us-east-1`, with the region as the host
and the other options as godynamo keys. The new URL is
`dynamodb://key:secret@host:port?region=us-east-1`. The host is the endpoint,
such as `dynamodb.us-east-1.amazonaws.com` or `localhost:8000`, and the region
is the key `region`, which the driver requires. The driver speaks HTTPS, and
`tls=false` selects HTTP. It refuses a path and any other key.

`GenDynamo` is `genRewrite` with the scheme `dynamodb`. The signature needs the
access key and the secret key, so a URL with no user, or no password, returns
`ErrMissingUser` (rule 10). A missing `region` is left to the driver, which
refuses it. It adds no port, because the driver uses 443 or 80 by scheme, and
the host defaults to `localhost`, which is the endpoint of DynamoDB Local.

THE PROVISIONAL PARTS

dbimp has not tagged the driver, which is in its working tree (the newest tag
is `v0.13.0`). `GenDynamo` was run through `dynamodb.ParseDSN` there, by a
replace in a scratch module. An AWS endpoint came back with TLS on and no port,
a local endpoint with port 8000 and `tls=false`, and a secret holding `/` came
back whole. A missing region, a path and an unknown key failed in the driver.
When dbimp tags the driver, run the generator through the tagged `ParseDSN`
again, as D54 asks.

THE CHECK AT THE TAG

dbimp tagged `v0.14.0` on 2026-10-07 with `drill`, `solr`, `elasticsearch`,
`opensearch` and `dynamodb`. D54 and D55 are no longer provisional. Every
generator, as `v0.46.0` ships it, was run through `ParseDSN` at `v0.14.0` from
`proxy.golang.org`, and every result matched the working tree. Each default
port, an explicit port, a password holding `@` or `/`, the aliases `dl`,
`elastic` and `open`, an IPv6 host, and every key of each driver came back as
intended. A path on `drill`, `elasticsearch`, `opensearch` and `dynamodb`, an
unknown key, and a missing region failed in the driver. The new `dynamodb` key
`token`, which carries the session token, passes through, and `TestParse`
covers it. dbimp D178 item 16 has dburl write `flavor` from the scheme, which
`GenTrino` and `GenPresto` already do (D50).

### D56. cql is renamed cassandra. Amends D23.

Ken decided on 2026-10-08 that the Cassandra driver moves from
`github.com/xo/cql` to `github.com/xo/cassandra`, and drops the CQL name, so
that it agrees with the other drivers. Ken confirmed to the dbmeta session that
the module is `github.com/xo/cassandra`, the driver registers `cassandra`, the
URL scheme is `cassandra://`, and `cql://` stays accepted as an alias.

The scheme is now `cassandra`, and `Driver` and `Dialect` are `cassandra`,
where all three were `cql`. `GoPackage` is `github.com/xo/cassandra`. The
aliases are `ca`, `cass`, `cql`, `datastax`, `scy` and `scylla`, so `cql://` and
`cassandra://` both parse. `GenCassandra` writes `cassandra://` whichever alias
was parsed, and it passes the user information, the keyspace path and the query
through. It adds no port, because gocql defaults to 9042 (D34).

THE URL

The cassandra session reported on 2026-10-08 that the driver reads only the
URL `cassandra://user:password@host:port/keyspace?key=value`. The user
information is the credentials, the host part is one host, the path is the
keyspace, and each extra host is a repeated `host` key. It refuses an unknown
key, a repeated key other than `host`, and any other scheme, so dburl rewrites
`cql://` and `scylla://` to `cassandra://`. `GenCassandra` already did the
rewrite and writes no host list.

THE PROVISIONAL PARTS

The new package has no tag, and Ken tags `v0.1.0` when he says. On 2026-10-08
the cassandra session reported that its working tree holds the module
`github.com/xo/cassandra` and registers `cassandra`, staged and not committed.
`GenCassandra` was run through its `ParseDSN` there, by a replace in a scratch
module. `cassandra://` with no host came back as `localhost`. A URL from `cql://`
came back with its host, port, keyspace, consistency and timeout as intended,
and one from `scylla://` came back with the IPv6 host and each repeated `host`
key. An unknown key, and a keyspace in the path and in the query, failed in the
driver. The package clause of the tree is still `cql`, which does not matter to
dburl. When the package is tagged, run `GenCassandra` through its `ParseDSN`
again, and release then. dbmeta changes its dialect constant, its driver map
and its test module together, and usql changes its driver import, so do not
release this before the tag.

THE CHECK AT THE TAG

`github.com/xo/cassandra` was tagged `v0.1.0` on 2026-10-08, at commit
`2d0de96`. `GenCassandra` was run through `ParseDSN` at `v0.1.0` from
`proxy.golang.org`, and every result matched the working tree. A URL with no
host came back as `localhost`, and a URL from `cql://` and one from `scylla://`
came back with the host, port, keyspace, consistency, timeout and each repeated
`host` key as intended. An unknown key, and a keyspace in the path and in the
query, failed in the driver. D56 is no longer provisional.
