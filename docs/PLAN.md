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
| [D7](#d7-defaults-cover-the-host-and-the-port-and-not-driver-options) | Amended by D16 |
| [D8](#d8-a-scheme-is-added-only-when-the-driver-is-expected-in-usql) | Decided |
| [D9](#d9-golangci-lint-runs-in-ci-at-a-pinned-version) | Decided |
| [D10](#d10-a-rule-that-has-no-test-is-not-a-rule) | Decided |
| [D11](#d11-netezza-keeps-sharing-genpostgres) | Decided |
| [D12](#d12-a-scheme-follows-its-driver-out-of-usql) | Decided |
| [D13](#d13-this-registry-writes-two-published-driver-tables) | Amended by D17 |
| [D14](#d14-an-alias-that-cannot-work-is-removed-not-left-failing) | Decided |
| [D15](#d15-dburl-does-not-validate-driver-option-values) | Decided |
| [D16](#d16-a-required-option-with-no-valid-empty-value-gets-a-default) | Decided |
| [D17](#d17-a-scheme-describes-its-own-database-and-driver) | Amended by D22 |
| [D18](#d18-a-scheme-records-how-the-database-is-deployed) | Decided |
| [D19](#d19-a-scheme-names-the-dialect-of-its-product) | Amended by D22 |
| [D20](#d20-the-schemes-of-four-removed-drivers-leave-in-one-release) | Decided |
| [D21](#d21-the-maxcompute-endpoint-protocol-comes-from-the-transport) | Decided |
| [D22](#d22-postgres-opens-pgx-and-pq-opens-libpq) | Decided |
| [D23](#d23-cql-opens-xocql-and-gets-a-url) | Decided |
| [D24](#d24-a-parsed-url-carries-its-dialect) | Decided |
| [D25](#d25-couchbase-opens-xodbimpcouchbase) | Decided |
| [D26](#d26-surrealdb-opens-xodbimpsurrealdb) | Decided |
| [D27](#d27-dburl-is-set-up-for-coding-agents-as-every-xo-repository-is) | Decided |
| [D28](#d28-neo4j-opens-xodbimpneo4j) | Decided |
| [D29](#d29-influxql-is-a-scheme-of-its-own-on-the-influxdb-driver) | Decided |

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

### D7. Defaults cover the host and the port, and not driver options. Amended by D16.

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

### D17. A scheme describes its own database and driver. Amends D13. Amended by D22.

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

### D19. A scheme names the dialect of its product. Amended by D22.

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

### D21. The maxcompute endpoint protocol comes from the transport. Decided.

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

### D22. postgres opens pgx, and pq opens lib/pq. Decided.

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

### D23. cql opens xo/cql, and gets a URL. Decided.

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

### D24. A parsed URL carries its Dialect. Decided.

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

### D25. couchbase opens xo/dbimp/couchbase. Decided.

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

### D26. surrealdb opens xo/dbimp/surrealdb. Decided.

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

### D28. neo4j opens xo/dbimp/neo4j. Decided.

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

### D29. influxql is a scheme of its own on the influxdb driver. Decided.

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
