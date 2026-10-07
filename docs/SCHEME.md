# Adding a Database Scheme

This is the task that dominates this repository. Roughly half of its commits
either add a scheme or fix a generator. Follow every step in order.

A scheme is the name at the front of a URL, such as `postgres` in
`postgres://host/db`. A generator is the func that turns a parsed URL into the
DSN string that one driver expects.

## 1. Decide whether the scheme belongs here

Add a scheme only when you expect the matching driver to be added to
[usql][usql] in the near future. `usql` is the authoritative list of the
drivers that `xo` projects support. A scheme with no driver behind it is a
promise this repository cannot keep.

`dburl` is the authoritative repository for every `xo` project that connects
to a database, runs queries, or reads data. `usql` and `dbtpl` both take their
connection strings from here.

The reverse holds too. When `usql` removes a driver, remove the scheme here.
Demotion to the `bad` build tag is not removal, and a demoted driver keeps its
scheme. `CONTRIBUTING.md` in `usql` holds both sets of rules. That is D12.

`dburl` never imports a driver and never opens a database connection. It
writes a string, and another program hands that string to the driver.

## 2. Read the driver before you write anything

`dburl` cannot test that its output is accepted, so the driver source is the
only evidence available. Do this before you write the generator:

1. Open `go.mod` in [usql][usql] and find the pinned version of the driver.
2. Read that exact version. `go env GOMODCACHE` gives the local path.
3. Find the func that parses the DSN. It is usually called `ParseDSN`,
   `ParseURL`, or `parseDSN`.
4. Write down what it accepts: the scheme, the default port, where the
   database name goes, and which query options it recognises.

If the parser rejects a scheme, or reads the database name from the path
rather than the query, the generator must match it exactly. A driver that
changed this between major versions broke every Presto user, and nothing in
this repository noticed. That is recorded as D4 in [PLAN.md](PLAN.md).

If you cannot reach the driver source, stop and say so. Do not write a
generator from documentation, from the driver README, or from memory. A
generator built on a guess is the defect in D4.

## 3. Choose the generator

Three mechanisms exist. Prefer the first one that fits.

`GenFromURL("scheme://localhost:PORT/")` takes a template. The values in the
template are the defaults, and anything in the passed URL overrides them. Use
it when the DSN is itself a URL and needs a default port.

`GenFromURL` rebuilds the query with `url.Values`. It writes a space as `+`,
and it joins a key that appears twice into one value. Read how the driver
decodes its query before you use it. pgx reads `+` as itself, so a scheme
whose driver is pgx uses `GenPgxFromURL`, which takes the same template.
`xo/cql` takes a `host` key that can repeat, so `GenCassandra` passes the
query through as it was written. D22 and D23 record both.

`GenScheme("name")` rewrites only the scheme and forces `localhost`. Use it
when the DSN is a URL and needs no default port.

A hand written `GenXxx(u *URL) (string, string, error)` in `dsn.go` covers
everything else. Use it when the DSN is not a URL, such as the `key=value`
form that lib/pq and ODBC take. Use it also when the generator needs to
reject something.

A generator returns an error when the URL lacks a field that the driver
requires: `ErrMissingHost`, `ErrMissingPath` or `ErrMissingUser`. It does not
fill the field in with a guess. `GenSurrealDB` returns `ErrMissingPath` when
the path does not name both the namespace and the database, and
`GenSchemeHost` returns `ErrMissingHost` for awsathena and bigquery, whose
host is a bucket or a project and not a server. A default host or port under
step 4 is not a guess, because it names a real server. That is D38.

## 4. Supply the defaults, then let the URL override them

A `dburl` URL carries the settings that identify the server, so the caller does
not have to write them. The generator sets the default host, and the passed
URL replaces it when it names one.

The generator sets a default port only when the driver has no default port
of its own, or when the driver's default is the wrong port for the product.
Read the driver's parser to find out, as step 2 says. `go-sql-driver/mysql`
adds 3306 itself, so `GenMysql` adds no port, but TiDB listens on 4000, so
`GenTiDB` adds 4000. pgx defaults to 5432, which is right for CrateDB and
wrong for CockroachDB, so `GenCrateDB` adds no port and `GenCockroachDB` adds
26257. A driver that fails without a port, such as go-ora, gets a default
port. That is D34.

```go
host, port := "localhost", "4000"
if h := u.Hostname(); h != "" {
    host = h
}
if p := u.Port(); p != "" {
    port = p
}
```

`GenFromURL` does the same thing, written as a template instead of as code.
Leave the port out of the template when the driver has its own default.

`GenPostgres` and `GenPgx` supply no default host and no default port on
purpose, because the driver then reads `PGHOST` and `PGPORT`, and a default
here would override them. That is D22.

Defaults stop there. Do not add an option that changes how the driver or the
database behaves. The client that opens the connection owns those, and `usql`
and `dbtpl` inject their own.

Two narrow exceptions exist. Set an option when it forces the driver or the
database into a standards compliant mode that a using package needs. Turning
on UTF-8 and enabling connection retries are the kind of thing that qualifies.
The codebase holds three: `sslmode=disable` in the CockroachDB template,
`sqlmode=disable` in `GenInfluxQL`, which selects the query language of the
scheme (D29), and `flavor` in `GenTrino` and `GenPresto`, which selects the
product (D50). If you are writing
a fourth, say why in [PLAN.md](PLAN.md). This is D7.

Set one also when the driver cannot start without it and has no valid empty
value for it. `hive` carries `auth=NONE` for that reason, because the driver
panics on a missing `auth`. That is D16, and its scope is narrow: a field the
driver needs to build a connection, never one that tunes it.

## 5. Know which of the two URL forms your scheme takes

Standard URLs have a host, and look like `scheme://user:pass@host/name`.
Most schemes take this form. Register them with the `Opaque` field set to
`false`.

Opaque URLs have a path on disk and no host, and look like
`scheme:/path/to/file.db`. Every scheme that takes this form is a database
held in a file. Register them with `Opaque` set to `true`.

If `Opaque` is `false` and a caller writes an opaque URL, `Parse` rebuilds it
as `scheme://<opaque>` and parses it again.

## 6. Register the scheme

Add an entry to `BaseSchemes` in `scheme.go`. The fields are named, because
six of them are strings and a positional entry turns a transposition into a
silent bug. That is D17.

```go
{
    Name:       "name",              // the scheme; the generator names the driver
    Generator:  GenName,
    Aliases:    []string{"alias"},
    Desc:       "Database Name",
    Home:       "https://example.com",
    GoPackage:  "github.com/owner/driver",
    DriverURL:  "https://github.com/owner/driver",
    Deployment: DeploymentServer,
    Dialect:    "name",
},
```

Set `Transport` when the scheme takes `+unix` or another transport, and set
`Opaque` for a database held in a file.

`Name` is the name of the scheme. `Parse` stores it as `URL.SchemeName`. The
generator returns the name to pass to `sql.Open` as its second value, which
`Parse` stores as `URL.Driver`. An empty second value means the `Name`, which
is right when the driver registers the scheme's own name, as `mysql` and
`sqlite3` do. Read the registered name from the driver source, where it calls
`sql.Register`. Do not guess it from the package name.

A scheme that opens a driver of another name returns that name. `tidb`,
`memsql` and `vitess` return `mysql`, `postgres`, `cockroachdb`, `redshift`
and `questdb` return `pgx`, `pq` returns `postgres`, which lib/pq registers,
and `influxql` returns `influxdb`. `Open` and `usql` pass `URL.Driver` to
`sql.Open`. That is D37.

Fill in the metadata as well. `Desc` is the database display name, as it
appears in the first column of the README table. `GoPackage` is the import
path of the driver, including the major version, and is not the module path:
for pgx the module is `github.com/jackc/pgx/v5` and the import path is
`github.com/jackc/pgx/v5/stdlib`. `DriverURL` is the driver's home page, which
carries no version. Every scheme except `file` sets both, including a scheme
that shares its driver with another. Set `RequiresCGO` when the driver needs
cgo. Leave `Home` blank unless you have the database provider's page.
`TestSchemeMetadata` checks all of this.

`GoPackage` is read outside this repository. `usql` builds its README table
from it, and dbmeta's tests import the driver that it names, so a change to
`GoPackage` changes which driver dbmeta tests against. Name the import path
that registers the driver, with the major version that `usql` pins, such as
`/v2` or `/v3`. dbmeta pins its own minor and patch release. Tell the dbmeta
session when you change it. That is D46.

Set `Deployment` to how the database is deployed, which is D18. Set `Dialect`
to the `Name` of the scheme that is canonical for the product, which is the
scheme's own `Name` unless you are adding a second Go driver for a product
that already has one. `pgx`, `pq` and `postgres` are all PostgreSQL and share
a `Dialect`. A product that speaks another's wire protocol has a `Dialect` of
its own, because its catalog differs: `tidb` opens the mysql driver, and its
`Dialect` is `tidb`. That is D19, as D37 amends it. `Parse` copies it to
`URL.Dialect`, which is how a consumer learns the product a URL connects to.
That is D24.

A two letter alias is registered automatically from the first two characters
of the name, unless one of the aliases is already two characters.

Add an alias only for a name the database is actually known by. An alias is
cheap to add and expensive to remove. `usql` publishes every alias in its
README table, so an alias is advertised as supported the moment it exists.

Presto is the warning. It carried five aliases, and three of them were `s`
suffixed variants meaning TLS. The v2 driver turned out to have no way to
select TLS from the scheme at all. Removing them was a breaking change to
something documented on another project's front page, and it took two
releases: one to make them fail, and one to take them out. That is D14.

Do not add an alias for a spelling you have not seen in use.

## 7. If the database lives in a file, register a file type

A database held in a file needs one more registration, or `usql mydb.duckdb`
and `file:mydb.duckdb` will not resolve to it. Add a call to
`RegisterFileType` in the `init` func in `scheme.go`:

```go
RegisterFileType("duckdb", isDuckdbHeader, `(?i)\.duckdb$`)
```

The three arguments are the driver name, a func that reads the file header,
and a regexp matching the file extension.

The two are consulted in different situations, and this is the part that is
easy to get wrong. When the file exists, `SchemeType` reads the first 64 bytes
and asks each header func in turn. The extension is never looked at. When the
file does not exist, no header can be read, so the extension regexp decides.
A scheme can therefore work on a path that does not exist and fail on the same
path once the file is created.

Write the header func with `bytes.Equal` or `bytes.HasPrefix` at a fixed
offset. Never use a regexp, for the reason in D3. Register the scheme itself
with `Opaque` set to `true`, as step 5 says, because these URLs carry a path
and no host.

`FileTypes` returns the registered names, and `usql` calls it when generating
its README to decide which rows carry a `file` alias. So registering a file
type changes another project's published table. That is D13.

## 8. Reuse a generator only while the drivers agree

The `Gen*` funcs are helpers, and reusing one is normal. `GenOpaque` serves
every database whose DSN is a path on disk, and `GenMysql` and `GenPostgres`
each serve several. Reuse says nothing about wire compatibility.

Reuse does claim one thing: that every driver behind that generator accepts
the same DSN. Check the claim against the new driver, the way step 2 says, and
write your own generator when it does not hold.

The `trino` scheme was registered against `GenPresto` and the claim went
stale. The two drivers now want incompatible DSNs, and it was split in
v0.27.0. The defect was that nobody reread the drivers, not that the func was
shared. That is D6.

## 9. Add the tests

Add cases to the table in `TestParse` in `dburl_test.go`. Cover the default
host and port, an explicit host and port, a user and password, and every
alias. Add a case to `TestBadParse` for every error the generator returns.

If the scheme detects a file by its header, the fixture must contain a valid
multi-byte UTF-8 sequence, or a newline. Those are the two cases a rune based
matcher gets wrong. A single invalid byte is not enough, because Go decodes it
as one rune and the bug does not appear. `TestIsDuckdbHeader` covers both.
That is recorded as D3.

## 10. Regenerate the README

The driver table in `README.md` between the `DRIVER DETAILS` markers is
generated from this registry. Run this in the repository root:

```sh
go run gen.go
```

Do not edit between those markers by hand, because the next run overwrites
the change. Edit the prose outside the markers freely. Every scheme with a
`Desc` gets a row.

`usql` writes the driver table in its own README from its own builder, which
reads this registry at the version `usql` pins. A scheme or an alias you
change here changes `usql`'s published documentation the next time `usql`
takes a new release and regenerates. `usql`'s table has a row only for a
scheme that `usql` has a driver for. That is D13, as D17 amends it.

## Before you commit

Run these three commands. All three must pass:

```sh
go test ./...
go vet ./...
golangci-lint run ./...
```

[usql]: https://github.com/xo/usql
