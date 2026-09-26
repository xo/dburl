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

`GenScheme("name")` rewrites only the scheme and forces `localhost`. Use it
when the DSN is a URL and needs no default port.

A hand written `GenXxx(u *URL) (string, string, error)` in `dsn.go` covers
everything else. Use it when the DSN is not a URL, such as the `key=value`
form that PostgreSQL and ODBC take. Use it also when the generator needs to
reject something.

## 4. Supply the defaults, then let the URL override them

A `dburl` URL carries the settings that identify the server, so the caller does
not have to write them. The generator sets the default host and the default
port, and the passed URL replaces each one that it names.

```go
host, port := "localhost", "5432"
if h := u.Hostname(); h != "" {
    host = h
}
if p := u.Port(); p != "" {
    port = p
}
```

`GenFromURL` does the same thing, written as a template instead of as code.

Defaults stop there. Do not add an option that changes how the driver or the
database behaves. The client that opens the connection owns those, and `usql`
and `dbtpl` inject their own.

One narrow exception exists. Set an option when it forces the driver or the
database into a standards compliant mode that a using package needs. Turning
on UTF-8 and enabling connection retries are the kind of thing that qualifies. The whole codebase holds two
examples, `sslmode=disable` in the CockroachDB template and `ServiceName` in
the DB2 path of `GenOdbc`. If you are writing a third, say why in
[PLAN.md](PLAN.md). This is D7.

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

Add an entry to `BaseSchemes` in `scheme.go`. The fields are positional:

```go
{
    "name",             // Driver: the sql.Register name the driver uses
    GenName, 0, false,  // Generator, Transport, Opaque
    []string{"alias"},  // Aliases
    "",                 // Override: a different Go driver name, or empty
},
```

The `Driver` field must be the exact name the driver passes to `sql.Register`.
Read it from the driver source. Do not guess it from the package name.

A two letter alias is registered automatically from the first two characters
of the name, unless one of the aliases is already two characters.

Add an alias only for a name the database is actually known by. An alias is
cheap to add and expensive to remove. `usql` publishes every alias in its
README table, so an alias is advertised as supported the moment it exists.

Presto is the warning. It carried five aliases, and two of them were `s`
suffixed variants meaning TLS. The v2 driver turned out to have no way to
select TLS from the scheme at all. Removing those two was a breaking change
to something documented on another project's front page.

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
generated. `usql` writes it, from its own driver list and from this registry,
with `go run gen.go -dburl-gen`. Do not edit between those markers by hand,
because the next run overwrites the change. Edit the prose outside the markers
freely.

The same run writes the driver table in `usql`'s own README, from the same
builder, which reads this registry. A scheme or an alias you change here
changes `usql`'s published documentation the next time anyone regenerates.
That is D13. A scheme with no `usql` driver gets no row in either table.

## Before you commit

Run these three commands. All three must pass:

```sh
go test ./...
go vet ./...
golangci-lint run ./...
```

[usql]: https://github.com/xo/usql
