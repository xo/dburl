# dburl

`dburl` translates a URL style database connection string into the DSN string
that one Go SQL driver expects. It is the authoritative source of connection
strings for every `xo` project that reaches a database, including
[usql][usql] and [dbtpl][dbtpl].

It is a string library. It imports the standard library and nothing else, it
never imports a database driver, and it never opens a connection.

## Which document to read

| If you are | Read |
| --- | --- |
| adding a database scheme | [docs/SCHEME.md](docs/SCHEME.md), every step in order |
| changing an existing generator | [docs/SCHEME.md](docs/SCHEME.md) sections 2 and 3 |
| asking why something is the way it is | the table at the top of [docs/PLAN.md](docs/PLAN.md) |
| looking for what the library does | [README.md](README.md) |

A document that is not in that table does not exist. If you cannot find where
something is written down, it is not written down. Ask. Do not decide it
yourself and do not write it as though it were settled.

## Hard rules

1. The module depends on the standard library and on nothing else. Adding a
   `require` line to `go.mod` fails `TestNoDependencies` and `depguard`.
2. Never import a database driver, and never open a database connection.
3. Read the driver before you write a generator. Use the version that `usql`
   pins in its `go.mod`. The parser is the evidence. Documentation is not.
4. Match a file header by comparing bytes. Never use a regexp on binary,
   because `.` in Go matches a rune and not a byte.
5. Do not write validation that compensates for a driver's broken parser.
   Emit the DSN and let the driver reject what it rejects.
6. A `Gen*` func is a helper. Reusing one across databases is normal and is
   not a claim of wire compatibility. What it does claim is that every driver
   behind it accepts the same DSN, so recheck that under rule 3 and split when
   the drivers stop agreeing.
7. Supply a default host and a default port. Do not supply options that change
   how the driver or the database behaves. The calling client owns those. The
   only two in the codebase are `sslmode=disable` for CockroachDB and
   `ServiceName` for DB2 over ODBC.
8. Add a scheme only when the matching driver is in `usql`, or is expected
   there soon. Remove a scheme when `usql` removes its driver. A driver that
   `usql` only demoted to the `bad` build tag is still in `usql`, so its
   scheme stays. See D12, and `CONTRIBUTING.md` in `usql`.
9. When a rule can be tested, write the test in the same commit. Some rules
   here cannot be tested, because the evidence lives in another repository.

## The four files

Adding a scheme touches exactly these, and nothing else:

1. `scheme.go` registers the scheme, its generator, and its aliases.
2. `dsn.go` holds the generator.
3. `dburl_test.go` holds the table driven cases.
4. `README.md` holds the row in the driver table, which `usql` generates.
   Do not edit between the `DRIVER DETAILS` markers by hand.

`dburl.go` holds `Parse`, `Open` and the `URL` type. Changing it means
changing behaviour for every scheme, so do that only on purpose.

## The two URL forms

A standard URL has a host: `scheme://user:pass@host/name`. Most schemes take
this form, and register with `Opaque` set to `false`.

An opaque URL has a path on disk and no host: `scheme:/path/to/file.db`.
Every scheme that takes this form is a database in a file. They register with
`Opaque` set to `true`.

## Go conventions

Match the surrounding code. It is terse on purpose.

Errors are constants of `type Error string`, declared in `dburl.go`, and named
with an `Err` prefix. Do not use `errors.New`. Wrap with
`fmt.Errorf("%w: ...", ErrInvalidQuery)` when the caller needs the detail.

Receivers are one or two letters. A generator takes `u *URL`. Error strings
start with a lowercase letter and do not begin with "failed to".

## Before you commit

All three must pass:

```sh
go test ./...
go vet ./...
golangci-lint run ./...
```

CI runs the tests and the linter. The linter version is pinned, for the reason
in D9.

## Writing documentation

Follow the plain English rules the repository already uses. Short sentences,
active voice, no contractions, and "must" rather than "should". State the
fact and not its importance.

[usql]: https://github.com/xo/usql
[dbtpl]: https://github.com/xo/dbtpl
