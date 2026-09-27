# Contributing to dburl

Read three things before you change anything.

[`AGENTS.md`](AGENTS.md) holds the rules: the standing rules, the hard rules,
the Go conventions and the checks to run. It is written for an AI coding
agent, and everything in it applies to a person too. `CLAUDE.md` holds one
line that imports it, for Claude Code.

[`docs/SCHEME.md`](docs/SCHEME.md) is every step for adding a database scheme,
in order. Most changes here add a scheme or change a generator.

[`docs/PLAN.md`](docs/PLAN.md) holds every decision this project made, with the
reasoning and what was rejected. The index at the top lists each decision
with its status. Read the status, because some decisions amend an earlier
one.

Do not decide an open question on your own. Ask Ken.

## Before you send a change

Run these three commands in the repository root. All three must pass:

```sh
go test ./...
go vet ./...
golangci-lint run ./...
```

If you changed the scheme registry, regenerate the driver table in
`README.md` first:

```sh
go run gen.go
```

## Agent skills

The repository carries two agent skills. A skill is a set of instructions
that a coding agent loads for a task. `simple-english` sets how prose is
written, and `go-pedantry` sets how Go is written.

`skills-lock.json` names the source of each skill. The `skills` command from
npm writes that file, and version 1.7.0 is the one measured. It writes each
skill into two folders. Codex and the other agents read
`.agents/skills/<name>`, and Claude Code reads `.claude/skills/<name>`.

To install or update a skill, run its command in the repository root:

```sh
npx skills@1.7.0 add AminBlg/SimpleEnglish --skill simple-english --agent codex claude-code --copy -y
npx skills@1.7.0 add oborchers/fractional-cto --skill go-pedantry --agent codex claude-code --copy -y
```

Keep `--copy`. Without it, the command writes `.claude/skills/<name>` as a
symbolic link. A Windows checkout writes a symbolic link as a text file, and
Claude Code then loads no skill and reports nothing. `TestSkillsAreCopies`
fails on a link, and it fails when the two folders differ. See D27.

`.claude/settings.local.json` holds the Claude Code permissions of one
person. The root `.gitignore` ignores it.
