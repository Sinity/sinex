# xtask

`xtask` is the primary automation surface for local Sinex work. It owns the
default verification loop, local infra orchestration, generated agent/docs
surfaces, and several analysis/debugging workflows that are specific to this
workspace.

The devShell exposes plain `xtask`. When you invoke it, the checkout-local
binary is built if necessary and then executed from the workspace target
directory, so edits to `xtask` itself are picked up normally during
development.

## Start Here

- [`command-guide.md`](command-guide.md): concise, high-signal operator guide for the commands humans and agents should actually remember and reach for.
- [`command-reference.md`](command-reference.md): generated public command tree with the current flags and subcommands.
- [`verification.md`](verification.md): performance contracts, verification lanes, and CI-parity details.
- [`../docs/proof-catalog.json`](../../docs/proof-catalog.json): generated proof graph joining Rust descriptors, payload inventory, command metadata, and scenario annotations.
- [`../.config/ast-grep/README.md`](../.config/ast-grep/README.md): generated rule catalog for the repo's `ast-grep` policy surface.
- [`sandbox/README.md`](sandbox/README.md): test harness architecture and sandbox patterns.
- [`commands/jobs.md`](commands/jobs.md): background job model and how it differs from history.
- [`commands/deps.md`](commands/deps.md): dependency analysis subcommands and output.

## Mental Model

- **Core loop**: `xtask check`, `xtask fix`, `xtask test`, `xtask work`, and `xtask build`.
- **Runtime/infra**: `xtask infra`, `xtask run`, `xtask status`, `xtask doctor`, `xtask jobs`, and `xtask reset`.
- **Investigation**: `xtask history`, `xtask analytics`, and `xtask deps`.
- **Docs/context**: `xtask docs sync`, `xtask docs check`, `xtask docs agents`, `xtask docs proof-catalog`, `xtask docs ast-grep-catalog`, `xtask docs schema-bundle`, and `xtask docs snapshot`.

`xtask exercise` remains a command-contract and xtask-surface regression runner.
Product/runtime semantics should live in Rust tests, proof-carrying scenarios,
benchmarks, or VM tests instead of new exercise entries.

The generated command guide is the selective memory surface. The generated
reference is the live public command tree. Together they replace hand-maintained
pointer docs and keep the command surface aligned with the clap definitions.

## Generated Surfaces

`xtask` owns these generated docs artifacts:

- `AGENTS.md`
- `xtask/docs/command-guide.md`
- `xtask/docs/command-reference.md`
- `docs/proof-catalog.json`
- `.config/ast-grep/README.md`
- the checked-in schema bundle under `schemas/`

Refresh them together with:

```bash
xtask docs sync
```

Check for drift without rewriting files with:

```bash
xtask docs check
```

Use the narrower `xtask docs agents` path only when you changed `CLAUDE.md` or
its transcluded includes and only need the local agent surface refreshed. Use
`xtask docs ast-grep-catalog` when you only changed `.config/ast-grep/rules/`
and want the rendered rule catalog refreshed. Use
`xtask docs schema-bundle` when you only need to refresh the tracked JSON schema
contract bundle.

## Output Modes

Every xtask command supports the shared global output controls documented in the
generated reference. In practice:

- prefer `--json` or `--format json` when another tool will parse the result
- use `--bg` for long-running work you want to observe through `xtask jobs`
- use human output for interactive inspection
