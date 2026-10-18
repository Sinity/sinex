# Sinex

Sinex is a durable record of one person's digital life. It keeps the exact
bytes of everything its sources observe, turns those bytes into timestamped
events, builds derived knowledge from the events in layers, and answers
questions about all of it. Stored bytes are the record of truth: every event
points at the byte range it came from, or at the events it was derived from,
and can be read again when a parser or rule changes.

This branch is the second rewrite, built to the target architecture. The
foundations are written in full; the rest of the workspace compiles with
`todo!()` bodies that name the architecture section they implement.

## What works

- The schema, declared in Rust and applied by `sinex-schema apply`.
- Admission: one set-based path for every event, with one live reading per
  record location or equivalence key, newer readings replacing older ones,
  hide, unhide and excision remembered across re-imports, and events and
  source progress committed together.
- The writer task, with live, derived and bulk priorities.
- The source framework: the parser trait, the drain, and the `AppendFile` and
  `SqliteRows` drivers, with Atuin and zsh history on them.
- `sinexd backfill`: read sources to the end through the writer and report
  counts and rates.

## Build and test

```bash
nix develop --command cargo run -p xtask -- check   # fmt, boundaries, clippy against the schema database
nix develop --command cargo run -p xtask -- test    # every test under nextest; database tests clone the schema template
nix develop --command cargo run -p xtask -- db ensure   # the schema database's URL, for an editor
```

A first import of the operator's shell history:

```bash
sinexd backfill --database URL --store DIR \
  --atuin ~/.local/share/atuin/history.db --zsh "$HISTFILE" --host HOST
```
