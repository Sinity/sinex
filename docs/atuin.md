# Atuin source

The Atuin collector reads an Atuin history SQLite database and admits each row as a `command.executed` event from source `terminal.atuin-history`.

## Configuration

```toml
sources = ["terminal.atuin-history"]

[atuin]
path = "/abs/history.db"
poll_interval_ms = 30000      # 100..=3600000, default 30000
full_interval_ms = 86400000   # 100..=604800000, default one day

[[privacy_rules]]
pattern = "tok_[a-z0-9_]+"
replacement = "[withheld]"
created_by = "operator"
```

`sources` and `[atuin]` must agree. Privacy rules are Rust regular expressions with literal replacement text, at most 32. They run once when an event interpretation is admitted (including replay), and later reads return the stored values. Privacy rules cannot be combined with the zsh source. Every response is also masked by the fixed [credential output filter](credential-output.md).

## Reading

The collector opens the database read-only (never creating it) and keeps the connection between polls. Each poll reads inside one SQLite read transaction.

- A full read compares every row. It runs on the first poll, after every reopen or restart, and when `full_interval_ms` has elapsed since the last successful full read. Reduced polls do not postpone it, and a backward wall-clock change cannot delay it because the deadline is also kept on a monotonic clock.
- Between full reads, a reduced poll reads rows above the last acknowledged rowid plus rows that were unfinished when last admitted, however old.
- The connection is dropped and the next poll does a full read when the file is renamed or replaced, a symlink is retargeted, the schema or version cookie changes (including after `VACUUM`), or a read fails.

Before each poll opens SQLite, the writer commits a `reading` observation, so queries report incomplete coverage while a read is blocked. Only reaching the end of the table counts as a completed read; open or read errors mark the source unavailable. With no inputs configured or stored, empty coverage is complete.

Each writer step holds at most 64 rows and 1 MiB. A single row larger than that leaves the poll unavailable. The query that finds previously unfinished rows has a two-second timeout. No indexes are added to the Atuin database.

## Events

Every row read, including unchanged rows, is stored as sorted-key JSON with an `_from` field naming the observation, table and rowid. The occurrence key is the registered input plus the Atuin row ID; rowid is only a read position. Fingerprints exclude `_from`.

- `duration = -1` with `exit = -1` reports `completion = end_not_recorded`: no end was recorded as of this read. It does not mean the command is still running.
- A non-negative duration, or any exit status other than `-1`, records an end.
- Exit `-1` maps to null. Other signed 64-bit values, including `-2`, are kept exactly; only zero means success.
- Timestamps and durations keep nanoseconds: the PostgreSQL timestamp holds microseconds and `ts_orig_tiebreak` holds the rest.

The parser recipe is `atuin-v3`. A row is suppressed as a duplicate only when both its source hash and recipe match the stored reading. A newer read under a changed recipe readmits the row and archives the old interpretation. An older observation never replaces a newer one. Soft-deleted rows become removed readings: kept in history, hidden from ordinary queries.

## Storage and crash recovery

Rows go to an open part file that is fsynced before the writer transaction commits the events, record failures, stored length, observation counts and cursor. Parts close into the blob store when a poll completes or at 100 MiB. On startup an interrupted part is truncated to its committed length and closed as partial. The collector then rereads from the start and duplicate suppression prevents repeated events. If the writer dies after commit but before replying, the committed events and their stored coordinates survive restart.

When the Atuin database disappears, stored events, `show` and `trace --bytes` keep working; queries return incomplete coverage (CLI exit 5) and `sources` reports the source unavailable. Unavailable polls never create a replacement database.

The `writer_kill` scenario (`xtask scenario writer_kill`) covers both: it kills the writer at each pause point, restarts it with and without the Atuin file, and compares the result with an uninterrupted run.

A full read is not a snapshot of the SQLite file, and a row missing from the provider is never admitted as a deletion.

## Exclusion

A listed [exclusion](exclusion.md) suppresses admission of its occurrence while the rows are still stored. `sources` counts them as `excluded`, so `rows_read = admitted + repeated + excluded + record_errors`.

See [queries.md](queries.md) for reading events and [sources.md](sources.md) for source status.
