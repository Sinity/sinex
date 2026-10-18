# Schema

`sinex-store` declares the whole PostgreSQL schema in Rust. `sinexd admin schema` applies and checks it without a running daemon and without compiled queries, so it can bootstrap the database the daemon is built against.

Events live in `core.events` and `reflection.events`, UUIDv7-partitioned TimescaleDB hypertables with occurrence side tables and an archive view. The schema check compares indexes, triggers, functions, views, Timescale settings and role grants with the declared catalog and fails on any difference, without resetting data.

## Commands

```sh
sinexd admin schema --connection-file /abs/admin-connection            # create, or inspect an existing database
sinexd admin schema --connection-file /abs/admin-connection --check    # inspect only
sinexd admin schema --connection-file /abs/admin-connection \
  --apply --dump-file /abs/new-backup.dump                              # upgrade a populated database
```

Inspection reports `checked_changes` and, when an upgrade is waiting, `pending_changes`; it changes nothing. A fresh database gets the same catalog as an upgraded one.

## Upgrades

Upgrades are a fixed, ordered list of changes (`schema_changes()`). Sinex recognizes the exact schemas it has published before and refuses anything else, including changed definitions, missing objects, unsafe grants or a change history that is not a prefix of the list. There is no general migration framework and no reset.

`--apply` takes the same admin connection as bootstrap. It locks out other schema owners and the writer, writes a custom-format `pg_dump` to a new mode-0600 file (dump stderr goes to the adjacent `DUMP.stderr`), and fsyncs both before the first change. If the dump fails or is empty, nothing is changed; use a new path to retry. Each change commits its schema, progress and definition record together, so an interrupted upgrade resumes where it stopped. Running it again after completion does nothing. The dump receives its connection settings through libpq environment variables, never on the command line, and TLS certificate and key inputs must be files.

While changes are pending, `serve` opens only the reader: `status` lists the waiting changes, and raw queries naming a non-canonical source or kind, `show` and `trace` still work. After `--apply` completes, `status` reports `restart_required`; restart to start the writer.

## Changes

1. Widens stored-file and two-parent shell constraints, registers zsh events and creates the applied-change records.
2. Adds the dirty position for shell grouping and its progress row, and seeds existing Atuin events for regrouping. Canonical events use payload schema 5; schema 3 rows remain valid.
3. Registers shell sessions and seeds dirty days from existing canonical commands ([sessions.md](sessions.md)).
4. Registers Chromium event types and payload schemas ([chromium.md](chromium.md)).
5. Makes the search text column nullable and adds an index for rows whose text is unavailable ([search.md](search.md)).
6. Allows signed Atuin exit statuses in payload schema 2 ([atuin.md](atuin.md)).
7. Replaces the search vector column with normalized token text and a trigram index, and marks the projection for a rebuild under retrieval version `sinex.shell-search.v5`.

No change rewrites existing events, stored bytes or provenance. Old writers refuse the upgraded catalog.
