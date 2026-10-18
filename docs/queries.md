# Raw event queries

`query.run` (CLI `sinexctl query`) reads stored events directly when the query is a valid raw `events` query as described below, or names a raw selector (`payload.*` or `ts_orig`), in which case its errors are reported as raw errors. Raw reads need no indexed projection. Everything else, such as `commands`, bare words, `events where WORDS` and `recall`, goes to [indexed search](search.md) or [sessions](sessions.md).

```sh
sinexctl --endpoint URL --token-file TOKEN query 'events' --json
sinexctl --endpoint URL --token-file TOKEN query 'events where source = terminal.atuin-history and kind = command.executed' --json
sinexctl --endpoint URL --token-file TOKEN query 'events where payload.exit_code = 2' --json
sinexctl --endpoint URL --token-file TOKEN query 'events where payload.command ~ "cargo test"' --json
```

## Supported forms

`events` alone, or `events where` followed by conditions joined with `and`. Fields:

- `source`, `kind`, `id`, `when` (alias `ts_orig`, RFC3339): equality.
- `payload.command`, `payload.url`, `payload.title`, `payload.target_path`: equality or literal substring `~`.
- `payload.profile`, `payload.database_generation`, `payload.guid`: equality.
- `payload.exit_code` (integer) and `payload.redirect` (boolean): equality.

A query is at most 16 KiB with at most 32 conditions. There is no `limit`, `sort`, `or` or pipeline (`events | limit 20`); those return `bad_query`. Chromium redirect hops are excluded unless the query names `payload.redirect`.

## Pages and cursors

Each page holds up to 100 rows ordered by original time (nanosecond precision, unknown time last), then interpretation ID. The response has an exact `total` and a `page` object with `size`, `cursor` (null at the end), `cursor_version` (currently 2), `first_commit_seq`, `expires_at`, `changed_since_first_page` and `change_notice`. Continue with the same query and `--cursor CURSOR`.

Each page reads the current database state; there is no snapshot across pages. A continuation resumes after the last returned key, so rows added before that key are not shown and rows added after it may be. If the commit position differs from the first page, `changed_since_first_page` is true. The cursor carries all its state, so it survives a daemon restart, and it expires 24 hours after the first page. A cursor for a different query, or an expired or older-version cursor, fails as `bad_query`. Reordering or requoting the same conditions is accepted.

`complete` reports whether configured sources finished their latest reads, independently of paging. Stored rows stay queryable when a source is unavailable, with `complete: false` and CLI exit 5.

## Deadlines and cancellation

`query.run` and `search` accept `timeout_ms` from 1 to 3,600,000. The API default is 10,000 ms; `sinexctl query` and `search` send 60,000 ms unless `--query-timeout-ms` is given (the flag is refused on other commands). The deadline covers getting a reader connection and the whole page, including the count. The CLI waits five seconds beyond it for the reply.

A deadline returns error kind `budget` (code -32006, CLI exit 6) with `execution_deadline`, `timeout_ms` and `estimate: null`, and no rows. Object `show`, `trace`, `sources` and `status` have a fixed 10-second deadline.

If the client disconnects, the daemon drops the request and closes that query's PostgreSQL connection; `client_connection_check_interval` is 100 ms, so a blocked statement aborts promptly. Cancellation never reaches the writer. A response is encoded completely before any bytes are sent, so a failure never produces a partial page.
