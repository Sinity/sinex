# Indexed search

Indexed search makes canonical shell commands (`command.canonical`, see [shell-grouping.md](shell-grouping.md)) searchable by text, project, time and other fields, through the CLI, the JSON-RPC API and an MCP server.

## Configuration

```toml
read_model_connection_file = "/abs/read-model-connection"

[[projects]]
name = "tide"
roots = ["/src/tide", "/realm/worktree/tide-*"]

[[projects]]
name = "tide-tools"
roots = ["/src/tide/tools"]
```

The connection must log in as `sinex_read_model`. Schema bootstrap creates that role with SELECT access and write access only to `core.search_text`, `core.event_project` and `derivation.read_model_position`; set up its authentication like the reader and writer roles. `[[projects]]` and `automata` require `read_model_connection_file`. Without it, raw event reads still work and indexed reads return `partial`.

Projects are assigned from the command's recorded cwd, without touching the filesystem. Roots match whole, case-sensitive path components, and the matching root with the most components wins. A trailing `*` in the last component matches zero or more characters: `/src/tide*` covers `/src/tide` and below; `/src/*` does not cover `/src`. A missing, empty or relative cwd, or one containing NUL or `.`/`..` components, has no project, and so does a tie between projects at the same depth. Changing the project list rebuilds the projection.

## CLI and MCP

```sh
sinexctl --endpoint URL --token-file TOKEN search cargo test --query 'and project = tide since 2026-09-14T00:00:00Z' --json
sinexctl --endpoint URL --token-file TOKEN search 'cargo test' --json
sinexctl --endpoint URL --token-file TOKEN query 'commands where needle and exit_code != 0 sort when desc limit 20' --json
sinexctl --endpoint URL --token-file TOKEN mcp
```

Separate `search` arguments are separate words; one argument containing whitespace becomes a quoted phrase. `--query` appends clauses. `query.run` and `search` take the query text, `page` (1-100, default 20), `cursor` and `timeout_ms`; deadlines work as in [queries.md](queries.md#deadlines-and-cancellation).

`sinexctl mcp` serves seven read tools over rmcp stdio: `sinex_recall`, `sinex_search`, `sinex_query`, `sinex_show`, `sinex_trace`, `sinex_sources` and `sinex_status`. `sinex_search` takes `words` (shared query text; use double quotes for a phrase) and optional `query`, `page`, `cursor` and `timeout_ms`. MCP `timeout_ms` is 1-10000 (default 10000); use the CLI for longer deadlines. The adapter forwards to the daemon, which does all parsing and masking. Errors are MCP tool errors, never empty results.

## Query language

`commands` selects canonical commands. Bare words and `events where WORDS` search the same population. Raw `events` queries are separate ([queries.md](queries.md)).

- Words, quoted phrases, a trailing word prefix, `and`, `or`, `not` and parentheses (AND binds tighter).
- Fields: `source`, `kind`, `project`, `cwd`, `command`, `exit_code`, `when`, `id`. Text fields support `=` and `!=`; `command` and `cwd` also support contains `~` and prefix `^`. `cwd ^ ROOT` matches whole path components. Integers and times support `= != < <= > >=`; IDs support `=` and `!=`; every field supports `exists`. Field operators must be unquoted.
- Times are RFC3339; `since RFC3339` is supported.
- `sort when [desc]` and `limit N` (1-10000).

Relative dates, globs, value lists, arbitrary payload fields, joins, aggregations and other sorts return `bad_query`. If the first word is ambiguous between a command name and a query unit, the query is refused; quote the word to search for it.

## Text matching and ranking

Text is split at Unicode word boundaries, case-folded and NFC-normalized, without stemming. Every string field contributes its full token sequence. A phrase must match consecutive tokens within one field. Matching uses exact SQL `LIKE` patterns over normalized tokens (with `C` collation), with a trigram GIN index to find candidates, so there is no fuzzy matching and no limit on token count, position or length.

Without `sort`, results are ranked by PostgreSQL `word_similarity` against the positive words of the query (words under `not` or in field conditions do not count), then by time ascending, then by ID. With `sort when`, ranking is off and unknown times sort last. Filtering, counting and ranking run over the whole matching population; short or common words may need a full scan and can hit the deadline.

## Results

Each hit has the canonical interpretation ID, parent IDs, kind, source, time, project, rank and a snippet. The snippet is masked as a whole string first, then cut to a 400-byte window; `spans` gives UTF-8 byte ranges of the matches. If masking changed the text, the spans are cleared and `highlights_unavailable = masked` is set.

Pages read current state; `resolved` gives `commit_seq`, `projection_seq` and the projection generation. Cursors expire after 24 hours, survive compatible restarts, and become invalid when the projection is rebuilt. A traversal stops after `limit` rows (default 10000) with `truncated` set; `total` still counts every match.

`complete` is false when a source's latest read did not finish or shell grouping work is pending. Stored hits stay available with `complete: false` and CLI exit 5.

## Projection maintenance

The read-model owner applies at most 64 commit-log entries per transaction and commits the projection rows with its position. Admission, replay, removal and exclusion all flow through the commit log. A search waits up to 500 ms for the projection to catch up to the current commit, then returns `partial`. Maintenance failures, missing log history or a projection that is not serving return `partial`, never empty success. `status.indexed_retrieval` reports progress, failures and `full_text.available`; when text is unavailable, text queries return `partial` while metadata-only queries still work.

The projection is rebuilt from the full commit log. Retrieval version `sinex.shell-search.v5` and [schema change 7](schema.md) introduced the token-text representation.

`show` reads the canonical event and its parents; `trace` follows parents to their stored material ranges, and CLI `trace --bytes` also returns the masked stored row. Both keep working after a source disappears.
