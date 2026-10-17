# Indexed shell retrieval subset

This native slice makes retained shell commands searchable through the packaged daemon, CLI and official `rmcp` stdio transport. It implements a registered subset of current `spec/query.md` §5, `spec/derivation.md` §1.1 and §7.3.1, and `spec/surfaces.md` §3-4 at specification revision `83f3041`. It advances slice 1a; it does not close the full M1 outcome, the general query language, general fusion or operational acceptance on a private corpus.

## Enable and use

Add an explicit projection credential and project roots to the ordinary daemon configuration:

```toml
read_model_connection_file = "/absolute/read-model-connection"

[[projects]]
name = "tide"
roots = ["/src/tide", "/realm/worktree/tide-*"]

[[projects]]
name = "tide-tools"
roots = ["/src/tide/tools"]
```

The connection must authenticate as `sinex_read_model`. Canonical schema bootstrap provisions that unprivileged role and grants it SELECT plus writes only to `core.search_text`, `core.event_project` and `derivation.read_model_position`. Configure its authentication through the isolated database owner as for the reader and writer; the daemon never copies another role's credential or assumes a role. It holds one restricted connection and a separate singleton advisory lock. Reader and model connections must see the actual writer run identity, so another database with the same schema is refused. It cannot admit or archive events. Existing configurations without this credential retain raw event reads; indexed reads return `partial`, never an empty substitute.

Projects use source payload paths, without filesystem or Git access. Roots match whole case-sensitive path components; the matching root with the greatest component count wins. A trailing `*` within a present final component matches zero or more characters (`/src/tide*` includes `/src/tide` and its descendants; `/src/*` excludes `/src`). Missing or empty cwd, relative paths, NUL and dot/dot-dot components are unknown and cannot produce a positive project. Distinct projects tied at greatest component depth are unknown; repeated roots for one project are harmless. The indexed contract version binds this meaning, so a version change reconstructs existing project projections. This subset implements `cwd` roots, not title rules or other source paths. Unknown configuration keys refuse. A list change reconstructs the projection without reminting events.

```sh
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token search cargo test --query 'and project = tide since 2026-09-14T00:00:00Z' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token search 'cargo test' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token query 'commands where needle and exit_code != 0 sort when desc limit 20' --json
sinexctl --endpoint http://127.0.0.1:41037/rpc --token-file /absolute/token mcp
```

Separate CLI arguments mean separate words. One argument containing whitespace becomes a quoted phrase. `--query` appends shared query clauses. `query.run` and `search` take query text, optional `page` (1-100; indexed default 20), `cursor`, and `timeout_ms`. CLI execution defaults to 60 seconds; API to 10 seconds. The CLI's `--query-timeout-ms` is separate from its transport allowance.

MCP advertises exactly `sinex_search`, `sinex_query`, `sinex_show`, `sinex_trace`, `sinex_sources` and `sinex_status`. `sinex_search.words` is shared query text, including literal double quotes for a phrase; optional `query` appends clauses. Search/query accept `page`, `cursor` and `timeout_ms` (1-10000). A longer MCP execution request refuses and advises the CLI. Other planned tools are not advertised. The adapter calls the configured authenticated daemon and forwards its structured document; it has no database access, query parser, evaluator or masking implementation. Captured text is untrusted data. Transport or application errors are MCP tool errors, never successful empty hits.

## Supported question and answer

Bare words and `events where WORDS` use search-result policy over eligible canonical shell commands. `commands` selects `command.canonical`. The existing raw `events` conjunction subset with `payload.*` selectors remains a separate read path, described in [Atuin](atuin.md). Explicit raw observations are not a hidden search fallback.

The indexed binder accepts words, quoted phrases, trailing word prefixes, `and`, `or`, `not` and parentheses with AND precedence. It binds the complete question before acquiring a reader connection. Field operators must be unquoted; quoted text remains valid as a value. Registered fields are `source`, `kind`, `project`, `cwd`, `command`, `exit_code`, `when` and `id`. Text equality/inequality is exact; `command` and `cwd` also support normalized contains `~` and prefix `^`. A `cwd ^ ROOT` uses whole case-sensitive components, never SQL pattern wildcards. Integer/time comparisons support `= != < <= > >=`; IDs equality/inequality; every field supports `exists`. Comparisons with null remain unknown under NOT. Time bounds require RFC3339; `since RFC3339` is supported. Relative/logical-day syntax, glob/list filter values, arbitrary payload fields, joins, historical reads, aggregations and other sorting refuse. First-word command/unit ambiguities refuse; quoting searches the literal word.

Text tokenization uses Unicode word boundaries; both query tokens and indexed tokens use explicit full non-Turkic case folding and NFC, without stemming. The dependency lock and projection configuration bind the Unicode table versions. ASCII hexadecimal lexemes encode the normalized Unicode tokens into PostgreSQL `tsvector` and `tsquery`, preserving positions. PostgreSQL's locale, default text-search configuration and stemmers therefore cannot change membership. Prefixes act on normalized token bytes. Field boundaries have a positional gap, so a phrase cannot bridge fields. Every string payload field is included. Exact-position limits are 16000 token positions, at most 256 occurrences of one token, and at most 900 UTF-8 bytes per normalized token. An event outside these limits stops projection maintenance and makes search unavailable; it is never silently shortened.

Without `sort`, queries containing positive words rank every semantically selected match by PostgreSQL `ts_rank`, then original time ascending at nanosecond precision, then interpretation ID. `sort when [desc]` disables ranking; unknown time sorts last in either direction. Filters and exact count execute in SQL over the projection. Only the selected page's event IDs join event payloads. GIN full-text and trigram indexes and a time index are separate from compressed event storage. No newest-match cap, sampled ranking, hidden raw scan or custom universal evaluator exists. The full document-frequency cost planner and corpus-scale performance evidence remain open.

Hits include canonical interpretation ID, parent observation IDs, kind/source/time, project, rank and a snippet. A snippet is cut to a 400-byte window only after its complete logical source string passes daemon masking, so clipping cannot detach a credential from its recognition context. It carries one source string plus UTF-8 byte ranges in `spans` marking matches; clients render those ranges as emphasis. This preserves each logical string for credential recognition. If presentation masking changes the snippet, its ranges are cleared and `highlights_unavailable = masked` is explicit. Indexed payloads and snippets use the values already transformed at interpretation admission. The daemon applies the fixed credential floor to the whole reply. The same rows, snippets and coverage qualification reach CLI/API/MCP.

Each indexed page is a fresh repeatable-read state. Its `resolved.commit_seq`, `projection_seq` and generation disclose its basis. A current-state cursor binds the structural bound question, generation, ordering key, first resolved clock and emitted limit progress; it expires after 24 hours. Compatible restart preserves continuation, while project/configuration reconstruction invalidates the old generation. A changed commit reports that fact; additions or moved ordering/rank keys can be absent or repeated across pages. No transaction remains open between requests. The default maximum traversal is 10000 rows, exposed through `truncated` when reached; an explicit `limit` may be smaller. Totals still describe the whole selected population, not only the page or remaining traversal.

`complete` reports source-read availability separately from indexed correctness. `coverage` states that retained observations do not prove continuous capture. A completed empty poll, a complete projection, or an exact count cannot establish that nothing happened. Configured unavailable sources can leave retained hits usable with `complete = false` and CLI exit 5.

## Canonical evidence and maintenance

The [selected Atuin/zsh grouping contract](shell-fusion.md) owns canonical parent selection and bounded replacement. Atuin-only inputs form real singleton canonical events; two-source inputs use the registered policy, with exact per-field and temporal evidence. Program/action normalization retains the declared program/action and mutation lists; unsafe program summaries remain unknown so normalization cannot detach credential context. Shell parse failure leaves the optional summary unknown while the full command stays searchable.

Admission, replay, removal and exclusion dirty the existing writer's reconciliation work. Replacement can temporarily retain qualified older evidence; every read fences canonical rows whose raw parents are currently archived or removed. Search still requires a serving, caught-up, failure-free projection. Pending or failed fusion explicitly makes answer completeness false and discloses its progress/failure. It never becomes complete stale or empty success.

The read-model owner processes at most 64 commit-log transitions per transaction. Projection rows and its position commit together. Admission upserts one event's projection; archive removes it. The maintainer indexes the writer's live canonical bodies until each canonical archive log arrives. Search separately applies the current raw-parent eligibility fence, including while bounded replacement leaves older canonical bodies live. Archived bodies supply no text. Before publishing catch-up, it checks equality with the complete eligible canonical ID population in its snapshot. Missing admission history cannot publish a complete empty index. Normal search requires a serving, failure-free projection exactly at that page's committed frontier; pending canonical work is disclosed in partial answers. It allows a short settle interval within the execution budget, then returns a typed partial error if still behind. Failure never returns rows or an empty success. Status names current progress and maintenance failure.

This store has no log pruning or public purge route. Its supported reconstruction basis is the complete retained log, including from position zero. Project-list changes discard the old serving copy and build non-serving rows until verified catch-up. There is no old-generation serving fallback. Snapshot bootstrap after pruning, registered replacement-generation retention guards, purge/cutover cleanup and arbitrary administrative deletion recovery remain unmet target criteria. They are not claimed by this slice.

`show` reads the canonical object and its parents. `trace` follows all its parents to exact retained material/range coordinates; CLI `trace --bytes` also reads the masked logical row. MCP trace never requests raw bytes. Navigation uses retained evidence after provider loss and restart. SQLite reconstruction and unregistered sources remain unavailable.

Search/query deadlines cover reader acquisition, settle waits, snapshot, count and page. Object show/trace, sources and status use a fixed 10-second whole-request deadline and the same exact-transport read leases; reader connections also have a 10-second statement timeout which query transactions override with their explicit budget. SQL statement timeouts provide the database bound. Client disconnect or MCP cancellation drops the HTTP read and closes its exact PostgreSQL transport; it cannot cancel source or replay commits. The existing fixed output floor remains mandatory. Synchronous presentation/encoding preemption and non-Linux transport cleanup remain unqualified, as in the earlier raw-reader contract.

## Qualification

Declared `indexed_retrieval_acceptance` runs the focused development route; `indexed_retrieval_package_acceptance` selects the actual Nix package. A separate typed SQL control checks unknown-time negation against PostgreSQL values; it does not claim the Atuin parser produces unknown starts. The independent synthetic fixture distinguishes whole-population ranking from newest sampling, words from phrases, prefix from stemming, Unicode case folding/NFC, path components, project depth, nanosecond bounds, unknown exit comparisons and boolean filtering. Real API/CLI and rmcp initialize/list/call exchanges compare identities, snippets and coverage on a stable retained basis.

The explicit `projection-staged-N` and `projection-committed-N` qualification points (N 0-63) use the existing one-shot Unix-socket rendezvous around real bounded projection transactions. The controller observes process/frontier evidence before killing the owned daemon. Earlier commits survive; the held transaction does not advance progress. Source replacement/removal while maintenance is held, same-state restart, retained replay, project-list reconstruction and fresh complete-log reconstruction must converge without duplicate/stale hits. Provider removal tests retained navigation, and real database locks exercise deadline/disconnect/MCP cancellation. Missing-log and credential-presentation controls refuse unsafe or incomplete success. These are synthetic process-kill receipts, not power-loss or full M1 acceptance.

The additional `sinex_recall` adapter uses the same authenticated query owner for the [configured shell-session subset](shell-sessions.md); unconfigured or pending sessions return unavailable.
