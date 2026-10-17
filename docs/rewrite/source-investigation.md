# Configured Atuin acquisition investigation

The existing `sources.list`, CLI `sources` and MCP `sinex_sources` inspect retained acquisition evidence. They preserve `last_run`, `cursor` and the first retained record diagnostic. Source acquisition outcomes describe reads and admitted interpretations. Event freshness, complete historical capture and human activity remain unknown.

## Clocks and acquisition state

Atuin `observed_at` remains the attempt's start and original reading order. After the poll returns, including its final writer reply and material closure, the collector samples its settlement observation and chooses its next start. Versioned `_sinex_acquisition_v1.schedule` metadata stores `settled_at`, `next_start_at` and `interval_ms`. Settlement is an observed acknowledgement, not an exact provider failure or database commit timestamp. The paired monotonic timer waits to that selected target, including time spent committing schedule metadata. Wall-clock changes can affect comparisons; the timer itself uses monotonic time. Checked arithmetic refuses unsupported intervals or clock overflow.

`acquisition.state` reports `reading`, `completed`, `unavailable`, `interrupted` or `unknown`. A completed pass with a valid planned next start becomes `overdue` strictly after that target, even with identical retained observations and event times. Its cause remains unknown. A failed outcome keeps its observed reason; a reading poll has no fabricated completion deadline. `schedule_evidence = unknown` discloses a missing or unsupported schedule. A legacy read preserves its recorded outcome in `last_run` without establishing current scheduling health. No initial deadline is inferred. Live classification requires the server-owned configured Atuin locator and the observation's actual writer run ID to match. A plan from an older run or now-unconfigured input remains labelled historical under `schedule_applicability`, with current acquisition state unknown. Source request parameters cannot override this basis.

The writer records scheduling metadata changes at a new canonical commit position. Existing raw continuation therefore notices these source-visible changes conservatively. Poll-start clocks, original stat/acquisition context, source provenance, event identity and retained bytes remain intact.

## Bounded inspection

The request accepts optional `input`, `after_input`, `before_seq`, `observation` and `after_receipt`. Input/observation and receipt identities are UUIDs; history sequences are positive integers. Detail requires `input`; receipt continuation also requires `observation`. `input` and `after_input` conflict. Unknown parameters refuse.

```
sinexctl --endpoint URL --token-file TOKEN sources --json
sinexctl --endpoint URL --token-file TOKEN sources --after-input UUID --json
sinexctl --endpoint URL --token-file TOKEN sources --input UUID --before-seq SEQ --json
sinexctl --endpoint URL --token-file TOKEN sources --input UUID --observation UUID --after-receipt UUID --json
```

Each response contains at most 16 inputs. Each input includes at most eight observations and 16 acquisition receipt references. `page.next_after_input`, `history.next_before_seq` and `receipts.next_after_receipt` give continuation values; `complete` discloses omitted rows. History proceeds toward older sequence numbers; receipts proceed by interpretation ID. Selecting an observation inspects its receipts independently of the history page. MCP advertises the same named arguments. Pages read their own current snapshot, rather than a pinned cross-request history.

`last_success` and `last_failure` resolve constant-size writer-maintained outcome references through the observation primary key. New observations inherit these references through the indexed latest input/sequence lookup. Repeated successful polls preserve the last failure. Missing predecessor summaries remain explicitly unknown under `outcome_reference_evidence`; current outcomes can establish their own side without fabricating older history. No background history scan or migration fills absent evidence.

The primary source query has a two-second PostgreSQL statement budget. Optional per-input history/receipt inspection uses a savepoint, a 100 ms statement budget and a 200 ms aggregate deadline, within the existing RPC deadline. Budget failure returns `inspection.status = unavailable` and incomplete detail while preserving primary latest-run/acquisition evidence. Retry with the input selector. A failed primary read or lost database connection still returns the existing typed read failure. Row caps bound materialized results, not an assertion that every database plan scans a fixed number of rows. Latest/history are keyed by the existing input/sequence uniqueness; outcome references are primary-key lookups. PostgreSQL may choose sequential scans on small relations; the retained plans disclose its actual choices. Receipt joins retain the statement budget because no new receipt index or schema is introduced. The public fixture retains actual query plans and a blocked optional-receipt control.

## Exact acquisition receipts

Receipts identify newly admitted interpretations, including retained versions marked removed. They do not count executions or distinct newly discovered native occurrences. Each reference names `event`, `material`, `start`, `end` and `record_state`; it hydrates no command bodies. Ordinary `show` and `trace --bytes` retain authority over eligibility and protected contents.

Modern capture material belongs to one observation. Terminal unavailable reads close their acknowledged partial part; a later observation closes any older capturing part before opening its own. Existing acknowledged spans, identities and original observation ownership stay intact. Receipt selection requires both material ownership and an original capture interpretation with `created_by_operation IS NULL`. Later retained replay uses an operation identity and cannot become that original acquisition's recovery. Unmarked legacy observations report unknown receipt completeness. This is compatible metadata/read behavior with no canonical DDL, schema fingerprint or upgrade change.

## Qualification

`source_investigation_acceptance` uses development clients in the existing isolated PostgreSQL harness. `source_investigation_package_acceptance -- --package-pin FILE` requires an explicitly bound immutable current-candidate package and never builds or chooses a fallback. The packaged route exercises actual CLI and MCP discovery/inspection, first failure and blocked empty read, unchanged rereading, an overdue held collector with responsive API, restart without a provider, unchanged repair, offline fresh-ID catch-up, retained reads after provider loss, damaging replay, partial failed-pass ownership, pagination and unknown legacy metadata. Its 19 inputs are acquired sequentially through one configured Atuin collector across daemon runs; they are not 19 concurrent live sources. Older input plans cannot become current overdue.

The existing explicit qualification arguments accept `collector-scheduled`: one collector waits at its recorded schedule until the owned Unix socket releases it with `c`. Writer and API continue normally. Disconnect/refusal/timeout fails the collector; shutdown cancels the hold. No ordinary configuration enables this control.

A competent baseline reads the same supplied synthetic timestamped acquisition journal and the exact provider through read-only SQLite, with the same explicitly supplied retained synthetic fixture backup. Fixture backups are audit inputs, not a claim of native physical SQLite snapshot support. The qualification independently computes nine phase answers through the journal/provider route and compares working, cause, labelled time and recovered membership with the native answers. It records commands, retained-input availability and interventions, preserving ties and unknowns. It establishes no measured human-effort advantage or new journal service. Earlier configured-continuation run 10392 remains separately qualified under its original package; it is neither rerun nor requalified for this candidate. Full M0/M1, historical coverage, event-quiet notifications, active-time inference, installed acceptance and daily capacity remain open.
