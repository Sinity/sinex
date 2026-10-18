# Source status

`sources.list` (CLI `sinexctl sources`, MCP `sinex_sources`) reports each configured input's latest read, its acquisition history and its record errors. It describes what Sinex read, not whether the underlying activity was captured in full.

```sh
sinexctl --endpoint URL --token-file TOKEN sources --json
sinexctl --endpoint URL --token-file TOKEN sources --after-input UUID --json
sinexctl --endpoint URL --token-file TOKEN sources --input UUID --before-seq SEQ --json
sinexctl --endpoint URL --token-file TOKEN sources --input UUID --observation UUID --after-receipt UUID --json
```

## Parameters and pages

Optional parameters: `input`, `after_input`, `before_seq`, `observation`, `after_receipt`. Inputs, observations and receipts are UUIDs; `before_seq` is a positive integer. History detail needs `input`; receipt paging also needs `observation`. `input` and `after_input` cannot be combined. Unknown parameters are rejected.

A response holds at most 16 inputs, each with at most eight observations and 16 receipt references. Continue with `page.next_after_input`, `history.next_before_seq` (toward older observations) or `receipts.next_after_receipt`; `complete` is false when rows were left out. Each page reads current state.

## Latest run

Each input's `last_run` includes `cursor` (the committed admission summary, or null) and `first_record_error`. When there are record errors, `first_record_error` names `observation_id`, `material_id`, `anchor_byte`, `end`, `reason`, `representation` and the first unparsed `record`, read from stored material and masked like `trace --bytes`. If those bytes cannot be read, `evidence_status` is `unavailable` and `record` and `end` are null. `last_success` and `last_failure` point to the latest successful and failed observations.

`acquisition.state` is `reading`, `completed`, `unavailable`, `interrupted` or `unknown`. For Atuin, a completed read with a planned next start becomes `overdue` once that time passes. The schedule is stored as `_sinex_acquisition_v1.schedule` with `settled_at` (when the collector saw the poll finish), `next_start_at` and `interval_ms`. `observed_at` stays the poll start. Older observations without this metadata report `schedule_evidence = unknown`. A schedule from an earlier writer run, or for an input that is no longer configured, is reported as historical with current state unknown.

## Receipts

Receipts list the interpretations a poll newly admitted, including removed readings, as `event`, `material`, `start`, `end` and `record_state`. They are not execution counts. Replayed interpretations are not receipts of the original read. Older observations without material ownership report unknown receipt completeness.

## Deadlines

The main source query has a two-second statement timeout. A cancelled statement returns `budget` with `reason = statement_cancelled` and `configured_statement_timeout_ms = 2000`. Per-input history and receipt detail uses a 100 ms statement limit and a 200 ms total budget; when that runs out, `inspection.status` is `unavailable` and the latest-run data is still returned. Retry with the `input` selector.
