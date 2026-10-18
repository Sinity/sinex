# Atuin replay

Replay reparses one closed stored Atuin material with the current parser and replaces its events through the writer. It works without the original Atuin database. API methods: `replay.plan`, `replay.start`, `replay.status`, `replay.cancel`.

## Access

```toml
operator_token_file = "/abs/operator-token"
```

The operator token must differ from `token_file`. The read token can plan replays and read status; starting and cancelling need the operator token. The server records the principal `foundation.operator`. Unfinished replays resume on startup only while `operator_token_file` is configured. `status` shows `replay_authorized` and recent outcomes.

## Usage

```sh
sinexctl --endpoint URL --token-file /abs/read-token replay 'materials where id = MATERIAL_UUID' --dry-run --json
sinexctl --endpoint URL --token-file /abs/operator-token replay 'materials where id = MATERIAL_UUID' --action-id ACTION_UUID --json
sinexctl --endpoint URL --token-file /abs/read-token replay-status OPERATION_UUID --json
sinexctl --endpoint URL --token-file /abs/operator-token replay-cancel OPERATION_UUID --json
```

The only selection is `materials where id = UUID`, with at most 4,096 stored records. There are no parser overrides.

`--dry-run` reports `selected`, `would_admit`, `would_archive`, `excluded` and chunk counts, and a `basis` that identifies the stored ranges, their current interpretations, exclusion state, the parser and the current privacy rules. Pass it back with `--basis BASIS` (API `expected_basis`) to require that nothing changed since the preview; a mismatch is refused before any effect. A replay without `--basis` resolves the selection itself.

If `--action-id` is omitted, the CLI generates one and prints it on stderr. Keep it: retrying the same action ID returns the existing operation, and the same ID with a different request conflicts. The CLI polls until the operation finishes. Disconnecting does not stop it; `replay-cancel` stops it between chunks, keeping chunks already committed. There is no undo.

## Behavior

Replay processes chunks of at most 64 records and 1 MiB. The whole selection is validated before the first replacement, and each chunk is checked again before it commits. Missing, corrupt or malformed stored records refuse the replay. Each chunk transaction archives the old readings, admits the new ones, updates live keys and the commit log, records an effects manifest in `core.operation_effects`, and advances progress.

New events cite the same stored material and range. Their IDs, processing time and `created_by_operation` change, even when the content is equal. Replay also restores valid rows that an older parser omitted, admits removed rows as removed readings, and skips excluded occurrences while still advancing progress. A row whose occurrence is currently read from another material is replaced only when the source fingerprint is equal; otherwise the replay is refused.

Progress (`processed`, `next_anchor`, `steps`) is separate from the `archived` and `admitted` counts.

## Outcomes

States are `recorded`, `running`, `completed`, `refused` and `cancelled`; `complete` is true only for `completed`. CLI exit codes: 0 completed, 2 invalid syntax, 4 refused, 5 cancelled or still running. A transport error means no reply arrived, not that nothing was committed: retry with the same action ID or check the operation.
