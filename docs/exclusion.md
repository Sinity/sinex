# Atuin exclusion

Exclusion hides one Atuin `command.executed` occurrence from normal reads and keeps it from being admitted again; undo removes the exclusion. API methods: `delete.run` and `delete.undo`. Both need the operator token (`operator_token_file`, see [replay.md](replay.md#access)). No stored bytes or interpretation rows are deleted.

```sh
sinexctl --endpoint URL --token-file /abs/operator-token delete RAW_REF --action-id ACTION_UUID --json
sinexctl --endpoint URL --token-file /abs/operator-token delete --undo RAW_REF --action-id ANOTHER_ACTION_UUID --json
```

API parameters: `{ "ref": "sinex:ev/UUID", "action_id": "UUID", "expected_key": null }`. The target is a raw interpretation UUID or `sinex:ev/UUID`, including an archived interpretation of the occurrence. What gets excluded is the occurrence's stable key (input plus Atuin row ID), so two identical commands with different Atuin IDs stay separate. `expected_key` (CLI `--expected-key`) is an optional precondition, at most 2,048 bytes. A canonical reference is refused and the error names its raw parent. A well-formed UUID that names no event records a `not_found` outcome; a malformed reference is `bad_query`.

## Effects

Exclusion inserts a `core.deleted_occurrences` row of kind `exclude`, archives the current interpretation and its canonical children, and records grouping and projection work in the same transaction. The archived readings stay available through `show` and `trace --bytes`.

Later reads and replays of the same key are suppressed: the rows are still stored and counted as `excluded` in `sources`, and replay advances past them without admitting anything.

Undo removes only the `exclude` row. It never unarchives anything or reads the provider. The occurrence comes back on the next normal Atuin read that sees it, or through a [replay](replay.md).

## Action IDs and retries

Delete and undo use the same action ID rules as replay: the CLI generates and prints an ID when none is given, a retry with the same ID returns the original result, and the same ID with a different target, method or expected key conflicts. The CLI checks that the reply matches the requested action, method and target; if it does not, or no reply arrives, it reports an indeterminate outcome (exit 5) and asks you to retry with the same ID. If a committed result cannot pass credential masking, the API returns `partial` with `retained_but_unpresentable` and the action and operation IDs.

The `exclusion` scenario (`xtask scenario exclusion`) covers this.
