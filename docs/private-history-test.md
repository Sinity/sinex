# Private history acceptance test

The ordinary suite uses synthetic inputs. The ignored test
`an_external_history_survives_a_stopped_admission_and_remains_queryable` also
accepts an explicitly prepared private Atuin snapshot. It interrupts the real
binary during admission, restarts it, compares captured commands with
independent answers, checks ordinary queries for the total and one
independently chosen row, then reads the copy again.

Keep the fixture outside the checkout in a private directory. Supply
`history.db`, copied consistently through SQLite's backup API, and
`expected.json` prepared independently from that copy. Its `commands` object
maps each present row's `id` to its exact `command`, selected with:

```sql
SELECT id, command FROM history WHERE deleted_at IS NULL ORDER BY id;
```

For example, a synthetic answer file is:

```json
{"commands":{"opaque-id":"printf 'synthetic command'"}}
```

Run the selected case through the declared development-shell operation:

```sh
agentctl job start sinex exec --workspace /path/to/checkout -- \
  env SINEX_TEST_HISTORY_DIR=/path/to/private-fixture \
  cargo run --locked --quiet -p xtask -- test -p sinexd \
  --run-ignored only \
  -E 'test(an_external_history_survives_a_stopped_admission_and_remains_queryable)'
```

The fixture and expected answers stay outside Git. The comparison assertions
report counts and omit command text.
