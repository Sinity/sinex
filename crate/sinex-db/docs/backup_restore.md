# Backup & Restore

Sinex's primary durability story is **replay from source materials**: every
event traces to a `raw.source_material_registry` row, and the source files
themselves live on disk. A full database loss is recoverable by re-ingesting
those materials through the standard pipeline. The PostgreSQL dump described
below is a defense-in-depth measure for state that does NOT come from source
materials — schema, derived events, operator-only state — not the primary
recovery path.

## The actual mechanism: `sinex-postgres-dump`

Deployment wires a daily logical dump, not a physical/WAL mechanism. This is
defined in the sinnix flake (`modules/services/sinex/bridge.nix`), not in
this repo — sinex itself ships no backup automation.

- **`sinex-postgres-dump.service`** runs `pg_dump --format=custom` against
  the live `sinex_prod` database as the `postgres` user, writing to a
  temp file and atomically renaming it into place on success.
- **`sinex-postgres-dump.timer`** fires daily at `03:12` local time
  (`OnCalendar = "*-*-* 03:12:00"`), with a 20-minute randomized delay and
  `Persistent = true` so a missed run (host asleep/down at 03:12) catches up
  on next boot.
- **Retention**: the service keeps the 14 most recent dumps and deletes
  older ones on every successful run (sorted by mtime, oldest beyond the
  14th trimmed).
- **Staging path**: dumps land at `/realm/staging/sinex-postgres/` as
  `sinex_prod-<UTC-timestamp>.dump` (e.g.
  `sinex_prod-20260731T031200Z.dump`), mode `0600`, directory mode `0700`,
  owned by `postgres`.
- **Lifecycle wiring**: both the service and timer are `PartOf` and
  `wantedBy` `sinex-runtime.target` — they follow the runtime target (not
  the auto-start policy), so they run whenever sinex is up, whether started
  manually or automatically. Neither unit is ever masked.

### Inspecting backup state

```bash
systemctl status sinex-postgres-dump.service sinex-postgres-dump.timer
systemctl list-timers sinex-postgres-dump.timer
ls -lh /realm/staging/sinex-postgres/
```

## When backups matter (and when they don't)

| Loss scenario | Backup is required | Why |
|---|---|---|
| Disk corruption / hardware failure on the DB volume | Yes | Source materials are unaffected, but the schema, derived events (automaton output), and operator-only state (`core.runs`, `audit.archived_events`, `core.tags`, etc.) only live in PG. |
| Accidental schema drop / table truncate | Yes | Same reason. |
| Bad replay or a buggy automaton emitting wrong derived events | No | Use `lifecycle.tombstone.create` to mark the bad operation; replay or re-derive. |
| Single-row corruption from a privacy-policy change | No | Replay the affected source-material slice with new privacy rules. |
| Full host loss including the data volume | Yes, plus source materials | Both layers need restore. Source materials should already be on a separate filesystem or backed up via the operator's regular file-level backup. |

## Restore drill (do this before relying on backups)

A backup that has never been restored is a hope, not a backup. Run this
drill against a non-production database after every change to the backup
configuration.

### 1. Pick the dump to restore

```bash
dump="$(ls -1t /realm/staging/sinex-postgres/sinex_prod-*.dump | head -1)"
echo "Restoring from $dump"
```

### 2. Create a scratch database and restore into it

Never restore over the live database. `pg_restore` targets a fresh,
separate database on the same (or a throwaway) PostgreSQL instance.

```bash
sudo -u postgres createdb sinex_restore_drill
sudo -u postgres pg_restore \
  --dbname=sinex_restore_drill \
  --format=custom \
  --no-owner \
  --jobs=4 \
  "$dump"
```

### 3. Verify parity

```bash
psql "host=/run/postgresql dbname=sinex_restore_drill" <<'SQL'
SELECT
  date_trunc('day', ts_coided) AS day,
  COUNT(*) AS events,
  COUNT(DISTINCT source) AS sources
FROM core.events
GROUP BY 1
ORDER BY 1 DESC
LIMIT 7;
SQL
```

Compare this against the same query on the live DB, or against a known
checkpoint recorded at dump time. Because this is a logical dump (not
WAL-continuous), the restore reflects the database exactly as of the dump's
`03:12` run — there is no point-in-time recovery between dumps. Day counts
should match through the dump time; anything after that is expected to be
missing (and is recoverable via replay from source materials, per the
primary durability story above).

### 4. Tear down the drill database

```bash
sudo -u postgres dropdb sinex_restore_drill
```

A passing drill leaves no production state mutated.

## Known limitations

- **No point-in-time recovery.** `pg_dump` is a logical, point-in-time-of-run
  snapshot — there is no WAL archiving, so recovery granularity is "as of
  the most recent daily dump at 03:12", not a chosen timestamp.
- **Source materials are not part of the dump.** They live on the
  filesystem under `services.sinex.stateRoot` / wherever source contracts
  store originals. Back those up with your usual file-level mechanism.
- **The blob CAS at `services.sinex.storage.blob.repositoryPath` is not in
  the DB dump.** Treat it like source materials.
- **NATS JetStream state lives outside PG.** Lost JetStream state is
  recoverable from `core.events` (already-persisted events) and replay,
  but consumers that were mid-flight will re-deliver from their
  checkpoints.
- **Restore has not been drilled in production as a scheduled, automated
  check** — only the dump side is automated. See `sinex-w98` for making the
  "backups are restorable" claim continuously verified rather than assumed.

## Off-host coverage

`/realm/staging/sinex-postgres/` is on-host staging, not off-host backup by
itself. Per the broader Sinity backup architecture, content under
`/realm/staging/` is periodically drained to `/outer-realm` by borg jobs,
which is what gives these dumps off-host durability against host/disk loss.
This repo does not own or document that drain step; it is host/fleet backup
policy, not a sinex mechanism.

## Acceptance signals

A backup setup is operationally trusted when:

- `systemctl list-timers sinex-postgres-dump.timer` shows a recent
  successful run.
- `/realm/staging/sinex-postgres/` holds up to 14 recent dumps and old ones
  are being pruned.
- At least one full restore drill (`pg_restore` into a scratch database) has
  completed end-to-end with parity verification, and the result was
  recorded in the operator's external runbook.
- The staging directory is confirmed reaching `/outer-realm` via the
  regular borg drain, not just accumulating on-host.
- Source-material and blob-CAS volumes are covered by a file-level backup
  with their own restore drill.

Until all hold, treat the deployment as *durable via replay only*, which is
the default sinex contract anyway.

## See also

- `crate/sinex-db/docs/data_lifecycle.md` — live → archive → tombstone semantics.
- `README.md#the-provenance-model-read-this-first` — why replay is the
  primary durability path.
