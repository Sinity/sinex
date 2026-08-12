# Sinex Snapshot Runbook

`sinexctl ops state snapshot` captures a point-in-time archive of the complete
sinex runtime state surface — Postgres, NATS JetStream, CAS blob repository,
and source runtime state — into a single zstd-compressed tar archive.

The default is a **quiesce-mode** backup: services must be stopped before the
snapshot runs, or `--auto-stop` can stop the active snapshot-writer units for
the command. The deployed NixOS shape uses `sinexd.service` and
`nats.service`; generated `sinex-*.service`/`.timer` writers are discovered
from systemd rather than assumed by a fixed glob.
`--mode live` is available for urgent forensic capture while services remain
active; it records `mode: live` in `manifest.json` and should be treated as a
weaker-consistency artifact. Operator runbooks use the
`sinexctl ops state ...` surface.

## Quick start

```bash
# Stop the deployed writers (or let snapshot discover and stop them with
# --auto-stop). Include any additional writer units reported by the command.
sudo systemctl stop sinexd.service nats.service

# Create a snapshot (defaults: zstd level 3, all components)
sinexctl ops state snapshot --output /var/backup/sinex/$(date +%Y-%m-%d).sinex.tar.zst

# Estimate sizes without writing anything
sinexctl ops state snapshot --output /var/backup/sinex/check.tar.zst --dry-run

# Capture without stopping services when preserving the live state is more
# important than component-level consistency
sinexctl ops state snapshot --output /var/backup/sinex/live-forensics.sinex.tar.zst --mode live
```

## Command reference

```
sinexctl ops state snapshot --output <path>
  [--compression <1-19>]           # zstd level, default 3
  [--workers <N>]                  # zstd parallel workers, default all cores
  [--mode quiesce|live]            # quiesce default; live does not stop services
  [--dry-run]                      # estimate sizes, no archive
  [--database-url <url>]           # override DATABASE_URL
  [--state-dir <path>]             # override the deployed SINEX_STATE_DIR
  [--nats-store-dir <path>]        # override the deployed JetStream store root
  [--auto-stop]                    # stop discovered active writer units automatically
  [--components postgres,nats,cas,state]  # subset, default all

sinexctl ops state restore --archive <path> --target-dir <empty-dir> --dry-run
  [--allow-non-empty-target]       # planning only; destructive restore still refuses ambiguity

sinexctl ops state restore --archive <path> --target-dir <empty-dir>
  --confirm-restore
  [--allow-active-services]        # only for explicitly isolated drill targets
```

### Components

| Component  | What is captured                                    |
|------------|-----------------------------------------------------|
| `postgres` | Full custom-format `pg_dump` of `DATABASE_URL`      |
| `nats`     | The `jetstream.store_dir` from the deployed `nats.service` config |
| `cas`      | `$STATE_DIR/blob-repository/` directory tree        |
| `state`    | Everything else under `$STATE_DIR` (spool, WALs, …) |

These paths are deployment-specific. The command derives `SINEX_STATE_DIR`,
`SINEX_CONTENT_STORE_PATH`, and `SINEX_NATS_JETSTREAM_STORE_DIR` from the
configured `sinexd.service` environment. For compatibility with older
deployments, NATS discovery can strictly read `jetstream.store_dir` from the
deployed `nats.service` configuration. On the current NixOS deployment they resolve to
`/var/lib/sinex/state`, `/var/lib/sinex/state/blob-repository`, and
`/var/lib/nats/jetstream`, respectively. If systemd cannot expose these
values, or the explicit override is not supplied for an alternate topology,
capture fails instead of recording an empty component.

## Archive layout

```
manifest.json                   -- JSON metadata + BLAKE3 checksums
postgres/
  sinex_prod.dump               -- pg_dump custom-format (-Fc -Z9)
nats/
  jetstream/                    -- NATS JetStream state tree from the live store_dir
  streams.summary.json          -- `nats stream ls --json` output (best-effort)
cas/
  blob-repository/              -- CAS BLAKE3 content store tree
state/                          -- remaining $STATE_DIR contents
```

The NATS component hash covers the JetStream state tree and excludes
`streams.summary.json` on both capture and restore. The summary is diagnostic
metadata and may be absent or regenerated without changing the state hash.

## Archive secrecy and key policy

Snapshot archives are **secret** by default. A normal archive may contain event
payloads, raw source material, NATS stream state, CAS blobs, runtime state,
private-mode state, and source identifiers. Treat the archive at least as
sensitive as the live Sinex state directory and database.

The snapshot command does not intentionally include TLS client keys, private
keys, token files, age keys, SSH keys, password-store material, or other host
credentials. That exclusion is a policy, not magic: if an operator stores keys
inside `$SINEX_STATE_DIR` or one of the selected component roots, the archive
inherits those secrets. Keep archives on encrypted storage and use encrypted
transport for off-host copies.

## Restore drill planning

Before a destructive restore, validate the archive and target with:

```bash
sinexctl ops state restore \
    --archive /var/backup/sinex/2026-05-15.sinex.tar.zst \
    --target-dir /tmp/sinex-restore-drill \
    --dry-run
```

The dry-run command does not extract or write restored state. It validates:

- `manifest.json` is readable from the archive.
- Component paths declared by the manifest are present in the tar; missing
  paths are reported as structured coverage evidence.
- The target path is an empty directory, or does not exist under an existing
  parent directory.
- Active snapshot-writer units are reported so destructive restore can quiesce
  them first. Failure to query systemd is an error, not an empty active-unit
  result.
- The restore drill comparison plan includes source count, Postgres table
  count, NATS JetStream member count when present, CAS blob count when present,
  and runtime private-mode state presence.

`--allow-non-empty-target` is only for planning against an already-prepared
drill directory. It does not permit destructive writes.

## Isolated restore drill execution

For file-backed components (`state`, `cas`, `nats`) and explicitly supplied
Postgres drill databases, `state restore` can execute only an isolated drill
into an empty target directory:

```bash
sinexctl ops state restore \
    --archive /var/backup/sinex/2026-05-15.sinex.tar.zst \
    --target-dir /tmp/sinex-restore-drill \
    --confirm-restore
```

For archives containing a non-empty Postgres dump, point the drill at an empty
throwaway database:

```bash
createdb sinex_restore_drill
sinexctl ops state restore \
    --archive /var/backup/sinex/2026-05-15.sinex.tar.zst \
    --target-dir /tmp/sinex-restore-drill \
    --restore-database-url "$SINEX_RESTORE_DATABASE_URL" \
    --confirm-restore
```

The restore database name must identify a disposable rehearsal target. Use a
name containing `dev`, `test`, `drill`, `restore`, `scratch`, or `tmp`, such as
`sinex_restore_drill`. This naming check complements the live PostgreSQL
emptiness query and prevents an empty production-shaped URL such as
`sinex_prod` from being accepted as a drill target.

Isolated drill execution refuses to run unless:

- `--confirm-restore` is present.
- The target directory is empty, or does not yet exist under an existing parent.
- Active snapshot-writer units are stopped, unless `--allow-active-services` is
  explicitly passed for an isolated drill target.
- Archives with non-empty `postgres` components include
  `--restore-database-url`, pointing at an empty drill database. The target
  URL must use a disposable rehearsal database name and the emptiness query
  must succeed; missing row-count evidence or a failed query makes the restore
  verdict fail closed.

The deployed-topology round-trip is an executable seam, not evidence that a
NixOS integration run occurred. Use a dedicated empty drill database and opt
in explicitly in a live deployment or NixOS VM:

```bash
SINEX_REAL_TOPOLOGY_TEST=1 \
DATABASE_URL="$DATABASE_URL" \
SINEX_REAL_RESTORE_DATABASE_URL="$SINEX_RESTORE_DATABASE_URL" \
  xtask test -p sinexctl -E 'test(real_deployed_topology_backup_restore_round_trip)'
```

The test discovers `SINEX_STATE_DIR`, the NATS `jetstream.store_dir`, and
active writer units from the live NixOS deployment, captures all components,
then restores the archive into the supplied empty PostgreSQL drill database
and isolated filesystem target. It is intentionally opt-in because it reads
live state and requires an operator-provisioned empty database. A focused
unit/integration test with fake command seams does not establish this
deployment evidence.

The JSON/YAML result includes `observed_checks` comparing the isolated drill
target against the manifest: source IDs, NATS JetStream member paths when
present, CAS blob count when present, and private-mode state presence. When a
Postgres drill database is supplied, it also compares exact row counts for the
tables listed in the snapshot manifest.

## Restore procedure (manual for Postgres/live state)

Live in-place restore remains manual. Use `state restore --dry-run` first, then
execute the explicit steps below in a prepared maintenance window. The isolated
restore drill above covers Postgres dumps only when the target is a deliberately
empty drill database.

### 1. Stop services (if running)

```bash
sudo systemctl stop sinexd.service nats.service
```

### 2. Extract the archive

```bash
RESTORE_DIR=/tmp/sinex-restore
mkdir -p "$RESTORE_DIR"
tar -xf /var/backup/sinex/2026-05-15.sinex.tar.zst \
    --use-compress-program=zstd \
    -C "$RESTORE_DIR"
```

If your `tar` supports `--auto-compress` / recognises the `.zst` suffix:

```bash
tar -xaf /var/backup/sinex/2026-05-15.sinex.tar.zst -C "$RESTORE_DIR"
```

### 3. Verify the manifest

Before extracting, the archive can be inspected directly:

```bash
sinexctl ops state inspect \
    --archive /var/backup/sinex/2026-05-15.sinex.tar.zst
```

The command reads `manifest.json`, lists the archive, and reports any manifest
component paths that are missing from the tar member list.

```bash
cat "$RESTORE_DIR/manifest.json" | jq .
```

Check `snapshot_id`, `created_at`, and that all expected components appear with
non-zero `bytes`.

### 4. Restore Postgres

```bash
# Drop + recreate (destructive — confirm before running)
sudo -u postgres psql -c "DROP DATABASE IF EXISTS sinex_prod;"
sudo -u postgres psql -c "CREATE DATABASE sinex_prod OWNER sinex;"

# Restore
pg_restore \
    --dbname=postgresql://sinex:sinex@/sinex_prod \
    --jobs=$(nproc) \
    "$RESTORE_DIR/postgres/sinex_prod.dump"
```

### 5. Restore NATS JetStream state

```bash
sudo systemctl stop nats  # if managed by NixOS
sudo mkdir -p /var/lib/nats/jetstream
sudo cp -a "$RESTORE_DIR/nats/jetstream/." /var/lib/nats/jetstream/
sudo chown -R nats:nats /var/lib/nats/jetstream
sudo systemctl start nats
```

### 6. Restore CAS blob repository

```bash
sudo mkdir -p /var/lib/sinex/state/blob-repository
sudo cp -a "$RESTORE_DIR/cas/blob-repository/." /var/lib/sinex/state/blob-repository/
sudo chown -R sinex:sinex /var/lib/sinex/state/blob-repository/
```

### 7. Restore remaining state

```bash
# Merge remaining state files (spool, etc.)
sudo cp -a "$RESTORE_DIR/state/." /var/lib/sinex/state/
sudo chown -R sinex:sinex /var/lib/sinex/state/
```

### 8. Apply schema

After restoring Postgres, re-run schema convergence to ensure the live schema
matches the codebase (needed if the schema version advanced between backup and
restore):

```bash
sinex-schema apply "$DATABASE_URL"
```

### 9. Start services

```bash
sudo systemctl start sinexd.service nats.service
```

### 10. Verify

```bash
sinexctl
sinexctl runtime health
sinexctl metrics telemetry current-health
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM core.events;"
```

Compare the event count against the `row_counts` field in `manifest.json` for
the `postgres` component.

## Disk space requirements

The command probes available disk space and refuses to start if less than
1.5× the estimated state size is free at the output path.  For a deployment
with ~292 GiB of live state, expect to need at least 450 GiB free.

Compressed archive size will be much smaller depending on data compressibility.
Use `--dry-run` to get size estimates before committing to a destination.

## Recommended archival cadence

For a horizon-3 wipe (complete state replacement):

1. Run `--dry-run` to confirm estimate and disk space.
2. Stop services.
3. Run the snapshot with a high compression level: `--compression 15`.
4. Verify the manifest: `sinexctl ops state inspect --archive <archive>`.
5. Copy to off-machine storage (e.g., `rsync` to NAS or object storage).
6. Proceed with the wipe only after confirming the archive is readable.

## Consistency modes

`--mode quiesce` is the normal backup mode. It refuses to run while discovered
snapshot-writer units are active unless `--auto-stop` is supplied, then
captures Postgres, NATS, CAS, and runtime state after quiescence. A systemd
inspection failure also refuses the snapshot, because “no units observed” is
not evidence that the deployment is quiescent.

`--mode live` is for forensic preservation when stopping services is not
acceptable. It does not stop services and ignores `--auto-stop`; the archive
may contain components observed at slightly different moments, and the manifest
records `mode: live` so restore drills and future readers can distinguish it
from a quiesced backup.

## Known limitations

- **No in-place Postgres/live restore execution** — `state restore` can execute
  isolated file-backed drills, but destructive live restore writes remain
  manual per this runbook.
- **No built-in archive encryption** — use filesystem-level, transport-level,
  or envelope encryption for archives that leave the host.
- **No incremental snapshots** — each run is a full capture.

## See also

- `crate/sinex-db/docs/backup_restore.md` — the `sinex-postgres-dump` daily `pg_dump` mechanism and restore drill.
- `crate/sinex-db/docs/data_lifecycle.md` — event lifecycle semantics.
