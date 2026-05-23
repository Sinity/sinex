# Sinex Snapshot Runbook

`sinexctl admin snapshot` captures a point-in-time archive of the complete
sinex runtime state surface — Postgres, NATS JetStream, CAS blob repository,
and per-source-worker state — into a single zstd-compressed tar archive.

This is a **quiesce-mode** backup: services must be stopped before the snapshot
runs.  A live snapshot (`--mode live`) is not available in this MVP.

## Quick start

```bash
# Stop sinex services (or let snapshot stop them automatically with --auto-stop)
sudo systemctl stop 'sinex-*'

# Create a snapshot (defaults: zstd level 3, all components)
sinexctl admin snapshot --output /var/backup/sinex/$(date +%Y-%m-%d).sinex.tar.zst

# Estimate sizes without writing anything
sinexctl admin snapshot --output /var/backup/sinex/check.tar.zst --dry-run
```

## Command reference

```
sinexctl admin snapshot --output <path>
  [--compression <1-19>]           # zstd level, default 3
  [--workers <N>]                  # zstd parallel workers, default all cores
  [--mode quiesce]                 # only quiesce supported in MVP
  [--dry-run]                      # estimate sizes, no archive
  [--database-url <url>]           # override DATABASE_URL
  [--state-dir <path>]             # override SINEX_STATE_DIR (default /var/lib/sinex)
  [--auto-stop]                    # stop sinex-* services automatically
  [--components postgres,nats,cas,state]  # subset, default all
```

### Components

| Component  | What is captured                                    |
|------------|-----------------------------------------------------|
| `postgres` | Full custom-format `pg_dump` of `DATABASE_URL`      |
| `nats`     | `$STATE_DIR/nats/jetstream/` directory tree         |
| `cas`      | `$STATE_DIR/blob-repository/` directory tree        |
| `state`    | Everything else under `$STATE_DIR` (spool, WALs, …) |

## Archive layout

```
manifest.json                   -- JSON metadata + BLAKE3 checksums
postgres/
  sinex_prod.dump               -- pg_dump custom-format (-Fc -Z9)
nats/
  jetstream/                    -- NATS JetStream state tree
  streams.summary.json          -- `nats stream ls --json` output (best-effort)
cas/
  blob-repository/              -- CAS BLAKE3 content store tree
state/                          -- remaining $STATE_DIR contents
```

## Restore procedure (manual)

MVP does not include a `restore` subcommand.  Restore is a manual procedure:

### 1. Stop services (if running)

```bash
sudo systemctl stop 'sinex-*'
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
sudo cp -a "$RESTORE_DIR/nats/jetstream/" /var/lib/sinex/nats/
sudo chown -R nats:nats /var/lib/sinex/nats/
sudo systemctl start nats
```

### 6. Restore CAS blob repository

```bash
sudo cp -a "$RESTORE_DIR/cas/blob-repository/" /var/lib/sinex/
sudo chown -R sinex:sinex /var/lib/sinex/blob-repository/
```

### 7. Restore remaining state

```bash
# Merge remaining state files (spool, etc.)
sudo cp -a "$RESTORE_DIR/state/." /var/lib/sinex/
sudo chown -R sinex:sinex /var/lib/sinex/
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
sudo systemctl start 'sinex-*'
```

### 10. Verify

```bash
sinexctl status
sinexctl telemetry current-health
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
4. Verify the manifest: `tar -tf <archive>` and inspect `manifest.json`.
5. Copy to off-machine storage (e.g., `rsync` to NAS or object storage).
6. Proceed with the wipe only after confirming the archive is readable.

## Known limitations

- **No live mode** — services must be stopped.  A future `--mode live` option
  is deferred.
- **No explicit restore subcommand** — restore is manual per this runbook.
- **No encryption** — use filesystem-level or transport-level encryption for
  archives that leave the host.
- **No incremental snapshots** — each run is a full capture.

## See also

- `docs/operations/backup.md` — WAL archiving and `pg_basebackup` setup.
- `crate/lib/sinex-db/docs/data_lifecycle.md` — event lifecycle semantics.
