# Backup & Restore

Sinex's primary durability story is **replay from source materials**: every
event traces to a `raw.source_material_registry` row, and the source files
themselves live on disk. A full database loss is recoverable by re-ingesting
those materials through the standard pipeline. PostgreSQL backups are a
defense-in-depth measure, not the primary recovery path.

This document covers two complementary mechanisms:

1. **Periodic full backups** via `pg_basebackup` — captures the database
   plus the in-flight WAL needed to make it consistent at backup time.
2. **Continuous WAL archiving** via `services.sinex.database.walArchiveCommand`
   — ships completed WAL segments to a separate location, enabling
   point-in-time recovery between full backups.

A trusted production deployment should have at least the periodic full
backup wired with a documented restore drill. WAL archiving is optional
and only useful if RPO < (full-backup interval) matters.

## When backups matter (and when they don't)

| Loss scenario | Backup is required | Why |
|---|---|---|
| Disk corruption / hardware failure on the DB volume | Yes | Source materials are unaffected, but the schema, derived events (automaton output), and operator-only state (`core.runs`, `audit.archived_events`, `core.tags`, etc.) only live in PG. |
| Accidental schema drop / table truncate | Yes | Same reason. |
| Bad replay or a buggy automaton emitting wrong synthesis events | No | Use `lifecycle.tombstone.create` to mark the bad operation; replay or re-derive. |
| Single-row corruption from a privacy-policy change | No | Replay the affected source-material slice with new privacy rules. |
| Full host loss including the data volume | Yes, plus source materials | Both layers need restore. Source materials should already be on a separate filesystem or backed up via the operator's regular file-level backup. |

## Periodic full backup

`pg_basebackup` produces a consistent on-disk snapshot. Run it on a timer;
verify restore at least once.

### NixOS wiring (recommended)

The sinex module does not ship a `pg_basebackup` timer. Add one to the host
configuration:

```nix
{ config, pkgs, ... }:
let
  backupRoot = "/var/backup/sinex";
in
{
  services.sinex.enable = true;

  # Backup user with replication role.
  services.postgresql.ensureUsers = [
    {
      name = "sinex_backup";
      ensureClauses.replication = true;
    }
  ];

  systemd.services.sinex-basebackup = {
    description = "Sinex PostgreSQL base backup";
    serviceConfig = {
      Type = "oneshot";
      User = "postgres";
      Group = "postgres";
      ExecStart = pkgs.writeShellScript "sinex-basebackup" ''
        set -euo pipefail
        target="${backupRoot}/$(date +%Y%m%d-%H%M%S)"
        mkdir -p "$target"
        ${config.services.postgresql.package}/bin/pg_basebackup \
          --host=/run/postgresql \
          --username=sinex_backup \
          --pgdata="$target" \
          --format=tar \
          --gzip \
          --progress \
          --checkpoint=fast \
          --wal-method=stream
        # Retain the last 14 successful backups; delete older.
        ls -1dt "${backupRoot}"/[0-9]* | tail -n +15 | xargs -r rm -rf
      '';
    };
  };

  systemd.timers.sinex-basebackup = {
    description = "Sinex PostgreSQL base backup timer";
    wantedBy = [ "timers.target" ];
    timerConfig = {
      OnCalendar = "daily";
      Persistent = true;
      RandomizedDelaySec = "1h";
    };
  };
}
```

Per `services.sinex.users.nodes` defaults, the `sinex` service user does
not have replication; `pg_basebackup` runs as `postgres` here. Adjust if
your deployment uses a different DB role.

### Inspecting backup state

```bash
systemctl status sinex-basebackup.service sinex-basebackup.timer
systemctl list-timers sinex-basebackup.timer
ls -lh /var/backup/sinex/
```

## Continuous WAL archiving (optional)

When set, `services.sinex.database.walArchiveCommand` becomes PostgreSQL's
`archive_command`. PostgreSQL calls it once per completed WAL segment.

```nix
services.sinex.database = {
  walArchiveCommand =
    "test ! -f /var/backup/sinex/wal/%f && cp %p /var/backup/sinex/wal/%f";
};
```

A more durable option ships WAL to remote object storage. `wal-g` is the
common choice:

```nix
services.sinex.database = {
  walArchiveCommand = "${pkgs.wal-g}/bin/wal-g wal-push %p";
};
```

WAL archiving combined with a recent base backup enables **point-in-time
recovery** — restore the base, then replay WAL up to a chosen timestamp.

PostgreSQL holds the archive_command exit code as gospel: if it returns
non-zero, PG retries indefinitely and refuses to recycle the segment.
A blocked archive_command will eventually fill the WAL directory.
Monitor with:

```bash
sudo -u postgres psql -c "SELECT * FROM pg_stat_archiver;"
```

`failed_count` should be 0 or rising slowly. If it climbs fast,
unblock the archive_command before the WAL volume fills.

## Restore drill (do this before relying on backups)

A backup that has never been restored is a hope, not a backup. Run this
drill against a non-production database after every change to the backup
configuration.

### 1. Stop sinex

```bash
systemctl stop sinex-ingestd sinex-gateway 'sinex-*'
# Stop everything in the unit list — see `xtask status` for the live set.
```

### 2. Stage the restore target

```bash
backup_archive="$(ls -1dt /var/backup/sinex/[0-9]* | head -1)"
echo "Restoring from $backup_archive"
restore_root=/var/lib/postgresql.restore
sudo rm -rf "$restore_root"
sudo install -d -o postgres -g postgres -m 0700 "$restore_root"
sudo -u postgres tar -xzf "$backup_archive/base.tar.gz" -C "$restore_root"
```

### 3. Replay WAL (if WAL archiving is configured)

```bash
sudo -u postgres tar -xzf "$backup_archive/pg_wal.tar.gz" -C "$restore_root/pg_wal"
sudo -u postgres tee "$restore_root/recovery.signal" </dev/null
# point recovery_target_time at the desired wall-clock instant in
# postgresql.conf, or leave unset to replay through end of archive.
```

### 4. Start PostgreSQL against the restore target

The cleanest pattern: a separate PostgreSQL instance pointed at
`$restore_root`. Once it reaches the recovery target it converts to a
normal primary; verify the event count there before touching the live
data directory.

```bash
sudo -u postgres /run/current-system/sw/bin/postgres -D "$restore_root" \
  --port=5433
```

### 5. Verify parity

```bash
# In another shell:
psql "host=/var/run/postgresql port=5433 dbname=sinex_prod" <<'SQL'
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

Compare this against the same query on the live DB (before the failure)
or against a known checkpoint. The day counts should match within the
RPO window. If they diverge wildly, the restore did not replay all
needed WAL — investigate before treating the backup as good.

### 6. Tear down the verification instance

```bash
sudo -u postgres pg_ctl stop -D "$restore_root"
sudo rm -rf "$restore_root"
```

A passing drill leaves no production state mutated.

## Known limitations

- **Source materials are not backed up by `pg_basebackup`.** They live on
  the filesystem under `services.sinex.stateRoot` / wherever ingestors
  store originals. Back those up with your usual file-level mechanism
  (restic, borgbackup, rsync-to-remote).
- **The blob CAS at `services.sinex.storage.blob.repositoryPath` is not in
  the DB backup.** Treat it like source materials.
- **NATS JetStream state lives outside PG.** Lost JetStream state is
  recoverable from `core.events` (already-persisted events) and replay,
  but consumers that were mid-flight will re-deliver from their
  checkpoints.

## Acceptance signals

A backup setup is operationally trusted when:

- `systemctl list-timers sinex-basebackup.timer` shows a recent successful run.
- At least one full restore drill has completed end-to-end with parity
  verification, and the result was recorded somewhere durable (commit a
  note to `thoughtspace/`, or the host's runbook repo).
- `pg_stat_archiver.failed_count` is stable at 0 (if WAL archiving is
  enabled).
- Source-material and blob-CAS volumes are covered by a file-level
  backup with their own restore drill.

Until all four hold, treat the deployment as *durable via replay only*,
which is the default sinex contract anyway.

## See also

- `services.sinex.database.walArchiveCommand` — option doc in `nixos/modules/default.nix`.
- `crate/lib/sinex-db/docs/data_lifecycle.md` — live → archive → tombstone semantics.
- `docs/architecture/provenance.md` (linked from CLAUDE.md) — why replay
  is the primary durability path.
