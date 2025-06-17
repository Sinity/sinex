# TIM-PostgreSQLBackupDR_pgBackRest: PostgreSQL Backup & Disaster Recovery with `pgBackRest`

*   **Relevant ADR:** (N/A directly, core operational procedure)
*   **Original UG Context:** Section 23.1
*   **Vision Document Reference:** Part VI.3

This TIM details the use of `pgBackRest` for comprehensive backup, restore, and disaster recovery (DR) of the Exocortex PostgreSQL database.

## 1. Rationale Summary [CR5, SA4]

`pgBackRest` is a feature-rich, reliable, and performant open-source backup tool for PostgreSQL. It supports full/differential/incremental backups, WAL archiving, PITR, parallel operations, compression, and encryption, making it ideal for Exocortex's data durability needs.

## 2. `pgBackRest` Setup and Configuration

### 2.1. Installation (NixOS)

*   `pgBackRest` is typically available as a Nix package: `pkgs.pgbackrest`.
*   Ensure it's installed on the PostgreSQL server host (and potentially on a separate backup management host if using remote backups).

### 2.2. PostgreSQL Configuration for Archiving (`postgresql.conf`) [UG Sec 23.1.1]

These settings are managed via `services.postgresql.settings` in NixOS.
*   `wal_level = replica` (or `logical` if logical replication is also used).
*   `archive_mode = on` (or `always` in PG13+).
*   `archive_command = 'pgbackrest --stanza=<your_stanza_name> archive-push %p'`
    *   The PostgreSQL user (e.g., `postgres`) must have execute permissions on `pgbackrest` and write access to its log directory.
*   `max_wal_senders`: Sufficient for streaming base backups and any replicas.
*   `wal_keep_size` (PG13+): Size of WALs to keep in `pg_wal` for standby catch-up.

### 2.3. `pgBackRest` Configuration (`pgbackrest.conf`) [UG Sec 23.1.2, SA4]

Typically at `/etc/pgbackrest.conf` or `/etc/pgbackrest/pgbackrest.conf`. Managed by NixOS (e.g., via `environment.etc."pgbackrest/pgbackrest.conf".text = ''...'';`).

```ini
# /etc/pgbackrest/pgbackrest.conf
[sinexdb_main_stanza] # Stanza name for the Exocortex DB
pg1-path=/var/lib/postgresql/16/main    # PGDATA directory (adjust PG version)
pg1-host=localhost                      # Or DB server IP/hostname if pgBackRest runs elsewhere
pg1-port=5432
pg1-socket-path=/run/postgresql         # Recommended for local DB
# pg1-user=postgres                     # If specific user needed for connection

[global]
repo1-path=/var/lib/pgbackrest_repo       # Backup repository path (ensure this dir exists, owned by postgres or pgbackrest user)
# Example: For S3 repository (repo1-type=s3)
# repo1-s3-bucket=your-s3-backup-bucket-name
# repo1-s3-region=your-s3-region
# repo1-s3-endpoint=your-s3-endpoint # For S3-compatible storage
# repo1-s3-key=YOUR_AWS_ACCESS_KEY_ID (better to use IAM roles or env vars)
# repo1-s3-key-secret=YOUR_AWS_SECRET_ACCESS_KEY (better to use IAM roles or env vars)
# repo1-s3-key-type=auto # Or 'shared' for minio

repo1-retention-full=3                  # Keep last 3 full backups
# repo1-retention-diff=2                  # For differential: keep diffs for last 2 full
repo1-retention-archive-type=full       # Keep WALs needed to restore all retained full backups
                                        # Or 'diff' to keep WALs for retained diffs too.
# repo1-retention-archive=3             # Number of full backups whose WALs are kept (usually same as repo1-retention-full)

# Encryption (recommended for off-site/cloud repos)
# repo1-cipher-type=aes-256-cbc
# repo1-cipher-pass=your_pgbackrest_repo_encryption_passphrase # Store this securely (e.g., via agenix for config generation)

# Compression [CR5]
compress=y
compress-type=zst                       # zstd recommended (good ratio/speed)
compress-level=3                        # zstd level (1-19)
# compress-level-network=1              # Lower level for network transfer if CPU is bottleneck on client

# Performance & Logging
start-fast=y                            # Allow backup to start even if a checkpoint is running
process-max=4                           # Parallel processes for backup/restore [CR5]
log-level-console=info
log-level-file=detail
log-path=/var/log/pgbackrest            # Ensure dir exists and is writable by pgbackrest user
# backup-standby=y                      # If backups should be taken from a standby server

# WAL Archiving (if pgBackRest handles it via archive-push in archive_command)
archive-async=y                         # Async WAL archiving
archive-queue-max=256GB                 # Max WAL queue size on PG server before archive-push blocks
# archive-push-queue-max = 1GB          # Max size of WAL files to queue on the pgBackRest host before pushing to repo (if different host)
```
*   **Initialize Stanza:** After config, run as `pgbackrest` user (often `postgres`):
    `pgbackrest --stanza=sinexdb_main_stanza stanza-create`
*   **Permissions:** PostgreSQL user needs to run `pgbackrest archive-push`. `pgbackrest` user needs read access to PGDATA for backups and write access to `repo1-path` and `log-path`.

## 3. Backup Operations [UG Sec 23.1.2, SA4]

Run as `pgbackrest` user. Schedule with systemd timers.

*   **Check Configuration:** `pgbackrest --stanza=sinexdb_main_stanza check`
*   **Full Backup:** `pgbackrest --stanza=sinexdb_main_stanza --type=full backup`
*   **Differential Backup:** `pgbackrest --stanza=sinexdb_main_stanza --type=diff backup`
*   **Incremental Backup:** `pgbackrest --stanza=sinexdb_main_stanza --type=incr backup`
*   **Show Backup Info:** `pgbackrest --stanza=sinexdb_main_stanza info`
*   **Expire Old Backups/WALs (apply retention):** `pgbackrest --stanza=sinexdb_main_stanza expire` (run this regularly)

## 4. Restore Operations (Point-in-Time Recovery - PITR)

Performed when PostgreSQL server is stopped. Restores to PGDATA path defined in stanza (`pg1-path`).

*   **Restore to Latest Available Point:**
    `pgbackrest --stanza=sinexdb_main_stanza restore`
*   **Restore to Specific Time (PITR):**
    `pgbackrest --stanza=sinexdb_main_stanza --type=time --target="YYYY-MM-DD HH:MM:SS+ZZ" restore`
*   **Restore to Named Backup:**
    `pgbackrest --stanza=sinexdb_main_stanza --set=<backup_label_from_info_cmd> restore`
*   **Other Targets:** `--type=xid --target=<transaction_id>`, `--type=lsn --target=<lsn>`.
*   **Restore to Different Directory (for testing or migration):**
    `pgbackrest --stanza=sinexdb_main_stanza --pg1-path=/new/pgdata_restore_test restore`
*   **Delta Restore:** If restoring to a PGDATA that was previously restored from the same backup set (and not extensively modified), `--delta` can speed it up by only copying changed files.

## 5. S3 Lifecycle Policies for Cloud WALs/Backups [UG Sec 23.1.3, CR5]

If `repo1-type=s3`, configure S3 bucket lifecycle policies:
*   Standard -> S3 Standard-IA (e.g., after 30 days).
*   S3 Standard-IA -> S3 Glacier Instant Retrieval (e.g., after 90 days).
*   S3 Glacier Instant Retrieval -> S3 Glacier Flexible Retrieval (e.g., after 180 days).
*   S3 Glacier Flexible Retrieval -> S3 Glacier Deep Archive (e.g., after 1 year).
*   Balance cost vs. RTO (recovery time from colder tiers is longer).

## 6. Automated Verification and Test Restores [UG Sec 23.1.4, CR5, `openai_sinex_6.md` Sec 12]

Essential to ensure backups are valid.

*   **Systemd Timer (`sinex-pgbackrest-restore-test.timer`):** Runs weekly/monthly.
*   **Service Unit (`sinex-pgbackrest-restore-test.service`):**
    1.  **Pre-script:** Sets up temporary, isolated PGDATA dir and minimal `postgresql.conf` for a test instance.
    2.  **`pgBackRest` Restore Command:**
        ```bash
        # ExecStart in systemd service (example)
        # /usr/bin/pgbackrest --stanza=sinexdb_main_stanza restore \
        #   --pg1-path=/var/lib/pgbackrest/test_restore_pgdata \
        #   --delta \
        #   --target-action=promote \ # Promote to primary after restore
        #   --type=time \
        #   --target="$(date -d '5 minutes ago' +'%Y-%m-%d %H:%M:%S %Z')" \
        #   --repo1-path=/var/lib/pgbackrest_repo \ # Or S3 config if repo is on S3
        #   --log-level-console=info
        ```
    3.  **Post-script:**
        a.  Start the restored temporary PostgreSQL instance using its dedicated PGDATA.
        b.  Connect (`psql`) and run verification queries (check tables exist, row counts, specific data points).
        c.  Log success/failure as `sinex.system.dr_test_completed` event.
        d.  Stop and clean up temporary instance and data.

## 7. Compression and Parallel Archiving Benefits [UG Sec 23.1.5, CR5]

*   **Compression (`zstd`):** Recommended (`compress-type=zst`, `compress-level=3`). ~50% better ratio than gzip for PG backups [CR5].
*   **Parallel Operations (`process-max`):** Speeds up backup/restore on multi-core servers. Also `archive-async=y` for parallel WAL archiving.

