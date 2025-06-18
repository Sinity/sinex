# TIM-DisasterRecoveryPlan: Exocortex Disaster Recovery Procedures

*   **Purpose:** Provides detailed, step-by-step procedures for recovering the Exocortex system from various disaster scenarios, including full host loss.
*   **Source:** Derived from original Vision Document Appendix G, incorporating details from backup TIMs.
*   **Dependencies:** Assumes backups are correctly configured and running as per `TIM-PostgreSQLBackupDR_pgBackRest.md`, `TIM-GitAnnexLargeFileMgmt.md`, and NixOS configuration is versioned as per `TIM-ReleaseEngineeringCICD.md`.

## 1. Introduction

This document outlines the disaster recovery (DR) plan for the Sinnix Exocortex. The primary goal is to restore the system to a consistent and functional state with minimal data loss (Recovery Point Objective - RPO) and within an acceptable timeframe (Recovery Time Objective - RTO).

*   **RPO Target:** Typically minutes to an hour (depends on WAL archiving frequency and last successful `git-annex` sync).
*   **RTO Target:** Hours (depends on hardware provisioning, data volume for restore).

## 2. Prerequisites for DR

*   Access to NixOS configuration Git repository (including `agenix` secrets).
*   Access to `pgBackRest` backup repository (local or S3).
*   Access to `git-annex` remote(s) or backup of annex content.
*   Access to `git bundle` of the `git-annex` Git repository metadata.
*   New or repaired hardware capable of running NixOS and Exocortex services.

## 3. DR Scenarios and Procedures

### 3.1. Scenario: Complete Host Failure (Hardware Loss, OS Corruption)

**Goal:** Restore entire Exocortex system to new hardware.

**Steps:**

1.  **Provision New Hardware:**
    *   Install base NixOS on the new machine. Ensure network connectivity.
2.  **Restore NixOS Configuration:**
    *   Clone the NixOS configuration Git repository (containing flakes, modules, `agenix` secrets) to the new host (e.g., into `/etc/nixos/`).
    *   Ensure necessary private keys for `agenix` (e.g., host SSH key if used for decryption, or user `age` key) are restored or accessible on the new host (e.g., via `scp` from a secure location, or if `agenix` secrets are encrypted to a portable user key).
    *   Run `sudo nixos-rebuild switch --flake /etc/nixos#yourHostConfigurationName`.
        *   This will install all Exocortex packages, set up systemd services (initially stopped), configure PostgreSQL (without data yet), `git-annex`, `pgBackRest` client, etc.
        *   `agenix` will decrypt secrets to `/run/agenix.d/` (or `/run/secrets/`).
3.  **Restore PostgreSQL Database (using `pgBackRest`):**
    *   Ensure PostgreSQL service is **stopped**. `sudo systemctl stop postgresql.service`.
    *   Ensure the target PGDATA directory (e.g., `/var/lib/postgresql/16/main`) is empty or doesn't exist (or `pgBackRest` will refuse to restore if it's a non-empty, initialized cluster).
    *   As the `postgres` user (or the user `pgBackRest` is configured to run as for restores):
        ```bash
        # Ensure pgbackrest.conf is correctly placed by NixOS config, or specify --config
        # Example: Restore to latest point-in-time
        sudo -u postgres pgbackrest --stanza=sinexdb_main_stanza restore \
            --pg1-path=/var/lib/postgresql/16/main \
            --log-level-console=info \
            --log-level-file=detail
        
        # Example: Restore to a specific point-in-time
        # sudo -u postgres pgbackrest --stanza=sinexdb_main_stanza restore \
        #   --type=time --target="YYYY-MM-DD HH:MM:SS+ZZ" \
        #   --pg1-path=/var/lib/postgresql/16/main ...

        # Example: Restore from a specific backup set label
        # sudo -u postgres pgbackrest --stanza=sinexdb_main_stanza restore \
        #   --set=<backup_label> \
        #   --pg1-path=/var/lib/postgresql/16/main ...
        ```
    *   `pgBackRest` will restore the base backup and then replay WALs from the archive.
    *   After restore completes, `pgBackRest` will create a `recovery.signal` file (or `standby.signal` if restoring a standby).
4.  **Start and Verify PostgreSQL:**
    *   `sudo systemctl start postgresql.service`.
    *   Check PostgreSQL logs for successful recovery and startup.
    *   Connect via `psql` and run basic checks (list databases, tables, query some data).
5.  **Restore `git-annex` Repository:**
    *   Create the main annex directory (e.g., `/srv/exocortex_annex_data`).
    *   Initialize as a Git repository: `cd /srv/exocortex_annex_data && git init`.
    *   Restore Git metadata (symlinks, branch history) from bundle:
        `git remote add origin_bundle /path/to/backup/git_repo_meta/annex_meta_YYYYMMDD.bundle`
        `git fetch origin_bundle`
        `git checkout main` (or your primary branch)
        `git remote rm origin_bundle`
    *   Initialize `git-annex`: `git annex init "My Exocortex Annex (Restored)"`.
    *   Configure `git-annex` remotes (if they were on separate storage, e.g., S3, external drive). Refer to your original annex setup or `TIM-GitAnnexLargeFileMgmt.md`.
        Example: `git annex initremote my_s3_backup type=S3 ...`
    *   Retrieve content from a backup remote:
        `git annex sync my_s3_backup` (to update git-annex branch info)
        `git annex get --all --from=my_s3_backup` (to download all annexed files from that remote. This can take a long time for large annexes. Prioritize essential files first if needed).
6.  **Start Exocortex Services:**
    *   `systemctl --user start sinex-*.service` (for user services).
    *   `sudo systemctl start sinex-system-*.service` (for system services if any).
7.  **Verify System Functionality:**
    *   Check agent logs (`journalctl --user -u sinex-*.service`).
    *   Run `exo system health`.
    *   Perform test queries, PKM note access, etc.
    *   Reprocess any items from ingestor local file-based DLQs if they existed and were also restored (these might be part of a user home backup if not in a system dir).

### 3.2. Scenario: PostgreSQL Database Corruption (Filesystem Intact)

**Goal:** Restore only the PostgreSQL database.

**Steps:** Similar to Scenario 3.1, Steps 3 & 4, but usually without needing to reprovision the entire OS or `git-annex`.
1.  Stop PostgreSQL service.
2.  Move or rename the corrupted PGDATA directory.
3.  Create a new empty PGDATA directory with correct permissions.
4.  Run `pgbackrest restore` as in 3.1 Step 3.
5.  Start and verify PostgreSQL as in 3.1 Step 4.
6.  Restart Exocortex agents that use the DB.

### 3.3. Scenario: Accidental Data Deletion/Modification (Logical Error)

**Goal:** Restore specific tables or data to a point *before* the logical error occurred.

**Steps:**
1.  **Identify Point-in-Time:** Determine the timestamp just before the erroneous operation.
2.  **Restore to a Separate Instance (Recommended):**
    *   Stop the main PostgreSQL instance (or prevent writes to affected tables).
    *   Provision a temporary PostgreSQL instance (e.g., different port, different PGDATA).
    *   `pgbackrest restore --stanza=sinexdb_main_stanza --type=time --target="<timestamp_before_error>" --pg1-path=/path/to/temp_restored_pgdata`.
    *   Start the temporary instance.
3.  **Data Recovery:**
    *   Use `pg_dump` from the temporary instance to dump the specific table(s) or data needed:
        `pg_dump -h localhost -p <temp_port> -U postgres -t 'schema.table_to_restore' <temp_db_name> > table_dump.sql`
    *   Or, use `psql` to `COPY` specific rows `TO STDOUT` from the temporary instance.
4.  **Apply to Production:**
    *   Carefully review the dumped data.
    *   On the production PostgreSQL instance:
        *   Delete incorrect data (if applicable).
        *   Import the correct data from `table_dump.sql` (e.g., `psql -d sinex_db < table_dump.sql`) or `COPY FROM STDIN`.
5.  Clean up temporary instance and data.

## 4. Post-Recovery Actions

*   Run full data integrity checks (`git annex fsck`, Exocortex link integrity agents).
*   Monitor system logs and Grafana dashboards closely.
*   Re-establish regular backup schedule from the newly restored system.
*   Document the DR event, any issues encountered, and lessons learned. Update this DR plan if necessary.

## 5. DR Plan Testing

*   Perform test restores (full or partial) to a non-production environment periodically (e.g., quarterly or annually) as per `TIM-PostgreSQLBackupDR_pgBackRest.md` (Section 6).
*   Simulate different failure scenarios.
*   Verify RPO and RTO targets can be met.

