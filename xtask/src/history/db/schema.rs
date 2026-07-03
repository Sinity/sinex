use color_eyre::eyre::{Result, WrapErr};
use rusqlite::{Connection, params};
use std::time::Duration;

use super::{HistoryDb, WrapperEventRow, with_sqlite_lock_retry};

pub(crate) const HISTORY_DB_SCHEMA_VERSION: i32 = 1;
pub(super) const SQLITE_LOCK_RETRY_ATTEMPTS: usize = 6;
pub(super) const SQLITE_LOCK_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
pub(super) const SQLITE_LOCK_RETRY_MAX_DELAY: Duration = Duration::from_millis(500);
const SQLITE_PERSISTENT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_EPHEMERAL_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SQLITE_QUERY_BUSY_TIMEOUT: Duration = Duration::from_secs(1);
pub(super) const SQLITE_STALE_CLEANUP_BUSY_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryDbOpenMode {
    Persistent,
    Ephemeral,
    Query,
}

impl HistoryDbOpenMode {
    fn pragmas(self) -> &'static str {
        match self {
            Self::Persistent => {
                "PRAGMA foreign_keys=ON;
                 PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA busy_timeout=5000;"
            }
            Self::Ephemeral => {
                "PRAGMA foreign_keys=ON;
                 PRAGMA journal_mode=MEMORY;
                 PRAGMA synchronous=OFF;
                 PRAGMA temp_store=MEMORY;
                 PRAGMA busy_timeout=5000;"
            }
            Self::Query => {
                "PRAGMA foreign_keys=ON;
                 PRAGMA query_only=ON;
                 PRAGMA busy_timeout=1000;"
            }
        }
    }

    const fn busy_timeout(self) -> Duration {
        match self {
            Self::Persistent => SQLITE_PERSISTENT_BUSY_TIMEOUT,
            Self::Ephemeral => SQLITE_EPHEMERAL_BUSY_TIMEOUT,
            Self::Query => SQLITE_QUERY_BUSY_TIMEOUT,
        }
    }
}

impl HistoryDb {
    pub(super) fn configure_connection(conn: &Connection, mode: HistoryDbOpenMode) -> Result<()> {
        conn.execute_batch(mode.pragmas())
            .context("failed to configure history database connection")
    }

    pub(super) fn with_busy_timeout<T, F>(
        &self,
        timeout: Duration,
        restore_mode: HistoryDbOpenMode,
        operation: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.conn
            .busy_timeout(timeout)
            .context("failed to configure temporary history database busy timeout")?;

        let operation_result = operation();
        let restore_result = self
            .conn
            .busy_timeout(restore_mode.busy_timeout())
            .context("failed to restore history database busy timeout");

        match (operation_result, restore_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (_, Err(error)) => Err(error),
        }
    }

    pub(super) fn schema_version(&self) -> Result<i32> {
        self.conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .context("failed to read history DB schema version")
    }

    pub(super) fn set_schema_version(&self, version: i32) -> Result<()> {
        self.conn
            .execute_batch(&format!("PRAGMA user_version = {version};"))
            .context("failed to persist history DB schema version")
    }

    /// Initialize the database schema from scratch.
    ///
    /// All tables are defined with their full canonical column sets. Existing
    /// history databases are preserved with compatibility `ALTER TABLE` additions;
    /// the history DB is an evidence ledger and must not be treated as cache.
    pub(super) fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS invocations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command TEXT NOT NULL,
                subcommand TEXT,
                profile TEXT,
                args_json TEXT,
                git_commit TEXT,
                git_dirty INTEGER DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                exit_code INTEGER,
                status TEXT NOT NULL DEFAULT 'running',
                cancel_reason TEXT,
                cancelled_by TEXT,
                host TEXT NOT NULL,
                cwd TEXT NOT NULL,
                pid INTEGER,
                is_background INTEGER DEFAULT 0,
                stdout_path TEXT,
                stderr_path TEXT,
                stdout_content TEXT,
                stderr_content TEXT,
                cpu_usage_avg REAL,
                memory_usage_max_mb REAL,
                process_cpu_usage_avg REAL,
                process_memory_usage_max_mb REAL,
                root_process_cpu_usage_avg REAL,
                root_process_memory_usage_max_mb REAL,
                shared_nix_daemon_cpu_usage_avg REAL,
                shared_nix_daemon_memory_usage_max_mb REAL,
                shared_nix_build_slice_cpu_usage_avg REAL,
                shared_nix_build_slice_memory_usage_max_mb REAL,
                shared_background_slice_cpu_usage_avg REAL,
                shared_background_slice_memory_usage_max_mb REAL,
                host_cpu_pressure_some_avg10_max REAL,
                host_io_pressure_some_avg10_max REAL,
                host_io_pressure_full_avg10_max REAL,
                host_memory_pressure_some_avg10_max REAL,
                host_memory_pressure_full_avg10_max REAL,
                host_block_read_mib_delta REAL,
                host_block_write_mib_delta REAL,
                host_block_read_iops_avg REAL,
                host_block_write_iops_avg REAL,
                host_block_busiest_device TEXT,
                host_block_busiest_device_total_mib_delta REAL,
                host_block_busiest_device_read_iops_avg REAL,
                host_block_busiest_device_write_iops_avg REAL,
                host_block_busiest_device_weighted_io_ms_per_s REAL,
                shm_free_min_mb REAL,
                shm_used_max_mb REAL,
                process_count_max INTEGER,
                resource_sample_count INTEGER,
                tree_fingerprint TEXT,
                scope_key TEXT,
                live_stage TEXT,
                pre_fix_errors INTEGER,
                pre_fix_warnings INTEGER,
                pre_fix_fixable INTEGER,
                launch_mode TEXT DEFAULT 'foreground'
            );

            CREATE TABLE IF NOT EXISTS test_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT NOT NULL,
                status TEXT NOT NULL,
                duration_secs REAL,
                attempt INTEGER DEFAULT 1,
                output TEXT,
                slot_name TEXT,
                slot_wait_ms INTEGER,
                cleanup_ms INTEGER,
                failure_message TEXT,
                failure_type TEXT,
                test_mode TEXT DEFAULT 'nextest',
                nats_context TEXT,
                UNIQUE(invocation_id, test_name, attempt)
            );

            CREATE TABLE IF NOT EXISTS build_diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                level TEXT NOT NULL,
                code TEXT,
                message TEXT NOT NULL,
                file_path TEXT,
                line INTEGER,
                col INTEGER,
                rendered TEXT,
                package TEXT,
                fix_replacement TEXT,
                fix_applicability TEXT,
                fix_byte_start INTEGER,
                fix_byte_end INTEGER,
                authority TEXT NOT NULL DEFAULT 'proof'
            );

            CREATE TABLE IF NOT EXISTS invocation_packages (
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                package TEXT NOT NULL,
                PRIMARY KEY (invocation_id, package)
            );

            CREATE TABLE IF NOT EXISTS stage_timings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                stage_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                success INTEGER NOT NULL DEFAULT 1,
                -- End-of-stage PSI (pressure-stall) snapshot for per-stage causal
                -- attribution of dev-loop slowdowns. avg10 is a 10s decaying average:
                -- meaningful for long stages (compile/test/clippy), coarse for sub-10s
                -- stages. Nullable: /proc/pressure may be unavailable.
                io_full_avg10 REAL,
                cpu_some_avg10 REAL,
                memory_some_avg10 REAL,
                -- Delta of /proc/pressure `total=` stall microseconds over
                -- [stage_start, stage_end]: exact stall μs attributable to the
                -- stage, length-independent (unlike the tail-biased avg10).
                -- Nullable: /proc/pressure may be unavailable, or a start/end
                -- counter may be missing.
                io_full_stall_us INTEGER,
                cpu_some_stall_us INTEGER,
                memory_some_stall_us INTEGER
            );

            CREATE TABLE IF NOT EXISTS invocation_progress (
                invocation_id INTEGER PRIMARY KEY REFERENCES invocations(id) ON DELETE CASCADE,
                phase TEXT,
                step TEXT,
                pct_done REAL,
                items_done INTEGER,
                items_total INTEGER,
                updated_at TEXT NOT NULL,
                mode TEXT,
                unit_kind TEXT,
                rate_per_sec REAL,
                eta_confidence TEXT,
                terminal_summary TEXT
            );

            CREATE TABLE IF NOT EXISTS proof_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                scope_json TEXT,
                artifact_json TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS test_proof_units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                reusable INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                test_filter TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS test_dependency_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                edge_kind TEXT NOT NULL,
                subject TEXT NOT NULL,
                fingerprint TEXT,
                origin TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, edge_kind, subject, origin)
            );

            CREATE TABLE IF NOT EXISTS coverage_regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                file_path TEXT NOT NULL,
                function_name TEXT,
                line_start INTEGER,
                line_end INTEGER,
                region_hash TEXT,
                content_hash TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS test_execution_manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                module_path TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_line INTEGER NOT NULL,
                binary_id TEXT,
                pid INTEGER NOT NULL,
                attempt_id TEXT NOT NULL,
                planner_version TEXT NOT NULL,
                content_hash TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, module_path, source_file, source_line)
            );

            CREATE TABLE IF NOT EXISTS impact_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                mode TEXT NOT NULL,
                changed_json TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                accepted_risk_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                impact_run_id INTEGER NOT NULL REFERENCES impact_runs(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                subject TEXT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                impact_run_id INTEGER REFERENCES impact_runs(id) ON DELETE SET NULL,
                sample_size INTEGER NOT NULL,
                sampled_json TEXT NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL,
                false_negative_count INTEGER NOT NULL DEFAULT 0,
                output_json TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS invocation_eta_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                phase TEXT NOT NULL,
                duration_secs REAL NOT NULL,
                sampled_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trace_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                ts            TEXT    NOT NULL,
                level         TEXT    NOT NULL,
                target        TEXT    NOT NULL,
                event_kind    TEXT,
                message       TEXT    NOT NULL,
                fields        TEXT
            );

            CREATE TABLE IF NOT EXISTS background_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                command TEXT NOT NULL,
                args_json TEXT,
                pid INTEGER,
                stdout_path TEXT,
                stderr_path TEXT,
                job_status TEXT NOT NULL DEFAULT 'running',
                exit_code INTEGER,
                started_at TEXT NOT NULL,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS background_job_logs (
                job_id INTEGER PRIMARY KEY REFERENCES background_jobs(id) ON DELETE CASCADE,
                stdout_content TEXT,
                stderr_content TEXT
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            -- Indices
            CREATE INDEX IF NOT EXISTS idx_invocations_command ON invocations(command);
            CREATE INDEX IF NOT EXISTS idx_invocations_started ON invocations(started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_status ON invocations(status);
            CREATE INDEX IF NOT EXISTS idx_invocations_command_status_started
                ON invocations(command, status, started_at);
            CREATE INDEX IF NOT EXISTS idx_invocations_background
                ON invocations(is_background, status)
                WHERE is_background = 1;
            CREATE INDEX IF NOT EXISTS idx_invocations_fingerprint
                ON invocations(command, tree_fingerprint, scope_key);
            CREATE INDEX IF NOT EXISTS idx_test_results_name ON test_results(test_name);
            CREATE INDEX IF NOT EXISTS idx_test_results_status ON test_results(status);
            CREATE INDEX IF NOT EXISTS idx_test_results_invocation ON test_results(invocation_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_build_diagnostics_identity
                ON build_diagnostics(
                    invocation_id,
                    level,
                    COALESCE(code, ''),
                    message,
                    COALESCE(file_path, ''),
                    COALESCE(line, -1),
                    COALESCE(col, -1),
                    COALESCE(rendered, ''),
                    COALESCE(package, ''),
                    COALESCE(fix_replacement, ''),
                    COALESCE(fix_applicability, ''),
                    COALESCE(fix_byte_start, -1),
                    COALESCE(fix_byte_end, -1)
                );
            CREATE INDEX IF NOT EXISTS idx_diagnostics_invocation ON build_diagnostics(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_stage_timings_invocation ON stage_timings(invocation_id);
            CREATE INDEX IF NOT EXISTS trace_events_invocation_idx  ON trace_events(invocation_id);
            CREATE INDEX IF NOT EXISTS trace_events_level_idx       ON trace_events(level);
            CREATE INDEX IF NOT EXISTS trace_events_event_kind_idx  ON trace_events(event_kind);
            CREATE INDEX IF NOT EXISTS trace_events_ts_idx          ON trace_events(ts);
            CREATE INDEX IF NOT EXISTS idx_background_jobs_status     ON background_jobs(job_status);
            CREATE INDEX IF NOT EXISTS idx_background_jobs_started    ON background_jobs(started_at);
            CREATE INDEX IF NOT EXISTS idx_background_jobs_invocation ON background_jobs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_eta_samples_command_phase ON invocation_eta_samples(command, phase);
            CREATE INDEX IF NOT EXISTS idx_invocation_progress_invocation ON invocation_progress(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_proof_evidence_exact
                ON proof_evidence(command, proof_kind, scope_key, input_fingerprint, status, finished_at);
            CREATE INDEX IF NOT EXISTS idx_test_proof_units_exact
                ON test_proof_units(proof_kind, scope_key, input_fingerprint, reusable, status, finished_at);
            CREATE INDEX IF NOT EXISTS idx_test_dependency_edges_subject
                ON test_dependency_edges(edge_kind, subject, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_coverage_regions_path
                ON coverage_regions(file_path, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_test_execution_manifest_source
                ON test_execution_manifests(source_file, package, test_name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_coverage_regions_identity
                ON coverage_regions(
                    invocation_id,
                    test_name,
                    file_path,
                    COALESCE(function_name, ''),
                    COALESCE(line_start, -1),
                    COALESCE(line_end, -1)
                );
            CREATE INDEX IF NOT EXISTS idx_impact_runs_invocation ON impact_runs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_impact_decisions_run ON impact_decisions(impact_run_id);
            CREATE INDEX IF NOT EXISTS idx_impact_audit_invocation ON impact_audit_runs(invocation_id);

            CREATE TABLE IF NOT EXISTS exercise_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                tier TEXT,
                total INTEGER NOT NULL,
                passed INTEGER NOT NULL,
                failed INTEGER NOT NULL,
                skipped INTEGER NOT NULL,
                duration_secs REAL NOT NULL,
                report_json TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS exercise_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES exercise_runs(id) ON DELETE CASCADE,
                exercise_id TEXT NOT NULL,
                exercise_tier TEXT,
                passed INTEGER NOT NULL,
                duration_secs REAL NOT NULL,
                error TEXT,
                step_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS drift_guard_bypasses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                git_branch TEXT,
                head_sha TEXT,
                push_succeeded INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_exercise_runs_invocation ON exercise_runs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_exercise_runs_recorded ON exercise_runs(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_exercise_results_run ON exercise_results(run_id);
            CREATE INDEX IF NOT EXISTS idx_exercise_results_id ON exercise_results(exercise_id);
            CREATE INDEX IF NOT EXISTS idx_drift_guard_bypasses_recorded ON drift_guard_bypasses(recorded_at);

            CREATE TABLE IF NOT EXISTS wrapper_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                command TEXT,
                args TEXT,
                force_rebuild INTEGER NOT NULL DEFAULT 0,
                rebuild_reason TEXT,
                stage_durations_json TEXT,
                UNIQUE(event, started_at)
            );
            CREATE INDEX IF NOT EXISTS idx_wrapper_events_started ON wrapper_events(started_at);
            ",
        )?;
        Ok(())
    }

    /// Upsert devshell wrapper rebuild events (from `xtask-wrapper-events.jsonl`)
    /// into the `wrapper_events` table so checkout-local rebuild cost — the
    /// `xtask_build` stage plus any schema/initdb bootstrap — is queryable via
    /// `xtask history query` and joinable with `invocations` by time, instead of
    /// living only in the append-only JSONL. Idempotent via
    /// `UNIQUE(event, started_at)` + `INSERT OR IGNORE`; returns rows inserted.
    pub fn upsert_wrapper_events(&self, rows: &[WrapperEventRow]) -> Result<usize> {
        // Ensure the table on write: init_schema is schema-version-gated and does
        // not re-run on already-initialized databases, so a newly added table
        // must be ensured here (same approach as `ensure_proof_schema`).
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS wrapper_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                command TEXT,
                args TEXT,
                force_rebuild INTEGER NOT NULL DEFAULT 0,
                rebuild_reason TEXT,
                stage_durations_json TEXT,
                UNIQUE(event, started_at)
            );
            CREATE INDEX IF NOT EXISTS idx_wrapper_events_started ON wrapper_events(started_at);",
        )?;
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO wrapper_events \
             (event, status, started_at, finished_at, duration_secs, command, args, \
              force_rebuild, rebuild_reason, stage_durations_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )?;
        let mut inserted = 0usize;
        for row in rows {
            inserted += stmt.execute(params![
                row.event,
                row.status,
                row.started_at,
                row.finished_at,
                row.duration_secs,
                row.command,
                row.args,
                i64::from(row.force_rebuild),
                row.rebuild_reason,
                row.stage_durations_json,
            ])?;
        }
        Ok(inserted)
    }

    pub(super) fn ensure_column_exists(
        &self,
        table: &str,
        column: &str,
        definition: &str,
    ) -> Result<()> {
        if self.column_exists(table, column)? {
            return Ok(());
        }

        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
        with_sqlite_lock_retry(
            &format!("add {table}.{column} compatibility column"),
            || match self.conn.execute(&sql, []) {
                Ok(_) => Ok(()),
                Err(error) => {
                    if self.column_exists(table, column)? {
                        return Ok(());
                    }
                    Err(error).with_context(|| {
                        format!("failed to add {table}.{column} compatibility column")
                    })
                }
            },
        )?;
        Ok(())
    }

    pub(super) fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let pragma = format!("PRAGMA table_info({table})");
        let mut stmt = self
            .conn
            .prepare(&pragma)
            .with_context(|| format!("failed to inspect {table} columns"))?;
        let exists = stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .flatten()
            .any(|name| name == column);

        Ok(exists)
    }

    pub(super) fn table_exists(&self, table: &str) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                params![table],
                |row| row.get::<_, i64>(0),
            )
            .map(|value| value != 0)
            .context("failed to inspect history DB tables")
    }

    pub(super) fn ensure_proof_schema(&self) -> Result<()> {
        if self.table_exists("proof_evidence")? && self.table_exists("test_proof_units")? {
            return Ok(());
        }
        if self.conn.is_readonly(rusqlite::DatabaseName::Main)? {
            return Ok(());
        }
        self.conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS proof_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                command TEXT NOT NULL,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                scope_json TEXT,
                artifact_json TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE TABLE IF NOT EXISTS test_proof_units (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER NOT NULL REFERENCES invocations(id) ON DELETE CASCADE,
                proof_kind TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                manifest_json TEXT NOT NULL,
                reusable INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                finished_at TEXT,
                duration_secs REAL,
                test_filter TEXT,
                UNIQUE(invocation_id, proof_kind, scope_key, input_fingerprint)
            );

            CREATE INDEX IF NOT EXISTS idx_proof_evidence_exact
                ON proof_evidence(command, proof_kind, scope_key, input_fingerprint, status, finished_at);
            CREATE INDEX IF NOT EXISTS idx_test_proof_units_exact
                ON test_proof_units(proof_kind, scope_key, input_fingerprint, reusable, status, finished_at);
            ",
        )?;
        // Add test_filter column for test-name granularity evidence (#1393 Phase 3).
        // The column is nullable — broad runs without a filter leave it NULL.
        let _ = self.conn.execute(
            "ALTER TABLE test_proof_units ADD COLUMN test_filter TEXT",
            [],
        );
        Ok(())
    }

    pub(super) fn ensure_impact_schema(&self) -> Result<()> {
        if self.table_exists("test_dependency_edges")?
            && self.table_exists("coverage_regions")?
            && self.table_exists("test_execution_manifests")?
            && self.table_exists("impact_runs")?
            && self.table_exists("impact_decisions")?
            && self.table_exists("impact_audit_runs")?
        {
            return Ok(());
        }
        if self.conn.is_readonly(rusqlite::DatabaseName::Main)? {
            return Ok(());
        }
        self.conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS test_dependency_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                edge_kind TEXT NOT NULL,
                subject TEXT NOT NULL,
                fingerprint TEXT,
                origin TEXT NOT NULL,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, edge_kind, subject, origin)
            );

            CREATE TABLE IF NOT EXISTS coverage_regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                file_path TEXT NOT NULL,
                function_name TEXT,
                line_start INTEGER,
                line_end INTEGER,
                region_hash TEXT,
                content_hash TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS test_execution_manifests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE CASCADE,
                test_name TEXT NOT NULL,
                package TEXT,
                module_path TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_line INTEGER NOT NULL,
                binary_id TEXT,
                pid INTEGER NOT NULL,
                attempt_id TEXT NOT NULL,
                planner_version TEXT NOT NULL,
                content_hash TEXT,
                recorded_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(invocation_id, test_name, module_path, source_file, source_line)
            );

            CREATE TABLE IF NOT EXISTS impact_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                mode TEXT NOT NULL,
                changed_json TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                accepted_risk_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                impact_run_id INTEGER NOT NULL REFERENCES impact_runs(id) ON DELETE CASCADE,
                action TEXT NOT NULL,
                subject TEXT,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS impact_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invocation_id INTEGER REFERENCES invocations(id) ON DELETE SET NULL,
                impact_run_id INTEGER REFERENCES impact_runs(id) ON DELETE SET NULL,
                sample_size INTEGER NOT NULL,
                sampled_json TEXT NOT NULL,
                command_json TEXT NOT NULL,
                status TEXT NOT NULL,
                false_negative_count INTEGER NOT NULL DEFAULT 0,
                output_json TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_test_dependency_edges_subject
                ON test_dependency_edges(edge_kind, subject, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_coverage_regions_path
                ON coverage_regions(file_path, package, test_name);
            CREATE INDEX IF NOT EXISTS idx_test_execution_manifest_source
                ON test_execution_manifests(source_file, package, test_name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_coverage_regions_identity
                ON coverage_regions(
                    invocation_id,
                    test_name,
                    file_path,
                    COALESCE(function_name, ''),
                    COALESCE(line_start, -1),
                    COALESCE(line_end, -1)
                );
            CREATE INDEX IF NOT EXISTS idx_impact_runs_invocation ON impact_runs(invocation_id);
            CREATE INDEX IF NOT EXISTS idx_impact_decisions_run ON impact_decisions(impact_run_id);
            CREATE INDEX IF NOT EXISTS idx_impact_audit_invocation ON impact_audit_runs(invocation_id);
            ",
        )?;
        Ok(())
    }

    pub(super) fn ensure_compat_schema(&self) -> Result<()> {
        self.ensure_proof_schema()?;
        self.ensure_impact_schema()?;
        self.ensure_column_exists("coverage_regions", "content_hash", "TEXT")?;
        self.ensure_column_exists("test_execution_manifests", "content_hash", "TEXT")?;
        self.ensure_column_exists("invocations", "process_cpu_usage_avg", "REAL")?;
        self.ensure_column_exists("invocations", "process_memory_usage_max_mb", "REAL")?;
        self.ensure_column_exists("invocations", "root_process_cpu_usage_avg", "REAL")?;
        self.ensure_column_exists("invocations", "root_process_memory_usage_max_mb", "REAL")?;
        self.ensure_column_exists("invocations", "shared_nix_daemon_cpu_usage_avg", "REAL")?;
        self.ensure_column_exists(
            "invocations",
            "shared_nix_daemon_memory_usage_max_mb",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_nix_build_slice_cpu_usage_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_nix_build_slice_memory_usage_max_mb",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_background_slice_cpu_usage_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "shared_background_slice_memory_usage_max_mb",
            "REAL",
        )?;
        self.ensure_column_exists("invocations", "host_cpu_pressure_some_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_io_pressure_some_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_io_pressure_full_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_memory_pressure_some_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_memory_pressure_full_avg10_max", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_read_mib_delta", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_write_mib_delta", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_read_iops_avg", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_write_iops_avg", "REAL")?;
        self.ensure_column_exists("invocations", "host_block_busiest_device", "TEXT")?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_total_mib_delta",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_read_iops_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_write_iops_avg",
            "REAL",
        )?;
        self.ensure_column_exists(
            "invocations",
            "host_block_busiest_device_weighted_io_ms_per_s",
            "REAL",
        )?;
        self.ensure_column_exists("invocations", "cancel_reason", "TEXT")?;
        self.ensure_column_exists("invocations", "cancelled_by", "TEXT")?;
        self.ensure_column_exists("invocations", "shm_free_min_mb", "REAL")?;
        self.ensure_column_exists("invocations", "shm_used_max_mb", "REAL")?;
        self.ensure_column_exists("invocations", "process_count_max", "INTEGER")?;
        self.ensure_column_exists("invocations", "resource_sample_count", "INTEGER")?;
        self.ensure_column_exists(
            "build_diagnostics",
            "authority",
            "TEXT NOT NULL DEFAULT 'proof'",
        )?;
        // Per-stage end-of-stage PSI snapshot (added for per-stage causal attribution
        // of dev-loop slowdowns). Nullable REAL — /proc/pressure may be unavailable.
        self.ensure_column_exists("stage_timings", "io_full_avg10", "REAL")?;
        self.ensure_column_exists("stage_timings", "cpu_some_avg10", "REAL")?;
        self.ensure_column_exists("stage_timings", "memory_some_avg10", "REAL")?;
        self.ensure_column_exists("stage_timings", "io_full_stall_us", "INTEGER")?;
        self.ensure_column_exists("stage_timings", "cpu_some_stall_us", "INTEGER")?;
        self.ensure_column_exists("stage_timings", "memory_some_stall_us", "INTEGER")?;
        Ok(())
    }
}
