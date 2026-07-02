use super::*;

impl HistoryDb {
    /// Mark invocations stuck in 'running' for over 10 minutes as 'cancelled',
    /// and aggressively reap zombies (alive PIDs running past 2× watchdog timeout).
    ///
    /// Called on `open()` to prevent orphaned invocations from accumulating
    /// when a process crashes before calling `finish_invocation()`.
    ///
    /// Three branches per candidate:
    /// - **Dead PID**: just mark cancelled (the crash/orphan safety net)
    /// - **Alive PID past zombie threshold**: SIGTERM → 2s wait → SIGKILL, then
    ///   mark killed with exit_code=124. This catches per-job watchdogs that
    ///   fail to survive their launching cgroup.
    /// - **Alive PID within legitimate window**: leave alone (drop guard handles
    ///   normal completion)
    pub(super) fn cleanup_stale_invocations(&self) -> Result<()> {
        let stale_candidates = if self.has_stale_invocations()? {
            self.stale_invocation_candidates()?
        } else {
            Vec::new()
        };
        let mut stale_invocation_ids = Vec::new();
        let mut zombie_invocation_ids = Vec::new();
        let mut orphaned_background_job_ids = self
            .finished_invocation_running_background_job_ids()?
            .into_iter()
            .collect::<HashSet<_>>();
        let mut killed_background_job_ids = HashSet::new();
        let mut reaped_zombies = 0usize;

        for candidate in stale_candidates {
            let pid_alive = candidate.pid.is_some_and(history_process_is_alive);

            if pid_alive {
                let Some(escape_threshold) =
                    background_watchdog_escape_threshold_secs(&candidate.command)
                else {
                    continue; // explicitly long-lived dev runtime, skip
                };
                if candidate.age_secs.unwrap_or(0.0) < escape_threshold {
                    continue; // legitimate long-running bg job, skip
                }

                // Zombie: alive but past the command-specific escape threshold.
                if let Some(pid) = candidate.pid {
                    try_reap_zombie_pid(pid);
                    reaped_zombies += 1;
                }
                zombie_invocation_ids.push(candidate.invocation_id);
                if let Some(background_job_id) = candidate.background_job_id {
                    killed_background_job_ids.insert(background_job_id);
                }
            } else {
                stale_invocation_ids.push(candidate.invocation_id);
                if let Some(background_job_id) = candidate.background_job_id {
                    orphaned_background_job_ids.insert(background_job_id);
                }
            }
        }

        if reaped_zombies > 0 {
            eprintln!(
                "ℹ️  Reaped {reaped_zombies} zombie invocation(s) (alive PID running past 2× watchdog timeout — see issue #1211)"
            );
        }

        let stale_cleaned = self.mark_stale_invocations_cancelled(
            &stale_invocation_ids,
            "stale_pid",
            "open_time_sweep",
            None,
            false,
        )?;
        let zombie_cleaned = self.mark_stale_invocations_cancelled(
            &zombie_invocation_ids,
            "zombie_reaped",
            "open_time_sweep",
            Some(124),
            true,
        )?;
        let cleaned = stale_cleaned + zombie_cleaned;
        if cleaned > 0 {
            eprintln!(
                "ℹ️  Cleaned up {cleaned} stale 'running' invocation(s) older than 10 minutes"
            );
        }

        self.mark_background_jobs_orphaned(
            &orphaned_background_job_ids.into_iter().collect::<Vec<_>>(),
        )?;
        self.mark_background_jobs_killed_by_watchdog(
            &killed_background_job_ids.into_iter().collect::<Vec<_>>(),
        )?;
        Ok(())
    }

    pub(super) fn has_stale_invocations(&self) -> Result<bool> {
        let has_stale: i64 = self
            .conn
            .query_row(
                r"
                SELECT EXISTS(
                    SELECT 1
                    FROM invocations
                    WHERE status = 'running'
                      AND started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
                )
                ",
                [],
                |row| row.get(0),
            )
            .context("failed to detect stale invocations before cleanup")?;

        Ok(has_stale != 0)
    }

    fn stale_invocation_candidates(&self) -> Result<Vec<StaleInvocationCandidate>> {
        let mut stmt = self
            .conn
            .prepare(
                r"
                SELECT
                    i.id,
                    bg.id,
                    COALESCE(bg.command, i.command),
                    COALESCE(i.pid, bg.pid),
                    (julianday('now') - julianday(i.started_at)) * 86400.0
                FROM invocations i
                LEFT JOIN background_jobs bg
                    ON bg.invocation_id = i.id
                   AND bg.job_status = 'running'
                WHERE i.status = 'running'
                  AND i.started_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
                ",
            )
            .context("failed to prepare stale invocation candidate query")?;
        let rows = stmt
            .query_map([], |row| {
                Ok(StaleInvocationCandidate {
                    invocation_id: row.get(0)?,
                    background_job_id: row.get(1)?,
                    command: row.get(2)?,
                    pid: row.get(3)?,
                    age_secs: row.get(4)?,
                })
            })
            .context("failed to execute stale invocation candidate query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect stale invocation candidates")
    }

    fn finished_invocation_running_background_job_ids(&self) -> Result<Vec<i64>> {
        let mut stmt = self
            .conn
            .prepare(
                r"
                SELECT bg.id
                FROM background_jobs bg
                JOIN invocations i ON i.id = bg.invocation_id
                WHERE bg.job_status = 'running'
                  AND i.finished_at IS NOT NULL
                ",
            )
            .context("failed to prepare finished invocation background-job repair query")?;
        let rows = stmt
            .query_map([], |row| row.get(0))
            .context("failed to execute finished invocation background-job repair query")?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .context("failed to collect finished invocation background-job repair candidates")
    }

    fn mark_stale_invocations_cancelled(
        &self,
        invocation_ids: &[i64],
        cancel_reason: &str,
        cancelled_by: &str,
        exit_code: Option<i32>,
        duration_known: bool,
    ) -> Result<usize> {
        if invocation_ids.is_empty() {
            return Ok(0);
        }

        // SQLite bind-variable limit is ~999; chunk at 500 for safety.
        // Guard with status IN ('running', 'pending') to avoid double-cancelling
        // rows that another process already transitioned to a terminal state.
        const BATCH: usize = 500;
        let mut total_cancelled = 0usize;
        for chunk in invocation_ids.chunks(BATCH) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                r"
                UPDATE invocations
                SET status = 'cancelled',
                    finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    exit_code = COALESCE(?3, exit_code),
                    duration_secs = CASE
                        WHEN ?4 THEN (julianday('now') - julianday(started_at)) * 86400
                        ELSE NULL
                    END,
                    cancel_reason = ?1,
                    cancelled_by = ?2
                WHERE id IN ({})
                  AND status IN ('running', 'pending')
                ",
                placeholders.join(",")
            );
            let params = std::iter::once(&cancel_reason as &dyn rusqlite::ToSql)
                .chain(std::iter::once(&cancelled_by as &dyn rusqlite::ToSql))
                .chain(std::iter::once(&exit_code as &dyn rusqlite::ToSql))
                .chain(std::iter::once(&duration_known as &dyn rusqlite::ToSql))
                .chain(chunk.iter().map(|id| id as &dyn rusqlite::ToSql));
            total_cancelled += self
                .conn
                .execute(&sql, rusqlite::params_from_iter(params))
                .context("failed to mark stale invocations as cancelled")?;
        }
        Ok(total_cancelled)
    }

    fn mark_background_jobs_orphaned(&self, background_job_ids: &[i64]) -> Result<()> {
        if background_job_ids.is_empty() {
            return Ok(());
        }

        // SQLite bind-variable limit is ~999; chunk at 500 for safety.
        // Guard with job_status = 'running' to avoid overwriting terminal states.
        const BATCH: usize = 500;
        for chunk in background_job_ids.chunks(BATCH) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                r"
                UPDATE background_jobs
                SET job_status = 'orphaned',
                    finished_at = COALESCE(
                        (
                            SELECT inv.finished_at
                            FROM invocations inv
                            WHERE inv.id = background_jobs.invocation_id
                        ),
                        strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    )
                WHERE id IN ({})
                  AND job_status = 'running'
                ",
                placeholders.join(",")
            );
            self.conn
                .execute(&sql, rusqlite::params_from_iter(chunk.iter()))
                .context("failed to mark stale background jobs as orphaned")?;
        }
        Ok(())
    }

    fn mark_background_jobs_killed_by_watchdog(&self, background_job_ids: &[i64]) -> Result<()> {
        if background_job_ids.is_empty() {
            return Ok(());
        }

        const BATCH: usize = 500;
        for chunk in background_job_ids.chunks(BATCH) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let sql = format!(
                r"
                UPDATE background_jobs
                SET job_status = 'killed',
                    exit_code = 124,
                    finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE id IN ({})
                  AND job_status = 'running'
                ",
                placeholders.join(",")
            );
            self.conn
                .execute(&sql, rusqlite::params_from_iter(chunk.iter()))
                .context("failed to mark zombie background jobs as killed")?;
        }
        Ok(())
    }

    /// Finish a background job and archive its log content in `background_job_logs`.
    ///
    /// Updates `background_jobs` row and inserts into `background_job_logs`.
    /// The invocation lifecycle is managed separately by `finish_invocation()`.
    pub fn finish_background_job(
        &self,
        job_id: i64,
        job_status: JobLifecycleStatus,
        exit_code: Option<i32>,
        _duration_secs: f64,
        stdout_path: Option<&std::path::Path>,
        stderr_path: Option<&std::path::Path>,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        let stdout_content = Self::read_background_job_log(stdout_path, "stdout")?;
        let stderr_content = Self::read_background_job_log(stderr_path, "stderr")?;

        self.conn.execute(
            r"UPDATE background_jobs
              SET finished_at = ?1, exit_code = ?2, job_status = ?3
              WHERE id = ?4",
            params![finished_at, exit_code, job_status.as_str(), job_id],
        )?;

        // Archive log content into dedicated table.
        if stdout_content.is_some() || stderr_content.is_some() {
            self.conn.execute(
                r"INSERT OR REPLACE INTO background_job_logs (job_id, stdout_content, stderr_content)
                  VALUES (?1, ?2, ?3)",
                params![job_id, stdout_content, stderr_content],
            )?;
        }

        Ok(())
    }

    fn read_background_job_log(
        path: Option<&std::path::Path>,
        stream_name: &str,
    ) -> Result<Option<String>> {
        let Some(path) = path else {
            return Ok(None);
        };
        let content = std::fs::read_to_string(path).with_context(|| {
            format!(
                "failed to read archived {stream_name} log from {}",
                path.display()
            )
        })?;
        Ok(Some(content))
    }

    /// Get log content for a completed job (reads from `background_job_logs`).
    pub fn get_job_logs(&self, job_id: i64) -> Result<(Option<String>, Option<String>)> {
        let result = self
            .conn
            .query_row(
                "SELECT stdout_content, stderr_content FROM background_job_logs WHERE job_id = ?1",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                },
            )
            .optional()?
            .unwrap_or((None, None));
        Ok(result)
    }

    /// Return the running background job PID for an invocation, if any.
    pub fn get_running_job_pid_for_invocation(&self, invocation_id: i64) -> Result<Option<u32>> {
        self.conn
            .query_row(
                r"
                SELECT pid
                FROM background_jobs
                WHERE invocation_id = ?1
                  AND job_status = 'running'
                  AND pid IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                ",
                params![invocation_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed to get running background job pid for invocation")
    }

    // ============ Background Job Methods ============

    /// Start a background job. Creates both an invocation row and a background_jobs row.
    ///
    /// Returns `(invocation_id, job_id)`. The invocation is the durable execution record;
    /// the job is the process handle. The child process claims the invocation via
    /// `XTASK_BG_INVOCATION_ID`; the job_id is used for directory naming and coordinator tracking.
    pub fn start_background_job(
        &self,
        command: &str,
        args: &[String],
        pid: Option<u32>,
        stdout_path: &Path,
        stderr_path: &Path,
    ) -> Result<(i64, i64)> {
        let args_json = serde_json::to_string(args)?;
        let git_snapshot = current_git_snapshot();
        let git_commit = git_snapshot.commit.clone();
        let git_dirty = git_snapshot.dirty;
        let host = crate::config::config().hostname.clone();
        let cwd = capture_working_directory(std::env::current_dir());
        let started_at = Timestamp::now().format_rfc3339();

        // Create the durable invocation record.
        self.conn.execute(
            r"INSERT INTO invocations
                (command, args_json, git_commit, git_dirty, started_at, host, cwd, status, launch_mode, is_background)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'running', 'background', 1)",
            params![command, args_json, git_commit, git_dirty, started_at, host, cwd],
        )?;
        let invocation_id = self.conn.last_insert_rowid();

        // Create the background job handle row.
        let stdout_str = if stdout_path == Path::new("") {
            None
        } else {
            Some(stdout_path.display().to_string())
        };
        let stderr_str = if stderr_path == Path::new("") {
            None
        } else {
            Some(stderr_path.display().to_string())
        };
        self.conn.execute(
            r"INSERT INTO background_jobs
                (invocation_id, command, args_json, pid, stdout_path, stderr_path, job_status, started_at)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', ?7)",
            params![invocation_id, command, args_json, pid, stdout_str, stderr_str, started_at],
        )?;
        let job_id = self.conn.last_insert_rowid();

        Ok((invocation_id, job_id))
    }

    /// Attach command metadata to a pre-created background invocation row.
    ///
    /// Background jobs are registered before spawning to reserve a stable ID.
    /// The child `xtask --fg` process then claims that row via `XTASK_BG_INVOCATION_ID`
    /// and records execution details on the same invocation.
    pub fn claim_background_invocation(
        &self,
        id: i64,
        command: &str,
        subcommand: Option<&str>,
        profile: Option<&str>,
        args_json: Option<&str>,
    ) -> Result<bool> {
        let updated = with_sqlite_lock_retry("claim background invocation", || {
            let updated = self.conn.execute(
                r"
                UPDATE invocations
                SET command = ?1,
                    subcommand = ?2,
                    profile = ?3,
                    args_json = COALESCE(?4, args_json)
                WHERE id = ?5 AND is_background = 1
                ",
                params![command, subcommand, profile, args_json, id],
            )?;
            Ok(updated)
        })?;
        Ok(updated == 1)
    }

    /// Get all active (running) background jobs.
    pub fn get_active_background_jobs(&self) -> Result<Vec<BackgroundJob>> {
        let mut stmt = self.conn.prepare(
            r"SELECT id, invocation_id, command, args_json, started_at, pid, stdout_path, stderr_path, job_status, exit_code
              FROM background_jobs
              WHERE job_status = 'running'
              ORDER BY started_at DESC",
        )?;
        let rows = stmt.query_map([], row_to_background_job)?;
        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Get a single background job by ID (O(1) direct SQL lookup).
    pub fn get_background_job_by_id(&self, id: i64) -> Result<Option<BackgroundJob>> {
        self.conn
            .query_row(
                r"SELECT id, invocation_id, command, args_json, started_at, pid, stdout_path, stderr_path, job_status, exit_code
                  FROM background_jobs WHERE id = ?1",
                params![id],
                row_to_background_job,
            )
            .optional()
            .context("failed to get background job by id")
    }

    /// Get all background job IDs (for prune orphan directory cleanup).
    pub fn get_all_background_job_ids(&self) -> Result<HashSet<i64>> {
        let mut stmt = self.conn.prepare("SELECT id FROM background_jobs")?;
        let rows = stmt.query_map([], |row| row.get::<_, i64>(0))?;
        let mut ids = HashSet::new();
        for id in rows {
            ids.insert(id?);
        }
        Ok(ids)
    }

    /// Get recent background jobs (including completed ones).
    pub fn get_recent_background_jobs(&self, limit: usize) -> Result<Vec<BackgroundJob>> {
        let mut stmt = self.conn.prepare(
            r"SELECT id, invocation_id, command, args_json, started_at, pid, stdout_path, stderr_path, job_status, exit_code
              FROM background_jobs
              ORDER BY started_at DESC
              LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], row_to_background_job)?;
        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect background jobs")
    }

    /// Update a background job's PID (used when process is spawned).
    pub fn update_job_pid(&self, job_id: i64, pid: u32) -> Result<()> {
        self.conn.execute(
            "UPDATE background_jobs SET pid = ?1 WHERE id = ?2",
            params![pid, job_id],
        )?;
        Ok(())
    }

    /// Update a background job's log file paths.
    pub fn update_job_paths(
        &self,
        job_id: i64,
        stdout_path: &std::path::Path,
        stderr_path: &std::path::Path,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE background_jobs SET stdout_path = ?1, stderr_path = ?2 WHERE id = ?3",
            params![
                stdout_path.display().to_string(),
                stderr_path.display().to_string(),
                job_id
            ],
        )?;
        Ok(())
    }

    /// Check if a background job's process is still running.
    pub fn is_job_running(&self, job_id: i64) -> Result<bool> {
        let pid: Option<u32> = self
            .conn
            .query_row(
                "SELECT pid FROM background_jobs WHERE id = ?1",
                params![job_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(pid.is_some_and(is_process_running))
    }
}
