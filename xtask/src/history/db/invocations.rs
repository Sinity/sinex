use super::*;

impl HistoryDb {
    /// Start a new invocation record. Returns the invocation ID.
    pub fn start_invocation(
        &self,
        command: &str,
        subcommand: Option<&str>,
        profile: Option<&str>,
        args_json: Option<&str>,
    ) -> Result<i64> {
        let git_snapshot = current_git_snapshot();
        let git_commit = git_snapshot.commit.clone();
        let git_dirty = git_snapshot.dirty;
        let host = crate::config::config().hostname.clone();
        let cwd = capture_working_directory(std::env::current_dir());
        let started_at = Timestamp::now().format_rfc3339();

        // Transition from synthetic to real: clear the marker and insert the
        // invocation row atomically so a crash between the two cannot leave the
        // DB in a state where the synthetic marker is gone but no real row exists.
        let is_synthetic = self.is_synthetic;
        with_sqlite_lock_retry("start invocation history row", || {
            self.conn.execute("BEGIN", [])?;
            if is_synthetic {
                match self
                    .conn
                    .execute("DELETE FROM metadata WHERE key = 'synthetic'", [])
                {
                    Ok(_) => {}
                    Err(err) => {
                        let _ = self.conn.execute("ROLLBACK", []);
                        return Err(color_eyre::eyre::Report::from(err))
                            .wrap_err("failed to clear synthetic marker");
                    }
                }
            }
            let result = self.conn.execute(
                r"
                INSERT INTO invocations (command, subcommand, profile, args_json, git_commit, git_dirty, started_at, host, cwd, status)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'running')
                ",
                params![command, subcommand, profile, args_json, git_commit, git_dirty, started_at, host, cwd],
            );
            match result {
                Ok(_) => {
                    self.conn.execute("COMMIT", [])?;
                }
                Err(err) => {
                    let _ = self.conn.execute("ROLLBACK", []);
                    return Err(err.into());
                }
            }
            Ok(())
        })?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Finish an invocation with the given status and exit code.
    pub fn finish_invocation(
        &self,
        id: i64,
        status: InvocationStatus,
        exit_code: Option<i32>,
        duration_secs: f64,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        with_sqlite_lock_retry("finish invocation history row", || {
            self.conn.execute(
                r"
                UPDATE invocations
                SET finished_at = ?1, duration_secs = ?2, exit_code = ?3, status = ?4
                WHERE id = ?5
                ",
                params![finished_at, duration_secs, exit_code, status.as_str(), id],
            )?;
            self.conn.execute(
                r"
                UPDATE proof_evidence
                SET finished_at = ?1, duration_secs = ?2, status = ?3
                WHERE invocation_id = ?4
                ",
                params![finished_at, duration_secs, status.as_str(), id],
            )?;
            self.conn.execute(
                r"
                UPDATE test_proof_units
                SET finished_at = ?1, duration_secs = ?2, status = ?3
                WHERE invocation_id = ?4
                ",
                params![finished_at, duration_secs, status.as_str(), id],
            )?;
            Ok(())
        })?;

        Ok(())
    }

    /// Finish a cancelled invocation and record why it was cancelled.
    pub fn finish_invocation_cancelled(
        &self,
        id: i64,
        exit_code: Option<i32>,
        duration_secs: f64,
        cancel_reason: &str,
        cancelled_by: &str,
    ) -> Result<()> {
        let finished_at = Timestamp::now().format_rfc3339();

        with_sqlite_lock_retry("finish cancelled invocation history row", || {
            self.conn.execute(
                r"
                UPDATE invocations
                SET finished_at = ?1,
                    duration_secs = ?2,
                    exit_code = ?3,
                    status = 'cancelled',
                    cancel_reason = ?4,
                    cancelled_by = ?5
                WHERE id = ?6
                ",
                params![
                    finished_at,
                    duration_secs,
                    exit_code,
                    cancel_reason,
                    cancelled_by,
                    id
                ],
            )?;
            Ok(())
        })?;

        Ok(())
    }

    /// Return cancellation metadata for an invocation, when present.
    pub fn get_invocation_cancel_metadata(
        &self,
        id: i64,
    ) -> Result<Option<(Option<String>, Option<String>)>> {
        self.conn
            .query_row(
                "SELECT cancel_reason, cancelled_by FROM invocations WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .context("failed to read invocation cancellation metadata")
    }

    /// Record timing for a pipeline stage (fmt, clippy, forbidden, compile, preflight).
    pub fn record_stage_timing(
        &self,
        invocation_id: i64,
        stage_name: &str,
        started_at: &str,
        duration_secs: f64,
        success: bool,
        pressure: StagePressure,
    ) -> Result<()> {
        with_sqlite_lock_retry("record stage timing", || {
            self.conn.execute(
                r"
                INSERT INTO stage_timings (
                    invocation_id, stage_name, started_at, duration_secs, success,
                    io_full_avg10, cpu_some_avg10, memory_some_avg10,
                    io_full_stall_us, cpu_some_stall_us, memory_some_stall_us
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ",
                params![
                    invocation_id,
                    stage_name,
                    started_at,
                    duration_secs,
                    i32::from(success),
                    pressure.io_full_avg10,
                    pressure.cpu_some_avg10,
                    pressure.memory_some_avg10,
                    pressure.io_full_stall_us,
                    pressure.cpu_some_stall_us,
                    pressure.memory_some_stall_us,
                ],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Set the currently executing pipeline stage for an in-flight invocation.
    ///
    /// This is written at `start_stage()` time and cleared at `finish_stage()` time,
    /// giving real-time visibility into what a running background job is doing.
    pub fn set_live_stage(&self, invocation_id: i64, stage: &str) -> Result<()> {
        with_sqlite_lock_retry("set live stage", || {
            self.conn.execute(
                "UPDATE invocations SET live_stage = ?1 WHERE id = ?2",
                params![stage, invocation_id],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Clear the live stage field (called when a stage finishes).
    pub fn clear_live_stage(&self, invocation_id: i64) -> Result<()> {
        with_sqlite_lock_retry("clear live stage", || {
            self.conn.execute(
                "UPDATE invocations SET live_stage = NULL WHERE id = ?1",
                params![invocation_id],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Get the currently executing stage for a running invocation.
    pub fn get_live_stage(&self, invocation_id: i64) -> Result<Option<String>> {
        let stage = self
            .conn
            .query_row(
                "SELECT live_stage FROM invocations WHERE id = ?1",
                params![invocation_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(stage)
    }

    /// Get all recorded stage timings for an invocation, ordered by start time.
    pub fn get_stage_timings_for_invocation(&self, invocation_id: i64) -> Result<Vec<StageTiming>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT invocation_id, stage_name, started_at, duration_secs, success,
                   io_full_avg10, cpu_some_avg10, memory_some_avg10,
                   io_full_stall_us, cpu_some_stall_us, memory_some_stall_us
            FROM stage_timings
            WHERE invocation_id = ?1
            ORDER BY started_at ASC
            ",
        )?;
        let rows = stmt
            .query_map(params![invocation_id], |row| {
                Ok(StageTiming {
                    invocation_id: row.get(0)?,
                    stage_name: row.get(1)?,
                    started_at: row.get(2)?,
                    duration_secs: row.get(3)?,
                    success: row.get::<_, i32>(4)? != 0,
                    io_full_avg10: row.get(5)?,
                    cpu_some_avg10: row.get(6)?,
                    memory_some_avg10: row.get(7)?,
                    io_full_stall_us: row.get(8)?,
                    cpu_some_stall_us: row.get(9)?,
                    memory_some_stall_us: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Write or update the live progress snapshot for an invocation.
    ///
    /// Called by CommandContext::report_progress() on stage transitions and
    /// incremental progress updates (e.g. nextest test count changes).
    pub fn write_progress(
        &self,
        invocation_id: i64,
        phase: Option<&str>,
        step: Option<&str>,
        pct_done: Option<f64>,
        items_done: Option<i64>,
        items_total: Option<i64>,
    ) -> Result<()> {
        self.write_progress_full(
            invocation_id,
            phase,
            step,
            pct_done,
            items_done,
            items_total,
            Some("indeterminate"),
            None,
            None,
            Some("none"),
            None,
        )
    }

    /// Write or update the live progress snapshot with full field set.
    ///
    /// Extends write_progress with mode, unit_kind, rate_per_sec, eta_confidence,
    /// and terminal_summary for richer progress reporting (e.g. determinate compilation).
    #[allow(clippy::too_many_arguments)]
    pub fn write_progress_full(
        &self,
        invocation_id: i64,
        phase: Option<&str>,
        step: Option<&str>,
        pct_done: Option<f64>,
        items_done: Option<i64>,
        items_total: Option<i64>,
        mode: Option<&str>,
        unit_kind: Option<&str>,
        rate_per_sec: Option<f64>,
        eta_confidence: Option<&str>,
        terminal_summary: Option<&str>,
    ) -> Result<()> {
        let updated_at = Timestamp::now().format_rfc3339();
        with_sqlite_lock_retry("write invocation progress", || {
            self.conn.execute(
                r"INSERT INTO invocation_progress
                      (invocation_id, phase, step, pct_done, items_done, items_total, updated_at,
                       mode, unit_kind, rate_per_sec, eta_confidence, terminal_summary)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                  ON CONFLICT(invocation_id) DO UPDATE SET
                      phase = excluded.phase,
                      step = excluded.step,
                      pct_done = excluded.pct_done,
                      items_done = excluded.items_done,
                      items_total = excluded.items_total,
                      updated_at = excluded.updated_at,
                      mode = excluded.mode,
                      unit_kind = excluded.unit_kind,
                      rate_per_sec = excluded.rate_per_sec,
                      eta_confidence = excluded.eta_confidence,
                      terminal_summary = excluded.terminal_summary",
                params![
                    invocation_id,
                    phase,
                    step,
                    pct_done,
                    items_done,
                    items_total,
                    updated_at,
                    mode,
                    unit_kind,
                    rate_per_sec,
                    eta_confidence,
                    terminal_summary,
                ],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Get the current progress snapshot for an invocation.
    pub fn get_progress(&self, invocation_id: i64) -> Result<Option<InvocationProgress>> {
        self.conn
            .query_row(
                r"SELECT invocation_id, phase, step, pct_done, items_done, items_total, updated_at,
                         mode, unit_kind, rate_per_sec, eta_confidence, terminal_summary
                  FROM invocation_progress WHERE invocation_id = ?1",
                params![invocation_id],
                |row| {
                    Ok(InvocationProgress {
                        invocation_id: row.get(0)?,
                        phase: row.get(1)?,
                        step: row.get(2)?,
                        pct_done: row.get(3)?,
                        items_done: row.get(4)?,
                        items_total: row.get(5)?,
                        updated_at: row.get(6)?,
                        mode: row.get(7)?,
                        unit_kind: row.get(8)?,
                        rate_per_sec: row.get(9)?,
                        eta_confidence: row.get(10)?,
                        terminal_summary: row.get(11)?,
                    })
                },
            )
            .optional()
            .context("failed to get invocation progress")
    }

    /// Record an ETA sample for a (command, phase) pair.
    ///
    /// Called by CommandContext::finish_stage() to accumulate timing data
    /// for future ETA estimates.
    pub fn record_eta_sample(
        &self,
        invocation_id: i64,
        command: &str,
        phase: &str,
        duration_secs: f64,
    ) -> Result<()> {
        let sampled_at = Timestamp::now().format_rfc3339();
        with_sqlite_lock_retry("record eta sample", || {
            self.conn.execute(
                r"INSERT INTO invocation_eta_samples (invocation_id, command, phase, duration_secs, sampled_at)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![invocation_id, command, phase, duration_secs, sampled_at],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Get the median duration for a (command, phase) pair over the last N samples.
    ///
    /// Returns None if fewer than 3 samples exist (insufficient data for a reliable estimate).
    pub fn get_eta_estimate(
        &self,
        command: &str,
        phase: &str,
        window: usize,
    ) -> Result<Option<f64>> {
        let limit = window.max(3);
        let mut stmt = self.conn.prepare(
            r"SELECT duration_secs FROM invocation_eta_samples
              WHERE command = ?1 AND phase = ?2
              ORDER BY sampled_at DESC
              LIMIT ?3",
        )?;
        let samples: Vec<f64> = stmt
            .query_map(params![command, phase, limit as i64], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        if samples.len() < 3 {
            return Ok(None);
        }

        // Median: sort and take middle value
        let mut sorted = samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = sorted.len() / 2;
        Ok(Some(sorted[mid]))
    }

    /// Get all distinct (phase, median_duration_secs) pairs for a command.
    ///
    /// Returns a list of `(phase, median_secs, sample_count)` tuples sorted by phase name.
    /// Phases with fewer than 3 samples are included but flagged via sample_count.
    pub fn get_eta_phases(&self, command: &str) -> Result<Vec<(String, Option<f64>, usize)>> {
        let mut stmt = self.conn.prepare(
            r"SELECT phase, duration_secs FROM invocation_eta_samples
              WHERE command = ?1
              ORDER BY phase, sampled_at DESC",
        )?;
        let rows: Vec<(String, f64)> = stmt
            .query_map(params![command], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        // Group by phase
        let mut by_phase: std::collections::BTreeMap<String, Vec<f64>> =
            std::collections::BTreeMap::new();
        for (phase, dur) in rows {
            by_phase.entry(phase).or_default().push(dur);
        }

        let result = by_phase
            .into_iter()
            .map(|(phase, mut samples)| {
                let count = samples.len();
                let median = if count >= 3 {
                    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    Some(samples[count / 2])
                } else {
                    None
                };
                (phase, median, count)
            })
            .collect();
        Ok(result)
    }

    /// Get recent invocations, optionally filtered by command.
    pub fn get_recent(
        &self,
        limit: usize,
        command_filter: Option<&str>,
    ) -> Result<Vec<Invocation>> {
        let mut query = InvocationQuery::new().limit(limit);
        if let Some(command_filter) = command_filter {
            query = query.command(command_filter);
        }
        query.run(self)
    }

    /// Get recent invocations with filtering, sorting, and pagination (G5).
    ///
    /// - `since_rfc3339`: if provided, only invocations started after this timestamp
    /// - `sort_by`: "started" (default), "duration", or "status"
    /// - `offset`: skip N entries for pagination
    pub fn get_recent_filtered(
        &self,
        limit: usize,
        offset: usize,
        command_filter: Option<&str>,
        since_rfc3339: Option<&str>,
        sort_by: &str,
    ) -> Result<Vec<Invocation>> {
        let mut query = InvocationQuery::new().limit(limit).offset(offset);
        if let Some(command_filter) = command_filter {
            query = query.command(command_filter);
        }
        if let Some(since_rfc3339) = since_rfc3339 {
            query = query.since_rfc3339(since_rfc3339);
        }
        query = match sort_by {
            "duration" => query.sort_duration(),
            "status" => query.sort_status(),
            _ => query.sort_started(),
        };
        query.run(self)
    }

    /// Get a specific invocation by database ID.
    pub fn get_invocation(&self, invocation_id: i64) -> Result<Option<Invocation>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            WHERE id = ?1
            LIMIT 1
            ",
        )?;

        stmt.query_row(params![invocation_id], row_to_invocation)
            .optional()
            .context("failed to get invocation by id")
    }

    /// Get the most recent invocation for a command.
    pub fn get_last(&self, command: &str) -> Result<Option<Invocation>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT id, command, subcommand, profile, args_json, git_commit, git_dirty,
                   started_at, finished_at, duration_secs, exit_code, status, host, cwd, live_stage
            FROM invocations
            WHERE command = ?1
            ORDER BY started_at DESC
            LIMIT 1
            ",
        )?;

        stmt.query_row(params![command], row_to_invocation)
            .optional()
            .context("failed to get last invocation")
    }

    /// Get statistics for a command.
    ///
    /// Only includes `success` and `failed` invocations — excludes `running`
    /// (incomplete) and `cancelled` (which have inflated durations from zombie
    /// cleanup, poisoning AVG calculations).
    pub fn get_stats(&self, command: &str, days: u32) -> Result<CommandStats> {
        let since = Timestamp::now() - time::Duration::days(i64::from(days));
        let since_str = since.format_rfc3339();

        let mut stmt = self.conn.prepare(
            r"
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failures,
                AVG(duration_secs) as avg_duration
            FROM invocations
            WHERE command = ?1 AND started_at >= ?2 AND status IN ('success', 'failed')
            ",
        )?;

        let stats = stmt.query_row(params![command, since_str], |row| {
            Ok(CommandStats {
                total: row.get(0)?,
                successes: row.get(1)?,
                failures: row.get(2)?,
                avg_duration_secs: row.get(3)?,
            })
        })?;

        Ok(stats)
    }

    /// Get the last completed invocation for a command that has a tree fingerprint.
    ///
    /// Used by the coordinator to check for "fresh" results.
    pub fn get_last_completed_with_fingerprint(
        &self,
        command: &str,
    ) -> Result<Option<InvocationWithFingerprint>> {
        self.conn
            .query_row(
                r"
                SELECT id, status, duration_secs, tree_fingerprint, scope_key
                FROM invocations
                WHERE command = ?1
                  AND status IN ('success', 'failed')
                  AND tree_fingerprint IS NOT NULL
                ORDER BY started_at DESC
                LIMIT 1
                ",
                params![command],
                |row| {
                    let status_str: String = row.get(1)?;
                    Ok(InvocationWithFingerprint {
                        id: row.get(0)?,
                        status: parse_stored_invocation_status(status_str)?,
                        duration_secs: row.get(2)?,
                        tree_fingerprint: row.get(3)?,
                        scope_key: row.get(4)?,
                    })
                },
            )
            .optional()
            .context("failed to get last completed invocation with fingerprint")
    }

    /// Get the newest successful invocation matching an exact freshness key.
    ///
    /// This is stricter than `get_last_completed_with_fingerprint`: it can find
    /// an older valid proof even when a newer invocation for the same command
    /// ran a different scope, and it never returns failed evidence.
    pub fn get_successful_invocation_by_fingerprint(
        &self,
        command: &str,
        tree_fingerprint: &str,
        scope_key: &str,
    ) -> Result<Option<InvocationWithFingerprint>> {
        self.conn
            .query_row(
                r"
                SELECT id, status, duration_secs, tree_fingerprint, scope_key
                FROM invocations
                WHERE command = ?1
                  AND status = 'success'
                  AND tree_fingerprint = ?2
                  AND scope_key = ?3
                ORDER BY started_at DESC
                LIMIT 1
                ",
                params![command, tree_fingerprint, scope_key],
                |row| {
                    let status_str: String = row.get(1)?;
                    Ok(InvocationWithFingerprint {
                        id: row.get(0)?,
                        status: parse_stored_invocation_status(status_str)?,
                        duration_secs: row.get(2)?,
                        tree_fingerprint: row.get(3)?,
                        scope_key: row.get(4)?,
                    })
                },
            )
            .optional()
            .context("failed to get successful invocation by fingerprint")
    }

    /// Get the newest successful proof evidence row matching an exact key.
    pub fn get_successful_proof_evidence(
        &self,
        command: &str,
        proof_kind: &str,
        input_fingerprint: &str,
        scope_key: &str,
    ) -> Result<Option<ProofEvidence>> {
        self.conn
            .query_row(
                r"
                SELECT
                    id,
                    invocation_id,
                    command,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    scope_json,
                    artifact_json
                FROM proof_evidence
                WHERE command = ?1
                  AND proof_kind = ?2
                  AND input_fingerprint = ?3
                  AND scope_key = ?4
                  AND status = 'success'
                ORDER BY finished_at DESC, id DESC
                LIMIT 1
                ",
                params![command, proof_kind, input_fingerprint, scope_key],
                row_to_proof_evidence,
            )
            .optional()
            .context("failed to get successful proof evidence")
    }

    /// Get the newest successful reusable test proof unit matching an exact key.
    pub fn get_successful_reusable_test_proof_unit(
        &self,
        proof_kind: &str,
        input_fingerprint: &str,
        scope_key: &str,
    ) -> Result<Option<TestProofUnit>> {
        self.conn
            .query_row(
                r"
                SELECT
                    id,
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    reusable,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    test_filter
                FROM test_proof_units
                WHERE proof_kind = ?1
                  AND input_fingerprint = ?2
                  AND scope_key = ?3
                  AND reusable = 1
                  AND status = 'success'
                ORDER BY finished_at DESC, id DESC
                LIMIT 1
                ",
                params![proof_kind, input_fingerprint, scope_key],
                row_to_test_proof_unit,
            )
            .optional()
            .context("failed to get successful reusable test proof unit")
    }

    /// Look up any test proof unit for a given scope key, ignoring the fingerprint.
    ///
    /// Used to detect stale proofs: a proof existed for this scope in a prior run
    /// but the current input fingerprint no longer matches (source/tooling changed).
    pub fn get_any_successful_test_proof_for_scope(
        &self,
        proof_kind: &str,
        scope_key: &str,
    ) -> Result<Option<TestProofUnit>> {
        self.conn
            .query_row(
                r"
                SELECT
                    id,
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    reusable,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    test_filter
                FROM test_proof_units
                WHERE proof_kind = ?1
                  AND scope_key = ?2
                  AND reusable = 1
                  AND status = 'success'
                ORDER BY finished_at DESC, id DESC
                LIMIT 1
                ",
                params![proof_kind, scope_key],
                row_to_test_proof_unit,
            )
            .optional()
            .context("failed to get any successful test proof for scope")
    }

    /// Update an invocation's tree fingerprint and scope key.
    ///
    /// Called after starting an invocation to record the coordination scope.
    pub fn update_invocation_fingerprint(
        &self,
        id: i64,
        tree_fingerprint: &str,
        scope_key: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET tree_fingerprint = ?1, scope_key = ?2 WHERE id = ?3",
            params![tree_fingerprint, scope_key, id],
        )?;
        Ok(())
    }

    /// Record the proof unit represented by a coordinated invocation.
    pub fn record_proof_evidence(
        &self,
        invocation_id: i64,
        command: &str,
        proof_kind: &str,
        scope_key: &str,
        input_fingerprint: &str,
        scope_json: Option<&str>,
        artifact_json: Option<&str>,
    ) -> Result<()> {
        with_sqlite_lock_retry("record proof evidence", || {
            self.conn.execute(
                r"
                INSERT INTO proof_evidence (
                    invocation_id,
                    command,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    scope_json,
                    artifact_json
                )
                SELECT
                    id,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    status,
                    started_at,
                    finished_at,
                    duration_secs,
                    ?6,
                    ?7
                FROM invocations
                WHERE id = ?1
                ON CONFLICT(invocation_id, proof_kind, scope_key, input_fingerprint)
                DO UPDATE SET
                    command = excluded.command,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    duration_secs = excluded.duration_secs,
                    scope_json = excluded.scope_json,
                    artifact_json = excluded.artifact_json
                ",
                params![
                    invocation_id,
                    command,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    scope_json,
                    artifact_json
                ],
            )?;
            Ok(())
        })
    }

    /// Record the resolved test manifest as a proof unit.
    /// Store the effective test filter for a proof unit, enabling per-test-name
    /// granularity evidence lookups (#1393 Phase 3).
    pub fn set_test_proof_filter(
        &self,
        invocation_id: i64,
        proof_kind: &str,
        scope_key: &str,
        input_fingerprint: &str,
        test_filter: &str,
    ) -> Result<()> {
        with_sqlite_lock_retry("set test proof filter", || {
            self.conn.execute(
                "UPDATE test_proof_units SET test_filter = ?5
                 WHERE invocation_id = ?1 AND proof_kind = ?2
                   AND scope_key = ?3 AND input_fingerprint = ?4",
                params![
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    test_filter
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_test_proof_unit(
        &self,
        invocation_id: i64,
        proof_kind: &str,
        scope_key: &str,
        input_fingerprint: &str,
        manifest_json: &str,
        reusable: bool,
    ) -> Result<()> {
        with_sqlite_lock_retry("record test proof unit", || {
            self.conn.execute(
                r"
                INSERT INTO test_proof_units (
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    reusable,
                    status,
                    started_at,
                    finished_at,
                    duration_secs
                )
                SELECT
                    id,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    ?6,
                    status,
                    started_at,
                    finished_at,
                    duration_secs
                FROM invocations
                WHERE id = ?1
                ON CONFLICT(invocation_id, proof_kind, scope_key, input_fingerprint)
                DO UPDATE SET
                    manifest_json = excluded.manifest_json,
                    reusable = excluded.reusable,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    duration_secs = excluded.duration_secs
                ",
                params![
                    invocation_id,
                    proof_kind,
                    scope_key,
                    input_fingerprint,
                    manifest_json,
                    i64::from(reusable)
                ],
            )?;
            Ok(())
        })
    }

    /// Update an invocation's semantic workload arguments.
    pub fn update_invocation_args(&self, id: i64, args_json: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE invocations SET args_json = ?1 WHERE id = ?2",
            params![args_json, id],
        )?;
        Ok(())
    }

    /// Prune old background job handles (from `background_jobs`) older than `older_than_days`.
    ///
    /// This removes operational job handles and their cached logs, but does NOT touch the
    /// `invocations` table. Durable execution history survives independently of job pruning.
    pub fn prune_background_jobs(&self, older_than_days: u32) -> Result<usize> {
        if older_than_days == 0 {
            return Ok(0);
        }
        let interval = format!("-{older_than_days} days");
        let deleted = self.conn.execute(
            r"DELETE FROM background_jobs
              WHERE finished_at IS NOT NULL
                AND finished_at < strftime('%Y-%m-%dT%H:%M:%SZ', 'now', ?1)",
            rusqlite::params![interval],
        )?;
        Ok(deleted)
    }
}
