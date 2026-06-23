use super::*;

impl HistoryDb {
    /// Record a completed exercise run into `exercise_runs` + `exercise_results`.
    ///
    /// Stores tier breakdown, pass/fail counts, duration, full report JSON, and
    /// per-exercise results so `xtask history exercise` can surface regressions.
    /// Called best-effort from `ExerciseCommand::execute()` via `ctx.with_history_db`.
    pub fn record_exercise_run(
        &self,
        invocation_id: i64,
        report: &crate::commands::exercise::ExerciseReport,
    ) -> Result<()> {
        validate_finite_duration_secs("exercise report", report.duration_secs)?;
        for entry in &report.results {
            validate_finite_duration_secs(
                &format!("exercise result '{}'", entry.id),
                entry.duration_secs,
            )?;
            for step in &entry.steps {
                validate_finite_duration_secs(
                    &format!("exercise step '{}' for '{}'", step.label, entry.id),
                    step.duration_secs,
                )?;
            }
        }

        let report_json = serde_json::to_string(report)
            .wrap_err("failed to serialize exercise report for history persistence")?;
        // Infer tier from results: if mixed, leave NULL (multi-tier run).
        let tier: Option<&str> = {
            let tiers: std::collections::HashSet<&str> =
                report.results.iter().map(|r| r.tier.as_str()).collect();
            if tiers.len() == 1 {
                tiers.into_iter().next()
            } else {
                None
            }
        };

        let run_id = self
            .conn
            .query_row(
                r"INSERT INTO exercise_runs
                (invocation_id, tier, total, passed, failed, skipped, duration_secs, report_json)
              VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
              RETURNING id",
                rusqlite::params![
                    invocation_id,
                    tier,
                    report.total as i64,
                    report.passed as i64,
                    report.failed as i64,
                    report.skipped as i64,
                    report.duration_secs,
                    Some(report_json),
                ],
                |row| row.get::<_, i64>(0),
            )
            .context("failed to insert exercise_run row")?;

        for entry in &report.results {
            self.conn
                .execute(
                    r"INSERT INTO exercise_results
                    (run_id, exercise_id, exercise_tier, passed, duration_secs, error, step_count)
                  VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        run_id,
                        entry.id,
                        entry.tier,
                        i64::from(entry.passed),
                        entry.duration_secs,
                        entry.error,
                        entry.steps.len() as i64,
                    ],
                )
                .context("failed to insert exercise_result row")?;
        }

        Ok(())
    }

    /// Fetch recent exercise runs for `xtask history exercise`.
    pub fn get_exercise_runs(&self, limit: usize) -> Result<Vec<ExerciseRunRow>> {
        let mut stmt = self.conn.prepare(
            r"SELECT er.id, er.invocation_id, er.tier, er.total, er.passed, er.failed,
                     er.skipped, er.duration_secs, er.recorded_at,
                     inv.status, inv.git_commit
              FROM exercise_runs er
              LEFT JOIN invocations inv ON inv.id = er.invocation_id
              ORDER BY er.recorded_at DESC
              LIMIT ?1",
        )?;
        let rows = stmt
            .query_map([limit as i64], |row| {
                Ok(ExerciseRunRow {
                    run_id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    tier: row.get(2)?,
                    total: row.get(3)?,
                    passed: row.get(4)?,
                    failed: row.get(5)?,
                    skipped: row.get(6)?,
                    duration_secs: row.get(7)?,
                    recorded_at: row.get(8)?,
                    invocation_status: row.get(9)?,
                    git_commit: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Fetch per-exercise results for a run.
    pub fn get_exercise_results_for_run(&self, run_id: i64) -> Result<Vec<ExerciseResultRow>> {
        let mut stmt = self.conn.prepare(
            r"SELECT exercise_id, exercise_tier, passed, duration_secs, error, step_count
              FROM exercise_results WHERE run_id = ?1 ORDER BY id",
        )?;
        let rows = stmt
            .query_map([run_id], |row| {
                Ok(ExerciseResultRow {
                    exercise_id: row.get(0)?,
                    exercise_tier: row.get(1)?,
                    passed: row.get::<_, i64>(2)? != 0,
                    duration_secs: row.get(3)?,
                    error: row.get(4)?,
                    step_count: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn record_fix_session_snapshot(
        &self,
        invocation_id: i64,
        errors: i64,
        warnings: i64,
        fixable: i64,
    ) -> Result<()> {
        self.conn.execute(
            r"UPDATE invocations
              SET pre_fix_errors = ?2, pre_fix_warnings = ?3, pre_fix_fixable = ?4
              WHERE id = ?1",
            rusqlite::params![invocation_id, errors, warnings, fixable],
        )?;
        Ok(())
    }

    /// Get recent fix sessions with their pre-fix diagnostic counts (G3).
    pub fn get_fix_sessions(&self, limit: usize) -> Result<Vec<FixSession>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT
                id,
                started_at,
                duration_secs,
                pre_fix_errors,
                pre_fix_warnings,
                pre_fix_fixable
            FROM invocations
            WHERE command = 'fix'
            ORDER BY started_at DESC
            LIMIT ?1
            ",
        )?;

        let rows = stmt.query_map(rusqlite::params![limit as i64], |row| {
            Ok(FixSession {
                invocation_id: row.get(0)?,
                started_at: row.get(1)?,
                duration_secs: row.get(2)?,
                pre_fix_errors: row.get(3)?,
                pre_fix_warnings: row.get(4)?,
                pre_fix_fixable: row.get(5)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G4: Package Enumeration
    // ──────────────────────────────────────────────────────────────────────
}
