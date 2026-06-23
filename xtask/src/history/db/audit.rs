use super::*;

impl HistoryDb {
    /// Check whether this database contains synthetic (seeded) data.
    pub fn check_synthetic(&self) -> Result<bool> {
        let exists = self
            .conn
            .query_row(
                "SELECT 1 FROM metadata WHERE key = 'synthetic' AND value = 'true' LIMIT 1",
                [],
                |_| Ok(true),
            )
            .optional()
            .context("failed to query synthetic history marker")?;
        Ok(exists.unwrap_or(false))
    }

    /// Mark the database as containing synthetic data.
    pub fn set_synthetic(&self) -> Result<()> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES ('synthetic', 'true')",
                [],
            )
            .context("failed to set synthetic marker")?;
        Ok(())
    }

    /// Print a one-time-per-process warning if this database is synthetic.
    ///
    /// Suppressed when `XTASK_SYNTHETIC_HISTORY=allow` is set (exercises use this).
    pub fn warn_if_synthetic(&self, path: &std::path::Path) {
        if !self.is_synthetic {
            return;
        }
        if std::env::var_os("XTASK_SYNTHETIC_HISTORY").as_deref()
            == Some(std::ffi::OsStr::new("allow"))
        {
            return;
        }
        SYNTHETIC_WARNING_EMITTED.get_or_init(|| {
            eprintln!(
                "\nWARNING: History database contains synthetic (seeded) data.\n  \
                Database: {}\n  \
                Seeded by: xtask exercise --seed or xtask history seed\n\n  \
                Results from history commands reflect fabricated data, not real usage.\n  \
                To start fresh: xtask reset --yes --history\n  \
                To suppress:    XTASK_SYNTHETIC_HISTORY=allow\n",
                path.display()
            );
        });
    }

    /// Record a drift guard bypass event (#1565).
    ///
    /// Called by the pre-push hook when `SINEX_SKIP_DRIFT_GUARD=1` is used.
    /// `push_succeeded` is set later (unknown at bypass time), so callers
    /// pass `None` initially and update after the push completes.
    pub fn record_drift_guard_bypass(
        &self,
        git_branch: Option<&str>,
        head_sha: Option<&str>,
        push_succeeded: Option<bool>,
    ) -> Result<i64> {
        let mut stmt = self.conn.prepare(
            "INSERT INTO drift_guard_bypasses (git_branch, head_sha, push_succeeded) VALUES (?1, ?2, ?3)",
        )?;
        let id = stmt.insert(params![git_branch, head_sha, push_succeeded])?;
        Ok(id)
    }

    /// Update an existing bypass row with the push outcome.
    pub fn update_drift_guard_bypass_outcome(&self, id: i64, push_succeeded: bool) -> Result<()> {
        self.conn.execute(
            "UPDATE drift_guard_bypasses SET push_succeeded = ?1 WHERE id = ?2",
            params![push_succeeded, id],
        )?;
        Ok(())
    }

    /// Return the number of drift guard bypasses recorded in the last `days` days.
    pub fn get_drift_guard_bypass_count(&self, days: i32) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM drift_guard_bypasses WHERE recorded_at >= datetime('now', ?1)",
            params![format!("-{days} days")],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Return the most recent drift guard bypass, if any.
    pub fn get_drift_guard_bypass_latest(&self) -> Result<Option<DriftGuardBypass>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, recorded_at, git_branch, head_sha, push_succeeded
             FROM drift_guard_bypasses
             ORDER BY recorded_at DESC LIMIT 1",
        )?;
        let row = stmt
            .query_row([], |row| {
                Ok(DriftGuardBypass {
                    id: row.get(0)?,
                    recorded_at: row.get::<_, String>(1)?,
                    git_branch: row.get(2)?,
                    head_sha: row.get(3)?,
                    push_succeeded: row.get(4)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    /// List the most recent drift-guard bypasses (security/hygiene audit trail).
    ///
    /// Surfaced via `xtask history view drift-guard-bypasses` so this table no
    /// longer requires a raw `sqlite3` query to inspect.
    pub fn get_drift_guard_bypasses(&self, limit: usize) -> Result<Vec<DriftGuardBypass>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, recorded_at, git_branch, head_sha, push_succeeded
             FROM drift_guard_bypasses
             ORDER BY recorded_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(DriftGuardBypass {
                    id: row.get(0)?,
                    recorded_at: row.get::<_, String>(1)?,
                    git_branch: row.get(2)?,
                    head_sha: row.get(3)?,
                    push_succeeded: row.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Recent impact-plan audit runs (skip-accuracy / false-negative evidence).
    pub fn get_impact_audit_runs(&self, limit: usize) -> Result<Vec<ImpactAuditRunRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, invocation_id, sample_size, status, false_negative_count, created_at
             FROM impact_audit_runs ORDER BY created_at DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(ImpactAuditRunRow {
                    id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    sample_size: row.get(2)?,
                    status: row.get(3)?,
                    false_negative_count: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Most recent internal trace events (newest first).
    pub fn get_recent_trace_events(&self, limit: usize) -> Result<Vec<TraceEventRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, invocation_id, ts, level, target, message
             FROM trace_events ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit as i64], |row| {
                Ok(TraceEventRow {
                    id: row.get(0)?,
                    invocation_id: row.get(1)?,
                    ts: row.get(2)?,
                    level: row.get(3)?,
                    target: row.get(4)?,
                    message: row.get(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}
