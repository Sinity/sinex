use color_eyre::eyre::{Context, Result};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use super::{
    HistoryDb, InvocationStatus, LATEST_PER_PACKAGE_CTE_CLOSE, LATEST_PER_PACKAGE_CTE_OPEN,
    parse_stored_invocation_status,
};

impl HistoryDb {
    /// Get diagnostic error/warning counts for a specific invocation (G5 --with-diagnostics).
    pub fn get_diagnostic_counts_for_invocation(
        &self,
        invocation_id: i64,
    ) -> Result<DiagnosticCounts> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT
                SUM(CASE WHEN level = 'error' THEN 1 ELSE 0 END),
                SUM(CASE WHEN level = 'warning' THEN 1 ELSE 0 END),
                SUM(CASE WHEN fix_applicability = 'MachineApplicable' THEN 1 ELSE 0 END)
            FROM build_diagnostics
            WHERE invocation_id = ?1
            ",
        )?;
        let (errors, warnings, fixable) = stmt
            .query_row(params![invocation_id], |row| {
                Ok((
                    row.get::<_, i64>(0).unwrap_or(0),
                    row.get::<_, i64>(1).unwrap_or(0),
                    row.get::<_, i64>(2).unwrap_or(0),
                ))
            })
            .unwrap_or((0, 0, 0));
        Ok(DiagnosticCounts {
            errors: errors as usize,
            warnings: warnings as usize,
            fixable: fixable as usize,
        })
    }

    // ============ Diagnostics Methods ============

    /// Record a build diagnostic (warning/error).
    pub fn record_diagnostic(
        &self,
        invocation_id: i64,
        diag: &crate::cargo_diagnostics::CompilerDiagnostic,
    ) -> Result<()> {
        self.conn.execute(
            r"
            INSERT OR IGNORE INTO build_diagnostics
                (invocation_id, level, code, message, file_path, line, col, rendered,
                 package, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ",
            params![
                invocation_id,
                diag.level,
                diag.code,
                diag.message,
                diag.file_path,
                diag.line,
                diag.column,
                diag.rendered,
                diag.package,
                diag.fix_replacement,
                diag.fix_applicability,
                diag.fix_byte_start,
                diag.fix_byte_end,
            ],
        )?;
        Ok(())
    }

    /// Record multiple diagnostics in a single transaction.
    ///
    /// Much more efficient than calling `record_diagnostic()` in a loop — uses a single
    /// prepared statement and wraps all inserts in one transaction.
    pub fn record_diagnostics_batch(
        &self,
        invocation_id: i64,
        diagnostics: &[crate::cargo_diagnostics::CompilerDiagnostic],
    ) -> Result<()> {
        if diagnostics.is_empty() {
            return Ok(());
        }
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                r"
                INSERT OR IGNORE INTO build_diagnostics
                    (invocation_id, level, code, message, file_path, line, col, rendered,
                     package, fix_replacement, fix_applicability, fix_byte_start, fix_byte_end)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ",
            )?;
            for diag in diagnostics {
                stmt.execute(params![
                    invocation_id,
                    diag.level,
                    diag.code,
                    diag.message,
                    diag.file_path,
                    diag.line,
                    diag.column,
                    diag.rendered,
                    diag.package,
                    diag.fix_replacement,
                    diag.fix_applicability,
                    diag.fix_byte_start,
                    diag.fix_byte_end,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Record which packages were compiled in an invocation (for package-scoped supersession).
    pub fn record_compiled_packages(
        &self,
        invocation_id: i64,
        packages: &std::collections::HashSet<String>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            "INSERT OR IGNORE INTO invocation_packages (invocation_id, package) VALUES (?1, ?2)",
        )?;
        for pkg in packages {
            stmt.execute(params![invocation_id, pkg])?;
        }
        Ok(())
    }

    /// Get packages compiled in a specific invocation (H5 — fresh path scope context).
    pub fn get_compiled_packages_for_invocation(&self, invocation_id: i64) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT package FROM invocation_packages WHERE invocation_id = ?1 ORDER BY package",
        )?;
        let rows = stmt.query_map(params![invocation_id], |row| row.get::<_, String>(0))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get diagnostics for an invocation.
    pub fn get_diagnostics(&self, invocation_id: i64) -> Result<Vec<StoredDiagnostic>> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered,
                   d.package, d.fix_replacement, d.fix_applicability, d.fix_byte_start, d.fix_byte_end,
                   i.command, i.started_at
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            WHERE d.invocation_id = ?1
            ORDER BY d.id
            ",
        )?;

        let rows = stmt.query_map(params![invocation_id], row_to_diagnostic_full)?;

        rows.collect::<Result<Vec<_>, _>>()
            .context("failed to collect diagnostics")
    }

    /// Get diagnostics from a specific invocation (by ID or "latest").
    pub fn get_diagnostics_for_invocation(
        &self,
        invocation: &str,
        command: Option<&str>,
    ) -> Result<Vec<StoredDiagnostic>> {
        let inv_id: Option<i64> = if invocation == "latest" {
            if let Some(cmd) = command {
                self.conn
                    .query_row(
                        r"
                        SELECT id FROM invocations
                        WHERE command = ?1 AND status IN ('success', 'failed')
                        ORDER BY started_at DESC LIMIT 1
                        ",
                        params![cmd],
                        |row| row.get(0),
                    )
                    .optional()?
            } else {
                self.conn
                    .query_row(
                        r"
                        SELECT id FROM invocations
                        WHERE status IN ('success', 'failed')
                        ORDER BY started_at DESC LIMIT 1
                        ",
                        [],
                        |row| row.get(0),
                    )
                    .optional()?
            }
        } else {
            // Parse as invocation ID
            invocation.parse::<i64>().ok()
        };

        match inv_id {
            Some(id) => self.get_diagnostics(id),
            None => Ok(vec![]),
        }
    }

    /// Get current diagnostics using package-scoped supersession.
    ///
    /// For each package, finds the most recent invocation that compiled it,
    /// and returns diagnostics from that invocation for that package only.
    /// This gives a "current state of the world" view — partial builds update
    /// only the packages they touched, preserving diagnostics from earlier runs
    /// for untouched packages.
    pub fn get_current_diagnostics(
        &self,
        level_filter: Option<&str>,
        file_pattern: Option<&str>,
        package_filter: Option<&str>,
        command_filter: Option<&str>,
        fixable_only: bool,
    ) -> Result<Vec<StoredDiagnostic>> {
        // Build the CTE query dynamically based on filters.
        // The CTE prefix is extracted as a const (shared with get_current_diagnostic_counts).
        let mut query = String::from(LATEST_PER_PACKAGE_CTE_OPEN);

        // Command filter in CTE
        let mut param_idx = 1;
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }

        query.push_str(LATEST_PER_PACKAGE_CTE_CLOSE);
        query.push_str(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered,
                   d.package, d.fix_replacement, d.fix_applicability, d.fix_byte_start, d.fix_byte_end,
                   i.command, i.started_at
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            JOIN latest_per_package lpp ON d.package = lpp.package
                                       AND d.invocation_id = lpp.latest_inv_id
            WHERE 1=1
            ",
        );

        if let Some(level) = level_filter {
            query.push_str(&format!(" AND d.level = ?{param_idx}"));
            params_vec.push(Box::new(level.to_string()));
            param_idx += 1;
        }

        if let Some(pattern) = file_pattern {
            query.push_str(&format!(" AND d.file_path LIKE ?{param_idx}"));
            params_vec.push(Box::new(format!("%{pattern}%")));
            param_idx += 1;
        }

        if let Some(pkg) = package_filter {
            query.push_str(&format!(" AND d.package = ?{param_idx}"));
            params_vec.push(Box::new(pkg.to_string()));
            param_idx += 1;
        }

        if fixable_only {
            query.push_str(&format!(" AND d.fix_applicability = ?{param_idx}"));
            params_vec.push(Box::new("MachineApplicable".to_string()));
            let _ = param_idx; // suppress unused warning
        }

        query.push_str(" ORDER BY d.level ASC, d.package ASC, d.file_path ASC, d.line ASC");

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_refs),
            row_to_diagnostic_full,
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get current diagnostic counts by level (package-scoped supersession).
    ///
    /// Returns a map of level → count using the same CTE as `get_current_diagnostics`
    /// but only fetching aggregate counts. Lightweight enough for the status summary.
    pub fn get_current_diagnostic_counts(&self) -> Result<DiagnosticCounts> {
        let query = format!(
            "{LATEST_PER_PACKAGE_CTE_OPEN}
            {LATEST_PER_PACKAGE_CTE_CLOSE}
            SELECT
                COALESCE(SUM(CASE WHEN d.level = 'error' THEN 1 ELSE 0 END), 0) as errors,
                COALESCE(SUM(CASE WHEN d.level = 'warning' THEN 1 ELSE 0 END), 0) as warnings,
                COALESCE(SUM(CASE WHEN d.fix_applicability = 'MachineApplicable' THEN 1 ELSE 0 END), 0) as fixable
            FROM build_diagnostics d
            JOIN latest_per_package lpp ON d.package = lpp.package
                                       AND d.invocation_id = lpp.latest_inv_id"
        );

        let mut stmt = self.conn.prepare(&query)?;
        let counts = stmt.query_row([], |row| {
            Ok(DiagnosticCounts {
                errors: row.get::<_, i64>(0)? as usize,
                warnings: row.get::<_, i64>(1)? as usize,
                fixable: row.get::<_, i64>(2)? as usize,
            })
        })?;

        Ok(counts)
    }

    /// Get the count of auto-fixable diagnostics in the current package-scoped view (G3).
    pub fn get_fixable_diagnostic_count(&self) -> Result<usize> {
        Ok(self.get_current_diagnostic_counts()?.fixable)
    }

    /// Get diagnostic counts per invocation for trend analysis.
    ///
    /// Returns the most recent `limit` check/build invocations with their
    /// error and warning counts. Used by `--trend`.
    pub fn get_diagnostic_trend(&self, limit: usize) -> Result<Vec<DiagnosticTrendPoint>> {
        let query = r"
            SELECT
                i.id,
                i.command,
                i.started_at,
                i.status,
                COALESCE(SUM(CASE WHEN d.level = 'error' THEN 1 ELSE 0 END), 0) as errors,
                COALESCE(SUM(CASE WHEN d.level = 'warning' THEN 1 ELSE 0 END), 0) as warnings,
                COUNT(d.id) as total
            FROM invocations i
            LEFT JOIN build_diagnostics d ON d.invocation_id = i.id
            WHERE i.command IN ('check', 'build')
              AND i.status IN ('success', 'failed')
            GROUP BY i.id
            ORDER BY i.started_at DESC
            LIMIT ?1
        ";

        let mut stmt = self.conn.prepare(query)?;
        let rows = stmt.query_map(rusqlite::params![limit], |row| {
            let status_str: String = row.get(3)?;
            Ok(DiagnosticTrendPoint {
                invocation_id: row.get(0)?,
                command: row.get(1)?,
                started_at: row.get(2)?,
                status: parse_stored_invocation_status(status_str)?,
                errors: row.get::<_, i64>(4)? as usize,
                warnings: row.get::<_, i64>(5)? as usize,
                total: row.get::<_, i64>(6)? as usize,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        // Return in chronological order (oldest first)
        results.reverse();
        Ok(results)
    }

    /// Get recent diagnostics across all invocations (raw accumulated, used by `--all`).
    pub fn get_recent_diagnostics_all(
        &self,
        limit: usize,
        level_filter: Option<&str>,
        file_pattern: Option<&str>,
        command_filter: Option<&str>,
        package_filter: Option<&str>,
    ) -> Result<Vec<StoredDiagnostic>> {
        let mut query = String::from(
            r"
            SELECT d.id, d.level, d.code, d.message, d.file_path, d.line, d.col, d.rendered,
                   d.package, d.fix_replacement, d.fix_applicability, d.fix_byte_start, d.fix_byte_end,
                   i.command, i.started_at
            FROM build_diagnostics d
            JOIN invocations i ON d.invocation_id = i.id
            WHERE 1=1
            ",
        );

        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut param_idx = 1;

        if let Some(level) = level_filter {
            query.push_str(&format!(" AND d.level = ?{param_idx}"));
            params_vec.push(Box::new(level.to_string()));
            param_idx += 1;
        }
        if let Some(pattern) = file_pattern {
            query.push_str(&format!(" AND d.file_path LIKE ?{param_idx}"));
            params_vec.push(Box::new(format!("%{pattern}%")));
            param_idx += 1;
        }
        if let Some(cmd) = command_filter {
            query.push_str(&format!(" AND i.command = ?{param_idx}"));
            params_vec.push(Box::new(cmd.to_string()));
            param_idx += 1;
        }
        if let Some(pkg) = package_filter {
            query.push_str(&format!(" AND d.package = ?{param_idx}"));
            params_vec.push(Box::new(pkg.to_string()));
            param_idx += 1;
        }

        query.push_str(&format!(
            " ORDER BY i.started_at DESC, d.id DESC LIMIT ?{param_idx}"
        ));
        params_vec.push(Box::new(limit as i64));

        let mut stmt = self.conn.prepare(&query)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params_vec.iter().map(std::convert::AsRef::as_ref).collect();

        let rows = stmt.query_map(
            rusqlite::params_from_iter(params_refs),
            row_to_diagnostic_full,
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    // ──────────────────────────────────────────────────────────────────────
    // G1: Diagnostic Delta — compare two invocations' diagnostic sets
    // ──────────────────────────────────────────────────────────────────────

    /// Compute the diagnostic delta between two invocations.
    ///
    /// Matches diagnostics by their identity key: `(level, code, message, file_path, line, col, package)`.
    /// - `new`: diagnostics in `to_id` but not `from_id` (newly appeared)
    /// - `resolved`: diagnostics in `from_id` but not `to_id` (fixed)
    /// - `persistent`: diagnostics in both invocations
    pub fn get_diagnostic_delta(&self, from_id: i64, to_id: i64) -> Result<DiagnosticDelta> {
        fn identity_key(d: &StoredDiagnostic) -> String {
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                d.level,
                d.code.as_deref().unwrap_or(""),
                d.message,
                d.file_path.as_deref().unwrap_or(""),
                d.line.map(|v| v.to_string()).unwrap_or_default(),
                d.col.map(|v| v.to_string()).unwrap_or_default(),
                d.package.as_deref().unwrap_or(""),
            )
        }

        let from_diags = self.get_diagnostics(from_id)?;
        let to_diags = self.get_diagnostics(to_id)?;

        let from_keys: std::collections::HashSet<String> =
            from_diags.iter().map(identity_key).collect();
        let to_keys: std::collections::HashSet<String> =
            to_diags.iter().map(identity_key).collect();

        let new: Vec<StoredDiagnostic> = to_diags
            .iter()
            .filter(|d| !from_keys.contains(&identity_key(d)))
            .cloned()
            .collect();
        let resolved: Vec<StoredDiagnostic> = from_diags
            .iter()
            .filter(|d| !to_keys.contains(&identity_key(d)))
            .cloned()
            .collect();
        let persistent: Vec<StoredDiagnostic> = to_diags
            .iter()
            .filter(|d| from_keys.contains(&identity_key(d)))
            .cloned()
            .collect();

        Ok(DiagnosticDelta {
            new,
            resolved,
            persistent,
        })
    }

    /// I3: Get diagnostic lifecycle status across invocations.
    ///
    /// Classifies each unique (package, level, code, message) tuple as:
    /// - `new`: only appeared in the latest invocation for its package
    /// - `chronic`: present in 3+ invocations and still in the latest
    /// - `recurring`: appeared more than once but not chronic, still in latest
    /// - `resolved`: was present before but NOT in the latest invocation
    pub fn get_diagnostic_lifecycle(
        &self,
        package: Option<&str>,
        code: Option<&str>,
        level: Option<&str>,
        lifecycle_status: Option<&str>,
        limit: usize,
    ) -> Result<Vec<DiagnosticLifecycle>> {
        let mut sql = String::from(
            r"
            WITH latest_per_package AS (
                SELECT ip.package, MAX(i.id) as latest_inv_id
                FROM invocation_packages ip
                JOIN invocations i ON ip.invocation_id = i.id
                WHERE i.status IN ('success', 'failed')
                GROUP BY ip.package
            ),
            diag_occurrences AS (
                SELECT
                    bd.package,
                    bd.level,
                    bd.code,
                    bd.message,
                    COUNT(DISTINCT bd.invocation_id) as occurrence_count,
                    MIN(bd.invocation_id) as first_seen,
                    MAX(bd.invocation_id) as last_seen
                FROM build_diagnostics bd
                WHERE bd.package IS NOT NULL
                GROUP BY bd.package, bd.level, COALESCE(bd.code, ''), bd.message
            ),
            lifecycle AS (
                SELECT
                    d.package, d.level, d.code, d.message,
                    d.occurrence_count, d.first_seen, d.last_seen,
                    CASE
                        WHEN lpp.latest_inv_id IS NULL THEN 'resolved'
                        WHEN d.last_seen < lpp.latest_inv_id THEN 'resolved'
                        WHEN d.first_seen = d.last_seen THEN 'new'
                        WHEN d.occurrence_count >= 3 THEN 'chronic'
                        ELSE 'recurring'
                    END as status
                FROM diag_occurrences d
                LEFT JOIN latest_per_package lpp ON d.package = lpp.package
            )
            SELECT package, level, code, message, occurrence_count, first_seen, last_seen, status
            FROM lifecycle
            WHERE 1=1
            ",
        );

        let mut params: Vec<String> = Vec::new();
        let mut idx = 1usize;

        if let Some(pkg) = package {
            sql.push_str(&format!(" AND package = ?{idx}"));
            params.push(pkg.to_string());
            idx += 1;
        }
        if let Some(c) = code {
            sql.push_str(&format!(" AND COALESCE(code, '') = ?{idx}"));
            params.push(c.to_string());
            idx += 1;
        }
        if let Some(l) = level {
            sql.push_str(&format!(" AND level = ?{idx}"));
            params.push(l.to_string());
            idx += 1;
        }
        if let Some(s) = lifecycle_status {
            sql.push_str(&format!(" AND status = ?{idx}"));
            params.push(s.to_string());
            idx += 1;
        }
        let _ = idx;
        sql.push_str(" ORDER BY status, occurrence_count DESC, package");
        sql.push_str(&format!(" LIMIT {limit}"));

        let mut stmt = self
            .conn
            .prepare(&sql)
            .context("failed to prepare lifecycle query")?;
        let refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let rows = stmt
            .query_map(refs.as_slice(), |row| {
                let status_str: String = row.get(7)?;
                let status = match status_str.as_str() {
                    "new" => LifecycleStatus::New,
                    "chronic" => LifecycleStatus::Chronic,
                    "recurring" => LifecycleStatus::Recurring,
                    _ => LifecycleStatus::Resolved,
                };
                Ok(DiagnosticLifecycle {
                    package: row.get(0)?,
                    level: row.get(1)?,
                    code: row.get(2)?,
                    message: row.get(3)?,
                    occurrence_count: row.get::<_, i64>(4)? as usize,
                    first_seen: row.get(5)?,
                    last_seen: row.get(6)?,
                    status,
                })
            })
            .context("failed to execute lifecycle query")?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to collect lifecycle results")?;
        Ok(rows)
    }
}

/// Lifecycle status of a diagnostic across invocations (I3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LifecycleStatus {
    /// Only appeared in the latest invocation for this package.
    New,
    /// Present in 3+ invocations and still in the latest.
    Chronic,
    /// Appeared more than once but not chronic; still in the latest.
    Recurring,
    /// Was present before but NOT in the latest invocation.
    Resolved,
}

/// A diagnostic with its lifecycle status across invocations (I3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticLifecycle {
    pub package: Option<String>,
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub status: LifecycleStatus,
    pub first_seen: i64,
    pub last_seen: i64,
    pub occurrence_count: usize,
}

/// Map a full diagnostic row (15 columns) to `StoredDiagnostic`.
pub(super) fn row_to_diagnostic_full(row: &rusqlite::Row) -> rusqlite::Result<StoredDiagnostic> {
    Ok(StoredDiagnostic {
        id: row.get(0)?,
        level: row.get(1)?,
        code: row.get(2)?,
        message: row.get(3)?,
        file_path: row.get(4)?,
        line: row.get(5)?,
        col: row.get(6)?,
        rendered: row.get(7)?,
        package: row.get(8)?,
        fix_replacement: row.get(9)?,
        fix_applicability: row.get(10)?,
        fix_byte_start: row.get(11)?,
        fix_byte_end: row.get(12)?,
        source_command: row.get(13)?,
        source_time: row.get(14)?,
    })
}

/// A stored build diagnostic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDiagnostic {
    pub id: i64,
    pub level: String,
    pub code: Option<String>,
    pub message: String,
    pub file_path: Option<String>,
    pub line: Option<u32>,
    pub col: Option<u32>,
    pub rendered: Option<String>,
    pub package: Option<String>,
    pub fix_replacement: Option<String>,
    pub fix_applicability: Option<String>,
    pub fix_byte_start: Option<u32>,
    pub fix_byte_end: Option<u32>,
    /// Source command that produced this diagnostic (e.g. "check")
    pub source_command: Option<String>,
    /// When the source invocation ran
    pub source_time: Option<String>,
}

/// Aggregate diagnostic counts by level (used by `status --summary`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticCounts {
    pub errors: usize,
    pub warnings: usize,
    /// Count of auto-fixable diagnostics (MachineApplicable applicability).
    pub fixable: usize,
}

impl DiagnosticCounts {
    #[must_use]
    pub fn total(&self) -> usize {
        self.errors + self.warnings
    }
}

/// Delta between two invocations' diagnostic sets (G1).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticDelta {
    /// Diagnostics present in `to` but not in `from` (newly appeared).
    pub new: Vec<StoredDiagnostic>,
    /// Diagnostics present in `from` but not in `to` (resolved/fixed).
    pub resolved: Vec<StoredDiagnostic>,
    /// Diagnostics present in both (persistent).
    pub persistent: Vec<StoredDiagnostic>,
}

/// A single point in the diagnostic trend timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticTrendPoint {
    pub invocation_id: i64,
    pub command: String,
    pub started_at: String,
    pub status: InvocationStatus,
    pub errors: usize,
    pub warnings: usize,
    pub total: usize,
}
