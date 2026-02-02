use super::{
    runner::{Scenario, ScenarioResult},
    stats::{compare_with_baseline, Regression, RunStats},
};
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;

pub(super) struct HistoryDb {
    conn: Connection,
}

impl HistoryDb {
    pub(super) fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open history database at {}", path.display()))?;

        Self::init_schema(&conn)?;

        Ok(Self { conn })
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        // Legacy schema migration: drop obsolete clean-after-use column.
        if table_exists(conn, "results")? && table_has_column(conn, "results", "clean_after_use")? {
            migrate_results_drop_clean_after_use(conn)?;
        }

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                git_sha TEXT,
                git_branch TEXT,
                git_dirty INTEGER,
                mode TEXT NOT NULL,
                profile TEXT NOT NULL,
                rustc_version TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                threads INTEGER NOT NULL,
                median_ms REAL NOT NULL,
                mean_ms REAL NOT NULL,
                stddev_ms REAL NOT NULL,
                min_ms REAL NOT NULL,
                max_ms REAL NOT NULL,
                sample_count INTEGER NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_results_run_id ON results(run_id);
            CREATE INDEX IF NOT EXISTS idx_results_scenario ON results(threads);
            "#,
        )
        .context("Failed to initialize history database schema")?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn save_run(
        &self,
        mode: &str,
        profile: &str,
        git_sha: &str,
        git_branch: &str,
        git_dirty: bool,
        rustc_version: &str,
        results: &[ScenarioResult],
    ) -> Result<i64> {
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "unknown".to_string());

        let run_id = self.conn.query_row(
            "INSERT INTO runs (timestamp, git_sha, git_branch, git_dirty, mode, profile, rustc_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             RETURNING id",
            params![
                timestamp,
                git_sha,
                git_branch,
                i32::from(git_dirty),
                mode,
                profile,
                rustc_version,
            ],
            |row| row.get(0),
        )?;

        for result in results {
            self.conn.execute(
                "INSERT INTO results (run_id, threads, median_ms, mean_ms, stddev_ms, min_ms, max_ms, sample_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    run_id,
                    result.scenario.threads,
                    result.stats.median_ms,
                    result.stats.mean_ms,
                    result.stats.stddev_ms,
                    result.stats.min_ms,
                    result.stats.max_ms,
                    result.stats.sample_count,
                ],
            )?;
        }

        Ok(run_id)
    }

    pub(super) fn get_trend(&self, scenario: &Scenario, limit: usize) -> Result<Vec<HistoryPoint>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.median_ms, r.mean_ms, runs.timestamp, runs.git_sha
             FROM results r
             JOIN runs ON r.run_id = runs.id
             WHERE r.threads = ?1
             ORDER BY runs.created_at DESC
             LIMIT ?2",
        )?;

        let points = stmt
            .query_map(params![scenario.threads, limit], |row| {
                Ok(HistoryPoint {
                    median_ms: row.get(0)?,
                    mean_ms: row.get(1)?,
                    timestamp: row.get(2)?,
                    git_sha: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(points)
    }

    pub(super) fn get_baseline(
        &self,
        scenario: &Scenario,
        exclude_run_id: Option<i64>,
    ) -> Result<Option<RunStats>> {
        let result = if let Some(run_id) = exclude_run_id {
            self.conn.query_row(
                "SELECT median_ms, mean_ms, stddev_ms, min_ms, max_ms, sample_count
                 FROM results
                 WHERE threads = ?1
                   AND run_id != ?2
                 ORDER BY id DESC
                 LIMIT 1",
                params![scenario.threads, run_id],
                row_to_run_stats,
            )
        } else {
            self.conn.query_row(
                "SELECT median_ms, mean_ms, stddev_ms, min_ms, max_ms, sample_count
                 FROM results
                 WHERE threads = ?1
                 ORDER BY id DESC
                 LIMIT 1",
                params![scenario.threads],
                row_to_run_stats,
            )
        };

        match result {
            Ok(stats) => Ok(Some(stats)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) fn summarize_scenarios(
        &self,
        results: &[ScenarioResult],
        exclude_run_id: Option<i64>,
        regression_threshold_pct: f64,
        trend_limit: usize,
    ) -> Result<Vec<ScenarioHistorySummary>> {
        let mut summaries = Vec::with_capacity(results.len());

        for result in results {
            let trend = self.get_trend(&result.scenario, trend_limit)?;
            let baseline = self.get_baseline(&result.scenario, exclude_run_id)?;
            let regression = if let Some(baseline) = baseline.as_ref() {
                compare_with_baseline(&result.stats, baseline, regression_threshold_pct)
            } else {
                Regression::None
            };

            summaries.push(ScenarioHistorySummary {
                scenario_key: result.scenario.key(),
                trend,
                baseline,
                regression,
            });
        }

        Ok(summaries)
    }
}

fn row_to_run_stats(row: &rusqlite::Row<'_>) -> Result<RunStats, rusqlite::Error> {
    Ok(RunStats {
        median_ms: row.get(0)?,
        mean_ms: row.get(1)?,
        stddev_ms: row.get(2)?,
        ci95_lower: 0.0,
        ci95_upper: 0.0,
        min_ms: row.get(3)?,
        max_ms: row.get(4)?,
        outliers: vec![],
        sample_count: row.get(5)?,
    })
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")?;
    Ok(stmt.exists(params![table])?)
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name.eq_ignore_ascii_case(column) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn migrate_results_drop_clean_after_use(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        BEGIN;
        ALTER TABLE results RENAME TO results_old;

        CREATE TABLE results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            threads INTEGER NOT NULL,
            median_ms REAL NOT NULL,
            mean_ms REAL NOT NULL,
            stddev_ms REAL NOT NULL,
            min_ms REAL NOT NULL,
            max_ms REAL NOT NULL,
            sample_count INTEGER NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );

        INSERT INTO results (
            run_id,
            threads,
            median_ms,
            mean_ms,
            stddev_ms,
            min_ms,
            max_ms,
            sample_count
        )
        SELECT
            run_id,
            threads,
            median_ms,
            mean_ms,
            stddev_ms,
            min_ms,
            max_ms,
            sample_count
        FROM results_old;

        DROP TABLE results_old;

        CREATE INDEX IF NOT EXISTS idx_results_run_id ON results(run_id);
        CREATE INDEX IF NOT EXISTS idx_results_scenario ON results(threads);
        COMMIT;
        "#,
    )
    .context("Failed to migrate bench history schema")?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(super) struct HistoryPoint {
    pub median_ms: f64,
    pub mean_ms: f64,
    pub timestamp: String,
    pub git_sha: String,
}

#[derive(Debug, Clone)]
pub(super) struct ScenarioHistorySummary {
    pub scenario_key: String,
    pub trend: Vec<HistoryPoint>,
    pub baseline: Option<RunStats>,
    pub regression: Regression,
}

impl ScenarioHistorySummary {
    pub(super) fn regression_description(&self) -> String {
        match &self.regression {
            Regression::None => "No regression detected".to_string(),
            Regression::Detected {
                current_ms,
                baseline_ms,
                pct_change,
                threshold_pct,
            } => format!(
                "Regression detected: median {:.1}ms vs {:.1}ms (change {:.1}% > {:.1}% threshold)",
                current_ms, baseline_ms, pct_change, threshold_pct
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct HistoryReport {
    pub run_id: i64,
    pub scenarios: Vec<ScenarioHistorySummary>,
}
