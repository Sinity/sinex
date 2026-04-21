//! Synthetic history seed generator.
//!
//! Writes realistic-looking invocation data to a [`HistoryDb`] so that history
//! commands (`xtask history list`, `xtask history diagnostics`, …) show rich,
//! representative output without requiring a real project run history.
//!
//! # Distributions (hardcoded, realistic)
//!
//! - Commands: 40% check, 30% test, 15% build, 10% fix, 5% other
//! - Success rate: ~85%
//! - Test-specific failure rate: ~5%
//! - Diagnostics: trending downward (improving workspace), 20–30 total across 5 packages
//! - Stage timings: clippy ~18s ±10%, preflight ~0.3s

use color_eyre::eyre::Result;
use rand::{Rng, RngExt};
use rusqlite::params;

use crate::history::HistoryDb;

const PACKAGES: &[&str] = &[
    "sinex-primitives",
    "sinex-db",
    "sinex-gateway",
    "sinex-ingestd",
    "sinex-node-sdk",
];

const COMMANDS: &[(&str, u32)] = &[
    ("check", 40),
    ("test", 30),
    ("build", 15),
    ("fix", 10),
    ("status", 5),
];

/// Options controlling the seed volume.
#[derive(Debug, Clone)]
pub struct SeedOptions {
    /// How many calendar days the synthetic history should span.
    pub days: u32,
    /// Total number of invocations to generate.
    pub invocations: u32,
}

impl Default for SeedOptions {
    fn default() -> Self {
        Self {
            days: 30,
            invocations: 100,
        }
    }
}

/// Write synthetic invocation history into `db`.
///
/// Marks the database as synthetic (see [`HistoryDb::is_synthetic`]).  Any
/// subsequent call to [`HistoryDb::start_invocation`] will clear the marker,
/// transitioning the DB to real usage without any manual intervention.
pub fn seed_history(db: &HistoryDb, options: &SeedOptions) -> Result<()> {
    let mut rng = rand::rng();

    let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
    let window_secs = i64::from(options.days) * 86_400;
    let start_unix = now_unix - window_secs;

    // Pick timestamps spread across the window, roughly increasing.
    let total = i64::from(options.invocations);
    let avg_gap = if total > 0 { window_secs / total } else { 3600 };

    // Diagnostic message count per-package starts high and trends down.
    let mut pkg_diag_count: Vec<u32> = PACKAGES
        .iter()
        .map(|_| rng.random_range(4u32..=7))
        .collect();

    let mut current_ts = start_unix;

    for i in 0..options.invocations {
        // Advance time: avg gap ± 30%
        let gap = avg_gap + rng.random_range(-(avg_gap / 3)..(avg_gap / 3));
        current_ts += gap.max(60);

        let command = weighted_command(&mut rng);
        let success = rng.random_range(0u32..100) < 85;
        let status = if success { "success" } else { "failed" };
        let duration = match command {
            "check" => rng.random_range(3.0f64..25.0),
            "test" => rng.random_range(15.0..90.0),
            "build" => rng.random_range(10.0..60.0),
            "fix" => rng.random_range(2.0..15.0),
            _ => rng.random_range(0.5..3.0),
        };

        let started_at = format_ts(current_ts);
        let finished_at = format_ts(current_ts + duration as i64);
        let host = "devhost";
        let cwd = "/realm/project/sinex";

        db.conn.execute(
            r"
            INSERT INTO invocations (
                command, started_at, finished_at, duration_secs, status, host, cwd, git_dirty
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)
            ",
            params![
                command,
                started_at,
                finished_at,
                duration,
                status,
                host,
                cwd
            ],
        )?;

        let inv_id = db.conn.last_insert_rowid();

        // Add invocation_packages for check / build / test
        if matches!(command, "check" | "build" | "test") {
            // Pick 1-3 packages for this invocation
            let pkg_count = rng.random_range(1usize..=3.min(PACKAGES.len()));
            // Simple deterministic selection: pick sequentially from offset
            let offset = (i as usize) % PACKAGES.len();
            for j in 0..pkg_count {
                let pkg = PACKAGES[(offset + j) % PACKAGES.len()];
                let _ = db.conn.execute(
                    "INSERT OR IGNORE INTO invocation_packages (invocation_id, package) VALUES (?1, ?2)",
                    params![inv_id, pkg],
                );
            }
        }

        // Add build_diagnostics for check (trending downward)
        if command == "check" {
            // Every ~5 check invocations, reduce diagnostic count
            if i % 5 == 0 {
                for count in &mut pkg_diag_count {
                    if *count > 0 && rng.random_range(0u32..3) == 0 {
                        *count -= 1;
                    }
                }
            }
            for (pkg_idx, &count) in pkg_diag_count.iter().enumerate() {
                let pkg = PACKAGES[pkg_idx];
                for d in 0..count {
                    let (level, code, message) = synthetic_diagnostic(&mut rng, d);
                    let file_path = format!("crate/lib/{pkg}/src/lib.rs");
                    let line: i64 = 10 + i64::from(d) * 15 + rng.random_range(0i64..10);
                    let _ = db.conn.execute(
                        r"
                        INSERT OR IGNORE INTO build_diagnostics
                            (invocation_id, level, code, message, file_path, line, package, rendered)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        ",
                        params![
                            inv_id,
                            level,
                            code,
                            message,
                            file_path,
                            line,
                            pkg,
                            format!("{level}: {message}"),
                        ],
                    );
                }
            }
        }

        // Add stage_timings for check / build
        if matches!(command, "check" | "build") {
            let preflight_dur = 0.3 + rng.random_range(-0.05f64..0.1);
            let _ = db.conn.execute(
                r"
                INSERT INTO stage_timings (invocation_id, stage_name, started_at, duration_secs, success)
                VALUES (?1, 'preflight', ?2, ?3, 1)
                ",
                params![inv_id, started_at, preflight_dur],
            );
            if command == "check" {
                let clippy_dur = 18.0 + rng.random_range(-1.8f64..1.8);
                let _ = db.conn.execute(
                    r"
                    INSERT INTO stage_timings (invocation_id, stage_name, started_at, duration_secs, success)
                    VALUES (?1, 'clippy', ?2, ?3, ?4)
                    ",
                    params![inv_id, started_at, clippy_dur, i32::from(success)],
                );
            }
        }

        // Add test_results for test invocations
        if command == "test" {
            let test_count = rng.random_range(20usize..80);
            let fail_budget = if rng.random_range(0u32..100) < 5 {
                rng.random_range(1usize..4)
            } else {
                0
            };

            for t in 0..test_count {
                let failed = t < fail_budget;
                let test_status = if failed { "fail" } else { "pass" };
                let pkg = PACKAGES[t % PACKAGES.len()];
                let test_name = format!("{pkg}::tests::synthetic_test_{t}");
                let dur = rng.random_range(0.01f64..2.0);
                let _ = db.conn.execute(
                    r"
                    INSERT OR IGNORE INTO test_results
                        (invocation_id, test_name, package, status, duration_secs, attempt)
                    VALUES (?1, ?2, ?3, ?4, ?5, 1)
                    ",
                    params![inv_id, test_name, pkg, test_status, dur],
                );
            }
        }
    }

    // Mark database as synthetic
    db.set_synthetic()?;

    Ok(())
}

fn weighted_command(rng: &mut impl Rng) -> &'static str {
    let roll: u32 = rng.random_range(0..100);
    let mut acc = 0;
    for &(cmd, weight) in COMMANDS {
        acc += weight;
        if roll < acc {
            return cmd;
        }
    }
    "check"
}

fn synthetic_diagnostic<'a>(rng: &mut impl Rng, idx: u32) -> (&'a str, &'a str, &'a str) {
    let warnings = [
        ("warning", "dead_code", "unused variable `result`"),
        ("warning", "unused_imports", "unused import: `std::fmt`"),
        (
            "warning",
            "clippy::unwrap_used",
            "used `unwrap()` on a `Result`",
        ),
        (
            "warning",
            "clippy::needless_pass_by_ref_mut",
            "argument `x` is passed by mutable reference",
        ),
        ("error", "E0308", "mismatched types"),
        ("warning", "clippy::redundant_clone", "redundant clone"),
    ];
    let i = (idx as usize + rng.random_range(0usize..3)) % warnings.len();
    warnings[i]
}

fn format_ts(unix: i64) -> String {
    use time::{OffsetDateTime, format_description};
    let dt =
        OffsetDateTime::from_unix_timestamp(unix).unwrap_or_else(|_| OffsetDateTime::now_utc());
    let fmt = format_description::parse("[year]-[month]-[day]T[hour]:[minute]:[second]Z").unwrap();
    dt.format(&fmt).unwrap_or_else(|_| unix.to_string())
}
