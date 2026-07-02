//! Chaos test: simulate system clock skew (advance and restore).
//!
//! Advances system clock 1 hour, generates events, restores clock,
//! verifies no catastrophic timestamp corruption and hypertable integrity.

use std::process::Command;
use std::time::Duration;

use color_eyre::eyre::{Result, eyre};
use sqlx::PgPool;

use crate::runner::{EvidenceKind, MissingEvidencePolicy, TestRunner};

use super::chaos_support::{
    command_status, observed_event_count, report_service_active, report_watched_files_written,
    wait_for_event_count_increase,
};

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Clock Skew tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;
    let mut skew = ClockSkewState::default();

    test_baseline_monotonic(runner, &pool).await;
    test_event_engine_survives_clock_advance(runner, &pool, &mut skew).await;
    test_events_reach_db_despite_skew(runner, &pool, &skew).await;
    test_no_catastrophic_timestamp_corruption(runner, &pool, &mut skew).await;
    test_hypertable_chunk_structure_intact(runner, &pool).await;

    Ok(())
}

#[derive(Debug, Default)]
struct ClockSkewState {
    original_epoch: Option<i64>,
    advanced_epoch: Option<i64>,
}

impl ClockSkewState {
    fn skew_was_injected(&self) -> bool {
        self.original_epoch.is_some() && self.advanced_epoch.is_some()
    }
}

fn read_epoch() -> Result<i64> {
    let output = Command::new("date").args(["+%s"]).output()?;
    if !output.status.success() {
        return Err(eyre!(
            "date +%s failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let stdout = String::from_utf8(output.stdout)?;
    let epoch = stdout
        .trim()
        .parse::<i64>()
        .map_err(|e| eyre!("date +%s returned non-integer epoch {stdout:?}: {e}"))?;
    if epoch <= 0 {
        return Err(eyre!("date +%s returned non-positive epoch {epoch}"));
    }
    Ok(epoch)
}

fn skip_without_injected_skew(runner: &mut TestRunner, name: &str, skew: &ClockSkewState) -> bool {
    !runner.require_evidence(
        name,
        EvidenceKind::FaultInjection,
        skew.skew_was_injected(),
        "clock skew was not injected; VM lacks clock-setting capability or epoch capture failed",
        MissingEvidencePolicy::Skip,
    )
}

fn restore_original_clock(runner: &mut TestRunner, name: &str, skew: &mut ClockSkewState) -> bool {
    let Some(original_epoch) = skew.original_epoch else {
        runner.require_evidence(
            name,
            EvidenceKind::FaultInjection,
            false,
            "original epoch was not captured before clock skew; cannot prove restore",
            MissingEvidencePolicy::Block,
        );
        return false;
    };

    if command_status("date", &["-s", &format!("@{original_epoch}")]) {
        true
    } else {
        runner.require_evidence(
            name,
            EvidenceKind::FaultInjection,
            false,
            "date -s restore failed after successful clock skew injection",
            MissingEvidencePolicy::Block,
        );
        false
    }
}

async fn test_baseline_monotonic(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-clock-skew: baseline events captured and ts_coided monotonic";

    if !report_watched_files_written(runner, name, "clock-baseline", 10, "baseline") {
        return;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check ts_coided monotonicity
    let violations = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM (\
           SELECT ts_coided, LAG(ts_coided) OVER (ORDER BY id) AS prev_ts \
           FROM core.events\
         ) t \
         WHERE prev_ts IS NOT NULL AND ts_coided < prev_ts",
    )
    .fetch_one(pool)
    .await;

    match violations {
        Ok(0) => runner.pass(name),
        Ok(v) => runner.fail(
            name,
            &format!("{v} timestamp ordering violations at baseline"),
        ),
        Err(error) => runner.fail(
            name,
            &format!("ts_coided monotonicity query failed: {error:#}"),
        ),
    }
}

async fn test_event_engine_survives_clock_advance(
    runner: &mut TestRunner,
    _pool: &PgPool,
    skew: &mut ClockSkewState,
) {
    let name = "chaos-clock-skew: event_engine survives clock advance";

    let current_epoch = match read_epoch() {
        Ok(epoch) => epoch,
        Err(error) => {
            runner.fail(name, &format!("could not read current epoch: {error}"));
            return;
        }
    };

    // Advance clock by 1 hour (3600 seconds)
    let new_epoch = current_epoch + 3600;
    let set_result = command_status("date", &["-s", &format!("@{new_epoch}")]);

    if !set_result {
        runner.require_evidence(
            name,
            EvidenceKind::FaultInjection,
            false,
            "date -s command failed; VM lacks clock-setting capability for this chaos scenario",
            MissingEvidencePolicy::Skip,
        );
        return;
    }

    skew.original_epoch = Some(current_epoch);
    skew.advanced_epoch = Some(new_epoch);

    tokio::time::sleep(Duration::from_secs(3)).await;

    if !report_service_active(runner, name, "event_engine crashed after clock advance") {
        restore_original_clock(runner, name, skew);
    }
}

async fn test_events_reach_db_despite_skew(
    runner: &mut TestRunner,
    pool: &PgPool,
    skew: &ClockSkewState,
) {
    let name = "chaos-clock-skew: events reach DB despite clock skew";

    if skip_without_injected_skew(runner, name, skew) {
        return;
    }

    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if !report_watched_files_written(runner, name, "clock-during", 20, "during") {
        return;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let Some(after) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if after > before {
        runner.pass(name);
    } else {
        runner.fail(
            name,
            &format!("no events during skew (before={before}, after={after})"),
        );
    }
}

async fn test_no_catastrophic_timestamp_corruption(
    runner: &mut TestRunner,
    pool: &PgPool,
    skew: &mut ClockSkewState,
) {
    let name = "chaos-clock-skew: no catastrophic timestamp corruption";

    if skip_without_injected_skew(runner, name, skew) {
        return;
    }

    if !restore_original_clock(runner, name, skew) {
        return;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Generate post-restore events
    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if !report_watched_files_written(runner, name, "clock-post", 10, "post") {
        return;
    }

    let reached_db = wait_for_event_count_increase(
        pool,
        before,
        Duration::from_secs(30),
        Duration::from_secs(2),
    )
    .await;
    match reached_db {
        Ok(Some(_)) => {}
        Ok(None) => {
            runner.fail(name, "post-restore events did not reach DB");
            return;
        }
        Err(error) => {
            runner.require_evidence(
                name,
                EvidenceKind::Database,
                false,
                &format!("event count query failed while waiting after clock restore: {error:#}"),
                MissingEvidencePolicy::Block,
            );
            return;
        }
    }

    // Check ts_coided ordering violations (as proxy for corruption)
    let violations = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM (\
           SELECT ts_coided, LAG(ts_coided) OVER (ORDER BY id) AS prev_ts \
           FROM core.events\
         ) t \
         WHERE prev_ts IS NOT NULL AND ts_coided < prev_ts",
    )
    .fetch_one(pool)
    .await;

    match violations {
        Ok(v) => {
            let Some(final_count) = observed_event_count(runner, name, pool).await else {
                return;
            };
            // Allow up to 50% corruption as "catastrophic" threshold
            if v as f64 > (final_count as f64 * 0.5) {
                runner.fail(
                    name,
                    &format!(
                        "{v} timestamp violations out of {final_count} events (>50%, catastrophic)"
                    ),
                );
            } else {
                runner.pass(name);
            }
        }
        Err(error) => runner.fail(
            name,
            &format!("ts_coided violation check failed: {error:#}"),
        ),
    }
}

async fn test_hypertable_chunk_structure_intact(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-clock-skew: hypertable chunk structure intact";

    let result = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'events'",
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(chunk_count) if chunk_count >= 1 => runner.pass(name),
        Ok(0) => runner.fail(name, "hypertable has no chunks"),
        Ok(_n) => runner.pass(name), // Any chunks > 0 is good
        Err(e) => runner.fail(name, &format!("chunk query failed: {e}")),
    }
}

#[cfg(test)]
#[path = "chaos_clock_skew_test.rs"]
mod tests;
