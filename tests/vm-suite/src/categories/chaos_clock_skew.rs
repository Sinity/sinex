//! Chaos test: simulate system clock skew (advance and restore).
//!
//! Advances system clock 1 hour, generates events, restores clock,
//! verifies no catastrophic timestamp corruption and hypertable integrity.

use std::process::Command;
use std::time::Duration;

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::{TestOutcome, TestRunner};

use super::chaos_support::{
    command_status, observed_event_count, report_service_active, report_watched_files_written,
    wait_for_event_count_increase,
};

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Clock Skew tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_monotonic(runner, &pool).await;
    test_event_engine_survives_clock_advance(runner, &pool).await;
    test_events_reach_db_despite_skew(runner, &pool).await;
    test_no_catastrophic_timestamp_corruption(runner, &pool).await;
    test_hypertable_chunk_structure_intact(runner, &pool).await;

    Ok(())
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
    .await
    .ok();

    match violations {
        Some(0) => runner.pass(name),
        Some(v) => runner.fail(
            name,
            &format!("{v} timestamp ordering violations at baseline"),
        ),
        None => runner.fail(name, "ts_coided monotonicity query failed"),
    }
}

async fn test_event_engine_survives_clock_advance(runner: &mut TestRunner, _pool: &PgPool) {
    let name = "chaos-clock-skew: event_engine survives clock advance";

    // Read current epoch
    let epoch_output = Command::new("date").args(["+%s"]).output().ok();

    let current_epoch: i64 = epoch_output
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);

    if current_epoch == 0 {
        runner.fail(name, "could not read current epoch");
        return;
    }

    // Advance clock by 1 hour (3600 seconds)
    let new_epoch = current_epoch + 3600;
    let set_result = command_status("date", &["-s", &format!("@{new_epoch}")]);

    if !set_result {
        runner.record(
            name,
            TestOutcome::Skipped,
            "date -s command failed; VM lacks clock-setting capability for this chaos scenario",
        );
        return;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    if !report_service_active(runner, name, "event_engine crashed after clock advance") {
        // Restore clock before returning
        let _ = command_status("date", &["-s", &format!("@{current_epoch}")]);
    }
}

async fn test_events_reach_db_despite_skew(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-clock-skew: events reach DB despite clock skew";

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

async fn test_no_catastrophic_timestamp_corruption(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-clock-skew: no catastrophic timestamp corruption";

    // Read current epoch and restore clock first
    let epoch_output = Command::new("date").args(["+%s"]).output().ok();

    let current_epoch: i64 = epoch_output
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);

    // Restore to original time (subtract 1 hour)
    let original_epoch = current_epoch - 3600;
    let _ = command_status("date", &["-s", &format!("@{original_epoch}")]);

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
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("event count query failed while waiting after clock restore: {error:#}"),
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
    .await
    .ok();

    match violations {
        Some(v) => {
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
        None => runner.fail(name, "ts_coided violation check failed"),
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
