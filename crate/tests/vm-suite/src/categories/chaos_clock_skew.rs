//! Chaos test: simulate system clock skew (advance and restore).
//!
//! Advances system clock 1 hour, generates events, restores clock,
//! verifies no catastrophic timestamp corruption and hypertable integrity.

use std::process::Command;
use std::time::{Duration, Instant};

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::TestRunner;

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Clock Skew tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_monotonic(runner, &pool).await;
    test_ingestd_survives_clock_advance(runner, &pool).await;
    test_events_reach_db_despite_skew(runner, &pool).await;
    test_no_catastrophic_timestamp_corruption(runner, &pool).await;
    test_hypertable_chunk_structure_intact(runner, &pool).await;

    Ok(())
}

async fn test_baseline_monotonic(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-clock-skew: baseline events captured and ts_coided monotonic";

    let watched = "/var/lib/sinex/watched";
    let _ = std::fs::create_dir_all(watched);

    // Generate 10 pre-skew files
    for i in 0..10_u32 {
        let _ = std::fs::write(
            format!("{watched}/clock-baseline-{i}.txt"),
            format!("baseline {i}"),
        );
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Check ts_coided monotonicity
    let violations: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM (\
           SELECT ts_coided, LAG(ts_coided) OVER (ORDER BY id) AS prev_ts \
           FROM core.events\
         ) t \
         WHERE prev_ts IS NOT NULL AND ts_coided < prev_ts"
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();

    match violations {
        Some(0) => runner.pass(name),
        Some(v) => runner.fail(
            name,
            &format!("{v} timestamp ordering violations at baseline"),
        ),
        None => runner.fail(name, "ts_coided monotonicity query failed"),
    }
}

async fn test_ingestd_survives_clock_advance(runner: &mut TestRunner, _pool: &PgPool) {
    let name = "chaos-clock-skew: ingestd survives clock advance";

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
    let set_result = Command::new("date")
        .args(["-s", &format!("@{new_epoch}")])
        .status()
        .is_ok_and(|s| s.success());

    if !set_result {
        runner.fail(name, "date -s command failed");
        return;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check ingestd still active
    let active = Command::new("systemctl")
        .args(["is-active", "--quiet", "sinex-ingestd"])
        .status()
        .is_ok_and(|s| s.success());

    if active {
        runner.pass(name);
    } else {
        runner.fail(name, "ingestd crashed after clock advance");
        // Restore clock before returning
        let _ = Command::new("date")
            .args(["-s", &format!("@{current_epoch}")])
            .status();
    }
}

async fn test_events_reach_db_despite_skew(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-clock-skew: events reach DB despite clock skew";

    let before = event_count(pool).await;
    let watched = "/var/lib/sinex/watched";

    // Generate events during skew
    for i in 0..20_u32 {
        let _ = std::fs::write(
            format!("{watched}/clock-during-{i}.txt"),
            format!("during {i}"),
        );
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let after = event_count(pool).await;
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
    let _ = Command::new("date")
        .args(["-s", &format!("@{original_epoch}")])
        .status();

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Generate post-restore events
    let before = event_count(pool).await;
    let watched = "/var/lib/sinex/watched";
    for i in 0..10_u32 {
        let _ = std::fs::write(format!("{watched}/clock-post-{i}.txt"), format!("post {i}"));
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after = event_count(pool).await;
        if after > before {
            break;
        }
        if Instant::now() >= deadline {
            runner.fail(name, "post-restore events did not reach DB");
            return;
        }
    }

    // Check ts_coided ordering violations (as proxy for corruption)
    let violations: Option<i64> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM (\
           SELECT ts_coided, LAG(ts_coided) OVER (ORDER BY id) AS prev_ts \
           FROM core.events\
         ) t \
         WHERE prev_ts IS NOT NULL AND ts_coided < prev_ts"
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();

    match violations {
        Some(v) => {
            let final_count = event_count(pool).await;
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

    let result: Result<Option<i64>, _> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'events'"
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(Some(chunk_count)) if chunk_count >= 1 => runner.pass(name),
        Ok(Some(0)) => runner.fail(name, "hypertable has no chunks"),
        Ok(Some(_n)) => runner.pass(name), // Any chunks > 0 is good
        Ok(None) => runner.fail(name, "hypertable chunk count is NULL"),
        Err(e) => runner.fail(name, &format!("chunk query failed: {e}")),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

async fn event_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar!("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
}
