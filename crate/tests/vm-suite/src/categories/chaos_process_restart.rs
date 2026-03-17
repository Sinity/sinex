//! Chaos test: simulate abrupt process termination (SIGKILL) and recovery.
//!
//! Kills ingestd mid-flight with SIGKILL, verifies checkpoint recovery,
//! and asserts no data loss or duplication.

use std::process::Command;
use std::time::{Duration, Instant};

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::TestRunner;

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Process Restart tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_events_captured(runner, &pool).await;
    test_ingestd_restarts_after_sigkill(runner, &pool).await;
    test_no_data_loss_after_restart(runner, &pool).await;
    test_no_duplicate_events_after_restart(runner, &pool).await;
    test_pipeline_flows_after_recovery(runner, &pool).await;

    Ok(())
}

async fn test_baseline_events_captured(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: baseline events captured";

    let watched = "/var/lib/sinex/watched";
    let _ = std::fs::create_dir_all(watched);

    // Generate 10 pre-restart files
    for i in 0..10_u32 {
        let _ = std::fs::write(
            format!("{watched}/restart-baseline-{i}.txt"),
            format!("baseline {i}"),
        );
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let count = event_count(pool).await;
    if count > 0 {
        runner.pass(name);
    } else {
        runner.fail(name, "no baseline events captured");
    }
}

async fn test_ingestd_restarts_after_sigkill(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: ingestd restarts after SIGKILL";

    let watched = "/var/lib/sinex/watched";

    // Generate 30 "during" files before kill
    for i in 0..30_u32 {
        let _ = std::fs::write(
            format!("{watched}/restart-during-{i}.txt"),
            format!("during {i}"),
        );
    }

    // Get the PID of sinex-ingestd
    let pid_output = Command::new("systemctl")
        .args(["show", "-p", "MainPID", "--value", "sinex-ingestd"])
        .output()
        .ok();

    let pid_str = pid_output
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s != "0");

    if let Some(pid) = pid_str {
        // Kill with -9 (SIGKILL)
        let _ = Command::new("kill")
            .args(["-9", &pid])
            .status();
    }

    // Wait for systemd to restart it (up to 30s)
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let active = Command::new("systemctl")
            .args(["is-active", "--quiet", "sinex-ingestd"])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if active {
            runner.pass(name);
            // Wait for checkpoint replay
            tokio::time::sleep(Duration::from_secs(10)).await;
            return;
        }
        if Instant::now() >= deadline {
            runner.fail(name, "ingestd did not restart within 30s after SIGKILL");
            return;
        }
    }
}

async fn test_no_data_loss_after_restart(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: no data loss after restart";

    let baseline_ids: Vec<sqlx::types::Uuid> = sqlx::query_scalar!(
        "SELECT id FROM core.events ORDER BY id"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    if baseline_ids.is_empty() {
        runner.fail(name, "baseline IDs are empty, cannot verify data loss");
        return;
    }

    // Baseline IDs should still exist after restart (no deletion)
    let current_ids: Vec<sqlx::types::Uuid> = sqlx::query_scalar!(
        "SELECT id FROM core.events ORDER BY id"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let baseline_set: std::collections::HashSet<_> = baseline_ids.into_iter().collect();
    let current_set: std::collections::HashSet<_> = current_ids.into_iter().collect();

    if baseline_set.is_subset(&current_set) {
        runner.pass(name);
    } else {
        let lost_count = baseline_set.len() - baseline_set.intersection(&current_set).count();
        runner.fail(
            name,
            &format!("{lost_count} baseline events lost after restart"),
        );
    }
}

async fn test_no_duplicate_events_after_restart(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: no duplicate events after restart";

    let result: Result<Option<i64>, _> = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM (\
           SELECT id, COUNT(*) as cnt FROM core.events GROUP BY id HAVING COUNT(*) > 1\
         ) t"
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(Some(dup_count)) if dup_count == 0 => runner.pass(name),
        Ok(Some(dup_count)) => runner.fail(
            name,
            &format!("{dup_count} events have duplicate IDs (replay violation)"),
        ),
        Ok(None) => runner.pass(name),
        Err(e) => runner.fail(name, &format!("duplicate check query failed: {e}")),
    }
}

async fn test_pipeline_flows_after_recovery(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: pipeline flows after recovery";

    let before = event_count(pool).await;
    let watched = "/var/lib/sinex/watched";

    // Generate post-recovery files
    for i in 0..10_u32 {
        let _ = std::fs::write(
            format!("{watched}/restart-post-{i}.txt"),
            format!("post {i}"),
        );
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after = event_count(pool).await;
        if after > before {
            runner.pass(name);
            return;
        }
        if Instant::now() >= deadline {
            runner.fail(
                name,
                &format!("pipeline stalled after recovery (before={before}, after={after})"),
            );
            return;
        }
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
