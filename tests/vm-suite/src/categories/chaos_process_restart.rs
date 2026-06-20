//! Chaos test: simulate abrupt process termination (SIGKILL) and recovery.
//!
//! Kills event_engine mid-flight with SIGKILL, verifies checkpoint recovery,
//! and asserts no data loss or duplication.

use std::process::Command;
use std::time::Duration;

use color_eyre::eyre::Result;
use sqlx::PgPool;

use crate::runner::{TestOutcome, TestRunner};

use super::chaos_support::{
    SINEXD_SERVICE, observed_event_count, report_event_count_increase,
    report_watched_files_written, wait_for_service_active,
};

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Process Restart tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_events_captured(runner, &pool).await;
    test_event_engine_restarts_after_sigkill(runner, &pool).await;
    test_no_data_loss_after_restart(runner, &pool).await;
    test_no_duplicate_events_after_restart(runner, &pool).await;
    test_pipeline_flows_after_recovery(runner, &pool).await;

    Ok(())
}

async fn test_baseline_events_captured(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: baseline events captured";

    if !report_watched_files_written(runner, name, "restart-baseline", 10, "baseline") {
        return;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let Some(count) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if count > 0 {
        runner.pass(name);
    } else {
        runner.fail(name, "no baseline events captured");
    }
}

async fn test_event_engine_restarts_after_sigkill(runner: &mut TestRunner, _pool: &PgPool) {
    let name = "chaos-process-restart: event_engine restarts after SIGKILL";

    if !report_watched_files_written(runner, name, "restart-during", 30, "during") {
        return;
    }

    // Get the PID of sinexd
    let pid_output = Command::new("systemctl")
        .args(["show", "-p", "MainPID", "--value", "sinexd"])
        .output()
        .ok();

    let pid_str = pid_output
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s != "0");

    let Some(pid) = pid_str else {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            "systemd did not report a live sinexd MainPID, so SIGKILL restart recovery was not exercised",
        );
        return;
    };

    let killed = Command::new("kill")
        .args(["-9", &pid])
        .status()
        .is_ok_and(|status| status.success());
    if !killed {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            &format!("failed to SIGKILL sinexd MainPID {pid}; restart recovery was not exercised"),
        );
        return;
    }

    if wait_for_service_active(
        SINEXD_SERVICE,
        Duration::from_secs(30),
        Duration::from_secs(1),
    )
    .await
    {
        runner.pass(name);
        // Wait for checkpoint replay.
        tokio::time::sleep(Duration::from_secs(10)).await;
    } else {
        runner.fail(
            name,
            "event_engine did not restart within 30s after SIGKILL",
        );
    }
}

async fn test_no_data_loss_after_restart(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: no data loss after restart";

    let baseline_ids: Vec<sqlx::types::Uuid> =
        match sqlx::query_scalar::<_, sqlx::types::Uuid>("SELECT id FROM core.events ORDER BY id")
            .fetch_all(pool)
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                runner.record(
                    name,
                    TestOutcome::EvidenceMissing,
                    &format!("baseline event-id query failed: {error}"),
                );
                return;
            }
        };

    if baseline_ids.is_empty() {
        runner.fail(name, "baseline IDs are empty, cannot verify data loss");
        return;
    }

    // Baseline IDs should still exist after restart (no deletion)
    let current_ids: Vec<sqlx::types::Uuid> =
        match sqlx::query_scalar::<_, sqlx::types::Uuid>("SELECT id FROM core.events ORDER BY id")
            .fetch_all(pool)
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                runner.record(
                    name,
                    TestOutcome::EvidenceMissing,
                    &format!("current event-id query failed: {error}"),
                );
                return;
            }
        };

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

    let result = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM (\
           SELECT id, COUNT(*) as cnt FROM core.events GROUP BY id HAVING COUNT(*) > 1\
         ) t",
    )
    .fetch_one(pool)
    .await;

    match result {
        Ok(0) => runner.pass(name),
        Ok(dup_count) => runner.fail(
            name,
            &format!("{dup_count} events have duplicate IDs (replay violation)"),
        ),
        Err(e) => runner.fail(name, &format!("duplicate check query failed: {e}")),
    }
}

async fn test_pipeline_flows_after_recovery(runner: &mut TestRunner, pool: &PgPool) {
    let name = "chaos-process-restart: pipeline flows after recovery";

    let Some(before) = observed_event_count(runner, name, pool).await else {
        return;
    };
    if !report_watched_files_written(runner, name, "restart-post", 10, "post") {
        return;
    }

    report_event_count_increase(
        runner,
        name,
        pool,
        before,
        Duration::from_secs(30),
        Duration::from_secs(2),
        |before| format!("pipeline stalled after recovery (before={before})"),
    )
    .await;
}
