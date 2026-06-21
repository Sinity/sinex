//! Chaos test: simulate abrupt process termination (SIGKILL) and recovery.
//!
//! Kills event_engine mid-flight with SIGKILL, verifies checkpoint recovery,
//! and asserts no data loss or duplication.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::Duration;

use color_eyre::eyre::Result;
use sqlx::{PgPool, Row};

use crate::runner::{TestOutcome, TestRunner};

use super::chaos_support::{
    SINEXD_SERVICE, observed_event_count, report_event_count_increase,
    report_watched_files_written, wait_for_service_active,
};

pub async fn run(runner: &mut TestRunner, database_url: &str) -> Result<()> {
    println!("\n── Chaos: Process Restart tests ────────────────────────────────");

    let pool = PgPool::connect(database_url).await?;

    test_baseline_events_captured(runner, &pool).await;
    let pre_sigkill_baseline = test_event_engine_restarts_after_sigkill(runner, &pool).await;
    test_no_data_loss_after_restart(runner, &pool, pre_sigkill_baseline.as_ref()).await;
    test_no_duplicate_events_after_restart(runner, &pool, pre_sigkill_baseline.as_ref()).await;
    test_pipeline_flows_after_recovery(runner, &pool).await;

    Ok(())
}

#[derive(Debug, Clone)]
struct RestartBaseline {
    event_ids: Vec<sqlx::types::Uuid>,
    material_occurrences: BTreeMap<MaterialOccurrenceKey, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MaterialOccurrenceKey {
    source_material_id: sqlx::types::Uuid,
    anchor_byte: i64,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    offset_kind: Option<String>,
}

async fn test_baseline_events_captured(
    runner: &mut TestRunner,
    pool: &PgPool,
) -> Option<RestartBaseline> {
    let name = "chaos-process-restart: baseline events captured";

    if !report_watched_files_written(runner, name, "restart-baseline", 10, "baseline") {
        return None;
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    let Some(count) = observed_event_count(runner, name, pool).await else {
        return None;
    };
    if count <= 0 {
        runner.fail(name, "no baseline events captured");
        return None;
    }

    let Some(baseline) = capture_restart_baseline(runner, name, pool, "baseline").await else {
        return None;
    };

    runner.pass(name);
    Some(baseline)
}

async fn capture_restart_baseline(
    runner: &mut TestRunner,
    name: &str,
    pool: &PgPool,
    phase: &str,
) -> Option<RestartBaseline> {
    let event_ids = match event_ids(pool).await {
        Ok(event_ids) if !event_ids.is_empty() => event_ids,
        Ok(_) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("{phase} event count was positive but no event IDs could be snapshotted"),
            );
            return None;
        }
        Err(error) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("{phase} event-id snapshot failed: {error}"),
            );
            return None;
        }
    };

    let material_occurrences = match material_occurrence_counts(pool).await {
        Ok(counts) if !counts.is_empty() => counts,
        Ok(_) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!(
                    "{phase} events had no material occurrence anchors to compare after restart"
                ),
            );
            return None;
        }
        Err(error) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("{phase} material occurrence snapshot failed: {error}"),
            );
            return None;
        }
    };

    Some(RestartBaseline {
        event_ids,
        material_occurrences,
    })
}

async fn event_ids(pool: &PgPool) -> Result<Vec<sqlx::types::Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, sqlx::types::Uuid>("SELECT id FROM core.events ORDER BY id")
        .fetch_all(pool)
        .await
}

async fn material_occurrence_counts(
    pool: &PgPool,
) -> Result<BTreeMap<MaterialOccurrenceKey, i64>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT source_material_id, anchor_byte, offset_start, offset_end, offset_kind, \
                COUNT(*)::bigint AS event_count \
         FROM core.events \
         WHERE source_material_id IS NOT NULL AND anchor_byte IS NOT NULL \
         GROUP BY source_material_id, anchor_byte, offset_start, offset_end, offset_kind",
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let key = MaterialOccurrenceKey {
                source_material_id: row.try_get("source_material_id")?,
                anchor_byte: row.try_get("anchor_byte")?,
                offset_start: row.try_get("offset_start")?,
                offset_end: row.try_get("offset_end")?,
                offset_kind: row.try_get("offset_kind")?,
            };
            let count = row.try_get("event_count")?;
            Ok((key, count))
        })
        .collect()
}

fn missing_baseline_event_count(
    baseline_ids: &[sqlx::types::Uuid],
    current_ids: &[sqlx::types::Uuid],
) -> usize {
    let baseline_set: std::collections::HashSet<_> = baseline_ids.iter().copied().collect();
    let current_set: std::collections::HashSet<_> = current_ids.iter().copied().collect();
    baseline_set.difference(&current_set).count()
}

fn material_occurrence_count_regressions(
    baseline: &BTreeMap<MaterialOccurrenceKey, i64>,
    current: &BTreeMap<MaterialOccurrenceKey, i64>,
) -> Vec<(MaterialOccurrenceKey, i64, i64)> {
    baseline
        .iter()
        .filter_map(|(key, baseline_count)| {
            let current_count = current.get(key).copied().unwrap_or(0);
            (current_count > *baseline_count).then(|| (key.clone(), *baseline_count, current_count))
        })
        .collect()
}

async fn test_event_engine_restarts_after_sigkill(
    runner: &mut TestRunner,
    pool: &PgPool,
) -> Option<RestartBaseline> {
    let name = "chaos-process-restart: event_engine restarts after SIGKILL";

    if !report_watched_files_written(runner, name, "restart-during", 30, "during") {
        return None;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    let baseline = capture_restart_baseline(runner, name, pool, "pre-SIGKILL").await;

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
        return baseline;
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
        return baseline;
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
        baseline
    } else {
        runner.fail(
            name,
            "event_engine did not restart within 30s after SIGKILL",
        );
        baseline
    }
}

async fn test_no_data_loss_after_restart(
    runner: &mut TestRunner,
    pool: &PgPool,
    baseline: Option<&RestartBaseline>,
) {
    let name = "chaos-process-restart: no data loss after restart";

    let Some(baseline) = baseline else {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            "pre-SIGKILL event-id snapshot was not captured, so data-loss comparison cannot run",
        );
        return;
    };

    // Baseline IDs should still exist after restart (no deletion)
    let current_ids = match event_ids(pool).await {
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

    let lost_count = missing_baseline_event_count(&baseline.event_ids, &current_ids);
    if lost_count == 0 {
        runner.pass(name);
    } else {
        runner.fail(
            name,
            &format!("{lost_count} baseline events lost after restart"),
        );
    }
}

async fn test_no_duplicate_events_after_restart(
    runner: &mut TestRunner,
    pool: &PgPool,
    baseline: Option<&RestartBaseline>,
) {
    let name = "chaos-process-restart: no duplicate events after restart";

    let Some(baseline) = baseline else {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            "pre-SIGKILL material occurrence snapshot was not captured, so duplicate comparison cannot run",
        );
        return;
    };

    let current = match material_occurrence_counts(pool).await {
        Ok(counts) => counts,
        Err(error) => {
            runner.record(
                name,
                TestOutcome::EvidenceMissing,
                &format!("current material occurrence query failed: {error}"),
            );
            return;
        }
    };

    let regressions =
        material_occurrence_count_regressions(&baseline.material_occurrences, &current);
    if regressions.is_empty() {
        runner.pass(name);
    } else {
        let examples = regressions
            .iter()
            .take(3)
            .map(|(key, before, after)| {
                format!(
                    "{}@{} {:?}-{:?}/{:?}: {before}->{after}",
                    key.source_material_id,
                    key.anchor_byte,
                    key.offset_start,
                    key.offset_end,
                    key.offset_kind
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        runner.fail(
            name,
            &format!(
                "{} pre-fault material occurrence(s) gained duplicate events after restart; examples: {examples}",
                regressions.len()
            ),
        );
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sqlx::types::Uuid;

    use super::{
        MaterialOccurrenceKey, material_occurrence_count_regressions, missing_baseline_event_count,
    };

    fn uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    #[test]
    fn missing_baseline_event_count_detects_lost_pre_fault_events() {
        let kept = uuid(1);
        let lost = uuid(2);
        let extra = uuid(3);

        assert_eq!(
            missing_baseline_event_count(&[kept, lost], &[kept, extra]),
            1
        );
    }

    #[test]
    fn missing_baseline_event_count_allows_added_recovery_events() {
        let baseline = uuid(1);
        let recovered = uuid(2);

        assert_eq!(
            missing_baseline_event_count(&[baseline], &[baseline, recovered]),
            0
        );
    }

    fn occurrence(id: u128, anchor_byte: i64) -> MaterialOccurrenceKey {
        MaterialOccurrenceKey {
            source_material_id: uuid(id),
            anchor_byte,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
        }
    }

    #[test]
    fn material_occurrence_regressions_detect_replayed_duplicate_anchors() {
        let stable = occurrence(1, 10);
        let duplicated = occurrence(2, 20);
        let mut baseline = BTreeMap::new();
        baseline.insert(stable.clone(), 1);
        baseline.insert(duplicated.clone(), 1);

        let mut current = BTreeMap::new();
        current.insert(stable, 1);
        current.insert(duplicated.clone(), 2);
        current.insert(occurrence(3, 30), 1);

        assert_eq!(
            material_occurrence_count_regressions(&baseline, &current),
            vec![(duplicated, 1, 2)]
        );
    }

    #[test]
    fn material_occurrence_regressions_allow_new_occurrences() {
        let baseline_key = occurrence(1, 10);
        let mut baseline = BTreeMap::new();
        baseline.insert(baseline_key.clone(), 1);

        let mut current = BTreeMap::new();
        current.insert(baseline_key, 1);
        current.insert(occurrence(2, 20), 3);

        assert!(material_occurrence_count_regressions(&baseline, &current).is_empty());
    }
}
