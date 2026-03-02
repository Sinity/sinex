use crate::DbPool;
use sinex_primitives::error::Result;
use sinex_primitives::temporal::{Duration, Timestamp};
use crate::Ulid;
#[derive(Debug, Clone)]
pub struct CheckpointInconsistency {
    pub node_name: String,
    pub details: String,
    pub inconsistency_type: CheckpointInconsistencyType,
    pub events_potentially_missed: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckpointInconsistencyType {
    MissingCheckpoint,
    MissingEventReference,
    CheckpointBehindEvents,
    StaleCheckpoint,
    InvalidCheckpointFormat,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointKind {
    None,
    Internal,
    Stream,
    External,
    Timestamp,
    Unknown,
}
#[derive(Debug, Clone)]
pub struct CheckpointSnapshot {
    pub node_name: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub checkpoint_kind: CheckpointKind,
    pub last_processed_id: Option<Ulid>,
    pub processed_count: u64,
    pub last_activity: Timestamp,
}
impl CheckpointSnapshot {
    fn requires_event_id(&self) -> bool {
        matches!(self.checkpoint_kind, CheckpointKind::Internal)
    }
    fn supports_event_correlation(&self) -> bool {
        matches!(
            self.checkpoint_kind,
            CheckpointKind::Internal | CheckpointKind::Stream | CheckpointKind::None
        )
    }
}
pub mod checkpoint_verification {
    use super::*;
    use sinex_primitives::error::Result as SinexResult;

    pub async fn get_expected_automatons(pool: &DbPool) -> SinexResult<Vec<String>> {
        let names =
            sqlx::query_scalar!(r#"SELECT node_name FROM core.node_manifests ORDER BY node_name"#)
                .fetch_all(pool)
                .await?;
        Ok(names)
    }
    pub async fn verify_automaton_checkpoint_consistency(
        pool: &DbPool,
        snapshots: &[CheckpointSnapshot],
        node_name: &str,
    ) -> SinexResult<Vec<String>> {
        let snapshot = latest_snapshot_for_node(snapshots, node_name);
        let issues = analyze_node(pool, node_name, snapshot, 1_000, 24, 24).await?;
        Ok(issues.into_iter().map(|issue| issue.details).collect())
    }
}
async fn analyze_node(
    pool: &DbPool,
    node_name: &str,
    snapshot: Option<&CheckpointSnapshot>,
    max_events: usize,
    stale_window_hours: i64,
    check_window_hours: i64,
) -> Result<Vec<CheckpointInconsistency>> {
    let mut issues = Vec::new();
    let Some(snapshot) = snapshot else {
        issues.push(CheckpointInconsistency {
            node_name: node_name.to_string(),
            details: "No checkpoint found for node".to_string(),
            inconsistency_type: CheckpointInconsistencyType::MissingCheckpoint,
            events_potentially_missed: 0,
        });
        return Ok(issues);
    };
    if snapshot.requires_event_id()
        && snapshot.last_processed_id.is_none()
        && snapshot.processed_count > 0
    {
        issues.push(CheckpointInconsistency {
            node_name: node_name.to_string(),
            details: format!(
                "Checkpoint missing ULID reference despite processed_count={}",
                snapshot.processed_count
            ),
            inconsistency_type: CheckpointInconsistencyType::InvalidCheckpointFormat,
            events_potentially_missed: snapshot.processed_count,
        });
    }
    if snapshot.supports_event_correlation() {
        if let Some(last_processed_id) = snapshot.last_processed_id {
            let exists = sqlx::query_scalar!(
                r#"SELECT EXISTS(SELECT 1 FROM core.events WHERE id = $1::uuid::ulid)"#,
                last_processed_id.as_uuid()
            )
            .fetch_one(pool)
            .await?
            .unwrap_or(false);
            if !exists {
                issues.push(CheckpointInconsistency {
                    node_name: node_name.to_string(),
                    details: "Checkpoint references non-existent event".to_string(),
                    inconsistency_type: CheckpointInconsistencyType::MissingEventReference,
                    events_potentially_missed: 0,
                });
            }
        }
    }
    let newer_events: i64 = if snapshot.supports_event_correlation() {
        let window_cutoff = if check_window_hours > 0 {
            Some(Timestamp::now() - Duration::hours(check_window_hours))
        } else {
            None
        };
        if let Some(last_processed_id) = snapshot.last_processed_id {
            if let Some(cutoff) = window_cutoff {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) as "count!" FROM core.events WHERE id > $1::uuid::ulid AND ts_orig >= $2"#,
                    last_processed_id.as_uuid(),
                    cutoff.inner()
                )
                .fetch_one(pool)
                .await?
            } else {
                sqlx::query_scalar!(
                    r#"SELECT COUNT(*) as "count!" FROM core.events WHERE id > $1::uuid::ulid"#,
                    last_processed_id.as_uuid()
                )
                .fetch_one(pool)
                .await?
            }
        } else if let Some(cutoff) = window_cutoff {
            sqlx::query_scalar!(
                r#"SELECT COUNT(*) as "count!" FROM core.events WHERE ts_orig >= $1"#,
                cutoff.inner()
            )
            .fetch_one(pool)
            .await?
        } else {
            sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!" FROM core.events"#)
                .fetch_one(pool)
                .await?
        }
    } else {
        0
    };
    if newer_events > 0 {
        issues.push(CheckpointInconsistency {
            node_name: node_name.to_string(),
            details: format!("Checkpoint behind by {newer_events} events"),
            inconsistency_type: CheckpointInconsistencyType::CheckpointBehindEvents,
            events_potentially_missed: newer_events.min(max_events as i64).max(0) as u64,
        });
    }
    let hours_since_last_activity = (Timestamp::now() - snapshot.last_activity).whole_hours();
    if hours_since_last_activity >= stale_window_hours {
        issues.push(CheckpointInconsistency {
            node_name: node_name.to_string(),
            details: format!(
                "Checkpoint stale (last activity {hours_since_last_activity} hours ago)"
            ),
            inconsistency_type: CheckpointInconsistencyType::StaleCheckpoint,
            events_potentially_missed: newer_events.max(0) as u64,
        });
    }
    Ok(issues)
}
fn latest_snapshot_for_node<'a>(
    snapshots: &'a [CheckpointSnapshot],
    node_name: &str,
) -> Option<&'a CheckpointSnapshot> {
    snapshots
        .iter()
        .filter(|snapshot| snapshot.node_name == node_name)
        .max_by_key(|snapshot| snapshot.last_activity)
}
