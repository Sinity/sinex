// Checkpoint test helper functions
//
// Shared utilities for checkpoint consistency verification tests.
// These helpers analyze checkpoint state, save/fetch checkpoint data,
// and detect various checkpoint inconsistencies.

use async_nats::jetstream::kv::Operation;
use color_eyre::eyre::eyre;
use futures::TryStreamExt;
use sinex_db::DbPool;
use sinex_node_sdk::checkpoint::parse_checkpoint_key;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::Timestamp;
use sinex_primitives::ids::Ulid;
use time::Duration;
use xtask::sandbox::prelude::*;

/// Types of checkpoint inconsistencies that can be detected
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)] // Test infrastructure for checkpoint consistency analysis
pub(crate) enum CheckpointInconsistencyType {
    /// No checkpoint exists for an expected node
    MissingCheckpoint,
    /// Checkpoint references an event that doesn't exist in the database
    MissingEventReference,
    /// Checkpoint is behind the latest events
    CheckpointBehindEvents,
    /// Checkpoint hasn't been updated within the expected window
    StaleCheckpoint,
    /// Checkpoint has an invalid or missing ULID despite processed work
    InvalidCheckpointFormat,
}

/// A detected checkpoint inconsistency
#[derive(Debug, Clone)]
#[allow(dead_code)] // Test infrastructure for checkpoint consistency analysis
pub(crate) struct CheckpointInconsistency {
    pub node_name: String,
    pub details: String,
    pub inconsistency_type: CheckpointInconsistencyType,
    pub events_potentially_missed: u64,
}

/// Analyze a checkpoint for consistency issues
///
/// This function checks:
/// - Whether the checkpoint exists
/// - Whether the referenced event exists
/// - Whether there are newer events that haven't been processed
/// - Whether the checkpoint is stale
#[allow(dead_code)] // Test infrastructure for checkpoint consistency analysis
pub(crate) async fn analyze_checkpoint(
    pool: &DbPool,
    kv: &async_nats::jetstream::kv::Store,
    node_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    source: &str,
    stale_after: Duration,
) -> TestResult<Vec<CheckpointInconsistency>> {
    let mut issues = Vec::new();

    let snapshot = fetch_checkpoint_state(kv, node_name, consumer_group, consumer_name).await?;
    let Some(snapshot) = snapshot else {
        issues.push(CheckpointInconsistency {
            node_name: node_name.to_string(),
            details: "No checkpoint found for node".to_string(),
            inconsistency_type: CheckpointInconsistencyType::MissingCheckpoint,
            events_potentially_missed: 0,
        });
        return Ok(issues);
    };

    let last_processed_id = match &snapshot.checkpoint {
        Checkpoint::Internal { event_id, .. } => Some(*event_id),
        Checkpoint::Stream { event_id, .. } => *event_id,
        Checkpoint::None | Checkpoint::External { .. } | Checkpoint::Timestamp { .. } => None,
    };

    if last_processed_id.is_none() && snapshot.processed_count > 0 {
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

    let newer_events: i64 = if let Some(last_id) = last_processed_id {
        let exists = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM core.events WHERE id = $1::uuid::ulid)"#,
            last_id.to_uuid()
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

        sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM core.events WHERE source = $1 AND id > $2::uuid::ulid"#,
            source,
            last_id.to_uuid()
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0)
    } else {
        sqlx::query_scalar!(
            r#"SELECT COUNT(*) FROM core.events WHERE source = $1"#,
            source
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0)
    };

    if newer_events > 0 {
        issues.push(CheckpointInconsistency {
            node_name: node_name.to_string(),
            details: format!("Checkpoint behind by {newer_events} events"),
            inconsistency_type: CheckpointInconsistencyType::CheckpointBehindEvents,
            events_potentially_missed: newer_events.max(0) as u64,
        });
    }

    let hours_since_last_activity = (Timestamp::now() - snapshot.last_activity).whole_hours();
    if hours_since_last_activity >= stale_after.whole_hours() {
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

/// Save a checkpoint state to the KV store
#[allow(dead_code)] // Test infrastructure for checkpoint consistency analysis
pub(crate) async fn save_checkpoint_state(
    kv: &async_nats::jetstream::kv::Store,
    node_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    checkpoint: Checkpoint,
    processed_count: u64,
    last_activity: Timestamp,
    data: Option<serde_json::Value>,
) -> TestResult<()> {
    let manager = CheckpointManager::new(
        kv.clone(),
        node_name.to_string(),
        consumer_group.to_string(),
        consumer_name.to_string(),
    );
    let state = CheckpointState {
        checkpoint,
        processed_count,
        last_activity,
        data,
        version: 2,
        revision: 0,
    };
    manager.save_checkpoint(&state).await?;
    Ok(())
}

/// Fetch checkpoint state from the KV store
pub(crate) async fn fetch_checkpoint_state(
    kv: &async_nats::jetstream::kv::Store,
    node_name: &str,
    consumer_group: &str,
    consumer_name: &str,
) -> TestResult<Option<CheckpointState>> {
    let mut keys = kv.keys().await?;
    while let Some(key) = keys.try_next().await? {
        let Some((proc, group, consumer)) = parse_checkpoint_key(&key) else {
            continue;
        };
        if proc == node_name && group == consumer_group && consumer == consumer_name {
            let entry = kv.entry(&key).await?;
            let Some(entry) = entry else {
                return Ok(None);
            };
            if !matches!(entry.operation, Operation::Put) || entry.value.is_empty() {
                return Ok(None);
            }
            let state = serde_json::from_slice(&entry.value)?;
            return Ok(Some(state));
        }
    }
    Ok(None)
}

/// Purge checkpoint state from the KV store
#[allow(dead_code)]
pub(crate) async fn purge_checkpoint_state(
    kv: &async_nats::jetstream::kv::Store,
    node_name: &str,
    consumer_group: &str,
    consumer_name: &str,
) -> TestResult<()> {
    let mut keys = kv.keys().await?;
    while let Some(key) = keys.try_next().await? {
        let Some((proc, group, consumer)) = parse_checkpoint_key(&key) else {
            continue;
        };
        if proc == node_name && group == consumer_group && consumer == consumer_name {
            kv.purge(&key).await?;
            break;
        }
    }
    Ok(())
}

/// Fetch the ULID of an event at a specific offset within a source
#[allow(dead_code)]
pub(crate) async fn fetch_event_ulid_at(
    pool: &DbPool,
    source: &str,
    offset: i64,
) -> TestResult<Ulid> {
    for attempt in 0..3 {
        if let Some(id_uuid) = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT id::uuid FROM core.events WHERE source = $1 ORDER BY id OFFSET $2 LIMIT 1",
        )
        .bind(source)
        .bind(offset)
        .fetch_optional(pool)
        .await?
        {
            let ulid = Ulid::from(id_uuid);
            return Ok(ulid);
        }

        tokio::time::sleep(std::time::Duration::from_millis(20 * (attempt + 1))).await;
    }

    let available: i64 = sqlx::query_scalar!(
        r#"SELECT COUNT(*) FROM core.events WHERE source = $1"#,
        source
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    Err(eyre!(
        "No event found for source {source} at offset {offset}; available events: {available}"
    ))
}
