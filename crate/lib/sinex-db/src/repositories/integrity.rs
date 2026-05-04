//! Integrity repository for database consistency verification.
//!
//! Provides methods for checkpoint verification and event integrity analysis
//! that were previously free functions in `crate::integrity`.

use super::common::Repository;
use crate::integrity::{
    CheckpointInconsistency, CheckpointInconsistencyType, CheckpointSnapshot,
};
use sinex_primitives::error::Result as SinexResult;
use sinex_primitives::temporal::{Duration, Timestamp};
use sqlx::PgPool;
use uuid::Uuid;

/// Repository for database integrity verification operations.
pub struct IntegrityRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for IntegrityRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> IntegrityRepository<'a> {
    /// Get the names of all expected automaton nodes from `core.node_manifests`.
    pub async fn get_expected_automatons(&self) -> SinexResult<Vec<String>> {
        let names =
            sqlx::query_scalar!(r#"SELECT node_name FROM core.node_manifests ORDER BY node_name"#)
                .fetch_all(self.pool)
                .await?;
        Ok(names)
    }

    /// Check whether an event with the given ID exists in `core.events`.
    pub async fn event_exists(&self, event_id: Uuid) -> SinexResult<bool> {
        let exists = sqlx::query_scalar!(
            r#"SELECT EXISTS(SELECT 1 FROM core.events WHERE id = $1::uuid)"#,
            event_id
        )
        .fetch_one(self.pool)
        .await?
        .unwrap_or(false);
        Ok(exists)
    }

    /// Count events newer than a given ID, optionally filtered by a `ts_orig`
    /// cutoff timestamp.
    pub async fn count_events_newer_than(
        &self,
        last_processed_id: Uuid,
        cutoff_ts: Option<Timestamp>,
    ) -> SinexResult<i64> {
        if let Some(cutoff) = cutoff_ts {
            Ok(sqlx::query_scalar!(
                r#"SELECT COUNT(*) as "count!" FROM core.events WHERE id > $1::uuid AND ts_orig >= $2"#,
                last_processed_id,
                cutoff.inner()
            )
            .fetch_one(self.pool)
            .await?)
        } else {
            Ok(sqlx::query_scalar!(
                r#"SELECT COUNT(*) as "count!" FROM core.events WHERE id > $1::uuid"#,
                last_processed_id
            )
            .fetch_one(self.pool)
            .await?)
        }
    }

    /// Count events since a given `ts_orig` cutoff, without an event-ID anchor.
    pub async fn count_events_since(&self, cutoff_ts: Timestamp) -> SinexResult<i64> {
        Ok(sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!" FROM core.events WHERE ts_orig >= $1"#,
            cutoff_ts.inner()
        )
        .fetch_one(self.pool)
        .await?)
    }

    /// Count total events in `core.events`.
    pub async fn count_total_events(&self) -> SinexResult<i64> {
        Ok(sqlx::query_scalar!(r#"SELECT COUNT(*) as "count!" FROM core.events"#)
            .fetch_one(self.pool)
            .await?)
    }

    /// Analyze a single node for checkpoint consistency issues.
    pub async fn analyze_node(
        &self,
        node_name: &str,
        snapshot: Option<&CheckpointSnapshot>,
        max_events: usize,
        stale_window_hours: i64,
        check_window_hours: i64,
    ) -> SinexResult<Vec<CheckpointInconsistency>> {
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
                    "Checkpoint missing UUIDv7 reference despite processed_count={}",
                    snapshot.processed_count
                ),
                inconsistency_type: CheckpointInconsistencyType::InvalidCheckpointFormat,
                events_potentially_missed: snapshot.processed_count,
            });
        }

        if snapshot.supports_event_correlation()
            && let Some(last_processed_id) = snapshot.last_processed_id
        {
            let exists = self.event_exists(last_processed_id).await?;
            if !exists {
                issues.push(CheckpointInconsistency {
                    node_name: node_name.to_string(),
                    details: "Checkpoint references non-existent event".to_string(),
                    inconsistency_type: CheckpointInconsistencyType::MissingEventReference,
                    events_potentially_missed: 0,
                });
            }
        }

        let newer_events: i64 = if snapshot.supports_event_correlation() {
            let window_cutoff = if check_window_hours > 0 {
                Some(Timestamp::now() - Duration::hours(check_window_hours))
            } else {
                None
            };

            if let Some(last_processed_id) = snapshot.last_processed_id {
                self.count_events_newer_than(last_processed_id, window_cutoff)
                    .await?
            } else if let Some(cutoff) = window_cutoff {
                self.count_events_since(cutoff).await?
            } else {
                self.count_total_events().await?
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

        let hours_since_last_activity =
            (Timestamp::now() - snapshot.last_activity).whole_hours();
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
}
