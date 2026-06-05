use sinex_primitives::temporal::Timestamp;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CheckpointInconsistency {
    pub module_name: String,
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
    pub module_name: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub checkpoint_kind: CheckpointKind,
    pub last_processed_id: Option<Uuid>,
    pub processed_count: u64,
    pub last_activity: Timestamp,
}

impl CheckpointSnapshot {
    #[must_use]
    pub fn requires_event_id(&self) -> bool {
        matches!(self.checkpoint_kind, CheckpointKind::Internal)
    }

    #[must_use]
    pub fn supports_event_correlation(&self) -> bool {
        matches!(
            self.checkpoint_kind,
            CheckpointKind::Internal | CheckpointKind::Stream | CheckpointKind::None
        )
    }
}

pub mod checkpoint_verification {
    use super::{CheckpointSnapshot, latest_snapshot_for_node};
    use crate::repositories::Repository;
    use crate::repositories::integrity::IntegrityRepository;
    use sinex_primitives::error::Result as SinexResult;
    use sqlx::PgPool;

    pub async fn get_expected_automatons(pool: &PgPool) -> SinexResult<Vec<String>> {
        IntegrityRepository::new(pool)
            .get_expected_automatons()
            .await
    }

    pub async fn verify_automaton_checkpoint_consistency(
        pool: &PgPool,
        snapshots: &[CheckpointSnapshot],
        module_name: &str,
    ) -> SinexResult<Vec<String>> {
        let snapshot = latest_snapshot_for_node(snapshots, module_name);
        let issues = IntegrityRepository::new(pool)
            .analyze_node(module_name, snapshot, 1_000, 24, 24)
            .await?;
        Ok(issues.into_iter().map(|issue| issue.details).collect())
    }
}

fn latest_snapshot_for_node<'a>(
    snapshots: &'a [CheckpointSnapshot],
    module_name: &str,
) -> Option<&'a CheckpointSnapshot> {
    snapshots
        .iter()
        .filter(|snapshot| snapshot.module_name == module_name)
        .max_by_key(|snapshot| snapshot.last_activity)
}
