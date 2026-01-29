use crate::DbPool;
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_schema::ulid::Ulid;
use std::collections::HashSet;
#[derive(Debug, Clone)]
pub struct IntegrityTestConfig {
    pub max_events_to_check: usize,
    pub check_window_hours: i64,
    pub include_deep_validation: bool,
    pub validate_checkpoints: bool,
    pub validate_ulid_ordering: bool,
    pub validate_schemas: bool,
    pub checkpoint_snapshots: Option<Vec<CheckpointSnapshot>>,
}
impl Default for IntegrityTestConfig {
    fn default() -> Self {
        Self {
            max_events_to_check: 1_000,
            check_window_hours: 24,
            include_deep_validation: false,
            validate_checkpoints: false,
            validate_ulid_ordering: false,
            validate_schemas: false,
            checkpoint_snapshots: None,
        }
    }
}
pub struct IntegrityTester<'a> {
    pool: &'a DbPool,
}
impl<'a> IntegrityTester<'a> {
    pub async fn new(pool: &'a DbPool) -> Result<Self> {
        Ok(Self { pool })
    }
    pub async fn run_integrity_tests(
        &self,
        config: IntegrityTestConfig,
    ) -> Result<IntegrityResults> {
        if !config.validate_checkpoints {
            return Ok(IntegrityResults {
                check_report: CheckReport {
                    checkpoint_inconsistencies: Vec::new(),
                },
            });
        }
        let snapshots = config.checkpoint_snapshots.as_deref().ok_or_else(|| {
            SinexError::configuration(
                "Checkpoint validation enabled but no checkpoint snapshots provided",
            )
        })?;
        let mut processors: Vec<String> = sqlx::query_scalar!(
            r#"SELECT processor_name FROM core.processor_manifests ORDER BY processor_name"#
        )
        .fetch_all(self.pool)
        .await?;
        let mut extra_processors: HashSet<String> = snapshots
            .iter()
            .map(|snapshot| snapshot.processor_name.clone())
            .collect();
        for name in &processors {
            extra_processors.remove(name);
        }
        processors.extend(extra_processors.into_iter());
        processors.sort();
        let mut issues = Vec::new();
        for processor in processors.into_iter() {
            let snapshot = latest_snapshot_for_processor(snapshots, &processor);
            let mut detected = analyze_processor(
                self.pool,
                &processor,
                snapshot,
                config.max_events_to_check,
                config.check_window_hours,
                config.check_window_hours,
            )
            .await?;
            issues.append(&mut detected);
        }
        Ok(IntegrityResults {
            check_report: CheckReport {
                checkpoint_inconsistencies: issues,
            },
        })
    }
}
pub struct IntegrityResults {
    pub check_report: CheckReport,
}
pub struct CheckReport {
    pub checkpoint_inconsistencies: Vec<CheckpointInconsistency>,
}
#[derive(Debug, Clone)]
pub struct CheckpointInconsistency {
    pub processor_name: String,
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
    pub processor_name: String,
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
        let names = sqlx::query_scalar!(
            r#"SELECT processor_name FROM core.processor_manifests ORDER BY processor_name"#
        )
        .fetch_all(pool)
        .await?;
        Ok(names)
    }
    pub async fn verify_automaton_checkpoint_consistency(
        pool: &DbPool,
        snapshots: &[CheckpointSnapshot],
        processor_name: &str,
    ) -> SinexResult<Vec<String>> {
        let snapshot = latest_snapshot_for_processor(snapshots, processor_name);
        let issues = analyze_processor(pool, processor_name, snapshot, 1_000, 24, 24).await?;
        Ok(issues.into_iter().map(|issue| issue.details).collect())
    }
}
/// Utilities used by schema validation tests to synthesize malformed events and
/// detect obvious anomalies before schema-level validation kicks in.
pub mod malformed_detection {
    use crate::models::event::{Event, OffsetKind, Provenance, SourceMaterial};
    use crate::validation::DEFAULT_MAX_PAYLOAD_BYTES;
    use crate::JsonValue;

    use serde_json::json;
    use sinex_primitives::domain::{EventSource, EventType, HostName};
    use sinex_primitives::Id;
    use sinex_primitives::Timestamp;
    /// Generate a fixed set of malformed events covering common anomaly classes.
    pub fn generate_malformed_test_events() -> Vec<Event<JsonValue>> {
        vec![
            build_event("malformed.generator", "null_payload", JsonValue::Null),
            build_event("", "empty_source", json!({"data": "test"})),
            build_event("test\0source", "null_byte_source", json!({"data": "test"})),
            build_event(
                "malformed.generator",
                "oversized_payload",
                json!({"blob": "x".repeat(DEFAULT_MAX_PAYLOAD_BYTES + 1024)}),
            ),
        ]
    }
    /// Perform quick heuristic checks that complement schema enforcement.
    pub fn detect_schema_anomalies(event: &Event<JsonValue>) -> Vec<String> {
        let mut anomalies = Vec::new();
        let source = event.source.as_ref();
        if source.is_empty() {
            anomalies.push("event source is empty".to_string());
        }
        if source.contains('\0') {
            anomalies.push("event source contains null bytes".to_string());
        }
        let event_type = event.event_type.as_ref();
        if event_type.contains('\0') {
            anomalies.push("event type contains null bytes".to_string());
        }
        if event_type.starts_with('.') || event_type.ends_with('.') {
            anomalies.push("event type has invalid dot placement".to_string());
        }
        let payload_size = serde_json::to_vec(&event.payload)
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        if payload_size > DEFAULT_MAX_PAYLOAD_BYTES {
            anomalies.push(format!(
                "payload size {payload_size} exceeds limit {DEFAULT_MAX_PAYLOAD_BYTES}"
            ));
        }
        if !event.payload.is_object() {
            anomalies.push("payload must be a JSON object".to_string());
        }
        anomalies
    }
    fn build_event(source: &str, event_type: &str, payload: JsonValue) -> Event<JsonValue> {
        Event {
            id: None,
            source: EventSource::from(source.to_string()),
            event_type: EventType::from(event_type.to_string()),
            payload,
            ts_orig: Some(Timestamp::now()),
            host: HostName::from_static("malformed-detector"),
            ingestor_version: None,
            payload_schema_id: None,
            provenance: Provenance::Material {
                id: Id::<SourceMaterial>::new(),
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
        }
    }
}
async fn analyze_processor(
    pool: &DbPool,
    processor_name: &str,
    snapshot: Option<&CheckpointSnapshot>,
    max_events: usize,
    stale_window_hours: i64,
    check_window_hours: i64,
) -> Result<Vec<CheckpointInconsistency>> {
    let mut issues = Vec::new();
    let Some(snapshot) = snapshot else {
        issues.push(CheckpointInconsistency {
            processor_name: processor_name.to_string(),
            details: "No checkpoint found for processor".to_string(),
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
            processor_name: processor_name.to_string(),
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
                    processor_name: processor_name.to_string(),
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
            processor_name: processor_name.to_string(),
            details: format!("Checkpoint behind by {} events", newer_events),
            inconsistency_type: CheckpointInconsistencyType::CheckpointBehindEvents,
            events_potentially_missed: newer_events.min(max_events as i64).max(0) as u64,
        });
    }
    let hours_since_last_activity = (Timestamp::now() - snapshot.last_activity).whole_hours();
    if hours_since_last_activity >= stale_window_hours {
        issues.push(CheckpointInconsistency {
            processor_name: processor_name.to_string(),
            details: format!(
                "Checkpoint stale (last activity {} hours ago)",
                hours_since_last_activity
            ),
            inconsistency_type: CheckpointInconsistencyType::StaleCheckpoint,
            events_potentially_missed: newer_events.max(0) as u64,
        });
    }
    Ok(issues)
}
fn latest_snapshot_for_processor<'a>(
    snapshots: &'a [CheckpointSnapshot],
    processor_name: &str,
) -> Option<&'a CheckpointSnapshot> {
    snapshots
        .iter()
        .filter(|snapshot| snapshot.processor_name == processor_name)
        .max_by_key(|snapshot| snapshot.last_activity)
}
