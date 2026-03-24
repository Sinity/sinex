use serde_json::json;
use sinex_db::validation::{EventValidator, ValidationError};
use sinex_primitives::prelude::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn event_validator_rejects_future_ts_orig() -> xtask::sandbox::TestResult<()> {
    let validator = EventValidator::with_validation_enabled(false);
    let event = Event {
        id: None,
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.event"),
        payload: json!({ "ok": true }),
        ts_orig: Some(Timestamp::now() + ::time::Duration::hours(2)),
        host: HostName::from_static("validator"),
        node_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        node_model: None,
    };

    let error = validator
        .validate(&event)
        .expect_err("future ts_orig should be rejected");
    assert_eq!(
        error,
        ValidationError::InvalidValue {
            field: "ts_orig".to_string(),
            reason: "timestamp is more than 3600 seconds in the future".to_string(),
        }
    );
    Ok(())
}
