//! Security- and chaos-focused validation regressions.

use sinex_primitives::db::models::event::{Event, Provenance, SourceMaterial};
use sinex_primitives::db::validation::{EventValidator, ValidationError};
use sinex_primitives::domain::{EventSource, EventType, HostName};
use sinex_primitives::Id;
use time::Duration;
use xtask::sandbox::{sinex_test, TestResult};

fn base_event() -> Event<serde_json::Value> {
    Event {
        id: Some(Id::new()),
        source: EventSource::from("security-chaos"),
        event_type: EventType::from("security.chaos"),
        ts_orig: Some(OffsetDateTime::now_utc()),
        host: HostName::from("security-host"),
        ingestor_version: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
        payload: serde_json::json!({"ok": true}),
        associated_blob_ids: None,
    }
}

#[sinex_test]
fn validator_rejects_future_ts_orig_beyond_drift() -> TestResult<()> {
    let mut event = base_event();
    event.ts_orig = Some(OffsetDateTime::now_utc() + Duration::hours(1));
    let validator = EventValidator::new(None, None, None, false);
    let err = validator.validate(&event).unwrap_err();
    assert!(
        matches!(err, ValidationError::InvalidValue { field, .. } if field == "ts_orig"),
        "expected ts_orig InvalidValue, got {err:?}"
    );
    Ok(())
}

#[sinex_test]
fn validator_rejects_null_byte_in_payload_string() -> TestResult<()> {
    let mut event = base_event();
    event.payload = serde_json::json!({"path": "bad\u{0000}path"});
    let validator = EventValidator::new(None, None, None, false);
    let err = validator.validate(&event).unwrap_err();
    assert!(
        matches!(err, ValidationError::SecurityValidation(msg) if msg.to_lowercase().contains("null byte")),
        "expected security validation error for null byte, got {err:?}"
    );
    Ok(())
}
