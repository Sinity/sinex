//! Security- and chaos-focused validation regressions.

use chrono::{Duration as ChronoDuration, Utc};
use sinex_core::db::models::event::{Event, Provenance, SourceMaterial};
use sinex_core::db::validation::{EventValidator, ValidationError};
use sinex_core::types::domain::{EventSource, EventType, HostName};
use sinex_core::types::Id;
use sinex_schema::ulid::Ulid;

fn base_event() -> Event<serde_json::Value> {
    Event {
        id: Some(Id::new()),
        source: EventSource::from("security-chaos"),
        event_type: EventType::from("security.chaos"),
        ts_ingest: Utc::now(),
        ts_orig: Utc::now(),
        ts_orig_subnano: None,
        host: HostName::from("security-host"),
        ingestor_version: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
        payload: serde_json::json!({"ok": true}),
        anchor_byte: None,
        associated_blob_ids: None,
    }
}

#[test]
fn validator_rejects_future_ts_orig_beyond_drift() {
    let mut event = base_event();
    event.ts_orig = Utc::now() + ChronoDuration::hours(1);
    // Keep ts_ingest now so drift check triggers.
    let validator = EventValidator::new(None, None, None, false);
    let err = validator.validate(&event).unwrap_err();
    assert!(
        matches!(err, ValidationError::InvalidValue { field, .. } if field == "ts_orig"),
        "expected ts_orig InvalidValue, got {err:?}"
    );
}

#[test]
fn validator_rejects_null_byte_in_payload_string() {
    let mut event = base_event();
    event.payload = serde_json::json!({"path": "bad\u{0000}path"});
    let validator = EventValidator::new(None, None, None, false);
    let err = validator.validate(&event).unwrap_err();
    assert!(
        matches!(err, ValidationError::SecurityValidation(msg) if msg.to_lowercase().contains("null byte")),
        "expected security validation error for null byte, got {err:?}"
    );
}
