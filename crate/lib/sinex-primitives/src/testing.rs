//! Testing utilities for domain primitives.
//!
//! Provides event fixtures and property testing strategies for domain types.

/// Create an event fixture for testing (in-memory only).
///
/// **WARNING**: Do NOT insert events created with this function into the database.
/// The random material ID will fail FK constraints. For DB tests, use
/// `Sandbox::publish()` instead.
///
/// # Example
/// ```rust,ignore
/// use sinex_primitives::testing::event_fixture;
/// use serde_json::json;
///
/// let event = event_fixture("fs-watcher", "file.created", json!({
///     "path": "/test/file.txt",
///     "size": 1024
/// }));
/// ```
pub fn event_fixture(
    source: impl Into<crate::EventSource>,
    event_type: impl Into<crate::EventType>,
    payload: crate::JsonValue,
) -> crate::Event<crate::JsonValue> {
    use crate::events::SourceMaterial;
    use crate::{Event, HostName, Id, OffsetKind, Provenance, Timestamp, Ulid};
    use std::str::FromStr;

    // Use a constant test material ID
    let material_id = Ulid::from_str("01H00000000000000000000000").expect("valid constant ULID");

    Event {
        id: None,
        source: source.into(),
        event_type: event_type.into(),
        payload,
        ts_orig: Some(Timestamp::now()),
        host: HostName::new(gethostname::gethostname().to_string_lossy().to_string()),
        ingestor_version: Some("test".to_string()),
        payload_schema_id: None,
        provenance: Provenance::Material {
            id: Id::<SourceMaterial>::from_ulid(material_id),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: OffsetKind::Byte,
        },
        associated_blob_ids: None,
    }
}

#[cfg(feature = "proptest")]
pub mod strategies {
    //! Property testing strategies for domain types.

    use crate::{EventSource, EventType, Ulid};
    use proptest::prelude::*;

    /// Generate random event sources.
    pub fn event_source() -> impl Strategy<Value = EventSource> {
        "[a-z][a-z0-9-]{0,30}".prop_map(EventSource::new)
    }

    /// Generate random event types.
    pub fn event_type() -> impl Strategy<Value = EventType> {
        "[a-z][a-z0-9.-]{0,30}".prop_map(EventType::from)
    }

    /// Generate random ULIDs.
    pub fn ulid_strategy() -> impl Strategy<Value = Ulid> {
        any::<u128>().prop_map(|bits| Ulid::from_bytes(bits.to_be_bytes()).unwrap())
    }
}
