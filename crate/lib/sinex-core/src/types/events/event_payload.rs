//! EventPayload trait for strongly-typed event payloads

use crate::domain::{EventSource, EventType};
use schemars::JsonSchema;
use serde::Serialize;

/// Trait for strongly-typed event payloads
///
/// Each payload type serves as the single source of truth for:
/// - Which source generates this event type (SOURCE)
/// - What the event type is (EVENT_TYPE)
/// - The schema version (VERSION)
///
/// Schema name is derived as "{SOURCE}.{EVENT_TYPE}"
/// Schema versioning is handled by the VERSION constant
pub trait EventPayload: Serialize + JsonSchema + Send + Sync + 'static {
    /// The source that generates this type of event
    const SOURCE: EventSource;

    /// The event type identifier
    const EVENT_TYPE: EventType;

    /// The schema version (semantic versioning)
    const VERSION: &'static str;
}

// Re-export common types used with payloads
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;
pub type JsonValue = serde_json::Value;

// Special implementation for JsonValue to support heterogeneous event processing
impl EventPayload for serde_json::Value {
    const SOURCE: EventSource = EventSource::from_static("system");
    const EVENT_TYPE: EventType = EventType::from_static("generic");
    const VERSION: &'static str = "1.0.0";

    // JsonValue payloads are version-neutral.
}
