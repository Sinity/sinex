//! EventPayload trait for strongly-typed event payloads

use crate::domain::{EventSource, EventType};
use crate::error::SinexError;
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

    /// Try to deserialize from a legacy version
    ///
    /// This method is called when deserializing events with older schema versions.
    /// The default implementation attempts direct deserialization, which works
    /// when new fields are optional.
    ///
    /// Override this method to handle breaking changes or provide custom migration logic.
    ///
    /// # Example
    /// ```ignore
    /// fn try_from_legacy(value: serde_json::Value, version: &str) -> Result<Self, SinexError>
    /// where
    ///     Self: Sized + DeserializeOwned
    /// {
    ///     match version {
    ///         "1.0.0" => {
    ///             let v1: FileCreatedPayloadV1 = serde_json::from_value(value)
    ///                 .map_err(|e| SinexError::serialization(format!("Failed to deserialize v1: {}", e)))?;
    ///             Ok(Self::from(v1))
    ///         }
    ///         _ => serde_json::from_value(value)
    ///             .map_err(|e| SinexError::serialization(format!("Failed to deserialize {}: {}", version, e))),
    ///     }
    /// }
    /// ```
    fn try_from_legacy(value: serde_json::Value, version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        // Default: attempt direct deserialization
        // This works when changes are backward-compatible (e.g., new optional fields)
        let _ = version; // Unused in default implementation
        serde_json::from_value(value)
            .map_err(|e| SinexError::serialization(format!("Failed to deserialize {version}: {e}")))
    }
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

    fn try_from_legacy(value: serde_json::Value, _version: &str) -> Result<Self, SinexError>
    where
        Self: Sized + serde::de::DeserializeOwned,
    {
        Ok(value)
    }
}
