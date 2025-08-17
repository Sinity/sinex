//! Typed event representation with compile-time type safety
//!
//! This module provides the `Event<T>` type which represents events with
//! strongly-typed payloads, enabling compile-time type safety for homogeneous
//! event processing while maintaining compatibility with `RawEvent` for
//! heterogeneous processing scenarios.

use crate::db::models::event::{EventId, OptionalTimestamp, SourceMaterial, Timestamp};
use crate::db::models::{Provenance, RawEvent};
use crate::types::domain::{EventSource, EventType, HostName};
use crate::types::events::EventPayload;
use crate::types::{Id, Ulid};
use crate::types_impl::non_empty::NonEmptyVec;
use crate::SinexError;
use serde::{Deserialize, Serialize};

/// A strongly-typed event with compile-time payload type safety
///
/// `Event<T>` provides the same structure as `RawEvent` but with a typed payload
/// of type `T` where `T: EventPayload`. This enables:
///
/// - Compile-time type safety for event processing
/// - Zero-cost abstractions (no runtime overhead for type checking)
/// - Automatic source and event_type derivation from the payload type
/// - Seamless conversion to/from `RawEvent` for mixed processing scenarios
///
/// # Example
/// ```ignore
/// use sinex_core::types::events::{Event, payloads::filesystem::FileCreatedPayload};
///
/// // Create a typed event
/// let payload = FileCreatedPayload {
///     path: "/home/user/document.txt".into(),
///     size: 1024,
///     // ...
/// };
/// let event = Event::new(payload);
///
/// // Convert to RawEvent for storage
/// let raw_event: RawEvent = event.into();
///
/// // Convert back from RawEvent (fallible)
/// let typed_event: Event<FileCreatedPayload> = Event::try_from(raw_event)?;
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event<T: EventPayload> {
    /// Event ID - None when creating, Some when from DB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Id<Event<T>>>,

    /// Event source (derived from T::SOURCE)
    pub source: EventSource,

    /// Event type (derived from T::EVENT_TYPE)
    pub event_type: EventType,

    /// Strongly-typed event payload
    pub payload: T,

    /// Original timestamp when the event occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts_orig: OptionalTimestamp,

    /// Hostname where the event was generated
    #[serde(default = "get_hostname")]
    pub host: HostName,

    /// Version of the ingestor that created this event
    pub ingestor_version: Option<String>,

    /// Schema ID for payload validation
    pub payload_schema_id: Option<Ulid>,

    /// Provenance tracking: either from events or source material
    pub provenance: Option<Provenance>,

    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

impl<T: EventPayload> Event<T> {
    /// Create a first-order typed event from Source Material
    ///
    /// This ensures the event has proper external provenance from the start.
    pub fn from_material(
        payload: T,
        material_id: impl Into<Id<SourceMaterial>>,
        anchor_byte: i64,
    ) -> Self {
        Self::new(payload).with_provenance(Provenance::from_material(
            material_id.into(),
            anchor_byte,
            None,
            None,
        ))
    }

    /// Create a synthesized typed event from other events
    ///
    /// This ensures the event has proper internal provenance from the start.
    pub fn from_events<I>(payload: T, parent_ids: I) -> Self
    where
        I: IntoIterator<Item = Id<RawEvent>>,
    {
        Self::new(payload).with_provenance(Provenance::from_synthesis(parent_ids).unwrap_or_else(
            || {
                panic!(
                    "from_synthesis requires at least one parent ID - this is a programming error"
                )
            },
        ))
    }
    /// Create a new typed event from a payload
    ///
    /// WARNING: This creates an event WITHOUT provenance, which violates the architecture!
    /// You MUST call `.with_provenance()` to set either Material or Synthesis provenance.
    ///
    /// Consider using `from_material()` or `from_events()` instead, which ensure
    /// proper provenance from the start.
    pub fn new(payload: T) -> Self {
        Self {
            id: None,
            source: T::SOURCE,
            event_type: T::EVENT_TYPE,
            payload,
            ts_orig: None,
            host: get_hostname(),
            ingestor_version: get_ingestor_version(),
            payload_schema_id: None,
            provenance: None,
            associated_blob_ids: None,
        }
    }

    /// Create a typed event from a payload with a specific timestamp
    pub fn with_timestamp(payload: T, ts_orig: Timestamp) -> Self {
        Self::new(payload).with_ts_orig(Some(ts_orig))
    }

    /// Builder pattern method to set timestamp origin
    pub fn with_ts_orig(mut self, ts: Option<Timestamp>) -> Self {
        self.ts_orig = ts;
        self
    }

    /// Builder pattern method to set provenance
    pub fn with_provenance(mut self, provenance: impl Into<Provenance>) -> Self {
        self.provenance = Some(provenance.into());
        self
    }

    /// Builder pattern method to set anchor byte
    // Note: anchor_byte is now part of Provenance::Material, not a separate field

    /// Builder pattern method to set associated blob IDs
    pub fn with_blob_ids(mut self, ids: Vec<Ulid>) -> Self {
        self.associated_blob_ids = Some(ids);
        self
    }

    /// Builder pattern method to set the host
    pub fn with_host(mut self, host: HostName) -> Self {
        self.host = host;
        self
    }

    /// Builder pattern method to set the schema ID
    pub fn with_schema_id(mut self, id: Ulid) -> Self {
        self.payload_schema_id = Some(id);
        self
    }
}

/// Conversion from typed Event<T> to RawEvent (infallible)
///
/// This serializes the typed payload to JSON for storage in the database.
impl<T: EventPayload> From<Event<T>> for RawEvent {
    fn from(typed: Event<T>) -> Self {
        // Serialize the typed payload to JSON
        let payload_json = serde_json::to_value(&typed.payload)
            .unwrap_or_else(|e| panic!("EventPayload serialization should never fail: {}", e));

        RawEvent {
            // Convert the ID type - this is safe because the underlying Ulid is the same
            id: typed.id.map(|id| Id::from_ulid(*id.as_ulid())),
            source: typed.source,
            event_type: typed.event_type,
            payload: payload_json,
            ts_orig: typed.ts_orig,
            host: typed.host,
            ingestor_version: typed.ingestor_version,
            payload_schema_id: typed.payload_schema_id,
            provenance: typed.provenance.unwrap_or_else(|| {
                // Create a dummy system bootstrap event ID for synthesis
                let system_bootstrap_id = EventId::from_ulid(
                    crate::types::Ulid::from_bytes([
                        0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    ])
                    .unwrap_or_else(|_| {
                        panic!("hardcoded ULID bytes should be valid - this is a programming error")
                    }),
                );
                Provenance::Synthesis {
                    source_event_ids: NonEmptyVec::single(system_bootstrap_id),
                    operation_id: None,
                }
            }),
            associated_blob_ids: typed.associated_blob_ids,
        }
    }
}

/// Conversion from RawEvent to typed Event<T> (fallible)
///
/// This deserializes the JSON payload to the typed representation.
/// Will fail if the payload cannot be deserialized to type T.
impl<T> TryFrom<RawEvent> for Event<T>
where
    T: EventPayload + serde::de::DeserializeOwned,
{
    type Error = SinexError;

    fn try_from(raw: RawEvent) -> Result<Self, Self::Error> {
        // Verify source and event_type match
        if raw.source != T::SOURCE {
            return Err(SinexError::serialization(format!(
                "Source mismatch: expected {}, got {}",
                T::SOURCE.as_str(),
                raw.source.as_str()
            )));
        }

        if raw.event_type != T::EVENT_TYPE {
            return Err(SinexError::serialization(format!(
                "Event type mismatch: expected {}, got {}",
                T::EVENT_TYPE.as_str(),
                raw.event_type.as_str()
            )));
        }

        // Deserialize the payload
        let payload: T = serde_json::from_value(raw.payload.clone()).map_err(|e| {
            SinexError::serialization(format!("Failed to deserialize payload: {}", e))
        })?;

        Ok(Event {
            // Convert the ID type - this is safe because the underlying Ulid is the same
            id: raw.id.map(|id| Id::from_ulid(*id.as_ulid())),
            source: raw.source,
            event_type: raw.event_type,
            payload,
            ts_orig: raw.ts_orig,
            host: raw.host,
            ingestor_version: raw.ingestor_version,
            payload_schema_id: raw.payload_schema_id,
            provenance: Some(raw.provenance),
            associated_blob_ids: raw.associated_blob_ids,
        })
    }
}

/// Helper function to get the hostname
fn get_hostname() -> HostName {
    HostName::new(
        gethostname::gethostname()
            .into_string()
            .unwrap_or_else(|_| "unknown".to_string()),
    )
}

/// Helper function to get the ingestor version
fn get_ingestor_version() -> Option<String> {
    option_env!("CARGO_PKG_VERSION").map(|v| v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::domain::SanitizedPath;
    use crate::types::events::payloads::filesystem::FileCreatedPayload;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_event_creation() -> color_eyre::eyre::Result<()> {
        let payload = FileCreatedPayload {
            path: SanitizedPath::new_unchecked("/test/file.txt"),
            size: 1024,
            created_at: chrono::Utc::now(),
            permissions: Some(0o644),
        };

        let event = Event::new(payload.clone());

        assert_eq!(event.source, FileCreatedPayload::SOURCE);
        assert_eq!(event.event_type, FileCreatedPayload::EVENT_TYPE);
        assert_eq!(event.payload, payload);
        assert!(event.id.is_none());
        Ok(())
    }

    #[sinex_test]
    fn test_event_to_raw_conversion() -> color_eyre::eyre::Result<()> {
        let payload = FileCreatedPayload {
            path: SanitizedPath::new_unchecked("/test/file.txt"),
            size: 1024,
            created_at: chrono::Utc::now(),
            permissions: Some(0o644),
        };

        let event = Event::new(payload.clone());
        let raw_event: RawEvent = event.into();

        assert_eq!(raw_event.source, FileCreatedPayload::SOURCE);
        assert_eq!(raw_event.event_type, FileCreatedPayload::EVENT_TYPE);

        // Verify payload is correctly serialized
        let payload_json = serde_json::to_value(&payload).unwrap();
        assert_eq!(raw_event.payload, payload_json);
        Ok(())
    }

    #[sinex_test]
    fn test_raw_to_event_conversion() -> color_eyre::eyre::Result<()> {
        let payload = FileCreatedPayload {
            path: SanitizedPath::new_unchecked("/test/file.txt"),
            size: 1024,
            created_at: chrono::Utc::now(),
            permissions: Some(0o644),
        };

        let event = Event::new(payload.clone());
        let raw_event: RawEvent = event.clone().into();
        let converted_event: Event<FileCreatedPayload> = Event::try_from(raw_event).unwrap();

        assert_eq!(converted_event.payload, payload);
        assert_eq!(converted_event.source, event.source);
        assert_eq!(converted_event.event_type, event.event_type);
        Ok(())
    }

    #[sinex_test]
    fn test_raw_to_event_conversion_type_mismatch() -> color_eyre::eyre::Result<()> {
        use crate::types::events::payloads::filesystem::FileDeletedPayload;

        let payload = FileCreatedPayload {
            path: SanitizedPath::new_unchecked("/test/file.txt"),
            size: 1024,
            created_at: chrono::Utc::now(),
            permissions: Some(0o644),
        };

        let event = Event::new(payload);
        let raw_event: RawEvent = event.into();

        // Try to convert to wrong type
        let result: Result<Event<FileDeletedPayload>, _> = Event::try_from(raw_event);
        assert!(result.is_err());
        Ok(())
    }

    #[sinex_test]
    fn test_builder_methods() -> color_eyre::eyre::Result<()> {
        let payload = FileCreatedPayload {
            path: SanitizedPath::new_unchecked("/test/file.txt"),
            size: 1024,
            created_at: chrono::Utc::now(),
            permissions: Some(0o644),
        };

        let ts = chrono::Utc::now();
        let schema_id = Ulid::new();
        let blob_id = Ulid::new();

        let event = Event::new(payload)
            .with_ts_orig(Some(ts))
            .with_schema_id(schema_id)
            .with_blob_ids(vec![blob_id]);

        assert_eq!(event.ts_orig, Some(ts));
        assert_eq!(event.payload_schema_id, Some(schema_id));
        assert_eq!(event.associated_blob_ids, Some(vec![blob_id]));
        Ok(())
    }
}
