//! Unified Event Type
//!
//! This module contains the unified Event struct that replaces the old
//! RawEvent/NewEvent dichotomy. An Event with id: None is a new event
//! to be inserted, while an Event with id: Some(...) is a persisted event.

use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;

// Type aliases for timestamp and JSON handling
pub type Timestamp = chrono::DateTime<chrono::Utc>;
pub type OptionalTimestamp = Option<chrono::DateTime<chrono::Utc>>;
pub type JsonValue = serde_json::Value;

/// Unified event structure for both creation and retrieval
///
/// This is the canonical event structure used throughout the system for both
/// raw observations and synthesized events. The distinction is made via the
/// source_event_ids field:
/// - Raw Event: source_event_ids is None or empty
/// - Synthesis Event: source_event_ids contains the source event IDs
///
/// The id field determines if this is a new event or a persisted one:
/// - id: None => New event to be created
/// - id: Some(id) => Event retrieved from database
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[builder(on(String, into))] // Convert &str to String automatically
pub struct Event {
    /// Event ID - None when creating, Some when from DB
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Ulid>,
    
    /// Event source (e.g., "fs-watcher", "terminal")
    pub source: String,
    
    /// Event type (e.g., "file.created", "command.executed")
    pub event_type: String,
    
    /// Event payload as JSON
    pub payload: JsonValue,
    
    /// Ingestion timestamp - set by database
    #[builder(default = chrono::Utc::now())]
    pub ts_ingest: Timestamp,
    
    /// Original timestamp when the event occurred
    pub ts_orig: OptionalTimestamp,
    
    /// Hostname where the event was generated
    #[builder(default = get_hostname())]
    pub host: String,
    
    /// Version of the ingestor that created this event
    pub ingestor_version: Option<String>,
    
    /// Schema ID for payload validation
    pub payload_schema_id: Option<Ulid>,
    
    /// Provenance field for event synthesis
    /// - None/empty: This is a raw event from an ingestor
    /// - Some(vec): This is a synthesis event derived from the listed events
    pub source_event_ids: Option<Vec<Ulid>>,
    
    /// External source material reference
    pub source_material_id: Option<Ulid>,
    
    pub source_material_offset_start: Option<i64>,
    
    pub source_material_offset_end: Option<i64>,
    
    /// Immutable anchor byte offset within source material
    pub anchor_byte: Option<i64>,
    
    /// Array of associated blob IDs (screenshots, recordings, etc.)
    pub associated_blob_ids: Option<Vec<Ulid>>,
}

impl Event {
    /// Check if this event has been persisted to the database
    pub fn is_persisted(&self) -> bool {
        self.id.is_some()
    }
    
    /// Check if this is a raw event (no source events)
    pub fn is_raw_event(&self) -> bool {
        self.source_event_ids.as_ref().map_or(true, |ids| ids.is_empty())
    }
    
    /// Check if this is a synthesis event (has source events)
    pub fn is_synthesis_event(&self) -> bool {
        self.source_event_ids.as_ref().map_or(false, |ids| !ids.is_empty())
    }
    
    /// Get the source event IDs if this is a synthesis event
    pub fn get_source_event_ids(&self) -> Option<&[Ulid]> {
        self.source_event_ids.as_deref()
    }
    
    /// Extract ingestion timestamp from ULID if persisted
    pub fn ts_ingest_from_ulid(&self) -> Option<Timestamp> {
        self.id.map(|id| id.timestamp())
    }
    
    /// Simple constructor for the most common use case
    pub fn simple(source: impl Into<String>, event_type: impl Into<String>, payload: JsonValue) -> Self {
        Event::builder()
            .source(source)
            .event_type(event_type)
            .payload(payload)
            .ts_ingest(chrono::Utc::now())
            .ts_orig(None)
            .host(get_hostname())
            .build()
    }
}

// Helper function to get hostname
fn get_hostname() -> String {
    gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "unknown".to_string())
}

// Implement sqlx traits for database compatibility
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::*;
    use sqlx::postgres::PgRow;
    use sqlx::{FromRow, Row};
    
    impl<'r> FromRow<'r, PgRow> for Event {
        fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
            Ok(Event {
                id: Some(row.try_get::<uuid::Uuid, _>("id")?.into()),
                source: row.try_get("source")?,
                event_type: row.try_get("event_type")?,
                ts_ingest: row.try_get("ts_ingest")?,
                ts_orig: row.try_get("ts_orig")?,
                host: row.try_get("host")?,
                ingestor_version: row.try_get("ingestor_version")?,
                payload_schema_id: row.try_get::<Option<uuid::Uuid>, _>("payload_schema_id")?
                    .map(|uuid| Ulid::from(uuid)),
                payload: row.try_get("payload")?,
                source_event_ids: row.try_get::<Option<Vec<uuid::Uuid>>, _>("source_event_ids")?
                    .map(|uuids| uuids.into_iter().map(|uuid| Ulid::from(uuid)).collect()),
                source_material_id: row.try_get::<Option<uuid::Uuid>, _>("source_material_id")?
                    .map(|uuid| Ulid::from(uuid)),
                source_material_offset_start: row.try_get("source_material_offset_start")?,
                source_material_offset_end: row.try_get("source_material_offset_end")?,
                anchor_byte: row.try_get("anchor_byte")?,
                associated_blob_ids: row.try_get::<Option<Vec<uuid::Uuid>>, _>("associated_blob_ids")?
                    .map(|uuids| uuids.into_iter().map(|uuid| Ulid::from(uuid)).collect()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_event_builder() {
        let event = Event::builder()
            .source("test")
            .event_type("test.created")
            .payload(json!({"message": "hello"}))
            .ts_ingest(chrono::Utc::now())
            .ts_orig(None)
            .host("test-host".to_string())
            .build();
            
        assert_eq!(event.source, "test");
        assert_eq!(event.event_type, "test.created");
        assert!(event.id.is_none());
        assert!(event.is_raw_event());
        assert!(!event.is_persisted());
    }
    
    #[test]
    fn test_simple_constructor() {
        let event = Event::simple("test", "test.created", json!({"message": "hello"}));
        
        assert_eq!(event.source, "test");
        assert_eq!(event.event_type, "test.created");
        assert!(event.id.is_none());
    }
    
    #[test]
    fn test_synthesis_event() {
        let source_ids = vec![Ulid::new(), Ulid::new()];
        let event = Event::builder()
            .source("processor")
            .event_type("analysis.completed")
            .payload(json!({"result": "success"}))
            .source_event_ids(source_ids.clone())
            .ts_ingest(chrono::Utc::now())
            .ts_orig(None)
            .host("test-host".to_string())
            .build();
            
        assert!(event.is_synthesis_event());
        assert!(!event.is_raw_event());
        assert_eq!(event.get_source_event_ids().unwrap(), &source_ids);
    }
}