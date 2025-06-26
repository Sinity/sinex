use chrono::Utc;
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use sqlx::FromRow;
use crate::{JsonValue, OptionalTimestamp, Timestamp};

/// Raw event structure
/// 
/// This is the canonical event structure used throughout the system.
/// NOTE: This struct uses ULID directly. When using with SQLX queries,
/// use type overrides like: `id::uuid as "id: _"` for proper type inference
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct RawEvent {
    pub id: Ulid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: Timestamp,
    pub ts_orig: OptionalTimestamp,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Ulid>,
    pub payload: JsonValue,
}

impl RawEvent {
    /// Extract ingestion timestamp from ULID (convenience method)
    pub fn ts_ingest_from_ulid(&self) -> Timestamp {
        self.id.timestamp()
    }
}

/// Builder for creating RawEvent instances
pub struct RawEventBuilder {
    source: String,
    event_type: String,
    payload: JsonValue,
    ts_orig: OptionalTimestamp,
    host: Option<String>,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Ulid>,
}

impl RawEventBuilder {
    pub fn new(source: impl Into<String>, event_type: impl Into<String>, payload: JsonValue) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
            ts_orig: None,
            host: None,
            ingestor_version: None,
            payload_schema_id: None,
        }
    }

    pub fn with_orig_timestamp(mut self, ts: Timestamp) -> Self {
        self.ts_orig = Some(ts);
        self
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }

    pub fn with_payload_schema_id(mut self, id: Ulid) -> Self {
        self.payload_schema_id = Some(id);
        self
    }

    pub fn build(self) -> RawEvent {
        let id = Ulid::new();
        let hostname = self.host.unwrap_or_else(|| {
            gethostname::gethostname()
                .to_string_lossy()
                .to_string()
        });

        RawEvent {
            id,
            source: self.source,
            event_type: self.event_type,
            ts_ingest: Utc::now(),
            ts_orig: self.ts_orig,
            host: hostname,
            ingestor_version: self.ingestor_version,
            payload_schema_id: self.payload_schema_id,
            payload: self.payload,
        }
    }
}