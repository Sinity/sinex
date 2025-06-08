use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_ulid::Ulid;
use uuid::Uuid;

/// Core raw event structure used throughout the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub ts_ingest: DateTime<Utc>,
    pub ts_orig: Option<DateTime<Utc>>,
    pub host: String,
    pub ingestor_version: Option<String>,
    pub payload_schema_id: Option<Uuid>,
    pub payload: serde_json::Value,
}

impl RawEvent {
    /// Convert database UUID to ULID for application layer
    pub fn id_as_ulid(&self) -> Result<Ulid, sinex_ulid::Error> {
        Ulid::from_bytes(*self.id.as_bytes())
    }
    
    pub fn payload_schema_id_as_ulid(&self) -> Option<Result<Ulid, sinex_ulid::Error>> {
        self.payload_schema_id.map(|uuid| Ulid::from_bytes(*uuid.as_bytes()))
    }
}

/// Builder for creating RawEvent instances
pub struct RawEventBuilder {
    source: String,
    event_type: String,
    payload: serde_json::Value,
    ts_orig: Option<DateTime<Utc>>,
    host: Option<String>,
    ingestor_version: Option<String>,
    payload_schema_id: Option<Uuid>,
}

impl RawEventBuilder {
    pub fn new(source: impl Into<String>, event_type: impl Into<String>, payload: serde_json::Value) -> Self {
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

    pub fn with_orig_timestamp(mut self, ts: DateTime<Utc>) -> Self {
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

    pub fn with_payload_schema_id(mut self, id: Uuid) -> Self {
        self.payload_schema_id = Some(id);
        self
    }

    pub fn build(self) -> RawEvent {
        let id_ulid = Ulid::new();
        let hostname = self.host.unwrap_or_else(|| {
            gethostname::gethostname()
                .to_string_lossy()
                .to_string()
        });

        RawEvent {
            id: Uuid::from_bytes(id_ulid.to_bytes()),
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