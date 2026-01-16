use crate::db::models::event::{Event, SourceMaterial};
use crate::types::domain::{EventSource, EventType};
use crate::types::non_empty::NonEmptyVec;
use crate::types::Id;
use crate::types::Ulid;
use crate::SinexError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// Alias needed for Provenance
pub type EventId = Id<Event<serde_json::Value>>;

/// Builder for constructing Events with type safety
pub struct EventBuilder<T, P> {
    pub(crate) id: Option<Id<Event<T>>>,
    pub(crate) source: EventSource,
    pub(crate) event_type: EventType,
    pub(crate) payload: T,
    pub(crate) timestamp: Option<DateTime<Utc>>,
    pub(crate) hostname: Option<crate::types::domain::HostName>,
    pub(crate) ingestor_version: Option<String>,
    pub(crate) schema_id: Option<String>, // Wait, event.rs uses Option<Ulid> for payload_schema_id?
    // event.rs: pub payload_schema_id: Option<Ulid>,
    pub(crate) payload_schema_id: Option<Ulid>,
    pub(crate) provenance: Option<P>, // In builder, P is state marker. But we hold generic P?
    // Wait, reusing pattern from event.rs:
    // pub struct EventBuilder<T, P = NoProvenance> { ... provenance: Option<Provenance>, ... }
    pub(crate) provenance_data: Option<Provenance>,
    pub(crate) associated_blob_ids: Option<Vec<Ulid>>,
    pub(crate) _phantom: std::marker::PhantomData<P>,
}

// Typestate markers
pub struct NoProvenance;
pub struct HasProvenance;

impl<T> EventBuilder<T, NoProvenance> {
    // Logic copied/adapted from event.rs
    pub fn new(source: EventSource, event_type: EventType, payload: T) -> Self {
        Self {
            id: None,
            source,
            event_type,
            payload,
            timestamp: None,
            hostname: None,
            ingestor_version: None,
            schema_id: None,
            payload_schema_id: None,
            provenance: None,
            provenance_data: None,
            associated_blob_ids: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set hostname
    pub fn hostname(mut self, hostname: impl Into<crate::types::domain::HostName>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set ingestor version
    pub fn ingestor_version(mut self, version: impl Into<String>) -> Self {
        self.ingestor_version = Some(version.into());
        self
    }

    /// Set schema ID
    pub fn schema_id(mut self, schema_id: Ulid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }

    /// Add a related Blob ID
    pub fn add_blob_id(mut self, blob_id: Ulid) -> Self {
        let mut blobs = self.associated_blob_ids.unwrap_or_default();
        blobs.push(blob_id);
        self.associated_blob_ids = Some(blobs);
        self
    }

    /// With provenance
    pub fn with_provenance(self, provenance: Provenance) -> EventBuilder<T, HasProvenance> {
        EventBuilder {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,
            timestamp: self.timestamp,
            hostname: self.hostname,
            ingestor_version: self.ingestor_version,
            schema_id: self.schema_id,
            payload_schema_id: self.payload_schema_id,
            provenance: None,
            provenance_data: Some(provenance),
            associated_blob_ids: self.associated_blob_ids,
            _phantom: std::marker::PhantomData,
        }
    }

    // Convenience methods...
    pub fn from_material(
        self,
        material_id: impl Into<Id<SourceMaterial>>,
        anchor: i64,
    ) -> EventBuilder<T, HasProvenance> {
        self.with_provenance(Provenance::from_material(material_id, anchor, None, None))
    }

    pub fn from_parents<I>(self, parents: I) -> Result<EventBuilder<T, HasProvenance>, SinexError>
    where
        I: IntoIterator<Item = EventId>,
    {
        let mut iter = parents.into_iter();
        let first = iter.next().ok_or_else(|| {
            SinexError::validation("from_parents requires at least one parent ID")
        })?;
        let rest: Vec<EventId> = iter.collect();
        let provenance = Provenance::from_synthesis_safe(first, rest);
        Ok(self.with_provenance(provenance))
    }
}

impl<T> EventBuilder<T, HasProvenance> {
    pub fn at_time(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn with_offset_start(mut self, offset: i64) -> Result<Self, SinexError> {
        match self.provenance_data.as_mut() {
            Some(Provenance::Material { offset_start, .. }) => {
                *offset_start = Some(offset);
                Ok(self)
            }
            _ => Err(SinexError::invalid_state(
                "Offset setters require material provenance",
            )),
        }
    }

    pub fn with_offset_end(mut self, offset: i64) -> Result<Self, SinexError> {
        match self.provenance_data.as_mut() {
            Some(Provenance::Material { offset_end, .. }) => {
                *offset_end = Some(offset);
                Ok(self)
            }
            _ => Err(SinexError::invalid_state(
                "Offset setters require material provenance",
            )),
        }
    }

    pub fn with_offset_kind(mut self, kind: OffsetKind) -> Result<Self, SinexError> {
        match self.provenance_data.as_mut() {
            Some(Provenance::Material { offset_kind, .. }) => {
                *offset_kind = kind;
                Ok(self)
            }
            _ => Err(SinexError::invalid_state(
                "Offset setters require material provenance",
            )),
        }
    }

    pub fn with_associated_blobs(mut self, blobs: Vec<Ulid>) -> Self {
        self.associated_blob_ids = Some(blobs);
        self
    }

    pub fn build(self) -> Result<Event<T>, SinexError> {
        let provenance = self.provenance_data.ok_or_else(|| {
            SinexError::invalid_state("EventBuilder missing provenance when building")
        })?;

        // We need to construct Event.
        // But Event fields are private?
        // No, Event fields are public in event.rs (except id maybe?).
        // L40: pub id: Option<Id<Event<T>>>.
        // All fields are pub!

        Ok(Event {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,
            ts_orig: self.timestamp.or_else(|| Some(Utc::now())),
            host: super::event::get_hostname(), // Need to make get_hostname public? or copy logic?
            ingestor_version: super::event::get_ingestor_version(), // same
            payload_schema_id: self.payload_schema_id,
            provenance,
            associated_blob_ids: self.associated_blob_ids,
        })
    }
}

// Copied Provenance Types

/// Provenance information tracking the origin of an event
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Provenance {
    /// Event derived from source material (first-order event)
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        offset_start: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        offset_end: Option<i64>,
        offset_kind: OffsetKind,
    },
    /// Event derived from other events (synthesized event)  
    Synthesis {
        source_event_ids: NonEmptyVec<EventId>,
        operation_id: Option<Id<Operation>>,
    },
}

/// Type of offset measurement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OffsetKind {
    Byte,
    Line,
    Record,
    Character,
}

impl Default for OffsetKind {
    fn default() -> Self {
        Self::Byte
    }
}

/// Marker type for operation IDs  
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Operation;

impl Provenance {
    pub fn from_material(
        id: impl Into<Id<SourceMaterial>>,
        anchor_byte: i64,
        offset_start: Option<i64>,
        offset_end: Option<i64>,
    ) -> Self {
        Provenance::Material {
            id: id.into(),
            anchor_byte,
            offset_start,
            offset_end,
            offset_kind: OffsetKind::default(),
        }
    }

    pub fn from_synthesis<I: IntoIterator<Item = EventId>>(ids: I) -> Option<Self> {
        let vec: Vec<EventId> = ids.into_iter().collect();
        NonEmptyVec::from_vec(vec).map(|source_event_ids| Provenance::Synthesis {
            source_event_ids,
            operation_id: None,
        })
    }

    pub fn from_synthesis_safe(first: EventId, rest: Vec<EventId>) -> Self {
        Provenance::Synthesis {
            source_event_ids: NonEmptyVec::from_head_tail(first, rest),
            operation_id: None,
        }
    }
}
