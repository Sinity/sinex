use super::Timestamp;
use super::{Event, SourceMaterial};
use crate::domain::{EventSource, EventType};
use crate::error::{Result, SinexError};
use crate::ids::Id;
use crate::non_empty::NonEmptyVec;
use crate::primitives::Uuid;
use serde::{Deserialize, Serialize};

// Alias needed for Provenance
pub type EventId = Id<Event<serde_json::Value>>;

/// Builder for constructing Events with type safety.
///
/// Uses typestate pattern: `EventBuilder<T, NoProvenance>` transitions to
/// `EventBuilder<T, HasProvenance>` when provenance is set. Only
/// `HasProvenance` exposes `.build()`.
pub struct EventBuilder<T, P> {
    pub(crate) id: Option<Id<Event<T>>>,
    pub(crate) source: EventSource,
    pub(crate) event_type: EventType,
    pub(crate) payload: T,
    pub(crate) timestamp: Option<Timestamp>,
    pub(crate) hostname: Option<crate::domain::HostName>,
    pub(crate) node_version: Option<String>,
    pub(crate) payload_schema_id: Option<Uuid>,
    pub(crate) provenance_data: Option<Provenance>,
    pub(crate) associated_blob_ids: Option<Vec<Uuid>>,
    pub(crate) _state: std::marker::PhantomData<P>,
}

// Typestate markers
pub struct NoProvenance;
pub struct HasProvenance;

impl<T> EventBuilder<T, NoProvenance> {
    /// Internal constructor - use `Event::builder(payload)` for typed payloads
    /// or `DynamicPayload::new(...).into_builder()` for `JsonValue`.
    pub fn new_internal(source: EventSource, event_type: EventType, payload: T) -> Self {
        Self {
            id: None,
            source,
            event_type,
            payload,
            timestamp: None,
            hostname: None,
            node_version: None,
            payload_schema_id: None,
            provenance_data: None,
            associated_blob_ids: None,
            _state: std::marker::PhantomData,
        }
    }
}

impl<T> EventBuilder<T, NoProvenance> {
    /// Set hostname
    pub fn hostname(mut self, hostname: impl Into<crate::domain::HostName>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set node version
    pub fn node_version(mut self, version: impl Into<String>) -> Self {
        self.node_version = Some(version.into());
        self
    }

    /// Set schema ID
    pub fn schema_id(mut self, schema_id: Uuid) -> Self {
        self.payload_schema_id = Some(schema_id);
        self
    }

    /// Add a related Blob ID
    pub fn add_blob_id(mut self, blob_id: Uuid) -> Self {
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
            node_version: self.node_version,
            payload_schema_id: self.payload_schema_id,
            provenance_data: Some(provenance),
            associated_blob_ids: self.associated_blob_ids,
            _state: std::marker::PhantomData,
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

    pub fn from_parents<I>(self, parents: I) -> Result<EventBuilder<T, HasProvenance>>
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
    pub fn at_time(mut self, ts: Timestamp) -> Self {
        self.timestamp = Some(ts);
        self
    }

    pub fn with_offset_start(mut self, offset: i64) -> Result<Self> {
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

    pub fn with_offset_end(mut self, offset: i64) -> Result<Self> {
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

    pub fn with_offset_kind(mut self, kind: OffsetKind) -> Result<Self> {
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

    pub fn with_associated_blobs(mut self, blobs: Vec<Uuid>) -> Self {
        self.associated_blob_ids = Some(blobs);
        self
    }

    pub fn build(self) -> Result<Event<T>> {
        let provenance = self.provenance_data.ok_or_else(|| {
            SinexError::invalid_state("EventBuilder missing provenance when building")
        })?;

        Ok(Event {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,
            ts_orig: self.timestamp.or_else(|| Some(Timestamp::now())),
            host: self.hostname.unwrap_or_else(get_hostname),
            node_version: self.node_version.or_else(get_node_version),
            payload_schema_id: self.payload_schema_id,
            provenance,
            associated_blob_ids: self.associated_blob_ids,
        })
    }
}

// Provenance types with flat wire-format serialization.

/// Provenance information tracking the origin of an event
///
/// Serializes to flat fields for the NATS wire format:
/// - Material: `{"source_material_id": "...", "anchor_byte": 0, ...}`
/// - Synthesis: `{"source_event_ids": ["...", "..."]}`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Provenance {
    /// Event derived from source material (first-order event)
    Material {
        id: Id<SourceMaterial>,
        anchor_byte: i64,
        offset_start: Option<i64>,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum OffsetKind {
    #[default]
    Byte,
    Line,
    Record,
    Character,
}

impl OffsetKind {
    /// Convert to wire format string
    #[must_use]
    pub fn as_wire_str(&self) -> &'static str {
        match self {
            OffsetKind::Byte => "byte",
            OffsetKind::Line => "line",
            OffsetKind::Record => "rowid",
            OffsetKind::Character => "logical",
        }
    }

    /// Parse from wire format string
    #[must_use]
    pub fn from_wire_str(s: &str) -> Self {
        match s {
            "byte" => OffsetKind::Byte,
            "line" => OffsetKind::Line,
            "rowid" => OffsetKind::Record,
            "logical" => OffsetKind::Character,
            _ => OffsetKind::Byte, // default fallback
        }
    }
}

impl Serialize for OffsetKind {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_wire_str())
    }
}

impl<'de> Deserialize<'de> for OffsetKind {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(OffsetKind::from_wire_str(&s))
    }
}

/// Flat wire format for provenance serialization
#[derive(Serialize, Deserialize)]
struct ProvenanceWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    source_material_id: Option<Id<SourceMaterial>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor_byte: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset_start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset_end: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset_kind: Option<OffsetKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_event_ids: Option<Vec<EventId>>,
}

impl Serialize for Provenance {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let wire = match self {
            Provenance::Material {
                id,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind,
            } => ProvenanceWire {
                source_material_id: Some(*id),
                anchor_byte: Some(*anchor_byte),
                offset_start: *offset_start,
                offset_end: *offset_end,
                offset_kind: if offset_start.is_some() && offset_end.is_some() {
                    Some(*offset_kind)
                } else {
                    None
                },
                source_event_ids: None,
            },
            Provenance::Synthesis {
                source_event_ids, ..
            } => ProvenanceWire {
                source_material_id: None,
                anchor_byte: None,
                offset_start: None,
                offset_end: None,
                offset_kind: None,
                source_event_ids: Some(source_event_ids.clone().into_vec()),
            },
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Provenance {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ProvenanceWire::deserialize(deserializer)?;

        match (wire.source_material_id, wire.source_event_ids) {
            (Some(id), None) => Ok(Provenance::Material {
                id,
                anchor_byte: wire.anchor_byte.unwrap_or(0),
                offset_start: wire.offset_start,
                offset_end: wire.offset_end,
                offset_kind: wire.offset_kind.unwrap_or_default(),
            }),
            (None, Some(ids)) => {
                let source_event_ids = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    serde::de::Error::custom("source_event_ids cannot be empty for Synthesis")
                })?;
                Ok(Provenance::Synthesis {
                    source_event_ids,
                    operation_id: None,
                })
            }
            (Some(_), Some(_)) => Err(serde::de::Error::custom(
                "cannot have both source_material_id and source_event_ids",
            )),
            (None, None) => Err(serde::de::Error::custom(
                "must have either source_material_id or source_event_ids",
            )),
        }
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

    #[must_use]
    pub fn from_synthesis_safe(first: EventId, rest: Vec<EventId>) -> Self {
        Provenance::Synthesis {
            source_event_ids: NonEmptyVec::from_head_tail(first, rest),
            operation_id: None,
        }
    }
}

// Helper function to get hostname (needed by builder)
#[must_use]
pub fn get_hostname() -> crate::domain::HostName {
    crate::domain::HostName::new(gethostname::gethostname().to_string_lossy().to_string())
}

// Helper function to get node version
#[must_use]
pub fn get_node_version() -> Option<String> {
    // Priority: compile-time git revision > None
    match option_env!("SINEX_GIT_REV") {
        Some(git_rev) if !git_rev.is_empty() && git_rev != "unknown" => {
            // Format: git-<short-rev> (e.g., "git-a1b2c3d")
            Some(format!("git-{git_rev}"))
        }
        _ => None,
    }
}
