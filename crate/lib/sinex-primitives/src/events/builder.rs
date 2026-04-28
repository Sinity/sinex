use super::Timestamp;
use super::{Event, SourceMaterial};
use crate::domain::{EventSource, EventType, HostName};
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
    pub(crate) node_run_id: Option<Uuid>,
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
            node_run_id: None,
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

    /// Set the node run ID (references `core.node_runs`)
    pub fn node_run_id(mut self, run_id: Uuid) -> Self {
        self.node_run_id = Some(run_id);
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
            node_run_id: self.node_run_id,
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

    /// Set operation ID on both the provenance (if Synthesis) and the event-level
    /// `created_by_operation_id` field.
    pub fn with_operation(mut self, operation_id: Id<Operation>) -> Self {
        if let Some(Provenance::Synthesis {
            operation_id: ref mut op_id,
            ..
        }) = self.provenance_data
        {
            *op_id = Some(operation_id);
        }
        self
    }

    pub fn build(self) -> Result<Event<T>> {
        let provenance = self.provenance_data.ok_or_else(|| {
            SinexError::invalid_state("EventBuilder missing provenance when building")
        })?;

        // Enforce the same offset invariants as Deserialize: offsets must be
        // either all-present (start + end + kind) or all-absent.
        if let Provenance::Material {
            offset_start,
            offset_end,
            offset_kind,
            ..
        } = &provenance
        {
            match (offset_start, offset_end) {
                (Some(_), None) | (None, Some(_)) => {
                    return Err(SinexError::invalid_state(
                        "Material provenance offsets must include both offset_start and offset_end",
                    ));
                }
                (None, None) if *offset_kind != OffsetKind::Byte => {
                    // A non-default offset_kind without offsets is invalid —
                    // OffsetKind::Byte is the sentinel "no offsets" value.
                    return Err(SinexError::invalid_state(
                        "Material provenance offset_kind requires both offset_start and offset_end",
                    ));
                }
                _ => {}
            }
        }

        // Auto-sync: synthesis operation lineage lives in the dedicated DB
        // column, so keep the event-level field aligned with provenance.
        let created_by_operation_id = provenance.operation_uuid();

        Ok(Event {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,
            ts_orig: self.timestamp.or_else(|| Some(Timestamp::now())),
            host: self.hostname.unwrap_or_else(get_hostname),
            node_run_id: self.node_run_id,
            payload_schema_id: self.payload_schema_id,
            provenance,
            associated_blob_ids: self.associated_blob_ids,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id,
            node_model: None,
        })
    }
}

// Provenance types with flat wire-format serialization.

/// Provenance information tracking the origin of an event.
///
/// Serializes to flat fields for the NATS wire format:
/// - Material: `{"source_material_id": "...", "anchor_byte": 0, ...}`
/// - Synthesis: `{"source_event_ids": ["...", "..."]}`
///
/// **Construct via [`Provenance::from_material`] or
/// [`Provenance::from_synthesis`]**, not via struct literals. The variant
/// fields are `pub` for pattern matching but raw struct construction
/// bypasses the [`EventBuilder`] typestate guarantees that the
/// architecture relies on (see issue #559). The XOR provenance check at
/// the database boundary still catches invalid shapes, but earlier surface
/// area is preferable.
#[derive(Debug, Clone, PartialEq)]
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
    pub fn try_from_wire_str(s: &str) -> Result<Self> {
        match s {
            "byte" => Ok(OffsetKind::Byte),
            "line" => Ok(OffsetKind::Line),
            "rowid" => Ok(OffsetKind::Record),
            "logical" => Ok(OffsetKind::Character),
            _ => Err(SinexError::validation("invalid offset kind")
                .with_context("offset_kind", s.to_string())),
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
        OffsetKind::try_from_wire_str(&s).map_err(serde::de::Error::custom)
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
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<Id<Operation>>,
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
                operation_id: None,
            },
            Provenance::Synthesis {
                source_event_ids,
                operation_id,
            } => ProvenanceWire {
                source_material_id: None,
                anchor_byte: None,
                offset_start: None,
                offset_end: None,
                offset_kind: None,
                source_event_ids: Some(source_event_ids.clone().into_vec()),
                operation_id: *operation_id,
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
            (Some(id), None) => {
                let anchor_byte = wire.anchor_byte.ok_or_else(|| {
                    serde::de::Error::custom("material provenance missing anchor_byte")
                })?;
                let offset_kind = match (wire.offset_start, wire.offset_end, wire.offset_kind) {
                    (None, None, None) => OffsetKind::Byte,
                    (Some(offset_start), Some(offset_end), Some(offset_kind)) => {
                        return Ok(Provenance::Material {
                            id,
                            anchor_byte,
                            offset_start: Some(offset_start),
                            offset_end: Some(offset_end),
                            offset_kind,
                        });
                    }
                    (Some(_), Some(_), None) => {
                        return Err(serde::de::Error::custom(
                            "material provenance offsets require offset_kind",
                        ));
                    }
                    (None, None, Some(_)) => {
                        return Err(serde::de::Error::custom(
                            "material provenance offset_kind requires offsets",
                        ));
                    }
                    _ => {
                        return Err(serde::de::Error::custom(
                            "material provenance offsets must include both offset_start and offset_end",
                        ));
                    }
                };

                Ok(Provenance::Material {
                    id,
                    anchor_byte,
                    offset_start: None,
                    offset_end: None,
                    offset_kind,
                })
            }
            (None, Some(ids)) => {
                let source_event_ids = NonEmptyVec::from_vec(ids).ok_or_else(|| {
                    serde::de::Error::custom("source_event_ids cannot be empty for Synthesis")
                })?;
                Ok(Provenance::Synthesis {
                    source_event_ids,
                    operation_id: wire.operation_id,
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
    pub(crate) fn from_synthesis_safe(first: EventId, rest: Vec<EventId>) -> Self {
        Provenance::Synthesis {
            source_event_ids: NonEmptyVec::from_head_tail(first, rest),
            operation_id: None,
        }
    }

    /// Set the operation ID on Synthesis provenance.
    /// No-op on Material provenance.
    #[must_use]
    pub fn with_operation(mut self, op_id: Id<Operation>) -> Self {
        if let Provenance::Synthesis {
            ref mut operation_id,
            ..
        } = self
        {
            *operation_id = Some(op_id);
        }
        self
    }

    /// Get the operation ID if this is Synthesis provenance.
    #[must_use]
    pub fn operation_id(&self) -> Option<Id<Operation>> {
        match self {
            Provenance::Synthesis { operation_id, .. } => *operation_id,
            Provenance::Material { .. } => None,
        }
    }

    /// Get the operation UUID used by event persistence, if any.
    #[must_use]
    pub fn operation_uuid(&self) -> Option<Uuid> {
        self.operation_id().map(|id| id.to_uuid())
    }
}

/// Cached stable host identity.
///
/// Prefers `/etc/machine-id` (a stable UUID assigned at OS provision time) over
/// `gethostname()` (mutable, ephemeral). Falls back to the hostname if the
/// machine-id file is absent or unreadable. If both sources are present but
/// invalid as hostnames, derive a deterministic fallback from their raw bytes
/// instead of collapsing multiple hosts to the same fabricated value. The value
/// is computed once and reused for the lifetime of the process.
static HOST_IDENTITY: std::sync::LazyLock<HostName> = std::sync::LazyLock::new(|| {
    resolve_host_identity(
        std::fs::read_to_string("/etc/machine-id").ok().as_deref(),
        Some(gethostname::gethostname().to_string_lossy().as_ref()),
    )
});

fn resolve_host_identity(machine_id: Option<&str>, hostname: Option<&str>) -> HostName {
    let machine_id = machine_id
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty());
    let hostname = hostname
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty());

    if let Some(candidate) = machine_id.and_then(validated_host_candidate) {
        return candidate;
    }
    if let Some(candidate) = hostname.and_then(validated_host_candidate) {
        return candidate;
    }
    if let Some(candidate) = machine_id {
        return derived_host_fallback(candidate);
    }
    if let Some(candidate) = hostname {
        return derived_host_fallback(candidate);
    }
    HostName::from_static("unknown-host")
}

fn validated_host_candidate(candidate: &str) -> Option<HostName> {
    HostName::new(candidate.to_owned()).ok()
}

fn derived_host_fallback(candidate: &str) -> HostName {
    let digest = blake3::hash(candidate.as_bytes()).to_hex();
    HostName::new(format!("host-{}", &digest[..16]))
        .unwrap_or_else(|_| HostName::from_static("unknown-host"))
}

// Helper function to get hostname (needed by builder)
#[must_use]
pub fn get_hostname() -> HostName {
    HOST_IDENTITY.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Inline because these exercise private host-identity resolution helpers directly.
    #[test]
    fn resolve_host_identity_prefers_valid_machine_id() {
        let host = resolve_host_identity(Some("0123456789abcdef"), Some("sinnix-prime"));
        assert_eq!(host.as_str(), "0123456789abcdef");
    }

    // Inline because these exercise private host-identity resolution helpers directly.
    #[test]
    fn resolve_host_identity_falls_back_to_valid_hostname() {
        let host = resolve_host_identity(Some("bad machine id"), Some("sinnix-prime"));
        assert_eq!(host.as_str(), "sinnix-prime");
    }

    // Inline because these exercise private host-identity resolution helpers directly.
    #[test]
    fn resolve_host_identity_derives_deterministic_fallback_from_invalid_inputs() {
        let host = resolve_host_identity(Some("bad machine id"), Some("bad host"));
        assert_eq!(host.as_str(), "host-887759893f18d0bb");
    }

    // Inline because these exercise private host-identity resolution helpers directly.
    #[test]
    fn resolve_host_identity_uses_unknown_host_only_when_no_identity_material_exists() {
        let host = resolve_host_identity(None, Some("   "));
        assert_eq!(host.as_str(), "unknown-host");
    }
}
