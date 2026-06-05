use super::Timestamp;
use super::{Event, SourceMaterial};
use crate::domain::{EventSource, EventType, HostName, TemporalSourceType};
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
    pub(crate) ts_quality: Option<TemporalSourceType>,
    pub(crate) hostname: Option<crate::domain::HostName>,
    pub(crate) module_run_id: Option<Uuid>,
    pub(crate) payload_schema_id: Option<Uuid>,
    pub(crate) provenance_data: Option<Provenance>,
    pub(crate) associated_blob_ids: Option<Vec<Uuid>>,
    pub(crate) anchor_payload_hash: Option<Vec<u8>>,
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
            ts_quality: None,
            hostname: None,
            module_run_id: None,
            payload_schema_id: None,
            provenance_data: None,
            associated_blob_ids: None,
            anchor_payload_hash: None,
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

    /// Set the runtime module run ID (references `core.runs`)
    pub fn module_run_id(mut self, run_id: Uuid) -> Self {
        self.module_run_id = Some(run_id);
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
            ts_quality: self.ts_quality,
            hostname: self.hostname,
            module_run_id: self.module_run_id,
            payload_schema_id: self.payload_schema_id,
            provenance_data: Some(provenance),
            associated_blob_ids: self.associated_blob_ids,
            anchor_payload_hash: self.anchor_payload_hash,
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
        let provenance = Provenance::from_derived(parents).ok_or_else(|| {
            SinexError::validation("from_parents requires at least one parent ID")
        })?;
        Ok(self.with_provenance(provenance))
    }
}

impl<T> EventBuilder<T, HasProvenance> {
    pub fn at_time(mut self, ts: Timestamp) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Set an explicit `ts_orig` together with its quality rung on the temporal
    /// ladder (#1570 Prong B). Use this when the caller already knows the
    /// timestamp's provenance — e.g. a parser that resolved a `#[timestamp]`
    /// field (`IntrinsicContent`) or a realtime monitor capturing live data
    /// (`RealtimeCapture`).
    #[must_use]
    pub fn at_time_with_quality(mut self, ts: Timestamp, quality: TemporalSourceType) -> Self {
        self.timestamp = Some(ts);
        self.ts_quality = Some(quality);
        self
    }

    /// Record the quality rung for `ts_orig` without changing the timestamp.
    #[must_use]
    pub fn ts_quality(mut self, quality: TemporalSourceType) -> Self {
        self.ts_quality = Some(quality);
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

    /// Stamp a precomputed 32-byte BLAKE3 hash on the event.
    ///
    /// Per #1447 the field is only meaningful on material-provenance events —
    /// derived events derive their identity from parent event ids. Setting a
    /// hash on a derived-provenance builder is silently dropped at `build()`
    /// time to keep the on-the-wire shape honest. Use
    /// [`Self::with_anchor_payload_from_bytes`] when you have the payload bytes
    /// and want the BLAKE3 computed for you.
    #[must_use]
    pub fn with_anchor_payload_hash(mut self, hash: [u8; 32]) -> Self {
        self.anchor_payload_hash = Some(hash.to_vec());
        self
    }

    /// Compute and stamp the 32-byte BLAKE3 hash of the supplied source-material
    /// byte range. Only meaningful on material-provenance events (see
    /// [`Self::with_anchor_payload_hash`]).
    #[must_use]
    pub fn with_anchor_payload_from_bytes(self, bytes: &[u8]) -> Self {
        let hash = blake3::hash(bytes);
        self.with_anchor_payload_hash(*hash.as_bytes())
    }

    /// Set operation ID on both the provenance (if Derived) and the event-level
    /// `created_by_operation_id` field.
    pub fn with_operation(mut self, operation_id: Id<OperationMarker>) -> Self {
        if let Some(Provenance::Derived {
            operation_id: ref mut op_id,
            ..
        }) = self.provenance_data
        {
            *op_id = Some(operation_id);
        }
        self
    }

    pub fn build(self) -> Result<Event<T>> {
        let provenance = self
            .provenance_data
            .ok_or_else(|| {
                SinexError::invalid_state("EventBuilder missing provenance when building")
            })?
            .into_canonical();

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

        // Auto-sync: derived operation lineage lives in the dedicated DB
        // column, so keep the event-level field aligned with provenance.
        let created_by_operation_id = provenance.operation_uuid();

        // anchor_payload_hash is only meaningful on material provenance; the
        // verify path keys off (source_material_id, anchor_byte, len) and
        // derived events derive identity from parent event ids. Drop any
        // hash supplied to a derived builder to keep on-the-wire honest.
        let anchor_payload_hash = match &provenance {
            Provenance::Material { .. } => self.anchor_payload_hash,
            Provenance::Derived { .. } => None,
        };

        // #1570 Prong B — builder inversion:
        // Material-provenance events without an explicit timestamp leave
        // `ts_orig = None` as the "derive me at persistence" signal; the event_engine
        // admission stage resolves it from the source-material timing tier.
        // Derived events have no material to resolve against, so they keep the
        // wall-clock fallback (their synthesis time). Any caller that set an
        // explicit time via `at_time`/`at_time_with_quality` keeps that value
        // regardless of provenance.
        let ts_orig = match (&provenance, self.timestamp) {
            (_, Some(ts)) => Some(ts),
            (Provenance::Material { .. }, None) => None,
            (Provenance::Derived { .. }, None) => Some(Timestamp::now()),
        };

        Ok(Event {
            id: self.id,
            source: self.source,
            event_type: self.event_type,
            payload: self.payload,
            ts_orig,
            ts_quality: self.ts_quality,
            host: self.hostname.unwrap_or_else(get_hostname),
            module_run_id: self.module_run_id,
            payload_schema_id: self.payload_schema_id,
            provenance,
            anchor_payload_hash,
            associated_blob_ids: self.associated_blob_ids,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id,
            automaton_model: None,
        })
    }
}

// Provenance types with flat wire-format serialization.

/// Provenance information tracking the origin of an event.
///
/// Serializes to flat fields for the NATS wire format:
/// - Material: `{"source_material_id": "...", "anchor_byte": 0, ...}`
/// - Derived: `{"source_event_ids": ["...", "..."]}`
///
/// **Construct via [`Provenance::from_material`] or
/// [`Provenance::from_derived`]**, not via struct literals. The variant
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
    Derived {
        source_event_ids: NonEmptyVec<EventId>,
        operation_id: Option<Id<OperationMarker>>,
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
    operation_id: Option<Id<OperationMarker>>,
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
            Provenance::Derived {
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
                let source_event_ids = canonicalize_source_event_ids(ids).ok_or_else(|| {
                    serde::de::Error::custom("source_event_ids cannot be empty for Derived")
                })?;
                Ok(Provenance::Derived {
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

/// Marker type for operation IDs used in `Id<OperationMarker>`.
///
/// Renamed from `Operation` to avoid collision with the RPC wire struct
/// `sinex_primitives::rpc::ops::Operation` — see issue #746 (A2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationMarker;

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

    pub fn from_derived<I: IntoIterator<Item = EventId>>(ids: I) -> Option<Self> {
        canonicalize_source_event_ids(ids).map(|source_event_ids| Provenance::Derived {
            source_event_ids,
            operation_id: None,
        })
    }

    #[must_use]
    pub fn into_canonical(self) -> Self {
        match self {
            Provenance::Derived {
                source_event_ids,
                operation_id,
            } => Provenance::Derived {
                source_event_ids: canonicalize_non_empty_source_event_ids(source_event_ids),
                operation_id,
            },
            material => material,
        }
    }

    /// Set the operation ID on Derived provenance.
    /// No-op on Material provenance.
    #[must_use]
    pub fn with_operation(mut self, op_id: Id<OperationMarker>) -> Self {
        if let Provenance::Derived {
            ref mut operation_id,
            ..
        } = self
        {
            *operation_id = Some(op_id);
        }
        self
    }

    /// Get the operation ID if this is Derived provenance.
    #[must_use]
    pub fn operation_id(&self) -> Option<Id<OperationMarker>> {
        match self {
            Provenance::Derived { operation_id, .. } => *operation_id,
            Provenance::Material { .. } => None,
        }
    }

    /// Get the operation UUID used by event persistence, if any.
    #[must_use]
    pub fn operation_uuid(&self) -> Option<Uuid> {
        self.operation_id().map(|id| id.to_uuid())
    }
}

fn canonicalize_source_event_ids<I>(ids: I) -> Option<NonEmptyVec<EventId>>
where
    I: IntoIterator<Item = EventId>,
{
    let mut ids: Vec<EventId> = ids.into_iter().collect();
    ids.sort_unstable();
    ids.dedup();
    NonEmptyVec::from_vec(ids)
}

fn canonicalize_non_empty_source_event_ids(ids: NonEmptyVec<EventId>) -> NonEmptyVec<EventId> {
    let mut ids = ids.into_vec();
    ids.sort_unstable();
    ids.dedup();

    // The input was non-empty, so sorting and deduplication cannot make it
    // empty. Rebuild without unwrap/expect to keep the invariant explicit.
    let mut iter = ids.into_iter();
    let Some(first) = iter.next() else {
        unreachable!("deduplicating a non-empty parent set cannot produce an empty set")
    };
    NonEmptyVec::from_head_tail(first, iter.collect())
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
    use xtask::sandbox::sinex_test;

    // Inline because these exercise private host-identity resolution helpers directly.
    #[sinex_test]
    async fn resolve_host_identity_prefers_valid_machine_id() -> TestResult<()> {
        let host = resolve_host_identity(Some("0123456789abcdef"), Some("sinnix-prime"));
        assert_eq!(host.as_str(), "0123456789abcdef");
        Ok(())
    }

    // Inline because these exercise private host-identity resolution helpers directly.
    #[sinex_test]
    async fn resolve_host_identity_falls_back_to_valid_hostname() -> TestResult<()> {
        let host = resolve_host_identity(Some("bad machine id"), Some("sinnix-prime"));
        assert_eq!(host.as_str(), "sinnix-prime");
        Ok(())
    }

    // Inline because these exercise private host-identity resolution helpers directly.
    #[sinex_test]
    async fn resolve_host_identity_derives_deterministic_fallback_from_invalid_inputs()
    -> TestResult<()> {
        let host = resolve_host_identity(Some("bad machine id"), Some("bad host"));
        assert_eq!(host.as_str(), "host-887759893f18d0bb");
        Ok(())
    }

    // Inline because these exercise private host-identity resolution helpers directly.
    #[sinex_test]
    async fn resolve_host_identity_uses_unknown_host_only_when_no_identity_material_exists()
    -> TestResult<()> {
        let host = resolve_host_identity(None, Some("   "));
        assert_eq!(host.as_str(), "unknown-host");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // #1570 Prong B — builder ts_orig inversion
    // -------------------------------------------------------------------------

    fn material_builder() -> EventBuilder<serde_json::Value, HasProvenance> {
        EventBuilder::new_internal(
            EventSource::from_static("test.source"),
            EventType::new("test.event").expect("valid event type"),
            serde_json::json!({}),
        )
        .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()), 0)
    }

    /// A material event with no explicit timestamp leaves `ts_orig = None` (the
    /// "derive me at persistence" signal) rather than being stamped `now()`.
    #[sinex_test]
    async fn material_event_without_timestamp_defers_ts_orig() -> TestResult<()> {
        let event = material_builder().build()?;
        assert_eq!(
            event.ts_orig, None,
            "material defers ts_orig to persistence"
        );
        assert_eq!(event.ts_quality, None);
        Ok(())
    }

    /// A parser that resolved intrinsic timing keeps it, with the rung recorded.
    #[sinex_test]
    async fn material_event_with_explicit_quality_is_owned_by_parser() -> TestResult<()> {
        let ts = Timestamp::from_const(time::macros::datetime!(2021-01-02 03:04:05 UTC));
        let event = material_builder()
            .at_time_with_quality(ts, TemporalSourceType::IntrinsicContent)
            .build()?;
        assert_eq!(event.ts_orig, Some(ts));
        assert_eq!(event.ts_quality, Some(TemporalSourceType::IntrinsicContent));
        Ok(())
    }

    /// The deferred signal is deterministic: re-building the same material event
    /// (as replay does) yields the same `None` — no ephemeral `now()` sneaks in.
    #[sinex_test]
    async fn material_deferral_is_replay_stable() -> TestResult<()> {
        assert_eq!(material_builder().build()?.ts_orig, None);
        assert_eq!(material_builder().build()?.ts_orig, None);
        Ok(())
    }

    /// Derived events have no source material to resolve against, so they keep
    /// the wall-clock synthesis-time fallback and a `None` quality rung.
    #[sinex_test]
    async fn derived_event_without_timestamp_uses_synthesis_now() -> TestResult<()> {
        let parent = Id::<Event>::from_uuid(Uuid::now_v7());
        let event = EventBuilder::new_internal(
            EventSource::from_static("test.source"),
            EventType::new("test.derived").expect("valid event type"),
            serde_json::json!({}),
        )
        .from_parents([parent])?
        .build()?;
        assert!(
            event.ts_orig.is_some(),
            "derived events keep synthesis-time ts_orig"
        );
        assert_eq!(event.ts_quality, None);
        Ok(())
    }
}
