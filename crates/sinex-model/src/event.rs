//! Events: one reading of a record in a material, or one result derived from
//! parent events. Built here from a typed payload; stored only by admission.
use crate::{
    Class, EquivalenceKey, EventId, EventPayload, KeyKind, MaterialId, ReplaceRule, TsOrig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Where in a material the record an event came from starts. Stable when the
/// material is read again, so it is the identity of a keyless record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordLocation {
    pub material: MaterialId,
    /// Byte offset of the record's first byte within the material.
    pub anchor: u64,
}

/// Which of two readings of one record is newer. Each source defines it so it
/// only grows: a file offset, a poll time, an export date. Only a newer reading
/// replaces a live one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReadingOrder(pub i64);

/// Whether a reading says the record exists or was removed at the source.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, crate::DbCheck,
)]
#[serde(rename_all = "snake_case")]
pub enum RecordState {
    #[default]
    Present,
    /// The source deleted it; this is a fact, recorded as a newer reading.
    Removed,
}

/// Where an event came from: exactly one of a record location or parent events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    Record {
        location: RecordLocation,
        /// Output number when one record yields several events.
        ordinal: u16,
    },
    Derived {
        parents: Vec<EventId>,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BuildError {
    #[error("payload of {0} did not serialize to a JSON object")]
    NotAnObject(&'static str),
    #[error("a derived event needs at least one parent")]
    NoParents,
    #[error("a location-keyed type ({0}) cannot be derived; derived events need key fields")]
    DerivedWithoutKey(&'static str),
    #[error("payload is not a valid {0}: {1}")]
    Payload(&'static str, String),
}

/// An event ready for admission.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub id: EventId,
    pub event_type: &'static str,
    pub source: String,
    pub origin: Origin,
    pub ts_orig: TsOrig,
    pub payload: Value,
    /// Set when this record has field identity; `None` uses its location.
    pub equivalence_key: Option<EquivalenceKey>,
    pub replace: ReplaceRule,
    pub reading_order: ReadingOrder,
    /// BLAKE3 of the record as read, without declared volatile fields. Two
    /// readings with the same hash are a repeat.
    pub record_hash: [u8; 32],
    pub record_state: RecordState,
    pub class: Class,
    /// How strongly each kind of evidence backs a derived event.
    pub support: Option<Value>,
    /// Stored bytes the payload owns beyond the material (excision deletes
    /// them with the event). Set at build; not read back from the store.
    pub cites: Vec<crate::Cited>,
}

impl Event {
    /// Build an event from a typed payload. The ID is minted here and never
    /// derived from content.
    pub fn build<P: EventPayload>(
        payload: &P,
        source: &str,
        origin: Origin,
        reading_order: ReadingOrder,
        record_hash: [u8; 32],
        class: Class,
    ) -> Result<Self, BuildError> {
        let value =
            serde_json::to_value(payload).map_err(|_| BuildError::NotAnObject(P::EVENT_TYPE))?;
        if !value.is_object() {
            return Err(BuildError::NotAnObject(P::EVENT_TYPE));
        }
        let equivalence_key = match payload.key_kind() {
            KeyKind::Location => None,
            KeyKind::Fields(fields) => Some(EquivalenceKey::of(P::EVENT_TYPE, fields, &value)),
        };
        if let Origin::Derived { parents } = &origin {
            if parents.is_empty() {
                return Err(BuildError::NoParents);
            }
            if equivalence_key.is_none() {
                return Err(BuildError::DerivedWithoutKey(P::EVENT_TYPE));
            }
        }
        Ok(Self {
            id: EventId::new(),
            event_type: P::EVENT_TYPE,
            source: source.to_owned(),
            origin,
            ts_orig: payload.ts_orig(),
            payload: value,
            equivalence_key,
            replace: P::REPLACE,
            reading_order,
            record_hash,
            record_state: payload.record_state(),
            class,
            support: None,
            cites: payload.cited_blobs(),
        })
    }

    pub fn location(&self) -> Option<(RecordLocation, u16)> {
        match self.origin {
            Origin::Record { location, ordinal } => Some((location, ordinal)),
            Origin::Derived { .. } => None,
        }
    }

    /// Decode the payload back into its type.
    pub fn payload_as<P: EventPayload>(&self) -> Option<P> {
        (self.event_type == P::EVENT_TYPE)
            .then(|| serde_json::from_value(self.payload.clone()).ok())
            .flatten()
    }
}
