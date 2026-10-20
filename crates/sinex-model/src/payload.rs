//! Typed payloads. Every event type is a Rust type deriving `EventPayload`,
//! which declares its type name, how a repeat of it is recognized, which
//! reading wins, and when it happened. Schemaless events are a fallback only
//! (operator 2025-08-01); they are not modelled here.
use crate::{Class, RecordState, TsOrig};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

/// How Sinex recognizes the same record seen again (identity rule 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyKind {
    /// No stable identity: one live reading per record location, event type and
    /// output number. Used when the source has no native ID (zsh history).
    Location,
    /// The listed payload fields identify the record across materials (an
    /// Atuin row ID, a browser visit ID).
    Fields(&'static [&'static str]),
}

/// What admission does with a second reading of a live record (identity rule 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, crate::DbCheck)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceRule {
    /// A record can change: a newer reading with different content replaces
    /// the live one, which is archived.
    ReplaceWhenNewer,
    /// A record never changes once read; any later reading is a repeat.
    SuppressRepeat,
}

/// The key under which a keyed record is remembered: BLAKE3 over the canonical
/// JSON of its key fields, in declared order.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct EquivalenceKey(pub String);

impl EquivalenceKey {
    /// The key for `payload` under `fields`. A missing field is part of the key
    /// as `null`, so a record without its identity field is still keyed
    /// consistently rather than dropped.
    pub fn of(event_type: &str, fields: &[&str], payload: &Value) -> Self {
        let values: Vec<&Value> = fields
            .iter()
            .map(|f| payload.get(*f).unwrap_or(&Value::Null))
            .collect();
        let mut hasher = blake3::Hasher::new();
        hasher.update(event_type.as_bytes());
        hasher.update(&[0]);
        hasher.update(
            serde_json::to_string(&values)
                .expect("JSON values always serialize")
                .as_bytes(),
        );
        Self(hasher.finalize().to_hex().to_string())
    }
}

/// Stored bytes an event's payload names and owns beyond its material: a
/// library acquisition's file, kept in the content store by the scan. The
/// event holds the only reference to them, so excising the event deletes the
/// bytes once no other event or material refers to them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Cited {
    /// The content store's name, `blake3:<hex>`.
    pub blob: String,
    pub size: u64,
}

pub trait EventPayload: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The stored event type, such as `command.executed`.
    const EVENT_TYPE: &'static str;
    const KEY: KeyKind;
    const REPLACE: ReplaceRule;
    /// Per-field privacy hints; rules in the store decide the effective class.
    const FIELD_CLASSES: &'static [(&'static str, Class)] = &[];
    /// Fields a newer reading does not erase: when it leaves one unknown
    /// (`null` or absent) and the reading it replaces had a value, admission
    /// keeps that value. For exports that drop details of things that are
    /// gone (a deleted video's title).
    const KEEP_KNOWN: &'static [&'static str] = &[];

    /// When the thing happened, by the source's evidence.
    fn ts_orig(&self) -> TsOrig;

    /// Whether this reading says the record was removed at the source.
    fn record_state(&self) -> RecordState {
        RecordState::Present
    }

    /// Stored bytes this reading owns beyond its material (see [`Cited`]).
    fn cited_blobs(&self) -> Vec<Cited> {
        Vec::new()
    }

    /// The JSON Schema registered for this type and used to validate payloads
    /// arriving over the NATS ingress.
    fn payload_schema() -> Value;
}

/// What a parser's `parse` returns per event: one typed payload, or an enum
/// over several when one source emits several event types (a compositor's
/// stream: focus, open, close, move). Every `EventPayload` is one; an enum
/// implements this by building its variant's event.
pub trait Payloads: Send + Sync + 'static {
    fn build(
        &self,
        source: &str,
        origin: crate::Origin,
        reading_order: crate::ReadingOrder,
        record_hash: [u8; 32],
        class: Class,
    ) -> Result<crate::Event, crate::BuildError>;
}

impl<P: EventPayload> Payloads for P {
    fn build(
        &self,
        source: &str,
        origin: crate::Origin,
        reading_order: crate::ReadingOrder,
        record_hash: [u8; 32],
        class: Class,
    ) -> Result<crate::Event, crate::BuildError> {
        crate::Event::build(self, source, origin, reading_order, record_hash, class)
    }
}

/// An enum over several payload types, implementing [`Payloads`] by building
/// the event of whichever variant it holds.
///
/// ```ignore
/// sinex_model::payload_enum! {
///     #[derive(Debug, Clone, PartialEq)]
///     pub enum Desktop { Focused(WindowFocused), Closed(WindowClosed) }
/// }
/// ```
#[macro_export]
macro_rules! payload_enum {
    ($(#[$meta:meta])* $vis:vis enum $name:ident { $($variant:ident($ty:ty)),+ $(,)? }) => {
        $(#[$meta])*
        $vis enum $name { $($variant($ty)),+ }

        impl $crate::Payloads for $name {
            fn build(
                &self,
                source: &str,
                origin: $crate::Origin,
                reading_order: $crate::ReadingOrder,
                record_hash: [u8; 32],
                class: $crate::Class,
            ) -> ::std::result::Result<$crate::Event, $crate::BuildError> {
                match self {
                    $(Self::$variant(payload) => $crate::Event::build(
                        payload, source, origin, reading_order, record_hash, class,
                    ),)+
                }
            }
        }
    };
}

/// Enums stored as text: the column's CHECK list is generated from the variants,
/// so the database and the Rust type cannot disagree.
pub trait DbCheck {
    const VALUES: &'static [&'static str];

    /// `CHECK (<column> IN (...))`.
    fn check(column: &str) -> String {
        let values: Vec<String> = Self::VALUES.iter().map(|v| format!("'{v}'")).collect();
        format!("CHECK ({column} IN ({}))", values.join(", "))
    }

    fn as_db(&self) -> &'static str;
}

#[cfg(test)]
mod tests;
