//! The source framework (architecture §5). Every source goes through the same
//! four steps: stage (what to read, with which parser), acquire (store exact
//! bytes as materials), parse (bytes to events with record locations), admit.
//!
//! A *driver* obtains bytes and frames them for one input shape, written once
//! and shared by every source of that shape. A *parser* maps one framed record
//! to its events and never reads files, networks or clocks. The *drain* owns
//! everything around `parse`: anchors, output numbering, catching a panic as a
//! failure for that record, record hashes, carried state, and building the
//! batch with the new cursor. A parser cannot drop or mislabel a record,
//! because it never sees the bookkeeping.
pub mod binding;
pub mod capture;
pub mod drain;
pub mod drivers;
pub mod live;
pub mod nats;
pub mod registry;
pub mod sources;

pub use binding::{Binding, InputKind, Reader, Settings};
pub use drain::{DrainInput, DrainOutput, Drainer, Limits, RecordContext, drain};
pub use drivers::{Acquired, DriverError, MaterialSink};
pub use registry::{DeclaredSource, Driver, Framer, Source, SourceSpec};

use serde::{Serialize, de::DeserializeOwned};
use sinex_model::Payloads;

/// Why one record could not be read. Becomes a failure row; the bytes stay in
/// the material, so a retry is a replay of exactly those bytes.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct RecordError(pub String);

impl RecordError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

/// How a framed record's bytes become the parser's input.
pub trait RecordShape<'a>: Sized {
    fn decode(raw: &'a [u8]) -> Result<Self, RecordError>;
    fn decode_context(raw: &'a [u8], _context: &RecordContext<'_>) -> Result<Self, RecordError> {
        Self::decode(raw)
    }
    fn hash_context(raw: &[u8], volatile: &[&str], _context: &RecordContext<'_>) -> [u8; 32] {
        Self::hash(raw, volatile)
    }

    /// BLAKE3 of the record as read, without the parser's declared volatile
    /// fields. Equal hashes make a later reading a repeat.
    fn hash(raw: &[u8], _volatile: &[&str]) -> [u8; 32] {
        *blake3::hash(raw).as_bytes()
    }
}

/// A line or other raw byte record.
impl<'a> RecordShape<'a> for &'a [u8] {
    fn decode(raw: &'a [u8]) -> Result<Self, RecordError> {
        Ok(raw)
    }
}

/// A JSON object record: a row from a database, an item from an API.
impl<'a> RecordShape<'a> for serde_json::Map<String, serde_json::Value> {
    fn decode(raw: &'a [u8]) -> Result<Self, RecordError> {
        serde_json::from_slice(raw)
            .map_err(|e| RecordError::new(format!("record is not a JSON object: {e}")))
    }

    fn decode_context(raw: &'a [u8], context: &RecordContext<'_>) -> Result<Self, RecordError> {
        let mut object = Self::decode(raw)?;
        if context.manifest["row_format"] == 2 {
            object = object
                .remove("row")
                .and_then(|r| r.as_object().cloned())
                .ok_or_else(|| RecordError::new("SQLite row envelope has no native row"))?;
        }
        Ok(object)
    }
    fn hash_context(raw:&[u8],volatile:&[&str],context:&RecordContext<'_>)->[u8;32] {
        if context.manifest["row_format"]!=2 {return Self::hash(raw,volatile);}
        let Ok(envelope)=serde_json::from_slice::<serde_json::Value>(raw) else {return *blake3::hash(raw).as_bytes();};
        let Some(mut object)=envelope.get("row").and_then(|r|r.as_object().cloned()) else {return *blake3::hash(raw).as_bytes();};
        for field in volatile {object.remove(*field);}
        let identity=serde_json::from_slice::<serde_json::Value>(raw).ok().and_then(|v|v.get("_sinex_rowid").cloned());
        *blake3::hash(serde_json::json!({"row":object,"rowid":identity}).to_string().as_bytes()).as_bytes()
    }

    fn hash(raw: &[u8], volatile: &[&str]) -> [u8; 32] {
        if volatile.is_empty() {
            return *blake3::hash(raw).as_bytes();
        }
        match serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(raw) {
            Ok(mut object) => {
                for field in volatile {
                    object.remove(*field);
                }
                *blake3::hash(serde_json::Value::Object(object).to_string().as_bytes()).as_bytes()
            }
            Err(_) => *blake3::hash(raw).as_bytes(),
        }
    }
}

/// A source's parser (architecture §5.2).
pub trait Parser: Send + Sync + 'static {
    /// The framed record: a line, a JSON row, an archive member's record.
    type Record<'a>: RecordShape<'a>;
    /// State carried from one record to the next (a multi-line command, an IRC
    /// nick). Saved with the cursor so a restart resumes exactly.
    type Carry: Default + Clone + Serialize + DeserializeOwned + Send + 'static;
    /// One typed payload, or an enum over several (see `Payloads`).
    type Payload: Payloads;

    /// Fields left out of the record hash because they change without the
    /// record changing (a last-seen time).
    const VOLATILE: &'static [&'static str] = &[];

    /// Map one framed record to its events. `context` is the record's
    /// material: its manifest and, for a material made of several members
    /// (an archive's member stream, a directory snapshot), the member the
    /// record is in. Most parsers ignore it; those that need a file's path or
    /// a member's name read it here.
    fn parse(
        &self,
        record: &Self::Record<'_>,
        carry: &Self::Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<Self::Payload>, Self::Carry), RecordError>;
}
