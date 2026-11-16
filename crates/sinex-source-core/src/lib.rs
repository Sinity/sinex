//! The source framework. Every source goes through the same four steps: plan
//! (what to read, with which parser), acquire (store exact bytes as materials),
//! parse (bytes to events with record locations), admit.
//!
//! A *driver* obtains bytes and frames them for one input shape, written once
//! and shared by every source of that shape. A *parser* maps one framed record
//! to its events and never reads files, networks or clocks. The *drain* owns
//! everything around `parse`: record_offsets, output numbering, catching a panic as a
//! failure for that record, record hashes, carried state, and building the
//! batch with the new cursor. A parser cannot drop or mislabel a record,
//! because it never sees the bookkeeping.
pub mod binding;
pub mod capture;
pub mod discovery;
pub mod drain;
pub mod drivers;
pub mod gaps;
pub mod markup;
pub mod nats;
pub mod registry;
pub mod sampling;
#[cfg(feature = "archives")]
pub mod shared_pass;
pub mod stage;

pub use binding::{Binding, InputFormat, Reader, Reading, Settings, StoredMaterial};
pub use drain::{DrainInput, DrainOutput, Drainer, Limits, RecordContext, drain};
pub use drivers::{Acquired, DriverError, MaterialSink};
pub use registry::{DeclaredSource, Expectation, Framer, Origin, Shape, Source, SourceSpec};

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

/// `Option` gives the reason alone; `Result` gives "reason: cause".
pub trait RecordResult<T> {
    fn record(self, reason: &'static str) -> Result<T, RecordError>;
    fn record_with(self, reason: impl FnOnce() -> String) -> Result<T, RecordError>;
}

impl<T> RecordResult<T> for Option<T> {
    fn record(self, reason: &'static str) -> Result<T, RecordError> {
        self.ok_or_else(|| RecordError::new(reason))
    }
    fn record_with(self, reason: impl FnOnce() -> String) -> Result<T, RecordError> {
        self.ok_or_else(|| RecordError::new(reason()))
    }
}

impl<T, E: std::fmt::Display> RecordResult<T> for Result<T, E> {
    fn record(self, reason: &'static str) -> Result<T, RecordError> {
        self.map_err(|cause| RecordError::new(format!("{reason}: {cause}")))
    }
    fn record_with(self, reason: impl FnOnce() -> String) -> Result<T, RecordError> {
        self.map_err(|cause| RecordError::new(format!("{}: {cause}", reason())))
    }
}

/// How a framed record's bytes become the parser's input.
pub trait RecordShape<'a>: Sized {
    /// Owned shapes can decode from a bounded stream; borrowed shapes keep their parser input.
    const STREAMING: bool = false;
    fn decode_reader(_reader: &mut dyn std::io::Read) -> Result<Self, RecordError> {
        Err(RecordError::new("record shape requires borrowed bytes"))
    }
    fn decoded_hash(&self, raw_hash: [u8; 32], _volatile: &[&str]) -> [u8; 32] {
        raw_hash
    }
    fn decode(raw: &'a [u8]) -> Result<Self, RecordError>;

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
    const STREAMING: bool = true;
    fn decode_reader(reader: &mut dyn std::io::Read) -> Result<Self, RecordError> {
        serde_json::from_reader(reader).record("record is not a JSON object")
    }
    fn decoded_hash(&self, raw_hash: [u8; 32], volatile: &[&str]) -> [u8; 32] {
        if volatile.is_empty() {
            return raw_hash;
        }
        let fields: std::collections::BTreeMap<_, _> = self
            .iter()
            .filter(|(name, _)| !volatile.contains(&name.as_str()))
            .collect();
        let mut hash = blake3::Hasher::new();
        serde_json::to_writer(&mut hash, &fields).expect("JSON objects serialize");
        *hash.finalize().as_bytes()
    }

    fn decode(raw: &'a [u8]) -> Result<Self, RecordError> {
        serde_json::from_slice(raw).record("record is not a JSON object")
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

/// A source's parser.
pub trait Parser: Send + Sync + 'static {
    /// The framed record: a line, a JSON row, an archive member's record.
    type Record<'a>: RecordShape<'a>;
    /// State carried from one record to the next (a multi-line command, an IRC
    /// nick). Saved with the cursor so a restart resumes exactly.
    type Carry: Default + Clone + Serialize + DeserializeOwned + Send + 'static;
    /// One typed payload, or an enum over several (see `Payloads`).
    type Payload: Payloads;

    /// Empty file records still describe captured documents.
    const READS_BLANK_RECORDS: bool = false;

    /// Fields left out of the record hash because they change without the
    /// record changing (a last-seen time).
    const VOLATILE: &'static [&'static str] = &[];

    /// Hash the fields that distinguish readings of this record. A parser
    /// whose payload depends on its file path includes that context too.
    fn record_hash(&self, raw: &[u8], _context: &RecordContext<'_>) -> [u8; 32] {
        <Self::Record<'_> as RecordShape>::hash(raw, Self::VOLATILE)
    }

    /// Hash an owned streamed record. Parsers with custom record identity override this with the same rule.
    fn streamed_record_hash(
        &self,
        record: &Self::Record<'_>,
        raw_hash: [u8; 32],
        _context: &RecordContext<'_>,
    ) -> [u8; 32] {
        record.decoded_hash(raw_hash, Self::VOLATILE)
    }

    /// State after a record could not be read. Parsers tracking continuity
    /// drop what they knew; other parsers keep their carried context.
    /// This state commits only with the record failure and its cursor.
    fn failed_record(&self, carry: &Self::Carry) -> Self::Carry {
        carry.clone()
    }

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

/// A parser whose records do not carry state to the next record.
pub trait StatelessParser: Send + Sync + 'static {
    type Record<'a>: RecordShape<'a>;
    type Payload: Payloads;
    const READS_BLANK_RECORDS: bool = false;
    const VOLATILE: &'static [&'static str] = &[];
    fn record_hash(&self, raw: &[u8], _context: &RecordContext<'_>) -> [u8; 32] {
        <Self::Record<'_> as RecordShape>::hash(raw, Self::VOLATILE)
    }
    fn streamed_record_hash(
        &self,
        record: &Self::Record<'_>,
        raw_hash: [u8; 32],
        _context: &RecordContext<'_>,
    ) -> [u8; 32] {
        record.decoded_hash(raw_hash, Self::VOLATILE)
    }
    fn parse(
        &self,
        record: &Self::Record<'_>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<Self::Payload>, RecordError>;
}

pub struct Stateless<P>(pub P);

impl<P: StatelessParser> Parser for Stateless<P> {
    type Record<'a> = P::Record<'a>;
    type Carry = ();
    type Payload = P::Payload;
    const READS_BLANK_RECORDS: bool = P::READS_BLANK_RECORDS;
    const VOLATILE: &'static [&'static str] = P::VOLATILE;
    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        self.0.record_hash(raw, context)
    }
    fn streamed_record_hash(
        &self,
        record: &Self::Record<'_>,
        raw_hash: [u8; 32],
        context: &RecordContext<'_>,
    ) -> [u8; 32] {
        self.0.streamed_record_hash(record, raw_hash, context)
    }
    fn parse(
        &self,
        record: &Self::Record<'_>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<Self::Payload>, ()), RecordError> {
        Ok((self.0.parse(record, context)?, ()))
    }
}

#[cfg(test)]
mod test_parser;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
