//! The drain: everything around `Parser::parse`.
use crate::{Parser, RecordError, RecordShape};
use serde_json::Value;
use sinex_model::{
    Class, Manifest, MaterialId, Member, Origin, Payloads, ReadingOrder, RecordLocation,
    RecordOutcome,
};

/// Batch size bounds. Batches are sized by events and bytes; a record too large
/// for a normal batch goes alone in a batch of one. Records sharing a byte
/// end stay together because one byte checkpoint cannot distinguish them.
/// No valid record is refused.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_events: usize,
    pub max_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_events: 10_000,
            max_bytes: 16 << 20,
        }
    }
}

/// Random-access stored records. Implementations keep their verified reader
/// open.
pub trait RecordRead: Send + Sync {
    fn length(&self) -> u64;
    fn read_exact_at(&self, start: u64, bytes: &mut [u8]) -> std::io::Result<()>;
}

/// Borrowed record storage. File reads hold only the current decoded record.
pub enum RecordBytes<'a> {
    Slice(&'a [u8]),
    File(&'a std::fs::File),
    Reader(&'a dyn RecordRead),
}

impl<'a> From<&'a [u8]> for RecordBytes<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self::Slice(bytes)
    }
}

impl<'a> RecordBytes<'a> {
    /// A bounded view of a record, including records larger than the part target.
    pub fn reader(&self, start: usize, end: usize) -> std::io::Result<Box<dyn std::io::Read + '_>> {
        if start > end {
            return Err(std::io::Error::other("invalid record range"));
        }
        match self {
            Self::Slice(bytes) => {
                Ok(Box::new(bytes.get(start..end).ok_or_else(|| {
                    std::io::Error::other("record range exceeds material")
                })?))
            }
            Self::File(file) => Ok(Box::new(RecordRange {
                source: RangeSource::File(file),
                at: start as u64,
                end: end as u64,
            })),
            Self::Reader(reader) => Ok(Box::new(RecordRange {
                source: RangeSource::Stored(*reader),
                at: start as u64,
                end: end as u64,
            })),
        }
    }
    pub fn record(&self, start: usize, end: usize) -> std::io::Result<std::borrow::Cow<'a, [u8]>> {
        match self {
            Self::Slice(bytes) => bytes
                .get(start..end)
                .map(std::borrow::Cow::Borrowed)
                .ok_or_else(|| std::io::Error::other("record range exceeds material")),
            Self::File(file) => {
                use std::os::unix::fs::FileExt;
                let mut bytes = vec![
                    0;
                    end.checked_sub(start).ok_or_else(|| std::io::Error::other(
                        "invalid record range"
                    ))?
                ];
                file.read_exact_at(&mut bytes, start as u64)?;
                Ok(std::borrow::Cow::Owned(bytes))
            }
            Self::Reader(reader) => {
                let mut bytes = vec![
                    0;
                    end.checked_sub(start).ok_or_else(|| std::io::Error::other(
                        "invalid record range"
                    ))?
                ];
                reader.read_exact_at(start as u64, &mut bytes)?;
                Ok(std::borrow::Cow::Owned(bytes))
            }
        }
    }
}

enum RangeSource<'a> {
    File(&'a std::fs::File),
    Stored(&'a dyn RecordRead),
}
struct RecordRange<'a> {
    source: RangeSource<'a>,
    at: u64,
    end: u64,
}
impl std::io::Read for RecordRange<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let count = out.len().min(self.end.saturating_sub(self.at) as usize);
        if count == 0 {
            return Ok(0);
        }
        match self.source {
            RangeSource::File(file) => {
                use std::os::unix::fs::FileExt;
                file.read_exact_at(&mut out[..count], self.at)?;
            }
            RangeSource::Stored(reader) => reader.read_exact_at(self.at, &mut out[..count])?,
        }
        self.at += count as u64;
        Ok(count)
    }
}
struct DigestRead<R> {
    reader: R,
    hash: blake3::Hasher,
    blank: bool,
    bytes: u64,
}
impl<R: std::io::Read> std::io::Read for DigestRead<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let count = self.reader.read(out)?;
        self.hash.update(&out[..count]);
        self.blank &= out[..count]
            .iter()
            .all(|byte| *byte == b' ' || *byte == b'\n');
        self.bytes += count as u64;
        Ok(count)
    }
}

pub struct DrainInput<'a> {
    pub source: &'a str,
    pub class: Class,
    pub material: MaterialId,
    /// Record bytes borrowed from memory or an owned temporary file.
    pub bytes: RecordBytes<'a>,
    /// Framed records: byte ranges within `bytes`, in order.
    pub records: &'a [(usize, usize)],
    /// The carried state before the first record.
    pub carry: Value,
    /// Which of two readings is newer, by the record's anchor.
    pub reading_order: &'a (dyn Fn(usize) -> ReadingOrder + Sync),
    pub limits: Limits,
    /// The material's manifest. Its members (archive member streams,
    /// directory snapshots) tell each record which member it is in.
    pub manifest: &'a Manifest,
}

/// What a parser may know about where its record came from.
#[derive(Clone, Copy, Debug)]
pub struct RecordContext<'a> {
    pub manifest: &'a Manifest,
    /// The manifest's member containing this record, if the material is made
    /// of members.
    pub member: Option<&'a Member>,
}

static NO_MANIFEST: std::sync::LazyLock<Manifest> = std::sync::LazyLock::new(Manifest::default);

impl RecordContext<'static> {
    /// A record read outside any material (a test, a live line).
    pub fn none() -> Self {
        Self {
            manifest: &NO_MANIFEST,
            member: None,
        }
    }
}

impl RecordContext<'_> {
    /// A moved file is a new reading even when its bytes are unchanged.
    /// Bundle offsets are excluded: packing files must not change a reading.
    pub fn hash_with_path(&self, raw: &[u8]) -> [u8; 32] {
        let bytes_hash = blake3::hash(raw);
        let Some(path) = self.path() else {
            return *bytes_hash.as_bytes();
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"record path\0");
        hash.update(bytes_hash.as_bytes());
        hash.update(path.as_bytes());
        let native = self
            .member
            .and_then(|member| member.details.get("native_path_bytes"))
            .unwrap_or_else(|| self.manifest.detail("native_path_bytes"));
        if let Some(native) = native.as_str() {
            hash.update(b"\0native_path_bytes\0");
            hash.update(native.as_bytes());
        }
        if let Some(node) = self.member.and_then(|member| member.node.as_ref()) {
            hash.update(b"\0file node\0");
            // Reading changes access time; packing chooses attribute offsets.
            let mut facts = node.clone();
            facts.atime_ns.clear();
            for attribute in &mut facts.extended_attributes {
                attribute.value_at = 0;
            }
            hash.update(&serde_json::to_vec(&facts).expect("file node serializes"));
        }
        *hash.finalize().as_bytes()
    }

    /// The member's path, or the material's own `path`.
    pub fn path(&self) -> Option<&str> {
        self.member
            .map(|m| m.path.as_str())
            .or(self.manifest.path.as_deref())
    }
}

/// One batch's worth of drained records.
#[derive(Debug)]
pub struct DrainOutput {
    pub outcomes: Vec<RecordOutcome>,
    /// Byte end (within the material) of the last record in this chunk.
    pub end: usize,
    /// Carried state after the last record in this chunk.
    pub carry: Value,
    pub events: usize,
    pub failures: usize,
}

/// Parse every framed record, in order, into chunks bounded by `limits`.
/// Collects [`Drainer`]; callers that admit chunks as they come use the
/// drainer directly, so memory holds one chunk, not a whole material's.
pub fn drain<P: Parser>(
    parser: &P,
    input: DrainInput<'_>,
) -> Result<Vec<DrainOutput>, RecordError> {
    Drainer::new(parser, input)?.collect()
}

/// A record parsed but not yet placed: it did not fit the chunk being
/// closed, so it opens the next one.
struct Pending<C> {
    outcome: RecordOutcome,
    size: usize,
    events: usize,
    end: usize,
    carry_after: C,
}

/// The drain as an iterator: each `next` parses records until a chunk is
/// full (by `limits`) and returns it. The chunk's carry is the state after
/// its last record, so a resume from any chunk's cursor is exact.
pub struct Drainer<'a, P: Parser> {
    parser: &'a P,
    input: DrainInput<'a>,
    next: usize,
    carry: P::Carry,
    pending: Option<Pending<P::Carry>>,
    finished: bool,
    emitted: bool,
}

impl<'a, P: Parser> Drainer<'a, P> {
    pub fn new(parser: &'a P, input: DrainInput<'a>) -> Result<Self, RecordError> {
        let length = match &input.bytes {
            RecordBytes::Slice(bytes) => bytes.len() as u64,
            RecordBytes::File(file) => file
                .metadata()
                .map_err(|e| RecordError::new(format!("temporary material file: {e}")))?
                .len(),
            RecordBytes::Reader(reader) => reader.length(),
        };
        if input
            .records
            .iter()
            .any(|&(start, end)| start > end || end as u64 > length)
        {
            return Err(RecordError::new("record range exceeds material"));
        }
        crate::drivers::dir_snapshot::validate_records(input.manifest, input.records)?;
        let carry: P::Carry = if input.carry.is_null() {
            P::Carry::default()
        } else {
            serde_json::from_value(input.carry.clone()).map_err(|e| {
                RecordError::new(format!("stored parser carry does not decode: {e}"))
            })?
        };
        Ok(Self {
            parser,
            input,
            next: 0,
            carry,
            pending: None,
            finished: false,
            emitted: false,
        })
    }

    /// Parse one record into its outcome, advancing the carry on success.
    fn outcome(&mut self, raw: &[u8], start: usize) -> RecordOutcome {
        let context = RecordContext {
            manifest: self.input.manifest,
            member: self.input.manifest.member_at(start),
        };
        let parsed = parse_one(self.parser, raw, &self.carry, &context);
        let hash = record_hash(self.parser, raw, &context);
        self.finish_outcome(parsed, hash, start)
    }

    fn streamed_outcome(
        &mut self,
        start: usize,
        end: usize,
    ) -> std::io::Result<Option<RecordOutcome>> {
        let context = RecordContext {
            manifest: self.input.manifest,
            member: self.input.manifest.member_at(start),
        };
        let mut reader = std::io::BufReader::with_capacity(
            65536,
            DigestRead {
                reader: self.input.bytes.reader(start, end)?,
                hash: blake3::Hasher::new(),
                blank: true,
                bytes: 0,
            },
        );
        let parsed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let record = <P::Record<'_> as RecordShape>::decode_reader(&mut reader)?;
            let hash = self.parser.streamed_record_hash(
                &record,
                *reader.get_ref().hash.finalize().as_bytes(),
                &context,
            );
            let parsed = self.parser.parse(&record, &self.carry, &context);
            Ok::<_, RecordError>((parsed, hash))
        }));
        // A failed decode can stop before EOF. Finish the bounded read for blank classification and IO faults.
        std::io::copy(&mut reader, &mut std::io::sink())?;
        if reader.get_ref().blank && reader.get_ref().bytes > 0 {
            return Ok(None);
        }
        let raw_hash = *reader.get_ref().hash.finalize().as_bytes();
        let (parsed, hash) = match parsed {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => (Err(error), raw_hash),
            Err(_) => (
                Err(RecordError::new(
                    "parser panicked while decoding a streamed record",
                )),
                raw_hash,
            ),
        };
        drop(reader);
        Ok(Some(self.finish_outcome(parsed, hash, start)))
    }

    fn finish_outcome(
        &mut self,
        parsed: Result<(Vec<P::Payload>, P::Carry), RecordError>,
        hash: [u8; 32],
        start: usize,
    ) -> RecordOutcome {
        let input = &self.input;
        let location = RecordLocation {
            material: input.material,
            anchor: start as u64,
        };
        match parsed {
            Ok((payloads, next)) => {
                self.carry = next;
                let order = (input.reading_order)(start);
                let mut events = Vec::with_capacity(payloads.len());
                let mut failure = None;
                for (i, payload) in payloads.iter().enumerate() {
                    let output_index = match u16::try_from(i) {
                        Ok(o) => o,
                        Err(_) => {
                            failure = Some("record produced more than 65535 events".to_string());
                            break;
                        }
                    };
                    match payload.build(
                        input.source,
                        Origin::Record {
                            location,
                            output_index,
                        },
                        order,
                        hash,
                        input.class,
                    ) {
                        Ok(event) if has_nul(&event.payload) => {
                            failure = Some(
                                "payload contains a NUL character, which jsonb cannot store".into(),
                            );
                            break;
                        }
                        Ok(event) => events.push(event),
                        Err(e) => {
                            failure = Some(e.to_string());
                            break;
                        }
                    }
                }
                match failure {
                    Some(reason) => {
                        self.carry = self.parser.failed_record(&self.carry);
                        RecordOutcome::Failure { location, reason }
                    },
                    None => RecordOutcome::Events {
                        location: Some(location),
                        events,
                    },
                }
            }
            Err(e) => {
                self.carry = self.parser.failed_record(&self.carry);
                RecordOutcome::Failure { location, reason: e.0 }
            },
        }
    }
}

pub(crate) fn record_hash<P: Parser>(
    parser: &P,
    raw: &[u8],
    context: &RecordContext<'_>,
) -> [u8; 32] {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parser.record_hash(raw, context)
    }))
    .unwrap_or_else(|_| *blake3::hash(raw).as_bytes())
}

fn empty_chunk() -> DrainOutput {
    DrainOutput {
        outcomes: Vec::new(),
        end: 0,
        carry: Value::Null,
        events: 0,
        failures: 0,
    }
}

fn place(chunk: &mut DrainOutput, outcome: RecordOutcome, events: usize, end: usize) {
    if matches!(outcome, RecordOutcome::Failure { .. }) {
        chunk.failures += 1;
    }
    chunk.events += events;
    chunk.outcomes.push(outcome);
    chunk.end = end;
}

impl<P: Parser> Iterator for Drainer<'_, P> {
    type Item = Result<DrainOutput, RecordError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut current = empty_chunk();
        let mut bytes_in_chunk = 0usize;
        // The carry after the chunk's last record. Serialized once per chunk,
        // not per record, so a growing carry costs nothing per record.
        let mut chunk_carry: Option<P::Carry> = None;
        if let Some(p) = self.pending.take() {
            place(&mut current, p.outcome, p.events, p.end);
            bytes_in_chunk = p.size;
            chunk_carry = Some(p.carry_after);
        }
        while let Some(&(start, end)) = self.input.records.get(self.next) {
            self.next += 1;
            let (outcome, size) = if <P::Record<'_> as RecordShape>::STREAMING
                && !crate::drivers::dir_snapshot::is_directory(self.input.manifest)
            {
                match self.streamed_outcome(start, end) {
                    Ok(Some(outcome)) => (outcome, end - start),
                    Ok(None) => {
                        current.end = end;
                        continue;
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(RecordError::new(format!(
                            "read record {start}..{end}: {error}"
                        ))));
                    }
                }
            } else {
                let raw = match self.input.bytes.record(start, end) {
                    Ok(raw) => raw,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(RecordError::new(format!(
                            "read record {start}..{end}: {error}"
                        ))));
                    }
                };
                let raw = match crate::drivers::dir_snapshot::decode_record(
                    self.input.manifest,
                    start,
                    raw,
                ) {
                    Ok(raw) => raw,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
                // Only spaces and newlines: no record is there, so nothing is
                // read and nothing fails.
                if !crate::drivers::dir_snapshot::is_directory(self.input.manifest)
                    && is_blank(&raw)
                {
                    current.end = end;
                    continue;
                }
                let size = raw.len();
                (self.outcome(&raw, start), size)
            };
            let events = match &outcome {
                RecordOutcome::Events { events, .. } => events.len(),
                RecordOutcome::Failure { .. } => 0,
            };
            // A byte checkpoint must cover every record ending there.
            let full = !current.outcomes.is_empty()
                && end != current.end
                && (current.events + events > self.input.limits.max_events
                    || bytes_in_chunk + size > self.input.limits.max_bytes);
            if full {
                // The closed chunk ends at the previous record; this record
                // opens the next one.
                self.pending = Some(Pending {
                    outcome,
                    size,
                    events,
                    end,
                    carry_after: self.carry.clone(),
                });
                current.carry =
                    serde_json::to_value(chunk_carry.as_ref().expect("a placed record"))
                        .expect("carry serializes");
                self.emitted = true;
                return Some(Ok(current));
            }
            place(&mut current, outcome, events, end);
            chunk_carry = Some(self.carry.clone());
            bytes_in_chunk += size;
        }
        self.finished = true;
        if !current.outcomes.is_empty() || !self.emitted {
            current.carry = serde_json::to_value(&self.carry).expect("carry serializes");
            self.emitted = true;
            return Some(Ok(current));
        }
        None
    }
}

fn parse_one<P: Parser>(
    parser: &P,
    raw: &[u8],
    carry: &P::Carry,
    context: &RecordContext<'_>,
) -> Result<(Vec<P::Payload>, P::Carry), RecordError> {
    if crate::drivers::dir_snapshot::is_directory(context.manifest)
        && let Some(reason) = context
            .member
            .and_then(|member| member.details.get("read_error"))
            .and_then(Value::as_str)
    {
        return Err(RecordError::new(format!(
            "{}: {reason}",
            context.path().unwrap_or_default()
        )));
    }
    // Decoding is the parser's code too: a panic there fails this record,
    // never the run.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let record = <P::Record<'_> as RecordShape>::decode(raw)?;
        parser.parse(&record, carry, context)
    })) {
        Ok(result) => result,
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "parser panicked".into());
            Err(RecordError::new(format!("parser panicked: {message}")))
        }
    }
}

fn is_blank(raw: &[u8]) -> bool {
    !raw.is_empty() && raw.iter().all(|b| *b == b' ' || *b == b'\n')
}

fn has_nul(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains('\0'),
        Value::Array(items) => items.iter().any(has_nul),
        Value::Object(map) => map.iter().any(|(k, v)| k.contains('\0') || has_nul(v)),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
