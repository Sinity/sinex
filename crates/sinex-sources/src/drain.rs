//! The drain: everything around `Parser::parse`.
use crate::{Parser, RecordError, RecordShape};
use serde_json::Value;
type NextRange<'a> = (usize, usize, Option<&'a Value>, bool);
use sinex_model::{
    Class, MaterialId, Origin, Payloads, ReadingOrder, RecordLocation, RecordOutcome,
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

/// Random-access retained records. Implementations keep their verified backing open.
pub trait RecordRead: Send + Sync {
    fn length(&self) -> u64;
    fn read_exact_at(&self, start: u64, bytes: &mut [u8]) -> std::io::Result<()>;
}

/// Borrowed record storage. File reads retain only the current decoded record.
pub enum RecordBytes<'a> {
    Slice(&'a [u8]),
    File(&'a std::fs::File),
    Reader(&'a dyn RecordRead),
    /// Whole-erased backing has no decryptable body; archive transitions remain.
    Erased(u64),
}

impl<'a> From<&'a [u8]> for RecordBytes<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self::Slice(bytes)
    }
}

impl<'a> RecordBytes<'a> {
    pub fn record(&self, start: usize, end: usize) -> std::io::Result<std::borrow::Cow<'a, [u8]>> {
        match self {
            Self::Erased(_) => Err(std::io::Error::other(
                "erased material has no retained plaintext",
            )),
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

struct ErasedSpan {
    end: usize,
    transition: Option<Value>,
}

/// Archive-owned transitions after scoped envelope authentication. Provider
/// manifests have no conversion into this type; its fields are private.
pub struct ErasedTransitions {
    entries: std::collections::BTreeMap<usize, ErasedSpan>,
    length: u64,
    whole: bool,
}
impl ErasedTransitions {
    /// Only the archive owner supplies authenticated coordinates and minimal
    /// parser tokens after opening the purpose-bound encrypted envelope.
    pub fn from_archive(
        records: impl IntoIterator<Item = (usize, usize, Option<Value>)>,
        length: u64,
        whole: bool,
    ) -> Result<Self, RecordError> {
        usize::try_from(length)
            .map_err(|_| RecordError::new("erased material exceeds address space"))?;
        let mut entries = std::collections::BTreeMap::new();
        for (start, end, transition) in records {
            if start >= end
                || end as u64 > length
                || entries
                    .insert(start, ErasedSpan { end, transition })
                    .is_some()
            {
                return Err(RecordError::new("invalid erased-record coordinates"));
            }
        }
        let mut through = 0;
        for (&start, span) in &entries {
            if start < through {
                return Err(RecordError::new("overlapping erased-record coordinates"));
            }
            through = span.end;
        }
        Ok(Self {
            entries,
            length,
            whole,
        })
    }
    pub(crate) fn contains_record(&self, start: usize, end: usize) -> bool {
        self.entries.get(&start).is_some_and(|span| span.end == end)
    }

    pub(crate) fn matches_whole_records(&self, records: &[(usize, usize)], length: u64) -> bool {
        self.whole
            && self.length == length
            && self.entries.len() == records.len()
            && self
                .entries
                .iter()
                .zip(records)
                .all(|((&start, span), range)| (start, span.end) == *range)
    }

    pub fn after(mut self, through: usize) -> Result<Self, RecordError> {
        if through as u64 > self.length
            || self
                .entries
                .iter()
                .any(|(&start, span)| start < through && through < span.end)
        {
            return Err(RecordError::new("resume cuts an erased record"));
        }
        self.entries.retain(|&start, _| start >= through);
        Ok(self)
    }
}

pub struct DrainInput<'a> {
    pub erased: Option<&'a ErasedTransitions>,
    pub source: &'a str,
    pub class: Class,
    pub material: MaterialId,
    /// Record bytes borrowed from memory or an owned staged file.
    pub bytes: RecordBytes<'a>,
    /// Framed records: byte ranges within `bytes`, in order.
    pub records: &'a [(usize, usize)],
    /// The carried state before the first record.
    pub carry: Value,
    /// Which of two readings is newer, by the record's anchor.
    pub reading_order: &'a (dyn Fn(usize) -> ReadingOrder + Sync),
    pub limits: Limits,
    /// The material's manifest. A `members` array of `{path, stream_offset,
    /// length, ...}` (archive member streams, directory snapshots) tells each
    /// record which member it is in.
    pub manifest: &'a Value,
}

/// What a parser may know about where its record came from.
#[derive(Clone, Copy, Debug)]
pub struct RecordContext<'a> {
    pub manifest: &'a Value,
    /// The manifest's member entry containing this record, if the material is
    /// made of members.
    pub member: Option<&'a Value>,
}

static NO_MANIFEST: Value = Value::Null;

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
    /// The member's path, or the material's own `path`.
    pub fn path(&self) -> Option<&str> {
        self.member
            .and_then(|m| m.get("path"))
            .or_else(|| self.manifest.get("path"))
            .and_then(Value::as_str)
    }
}

/// The member of `manifest` whose stream range holds byte `anchor`. Members
/// are in stream order and do not overlap.
pub fn member_at(manifest: &Value, anchor: usize) -> Option<&Value> {
    let members = manifest.get("members")?.as_array()?;
    let offset = |m: &Value| m.get("stream_offset").and_then(Value::as_u64).unwrap_or(0) as usize;
    let length = |m: &Value| m.get("length").and_then(Value::as_u64).unwrap_or(0) as usize;
    let i = members.partition_point(|m| offset(m) <= anchor);
    let member = members[..i].last()?;
    if length(member) == 0 && anchor == offset(member) {
        // Equal zero-length members at one byte anchor have no unique context.
        if members[..i - 1]
            .last()
            .is_some_and(|previous| offset(previous) == anchor && length(previous) == 0)
        {
            return None;
        }
        return Some(member);
    }
    (anchor < offset(member) + length(member)).then_some(member)
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
    erased_next:
        Option<std::iter::Peekable<std::collections::btree_map::Iter<'a, usize, ErasedSpan>>>,
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
                .map_err(|e| RecordError::new(format!("staged material: {e}")))?
                .len(),
            RecordBytes::Reader(reader) => reader.length(),
            RecordBytes::Erased(length) => *length,
        };
        if input.erased.is_some_and(|erased| erased.length != length)
            || matches!(&input.bytes, RecordBytes::Erased(_))
                && !input.erased.is_some_and(|erased| erased.whole)
        {
            return Err(RecordError::new(
                "erased backing needs its authenticated archive transitions",
            ));
        }
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
            erased_next: input.erased.map(|erased| erased.entries.iter().peekable()),
            input,
            next: 0,
            carry,
            pending: None,
            finished: false,
            emitted: false,
        })
    }

    fn next_range(&mut self) -> Result<Option<NextRange<'a>>, RecordError> {
        let ordinary = self.input.records.get(self.next).copied();
        let erased = self
            .erased_next
            .as_mut()
            .and_then(|records| records.peek())
            .map(|record| (*record.0, record.1.end, record.1.transition.as_ref()));
        if let Some((start, end, transition)) = erased
            && ordinary.is_none_or(|record| start <= record.0)
        {
            self.erased_next.as_mut().expect("erased iterator").next();
            while let Some(&(record_start, record_end)) = self.input.records.get(self.next) {
                if record_start >= end {
                    break;
                }
                if record_start < start || record_end > end {
                    return Err(RecordError::new("erased range overlaps a retained record"));
                }
                self.next += 1;
            }
            return Ok(Some((start, end, transition, true)));
        }
        if let Some((start, end)) = ordinary {
            if erased.is_some_and(|record| record.0 < end) {
                return Err(RecordError::new("erased range overlaps a retained record"));
            }
            self.next += 1;
            return Ok(Some((start, end, None, false)));
        }
        Ok(None)
    }

    fn check_erased(&self, start: usize, end: usize) -> Result<(), RecordError> {
        if matches!(&self.input.bytes, RecordBytes::Erased(_)) {
            return Ok(());
        }
        let mut at = start;
        while at < end {
            let next = at.saturating_add(65536).min(end);
            let bytes = self
                .input
                .bytes
                .record(at, next)
                .map_err(|error| RecordError::new(format!("read erased record: {error}")))?;
            if !bytes.iter().all(|byte| *byte == b' ' || *byte == b'\n') {
                return Err(RecordError::new(
                    "archive transition does not cover erased bytes",
                ));
            }
            at = next;
        }
        Ok(())
    }

    /// Parse one record into its outcome, advancing the carry on success.
    fn outcome(&mut self, raw: &[u8], start: usize) -> RecordOutcome {
        let input = &self.input;
        let location = RecordLocation {
            material: input.material,
            anchor: start as u64,
        };
        let context = RecordContext {
            manifest: input.manifest,
            member: member_at(input.manifest, start),
        };
        match parse_one(self.parser, raw, &self.carry, &context) {
            Ok((payloads, next)) => {
                self.carry = next;
                // A record shape's own hash that panics falls back to the
                // bytes' hash rather than stopping the run.
                let hash = record_hash::<P>(raw, &context);
                let order = (input.reading_order)(start);
                let mut events = Vec::with_capacity(payloads.len());
                let mut failure = None;
                for (i, payload) in payloads.iter().enumerate() {
                    let ordinal = match u16::try_from(i) {
                        Ok(o) => o,
                        Err(_) => {
                            failure = Some("record produced more than 65535 events".to_string());
                            break;
                        }
                    };
                    match payload.build(
                        input.source,
                        Origin::Record { location, ordinal },
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
                    Some(reason) => RecordOutcome::Failure { location, reason },
                    None => RecordOutcome::Events {
                        location: Some(location),
                        events,
                    },
                }
            }
            Err(e) => RecordOutcome::Failure {
                location,
                reason: e.0,
            },
        }
    }
}

/// Shared with acquisition exclusion matching, including records emitting no events.
pub(crate) fn record_hash<P: Parser>(raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
    std::panic::catch_unwind(|| {
        <P::Record<'_> as RecordShape>::hash_context(raw, P::VOLATILE, context)
    })
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
        loop {
            let (start, end, transition, erased) = match self.next_range() {
                Ok(Some(record)) => record,
                Ok(None) => break,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            if erased {
                if let Err(error) = self.check_erased(start, end) {
                    self.finished = true;
                    return Some(Err(error));
                }
                if let Some(transition) = transition {
                    let context = RecordContext {
                        manifest: self.input.manifest,
                        member: member_at(self.input.manifest, start),
                    };
                    self.carry = match self.parser.skip_erased(&self.carry, transition, &context) {
                        Ok(carry) => carry,
                        Err(error) => {
                            self.finished = true;
                            return Some(Err(error));
                        }
                    };
                    chunk_carry = Some(self.carry.clone());
                }
                current.end = end;
                continue;
            }
            let raw = match self.input.bytes.record(start, end) {
                Ok(raw) => raw,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(RecordError::new(format!(
                        "read record {start}..{end}: {error}"
                    ))));
                }
            };
            // An excision blanks a record to spaces (its newline kept): no
            // record is left there, so nothing is read and nothing fails.
            if is_blanked(&raw) {
                current.end = end;
                continue;
            }
            let outcome = self.outcome(&raw, start);
            let size = end - start;
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
        if let Some(erased) = self.input.erased
            && erased.whole
        {
            current.end = usize::try_from(erased.length).expect("erased coordinates checked");
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
    // Decoding is the parser's code too: a panic there fails this record,
    // never the run.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let record = <P::Record<'_> as RecordShape>::decode_context(raw, context)?;
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

fn is_blanked(raw: &[u8]) -> bool {
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
