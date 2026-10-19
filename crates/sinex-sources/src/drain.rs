//! The drain: everything around `Parser::parse`.
use crate::{Parser, RecordError, RecordShape};
use serde_json::Value;
use sinex_model::{
    Class, MaterialId, Origin, Payloads, ReadingOrder, RecordLocation, RecordOutcome,
};

/// Batch size bounds. Batches are sized by events and bytes; a record too large
/// for a normal batch goes alone in a batch of one. No valid record is refused.
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

pub struct DrainInput<'a> {
    pub source: &'a str,
    pub class: Class,
    pub material: MaterialId,
    /// The material's bytes.
    pub bytes: &'a [u8],
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
    Ok(Drainer::new(parser, input)?.collect())
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
    fn outcome(&mut self, start: usize, end: usize) -> RecordOutcome {
        let input = &self.input;
        let raw = &input.bytes[start..end];
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
                let hash = std::panic::catch_unwind(|| {
                    <P::Record<'_> as RecordShape>::hash(raw, P::VOLATILE)
                })
                .unwrap_or_else(|_| *blake3::hash(raw).as_bytes());
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
    type Item = DrainOutput;

    fn next(&mut self) -> Option<DrainOutput> {
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
            // An excision blanks a record to spaces (its newline kept): no
            // record is left there, so nothing is read and nothing fails.
            if is_blanked(&self.input.bytes[start..end]) {
                current.end = end;
                continue;
            }
            let outcome = self.outcome(start, end);
            let size = end - start;
            let events = match &outcome {
                RecordOutcome::Events { events, .. } => events.len(),
                RecordOutcome::Failure { .. } => 0,
            };
            let full = !current.outcomes.is_empty()
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
                return Some(current);
            }
            place(&mut current, outcome, events, end);
            chunk_carry = Some(self.carry.clone());
            bytes_in_chunk += size;
        }
        self.finished = true;
        if !current.outcomes.is_empty() || !self.emitted {
            current.carry = serde_json::to_value(&self.carry).expect("carry serializes");
            self.emitted = true;
            return Some(current);
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
