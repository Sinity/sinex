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
    pub reading_order: &'a dyn Fn(usize) -> ReadingOrder,
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
pub fn drain<P: Parser>(
    parser: &P,
    input: DrainInput<'_>,
) -> Result<Vec<DrainOutput>, RecordError> {
    let mut carry: P::Carry = if input.carry.is_null() {
        P::Carry::default()
    } else {
        serde_json::from_value(input.carry.clone())
            .map_err(|e| RecordError::new(format!("stored parser carry does not decode: {e}")))?
    };
    let mut chunk_carry = carry.clone();
    let mut chunks = Vec::new();
    let mut current = DrainOutput {
        outcomes: Vec::new(),
        end: 0,
        carry: Value::Null,
        events: 0,
        failures: 0,
    };
    let mut bytes_in_chunk = 0usize;
    for &(start, end) in input.records {
        let raw = &input.bytes[start..end];
        // An excision blanks a record to spaces (its newline kept): no record
        // is left there, so nothing is read and nothing fails.
        if is_blanked(raw) {
            current.end = end;
            continue;
        }
        let location = RecordLocation {
            material: input.material,
            anchor: start as u64,
        };
        let context = RecordContext {
            manifest: input.manifest,
            member: member_at(input.manifest, start),
        };
        let outcome = match parse_one(parser, raw, &carry, &context) {
            Ok((payloads, next)) => {
                carry = next;
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
        };
        let size = end - start;
        let n = match &outcome {
            RecordOutcome::Events { events, .. } => events.len(),
            RecordOutcome::Failure { .. } => 0,
        };
        let full = !current.outcomes.is_empty()
            && (current.events + n > input.limits.max_events
                || bytes_in_chunk + size > input.limits.max_bytes);
        if full {
            // The closed chunk ends at the previous record; its carry is the
            // state after that record. Serialized once per chunk, not per
            // record, so a growing carry costs nothing per record.
            current.carry = serde_json::to_value(&chunk_carry).expect("carry serializes");
            chunks.push(std::mem::replace(
                &mut current,
                DrainOutput {
                    outcomes: Vec::new(),
                    end: 0,
                    carry: Value::Null,
                    events: 0,
                    failures: 0,
                },
            ));
            bytes_in_chunk = 0;
        }
        if matches!(outcome, RecordOutcome::Failure { .. }) {
            current.failures += 1;
        }
        current.events += n;
        current.outcomes.push(outcome);
        current.end = end;
        chunk_carry = carry.clone();
        bytes_in_chunk += size;
    }
    if !current.outcomes.is_empty() || chunks.is_empty() {
        current.carry = serde_json::to_value(&carry).expect("carry serializes");
        chunks.push(current);
    }
    Ok(chunks)
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
        parser.parse_record(&record, carry, context)
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
