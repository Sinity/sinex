//! The drain: everything around `Parser::parse`.
use crate::{Parser, RecordError, RecordShape};
use serde_json::Value;
use sinex_model::{Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome};

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
        let location = RecordLocation {
            material: input.material,
            anchor: start as u64,
        };
        let outcome = match parse_one(parser, raw, &carry) {
            Ok((payloads, next)) => {
                carry = next;
                let hash = <P::Record<'_> as RecordShape>::hash(raw, P::VOLATILE);
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
                    match Event::build(
                        payload,
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
        current.carry = serde_json::to_value(&carry).expect("carry serializes");
        bytes_in_chunk += size;
    }
    if !current.outcomes.is_empty() || chunks.is_empty() {
        if current.outcomes.is_empty() {
            current.carry = serde_json::to_value(&carry).expect("carry serializes");
        }
        chunks.push(current);
    }
    Ok(chunks)
}

fn parse_one<P: Parser>(
    parser: &P,
    raw: &[u8],
    carry: &P::Carry,
) -> Result<(Vec<P::Payload>, P::Carry), RecordError> {
    let record = <P::Record<'_> as RecordShape>::decode(raw)?;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        parser.parse(&record, carry)
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

fn has_nul(value: &Value) -> bool {
    match value {
        Value::String(s) => s.contains('\0'),
        Value::Array(items) => items.iter().any(has_nul),
        Value::Object(map) => map.iter().any(|(k, v)| k.contains('\0') || has_nul(v)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_model::{Timestamp, kinds::CommandImported};

    /// Lines `n:command`; carry counts records; "panic" panics; "bad" fails.
    struct Lines;
    impl Parser for Lines {
        type Record<'a> = &'a [u8];
        type Carry = u32;
        type Payload = CommandImported;
        fn parse(
            &self,
            record: &&[u8],
            carry: &u32,
        ) -> Result<(Vec<CommandImported>, u32), RecordError> {
            let text = std::str::from_utf8(record)
                .map_err(|_| RecordError::new("utf8"))?
                .trim_end();
            if text == "panic" {
                panic!("boom");
            }
            if text == "bad" {
                return Err(RecordError::new("bad record"));
            }
            let payload = CommandImported {
                command: text.into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(i64::from(*carry)).unwrap(),
                header_elapsed: 0,
            };
            Ok((vec![payload], carry + 1))
        }
    }

    fn lines(bytes: &[u8]) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut start = 0;
        for (i, b) in bytes.iter().enumerate() {
            if *b == b'\n' {
                out.push((start, i + 1));
                start = i + 1;
            }
        }
        out
    }

    fn run(bytes: &[u8], carry: Value, limits: Limits) -> Vec<DrainOutput> {
        let records = lines(bytes);
        drain(
            &Lines,
            DrainInput {
                source: "t",
                class: Class::Open,
                material: MaterialId(uuid::Uuid::nil()),
                bytes,
                records: &records,
                carry,
                reading_order: &|a| ReadingOrder(a as i64),
                limits,
            },
        )
        .unwrap()
    }

    fn commands(chunks: &[DrainOutput]) -> Vec<(u64, String)> {
        chunks
            .iter()
            .flat_map(|c| &c.outcomes)
            .filter_map(|o| match o {
                RecordOutcome::Events { location, events } => Some((
                    location.unwrap().anchor,
                    events[0].payload["command"].as_str().unwrap().to_owned(),
                )),
                RecordOutcome::Failure { .. } => None,
            })
            .collect()
    }

    #[test]
    fn panics_and_errors_fail_only_their_record() {
        let out = run(b"a\npanic\nbad\nb\n", Value::Null, Limits::default());
        assert_eq!(commands(&out), vec![(0, "a".into()), (12, "b".into())]);
        let failures: Vec<_> = out[0]
            .outcomes
            .iter()
            .filter_map(|o| match o {
                RecordOutcome::Failure { location, reason } => {
                    Some((location.anchor, reason.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            failures,
            vec![
                (2, "parser panicked: boom".into()),
                (8, "bad record".into())
            ]
        );
    }

    #[test]
    fn chunks_respect_limits_and_carry_continues() {
        let bytes = b"a\nb\nc\nd\ne\n";
        let out = run(
            bytes,
            Value::Null,
            Limits {
                max_events: 2,
                max_bytes: 1 << 20,
            },
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].end, 4);
        assert_eq!(out[0].carry, serde_json::json!(2));
        assert_eq!(out[2].carry, serde_json::json!(5));
        // Resuming from a chunk's carry gives the same events as one pass.
        let tail = run(&bytes[4..], out[0].carry.clone(), Limits::default());
        let times: Vec<_> = tail[0]
            .outcomes
            .iter()
            .map(|o| match o {
                RecordOutcome::Events { events, .. } => {
                    events[0].ts_orig.at.unwrap().0.unix_timestamp()
                }
                _ => -1,
            })
            .collect();
        assert_eq!(times, vec![2, 3, 4]);
    }

    #[test]
    fn oversized_record_goes_alone() {
        let big = format!("{}\n", "x".repeat(100));
        let bytes = format!("a\n{big}b\n");
        let out = run(
            bytes.as_bytes(),
            Value::Null,
            Limits {
                max_events: 100,
                max_bytes: 10,
            },
        );
        assert_eq!(
            out.iter().map(|c| c.outcomes.len()).collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
    }

    #[test]
    fn nul_payloads_become_failures() {
        let out = run(b"a\0b\n", Value::Null, Limits::default());
        assert!(matches!(out[0].outcomes[0], RecordOutcome::Failure { .. }));
    }
}
