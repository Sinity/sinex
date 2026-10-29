//! Canonical signal rules, authenticated intents and host delivery observations.
use serde_json::{Map, Value, json};
use sinex_model::event_types::signal::{IntentRecorded, ReceiptRecorded, SignalRecord};
use sinex_model::{Class, Timestamp};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};

pub const SPEC: SourceSpec = SourceSpec {
    name: "sinex.signals",
    parser: "signals",
    parser_version: 1,
    driver: Driver::ApiRequest,
    purpose: "canonical signal rules and host delivery receipts",
    class: Class::Open,
    excisable: false,
    event_types: &[
        "signal.rule",
        "signal.checkpoint",
        "signal.evaluation",
        "signal.intent",
        "signal.emergency",
        "signal.delivery",
        "signal.handoff",
        "signal.received",
        "signal.receipt",
        "signal.desktop_receipt",
    ],
    framer: sinex_source_core::Framer::export(
        frame_complete,
        sinex_source_core::drivers::export_archive::Framing::Custom {
            name: "signals-json-values",
            frame,
        },
    ),
};

/// Host JSON lines and a producer's complete JSON frame share exact byte
/// anchors, including pretty-printed frames and an unterminated final value.
fn frame_complete(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    frame(bytes, true)
}

fn frame(bytes: &[u8], eof: bool) -> (Vec<(usize, usize)>, usize) {
    let mut records = Vec::new();
    let mut offset = 0;
    loop {
        let Some(start) = bytes[offset..]
            .iter()
            .position(|byte| !byte.is_ascii_whitespace())
            .map(|start| offset + start)
        else {
            return (records, if eof { bytes.len() } else { offset });
        };
        let mut values = serde_json::Deserializer::from_slice(&bytes[start..])
            .into_iter::<serde::de::IgnoredAny>();
        match values.next() {
            Some(Ok(_)) => {
                #[derive(serde::Deserialize)]
                struct Header<'a> {
                    #[serde(borrow, default)]
                    kind: Option<&'a str>,
                }
                if records.is_empty()
                    && serde_json::from_slice::<Header<'_>>(
                        &bytes[start..start + values.byte_offset()],
                    )
                    .ok()
                    .is_some_and(|header| header.kind == Some("signals"))
                {
                    return if eof {
                        (vec![(0, bytes.len())], bytes.len())
                    } else {
                        (vec![], 0)
                    };
                }
                let end = start + values.byte_offset();
                let trailing = &bytes[end..];
                let next = trailing
                    .iter()
                    .position(|byte| !byte.is_ascii_whitespace())
                    .map(|next| end + next);
                let newline = trailing
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map(|newline| end + newline + 1)
                    .filter(|newline| next.is_none_or(|next| *newline <= next));
                let decided = if let Some(newline) = newline {
                    newline
                } else if next.is_some() {
                    end
                } else if eof {
                    bytes.len()
                } else {
                    return (records, offset);
                };
                records.push((offset, decided));
                offset = decided;
                if offset == bytes.len() {
                    return (records, offset);
                }
            }
            _ if eof => {
                records.push((offset, bytes.len()));
                return (records, bytes.len());
            }
            _ => return (records, offset),
        }
    }
}

/// The acquisition parent is minted together with its derived reading, including
/// native replay. The writer resolves repeated acquisitions before admission.
pub struct ParsedSignal {
    pub record: SignalRecord,
    id: sinex_model::EventId,
    parents: Option<Vec<sinex_model::EventId>>,
}
impl ParsedSignal {
    fn source(record: SignalRecord) -> Self {
        Self {
            record,
            id: sinex_model::EventId::new(),
            parents: None,
        }
    }
}
impl sinex_model::Payloads for ParsedSignal {
    fn build(
        &self,
        source: &str,
        origin: sinex_model::Origin,
        order: sinex_model::ReadingOrder,
        hash: [u8; 32],
        class: Class,
    ) -> Result<sinex_model::Event, sinex_model::BuildError> {
        let origin = self
            .parents
            .as_ref()
            .map_or(origin, |parents| sinex_model::Origin::Derived {
                parents: parents.clone(),
            });
        let mut event = self.record.build(source, origin, order, hash, class)?;
        event.id = self.id;
        Ok(event)
    }
}

/// Full copied evidence has explicit parents; only its minimal acquisition is
/// an independent fact. Parent references identify exact canonical readings.
pub fn parsed_records(records: Vec<SignalRecord>) -> Result<Vec<ParsedSignal>, RecordError> {
    use sinex_model::event_types::signal::ReceivedRecorded;
    let mut parsed = Vec::new();
    for record in records {
        let full = match &record {
            SignalRecord::Intent(value) => {
                Some((&value.delivery_id, &value.inputs, value.received_at))
            }
            SignalRecord::Emergency(value) => {
                Some((&value.delivery_id, &value.inputs, value.completed_at))
            }
            _ => None,
        };
        let parents = if let Some((id, inputs, at)) = full {
            let received = ParsedSignal::source(SignalRecord::Received(ReceivedRecorded {
                delivery_id: id.clone(),
                received_at: at,
            }));
            let mut parents = vec![received.id];
            parents.extend_from_slice(inputs);
            parents.sort_unstable();
            parents.dedup();
            parsed.push(received);
            Some(parents)
        } else {
            None
        };
        parsed.push(ParsedSignal {
            record,
            id: sinex_model::EventId::new(),
            parents,
        });
    }
    Ok(parsed)
}

pub struct SignalParser;
impl Parser for SignalParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = ParsedSignal;
    fn parse(
        &self,
        record: &Map<String, Value>,
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<ParsedSignal>, ()), RecordError> {
        if record.get("kind").and_then(Value::as_str) == Some("signals") {
            let producer = context
                .manifest
                .details
                .get("authenticated_producer")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    RecordError::new("signal frame lacks authenticated producer provenance")
                })?;
            let family = context
                .manifest
                .details
                .get("authenticated_family")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    RecordError::new("signal frame lacks authenticated family provenance")
                })?;
            let at = context
                .manifest
                .details
                .get("received_at")
                .cloned()
                .ok_or_else(|| RecordError::new("signal frame lacks receipt clock"))?;
            let at: Timestamp =
                serde_json::from_value(at).map_err(|error| RecordError::new(error.to_string()))?;
            let attestation = context
                .manifest
                .details
                .get("author_attestation")
                .filter(|value| !value.is_null())
                .cloned()
                .map(serde_json::from_value::<sinex_protocol::signals::AuthorAttestation>)
                .transpose()
                .map_err(|error| RecordError::new(error.to_string()))?;
            return forwarded_records(
                &Value::Object(record.clone()),
                producer,
                family,
                at,
                attestation.as_ref(),
            )
            .and_then(parsed_records)
            .map(|records| (records, ()));
        }
        let mut record: SignalRecord = serde_json::from_value(Value::Object(record.clone()))
            .map_err(|error| RecordError::new(error.to_string()))?;
        if let SignalRecord::Rule(rule) = &mut record {
            let actor = context
                .manifest
                .details
                .get("actor")
                .and_then(Value::as_str)
                .ok_or_else(|| RecordError::new("signal rule has no authenticated actor"))?;
            let via = context
                .manifest
                .details
                .get("via")
                .and_then(Value::as_str)
                .ok_or_else(|| RecordError::new("signal rule has no authenticated route"))?;
            rule.active.author = match actor {
                "operator" => signals::Author::Operator,
                "sinexd" => signals::Author::Shipped,
                _ => signals::Author::Agent(actor.into()),
            };
            rule.active.via = via.into();
            if rule.name != rule.active.rule.name || rule.active.version.product != "sinex" {
                return Err(RecordError::new(
                    "signal rule identity differs from its host product",
                ));
            }
        }
        Ok((vec![ParsedSignal::source(record)], ()))
    }
}

pub fn forwarded_records(
    frame: &Value,
    producer: &str,
    family: &str,
    at: Timestamp,
    attestation: Option<&sinex_protocol::signals::AuthorAttestation>,
) -> Result<Vec<SignalRecord>, RecordError> {
    let sinex_protocol::Ingress::Signals {
        signals,
        receipts,
        emergency,
        ..
    } = serde_json::from_value(frame.clone())
        .map_err(|error| RecordError::new(error.to_string()))?
    else {
        return Err(RecordError::new("not a signal frame"));
    };
    let mut records = Vec::with_capacity(signals.len() + receipts.len());
    for signal in signals {
        let intent = signal.intent;
        let mut notice = signal.notice;
        sinex_protocol::signals::authorize_author(
            &intent,
            signal.active_rule.as_ref(),
            family,
            attestation,
        )
        .map_err(RecordError::new)?;
        if intent.delivery.trigger.rule.id == sinex_protocol::signals::EMERGENCY_RULE_ID {
            return Err(RecordError::new(
                "compiled emergency handoffs require the authenticated completion path",
            ));
        }
        if intent.delivery.trigger.rule.product != family
            || intent.delivery.id != intent.delivery.trigger.delivery_id()
            || notice.delivery_id != intent.delivery.id
            || notice.product != family
            || notice.current != intent.current
        {
            return Err(RecordError::new(
                "forwarded signal identity differs from authenticated family or frozen basis",
            ));
        }
        // Final interruption and capture-loss urgency belong to this host.
        notice.interrupt = false;
        notice.data_loss = false;
        notice.budget_passive_count = None;
        records.push(SignalRecord::Intent(Box::new(IntentRecorded {
            active_rule: signal.active_rule,
            inputs: signal.inputs,
            delivery_id: intent.delivery.id.clone(),
            producer: producer.into(),
            intent,
            notice,
            received_at: at,
        })));
    }
    for receipt in receipts {
        records.push(SignalRecord::Receipt(ReceiptRecorded {
            delivery_id: receipt.delivery_id.clone(),
            route: receipt.route.clone(),
            receipt,
            recorded_at: at,
        }));
    }
    for completion in emergency {
        sinex_protocol::signals::validate_emergency(&completion, family)
            .map_err(RecordError::new)?;
        records.push(SignalRecord::Handoff(
            sinex_model::event_types::signal::HandoffRecorded {
                delivery_id: completion.delivery_id.clone(),
                channel: completion.intent.delivery.channel.clone(),
                handed_at: completion.invoked_at,
            },
        ));
        for receipt in &completion.receipts {
            records.push(SignalRecord::DesktopReceipt(
                sinex_model::event_types::signal::DesktopReceiptRecorded {
                    delivery_id: completion.delivery_id.clone(),
                    route: format!("{:?}", receipt.route).to_lowercase(),
                    receipt: receipt.clone(),
                    recorded_at: completion.completed_at,
                },
            ));
        }
        records.push(SignalRecord::Emergency(Box::new(completion)));
    }
    Ok(records)
}

pub fn source() -> DeclaredSource<SignalParser> {
    DeclaredSource {
        spec: SPEC,
        parser: SignalParser,
    }
}

pub fn material(
    records: &[SignalRecord],
    actor: &str,
    via: &str,
    host: &str,
    at: Timestamp,
) -> sinex_source_core::drivers::Acquired {
    use sinex_model::{InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange};
    use sinex_source_core::drivers::{Acquired, NewMaterial};
    let mut bytes = Vec::new();
    let mut framed = Vec::new();
    for record in records {
        let start = bytes.len();
        serde_json::to_writer(&mut bytes, record).expect("typed signal record serializes");
        bytes.push(b'\n');
        framed.push((start, bytes.len()));
    }
    let order =
        i64::try_from(at.unix_ns()).expect("current signal receipt clock fits reading order");
    Acquired {
        input: InputRef {
            kind: "signal".into(),
            locator: "host".into(),
            host: host.into(),
        },
        observation: ObservationFacts {
            observed_at: Some(at),
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Appended,
        material: Some(NewMaterial {
            id: MaterialId::new(),
            bytes,
            stored: None,
            records: framed,
            part_index: 0,
            previous_part: None,
            manifest: sinex_model::Manifest::from_json(
                json!({"path":"signals","host":host,"actor":actor,"via":via,"media_type":"application/x-sinex-signals"}),
            ),
            reading_order: Box::new(move |_| ReadingOrder(order)),
            cursor_at: Box::new(|_| Ok(Value::Null)),
            aligned: false,
            removed: vec![],
            memory: None,
            owned_files: vec![],
        }),
        cursor_without_material: Value::Null,
    }
}

pub const FORMAT: sinex_source_core::binding::InputFormat =
    sinex_source_core::binding::InputFormat {
        name: "signals",
        discovery: sinex_source_core::discovery::DiscoverySpec {
            owner: sinex_source_core::discovery::Owner::System,
            locations: &[],
            capabilities: &[],
            credentials: &[],
            streams: &[],
            requests: &["signal"],
            request_only: true,
        },
        discover: crate::discovery::discover_format,
        specs: || vec![&SPEC],
        plan: None,
        source: |_| Ok(Box::new(source())),
        reader: None,
        capture: None,
    };

#[cfg(test)]
mod tests;
