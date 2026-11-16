//! Authored signal rules, the host deliverer's outcomes and the evaluator
//! reports other products forward.
use crate::sources::input_format;
use serde_json::{Map, Value, json};
use sinex_model::{
    InputRef, MaterialId, ObservationFacts, ReadingOrder, RevisionChange, Timestamp,
    event_types::signal::{ReportReceived, SignalRecord},
    json::JsonExt,
};
use sinex_source_core::{
    DeclaredSource, Expectation, Framer, Origin, RecordContext, RecordError, RecordResult,
    SourceSpec, Stateless, StatelessParser,
    discovery::DiscoverySpec,
    drivers::{
        Acquired, NewMaterial,
        export_archive::{Framing, frame_lines},
    },
};

pub const SPEC: SourceSpec = SourceSpec {
    name: "sinex.signals",
    parser: "signals",
    origin: Origin::Request,
    expect: Expectation::OnDemand,
    purpose: "authored signal rules, delivery outcomes and forwarded evaluator reports",
    event_types: &["signal.rule", "signal.outcome", "signal.report"],
    framer: Framer::export(
        frame_complete,
        Framing::Custom {
            name: "signals-json-values",
            frame,
        },
    ),
    ..SourceSpec::DEFAULT
};

/// Host JSON lines and a producer's complete JSON frame share exact byte
/// record offsets, including pretty-printed frames and an unterminated final value.
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

pub struct SignalParser;
impl StatelessParser for SignalParser {
    type Record<'a> = Map<String, Value>;
    type Payload = SignalRecord;
    fn parse(
        &self,
        record: &Map<String, Value>,
        context: &RecordContext<'_>,
    ) -> Result<Vec<SignalRecord>, RecordError> {
        let detail = |name: &str| {
            context
                .manifest
                .details
                .str(name)
                .record_with(|| format!("signal material lacks its {name}"))
        };
        if record.str("kind") == Some("signals") {
            let at = context
                .manifest
                .details
                .get("received_at")
                .cloned()
                .record("signal frame lacks receipt clock")?;
            let at: Timestamp = serde_json::from_value(at).record("invalid record")?;
            return forwarded_reports(
                &Value::Object(record.clone()),
                detail("authenticated_producer")?,
                detail("authenticated_family")?,
                at,
            )
            .map(|records| (records, ()));
        }
        let mut record: SignalRecord =
            serde_json::from_value(Value::Object(record.clone())).record("invalid record")?;
        match &mut record {
            SignalRecord::Rule(rule) => {
                rule.active.author = match detail("actor")? {
                    "operator" => signals::Author::Operator,
                    "sinexd" => signals::Author::Shipped,
                    actor => signals::Author::Agent(actor.into()),
                };
                rule.active.via = detail("via")?.into();
                if rule.name != rule.active.rule.name || rule.active.version.product != "sinex" {
                    return Err(RecordError::new(
                        "signal rule identity differs from its host product",
                    ));
                }
            }
            // Reports arrive only inside an authenticated producer's frame.
            SignalRecord::Report(_) => {
                return Err(RecordError::new(
                    "a forwarded report needs its authenticated frame",
                ));
            }
            SignalRecord::Outcome(_) => {}
        }
        Ok(vec![record])
    }
}

/// One record per report in a producer's frame. A report names only its own
/// product's rules: a producer cannot report for Sinex or another family.
pub fn forwarded_reports(
    frame: &Value,
    producer: &str,
    family: &str,
    at: Timestamp,
) -> Result<Vec<SignalRecord>, RecordError> {
    let sinex_protocol::Ingress::Signals { reports, .. } =
        serde_json::from_value(frame.clone()).record("invalid record")?
    else {
        return Err(RecordError::new("not a signal frame"));
    };
    reports
        .into_iter()
        .map(|report| {
            let product = match &report {
                signals::Report::Fired(fired) => &fired.trigger.rule.product,
                // Which product a cleared or retracted delivery belongs to is
                // checked by the deliverer, which knows its deliveries.
                signals::Report::Cleared { .. } | signals::Report::Retracted { .. } => family,
                signals::Report::Rule { version, .. }
                | signals::Report::RearmedLate { rule: version, .. } => &version.product,
                signals::Report::Withdrawn { product, .. }
                | signals::Report::Progress { product, .. } => product,
            };
            if product != family {
                return Err(RecordError::new(
                    "a forwarded report names a product other than its producer's family",
                ));
            }
            if let signals::Report::Fired(fired) = &report
                && fired.id != fired.trigger.delivery_id()
            {
                return Err(RecordError::new(
                    "a forwarded firing's identity differs from its trigger",
                ));
            }
            Ok(SignalRecord::Report(Box::new(ReportReceived {
                producer: producer.into(),
                family: family.into(),
                report,
                received_at: at,
            })))
        })
        .collect()
}

pub fn source() -> DeclaredSource<Stateless<SignalParser>> {
    DeclaredSource {
        spec: SPEC,
        parser: Stateless(SignalParser),
    }
}

pub fn material(
    records: &[SignalRecord],
    actor: &str,
    via: &str,
    host: &str,
    at: Timestamp,
) -> Acquired {
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
        memory: None,
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

            owned_files: vec![],
        }),
        cursor_without_material: Value::Null,
    }
}

input_format! { pub FORMAT = "signals" {
    specs: [SPEC], parser: |_b| Stateless(SignalParser),
    discovery: DiscoverySpec::DEFAULT,
    plan: none, reader: none,
} }

#[cfg(test)]
mod tests;
