//! Shared signal forwarding uses the ordinary authenticated ingress journal.
use super::*;
use serde_json::json;
use sinex_model::{EventPayload, MaterialId, Origin, Payloads, ReadingOrder, RecordLocation};

impl Ingest {
    pub(super) async fn signal_run(
        &mut self,
        producer: &str,
    ) -> Result<sinex_model::RunId, String> {
        let key = format!("signals:{producer}");
        if let Some(run) = self.runs.get(&key) {
            return Ok(*run);
        }
        let spec = sinex_sources::sources::signals::SPEC;
        let version = spec.parser_version.to_string();
        let run = sinex_store::runs::start(
            &mut self.conn,
            &sinex_store::runs::Manifest {
                name: spec.parser,
                kind: "source",
                version: &version,
                build: crate::build_identity::current()?,
                emits: spec.event_types,
            },
            &self.host,
        )
        .await
        .map_err(|error| error.to_string())?;
        self.runs.insert(key, run);
        Ok(run)
    }
    pub(super) async fn signals_frame(
        &mut self,
        producer: &str,
        frame: &Ingress,
        outer: Option<&Ingress>,
        body: &[u8],
    ) -> Result<(Option<SourceProgress>, Vec<RecordOutcome>), Check> {
        let Ingress::Signals {
            class,
            receipts,
            emergency,
            signals,
            coverage,
            ..
        } = frame
        else {
            return Err(Check::Malformed("not a signal frame".into()));
        };
        let authority = authority::write(&self.grants, producer)
            .map_err(Check::Configuration)?
            .clone();
        if !authority.forward_signals
            || authority.canonical_prefixes.is_empty()
            || !authority.classes.contains(class)
        {
            return Err(Check::Configuration(
                "signal forwarding requires the configured canonical family and class capability"
                    .into(),
            ));
        }
        if *class == sinex_model::Class::Sealed {
            return Err(Check::Configuration(
                "sealed producer intents require an unlocked acquisition path".into(),
            ));
        }
        let certified = if let Some(proof) = coverage {
            Some(
                sinex_store::read_coverage::open(&mut self.conn, &proof.stamp)
                    .await
                    .map_err(|error| Check::Refused(error.to_string()))?,
            )
        } else {
            None
        };
        if let Some(claims) = &certified {
            let grant = self.grants.get(producer).ok_or_else(|| {
                Check::Configuration("signal input read grants are unavailable".into())
            })?;
            let epoch = sinex_store::privacy::projection::epoch(&mut self.conn)
                .await
                .map_err(|error| Check::Unavailable(error.to_string()))?;
            for id in signals
                .iter()
                .flat_map(|signal| &signal.inputs)
                .chain(emergency.iter().flat_map(|completion| &completion.inputs))
            {
                let Some(input) = sinex_store::producer_reads::event_output(&mut self.conn, id.0)
                    .await
                    .map_err(|error| Check::Unavailable(error.to_string()))?
                else {
                    return Err(Check::Refused("signal input reading is unavailable".into()));
                };
                let class = input.class.as_ref().and_then(|class| {
                    serde_json::from_value::<sinex_model::Class>(Value::String(class.clone())).ok()
                });
                let position = sinex_store::signals::input_position(&mut self.conn, *id)
                    .await
                    .map_err(|error| Check::Unavailable(error.to_string()))?;
                if !epoch.ready()
                    || input.epoch != Some(claims.policy_epoch)
                    || input.epoch != Some(epoch.active)
                    || input.state.as_deref() != Some("complete")
                    || !class.is_some_and(|class| {
                        class != sinex_model::Class::Sealed && grant.allows(&input.kind, class)
                    })
                    || !claims.kinds.iter().any(|(kind, _)| kind.name == input.kind)
                    || !position.is_some_and(|position| position <= claims.position)
                {
                    return Err(Check::Refused(
                        "signal input is outside its certified readable basis".into(),
                    ));
                }
            }
        }
        for completion in emergency {
            if authority.emergency_sinex_blocked_host.as_deref() != Some(completion.host.as_str())
                || completion.host != self.host
            {
                return Err(Check::Configuration(
                    "emergency completion is not authorized for this producer and host".into(),
                ));
            }
            sinex_protocol::signals::validate_emergency(completion, &authority.family)
                .map_err(Check::Refused)?;
            let context = crate::public_context::verify_token(
                &mut self.conn,
                producer,
                &self.host,
                &completion.context_token,
            )
            .await
            .map_err(Check::Refused)?;
            let sinex_protocol::PublicContext::Available {
                day,
                quiet_hours,
                observed_at,
                ..
            } = context
            else {
                return Err(Check::Refused(
                    "emergency completion lacks an available owner policy context".into(),
                ));
            };
            if observed_at > completion.completed_at || completion.completed_at > Timestamp::now() {
                return Err(Check::Refused(
                    "emergency completion is outside its observed owner-policy interval".into(),
                ));
            }
            let zone = jiff::tz::TimeZone::get(&day.zone)
                .map_err(|error| Check::Refused(error.to_string()))?;
            let calendar = ::signals::DayCalendar::new(
                day.resolve(zone)
                    .map_err(|error| Check::Refused(error.to_string()))?,
            );
            let at =
                i64::try_from(completion.completed_at.unix_ns() / 1_000_000).map_err(|_| {
                    Check::Refused("emergency completion clock exceeds milliseconds".into())
                })?;
            if completion.intent.quiet_hours != quiet_hours
                || quiet_hours.is_some_and(|hours| {
                    ::signals::Calendar::quiet_until(&calendar, at, hours).is_some()
                })
            {
                return Err(Check::Refused(
                    "emergency completion contradicts its cached owner quiet policy".into(),
                ));
            }
        }
        let at = Timestamp::now();
        let value =
            serde_json::to_value(frame).map_err(|error| Check::Malformed(error.to_string()))?;
        let records = sinex_sources::sources::signals::forwarded_records(
            &value,
            producer,
            &authority.family,
            at,
            authority.author_attestation.as_ref(),
        )
        .map_err(|error| Check::Refused(error.to_string()))?;
        for record in &records {
            match record {
                sinex_model::event_types::signal::SignalRecord::Intent(incoming) => {
                    if let Some(previous) = sinex_store::signals::frozen_payload(
                        &mut self.conn,
                        sinex_model::event_types::signal::IntentRecorded::EVENT_TYPE,
                        &incoming.delivery_id,
                        &authority.family,
                        sinex_model::event_types::signal::ReceivedRecorded::EVENT_TYPE,
                    )
                    .await
                    .map_err(|error| Check::Unavailable(error.to_string()))?
                    {
                        if previous.position.is_none()
                            || certified.as_ref().is_none_or(|claims| {
                                previous
                                    .position
                                    .is_some_and(|position| claims.position < position)
                            })
                        {
                            return Err(Check::Refused(
                                "signal certified input position regressed or is unknown".into(),
                            ));
                        }
                        if previous.settings_identity.as_deref()
                            != certified
                                .as_ref()
                                .map(|claims| claims.settings_identity.as_str())
                        {
                            return Err(Check::Refused(
                                "signal original settings basis changed".into(),
                            ));
                        }
                        let previous: sinex_model::event_types::signal::IntentRecorded =
                            serde_json::from_value(previous.payload)
                                .map_err(|error| Check::Unavailable(error.to_string()))?;
                        if previous.intent != incoming.intent
                            || previous.notice != incoming.notice
                            || previous.active_rule!=incoming.active_rule
                            || !sinex_store::signals::same_input_lineage(
                                &mut self.conn,
                                &previous.inputs,
                                &incoming.inputs,
                            )
                            .await
                            .map_err(|error| Check::Unavailable(error.to_string()))?
                        {
                            return Err(Check::Refused(
                                "signal delivery ID already has different frozen evidence".into(),
                            ));
                        }
                    }
                }
                sinex_model::event_types::signal::SignalRecord::Emergency(incoming) => {
                    if let Some(previous) = sinex_store::signals::frozen_payload(
                        &mut self.conn,
                        sinex_model::event_types::signal::EmergencyCompleted::EVENT_TYPE,
                        &incoming.delivery_id,
                        &authority.family,
                        sinex_model::event_types::signal::ReceivedRecorded::EVENT_TYPE,
                    )
                    .await
                    .map_err(|error| Check::Unavailable(error.to_string()))?
                    {
                        if previous.payload
                            != serde_json::to_value(incoming)
                                .map_err(|error| Check::Malformed(error.to_string()))?
                        {
                            return Err(Check::Refused(
                                "emergency transfer ID already has different frozen evidence"
                                    .into(),
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        for receipt in receipts {
            if !sinex_store::signals::owns_handoff(
                &mut self.conn,
                sinex_model::event_types::signal::DeliveryRecorded::EVENT_TYPE,
                &receipt.delivery_id,
                &authority.family,
            )
            .await
            .map_err(|error| Check::Unavailable(error.to_string()))?
            {
                return Err(Check::Refused(
                    "producer receipts belong only to this family's value/action handoffs".into(),
                ));
            }
        }
        let blobs = self.blobs.clone();
        let root = self.blobs.root().to_owned();
        let producer_owned = producer.to_owned();
        let outer = outer.cloned();
        let body = body.to_vec();
        let (blob, length, hash) = tokio::task::spawn_blocking(move || -> Result<_, Check> {
            match outer {
                Some(Ingress::LargeFrame {
                    frame,
                    blob,
                    length,
                }) => prepare(
                    &blobs,
                    sinex_transport::frame_bytes::reader(
                        &root,
                        &producer_owned,
                        &frame,
                        &blob,
                        length,
                    )
                    .map_err(|error| Check::Unavailable(error.to_string()))?,
                ),
                _ => prepare(&blobs, std::io::Cursor::new(body)),
            }
        })
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))??;
        let material = MaterialId::new();
        let input = sinex_model::InputRef {
            kind: "signal-frame".into(),
            locator: frame.frame().into(),
            host: producer.into(),
        };
        let progress = SourceProgress {
            source: None,
            host: None,
            binding: format!("ingress:{producer}"),
            data_keys: vec![],
            input,
            observation: ObservationFacts {
                observed_at: Some(at),
                size: Some(length),
                changed: true,
                ..Default::default()
            },
            revision: RevisionChange::Appended,
            material: Some(MaterialRef {
                id: material,
                input: None,
                blob,
                length,
                part_index: 0,
                previous_part: None,
                parser: "signals".into(),
                parser_version: 1,
                manifest: sinex_model::Manifest::from_json(
                    json!({"media_type":"application/x-sinex-signals-frame","authenticated_producer":producer,"authenticated_family":authority.family,"received_at":at,"author_attestation":authority.author_attestation}),
                ),
            }),
            cursor: Value::Null,
            carry: Value::Null,
            memory: None,
        };
        let order = i64::try_from(at.unix_ns())
            .map_err(|_| Check::Unavailable("signal receipt order exceeds bigint".into()))?;
        let records = sinex_sources::sources::signals::parsed_records(records)
            .map_err(|error| Check::Refused(error.to_string()))?;
        let events = records
            .iter()
            .enumerate()
            .map(|(index, record)| {
                record
                    .build(
                        "sinex.signals",
                        Origin::Record {
                            location: RecordLocation {
                                material,
                                anchor: 0,
                            },
                            output_index: u64::try_from(index).map_err(|_| {
                                Check::Unavailable("signal output index exceeds u64".into())
                            })?,
                        },
                        ReadingOrder(order),
                        hash,
                        *class,
                    )
                    .map_err(|error| Check::Refused(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            Some(progress),
            vec![RecordOutcome::Events {
                location: Some(RecordLocation {
                    material,
                    anchor: 0,
                }),
                events,
            }],
        ))
    }
}

fn prepare(
    blobs: &Blobs,
    mut input: impl std::io::Read + std::io::Seek,
) -> Result<(String, u64, [u8; 32]), Check> {
    use std::io::SeekFrom;
    let length = input
        .seek(SeekFrom::End(0))
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    input
        .rewind()
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    let mut hasher = blake3::Hasher::new();
    std::io::copy(&mut input, &mut hasher)
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    input
        .rewind()
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    let blob = blobs
        .put_seekable(input)
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    Ok((blob, length, *hasher.finalize().as_bytes()))
}
