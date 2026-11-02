//! Evaluator reports forwarded by another product enter through the
//! ordinary authenticated ingress journal.
use super::*;
use serde_json::json;
use sinex_model::{MaterialId, Origin, Payloads, ReadingOrder, RecordLocation};

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
        let Ingress::Signals { class, .. } = frame else {
            return Err(Check::Malformed("not a signal frame".into()));
        };
        let authority = authority::write(&self.grants, producer)
            .map_err(Check::Configuration)?
            .clone();
        if !authority.forward_signals || !authority.classes.contains(class) {
            return Err(Check::Configuration(
                "forwarding evaluator reports requires the signal forwarding grant and the frame's class"
                    .into(),
            ));
        }
        if *class == sinex_model::Class::Sealed {
            return Err(Check::Configuration(
                "forwarded reports are routed in the open and cannot be sealed".into(),
            ));
        }
        let at = Timestamp::now();
        let value =
            serde_json::to_value(frame).map_err(|error| Check::Malformed(error.to_string()))?;
        let records = sinex_sources::sources::signals::forwarded_reports(
            &value,
            producer,
            &authority.family,
            at,
        )
        .map_err(|error| Check::Refused(error.to_string()))?;
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
                    json!({"media_type":"application/x-sinex-signals-frame","authenticated_producer":producer,"authenticated_family":authority.family,"received_at":at}),
                ),
            }),
            cursor: Value::Null,
            carry: Value::Null,
            memory: None,
        };
        let order = i64::try_from(at.unix_ns())
            .map_err(|_| Check::Unavailable("signal receipt order exceeds bigint".into()))?;
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

