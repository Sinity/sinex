use async_nats::jetstream;
use futures::StreamExt;
use serde_json::json;
use sinex_model::FrameRefusal;
use sinex_protocol::{Acknowledgment, Ingress, RecordedQuery, Subjects};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// A stand-in for `sinexd`'s ingress that records the frames it receives
/// and acknowledges each one.
pub(crate) async fn standin(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
) -> Arc<Mutex<Vec<Ingress>>> {
    standin_with_refusal(nats, subjects, producer, None, 0, FrameRefusal::Content).await
}

pub(crate) async fn standin_with_refusal(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
    refuse_offset: Option<u64>,
    remaining_refusals: usize,
    refusal_kind: FrameRefusal,
) -> Arc<Mutex<Vec<Ingress>>> {
    refusing(
        nats,
        subjects,
        producer,
        refuse_offset,
        remaining_refusals,
        refusal_kind,
        false,
    )
    .await
}

pub(crate) async fn refusing(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
    mut refuse_offset: Option<u64>,
    mut remaining_refusals: usize,
    refusal_kind: FrameRefusal,
    refuse_material: bool,
) -> Arc<Mutex<Vec<Ingress>>> {
    use async_nats::jetstream::{consumer, stream};
    let client = async_nats::connect(nats.url()).await.unwrap();
    let mut declarations = client.subscribe(subjects.connect(producer)).await.unwrap();
    let declarer = client.clone();
    tokio::spawn(async move {
        while let Some(message) = declarations.next().await {
            sinex_transport::replies::publish_json(
                &declarer,
                message.reply.unwrap().to_string(),
                sinex_protocol::ProducerWelcome {
                    refusal: None,
                    ..Default::default()
                },
                std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
        }
    });
    let js = jetstream::new(client.clone());
    let stream = js
        .get_or_create_stream(stream::Config {
            name: subjects.ingress_stream(),
            subjects: vec![subjects.ingress_all()],
            ..Default::default()
        })
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some("standin".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    let received: Arc<Mutex<Vec<Ingress>>> = Arc::default();
    let recorded = Arc::new(Mutex::new(HashMap::<String, Acknowledgment>::new()));
    let mut queries = client.subscribe(subjects.recorded(producer)).await.unwrap();
    client.flush().await.unwrap();
    let answers = recorded.clone();
    let responder = client.clone();
    tokio::spawn(async move {
        while let Some(message) = queries.next().await {
            let query: RecordedQuery = serde_json::from_slice(&message.payload).unwrap();
            let acknowledgments: Vec<_> = {
                let rows = answers.lock().unwrap();
                rows.values()
                    .filter(|receipt| {
                        (query.frames.contains(&receipt.frame)
                            || receipt
                                .number
                                .is_some_and(|number| query.numbers.contains(&number)))
                            && receipt.number.is_some_and(|number| number > query.after)
                    })
                    .cloned()
                    .collect()
            };
            sinex_transport::replies::publish_json(
                &responder,
                message.reply.unwrap().to_string(),
                sinex_protocol::RecordedPage {
                    acknowledgments,
                    next: None,
                },
                std::time::Duration::from_secs(10),
            )
            .await
            .unwrap();
        }
    });
    let (seen, acker, ack_subject) = (received.clone(), client, subjects.ack(producer));
    tokio::spawn(async move {
        let mut messages = consumer.messages().await.unwrap();
        while let Some(Ok(message)) = messages.next().await {
            let frame: Ingress = serde_json::from_slice(&message.payload).unwrap();
            let malformed = format!("malformed/{}", blake3::hash(&message.payload).to_hex());
            let previous = {
                let answers = recorded.lock().unwrap();
                answers
                    .get(frame.frame())
                    .or_else(|| answers.get(&malformed))
                    .cloned()
            };
            if let Some(ack) = previous {
                seen.lock().unwrap().push(frame);
                acker
                    .publish(
                        ack_subject.clone(),
                        serde_json::to_vec(&sinex_protocol::ReceiptHint {
                            number: ack.number.unwrap(),
                            frame_hash: blake3::hash(ack.frame.as_bytes()).to_hex().to_string(),
                        })
                        .unwrap()
                        .into(),
                    )
                    .await
                    .unwrap();
                message.ack().await.unwrap();
                continue;
            }
            let (material, mut admitted) = match &frame {
                Ingress::Material { material, .. } => (Some(material.id), vec![]),
                Ingress::Events { events, .. } => (
                    None,
                    events.iter().map(|_| sinex_model::EventId::new()).collect(),
                ),
                Ingress::Chunk { .. }
                | Ingress::Signals { .. }
                | Ingress::LargeFrame { .. }
                | Ingress::RegisterKinds { .. }
                | Ingress::ReplaySequence { .. } => (None, vec![]),
            };
            let refusal = match &frame {
                Ingress::Chunk { offset, .. } if refuse_offset == Some(*offset) => {
                    refuse_offset = None;
                    Some("synthetic interruption".into())
                }
                Ingress::Material { .. } if refuse_material && remaining_refusals > 0 => {
                    remaining_refusals -= 1;
                    Some("synthetic invalid material".into())
                }
                Ingress::Events { .. } | Ingress::LargeFrame { .. }
                    if !refuse_material && remaining_refusals > 0 && frame.frame() != "first" =>
                {
                    remaining_refusals -= 1;
                    Some("synthetic invalid event".into())
                }
                _ => None,
            };
            if refusal.is_some() {
                admitted.clear();
            }
            let commit_seq = if refusal.is_some() || admitted.is_empty() {
                None
            } else {
                Some(1)
            };
            let number = {
                let rows = recorded.lock().unwrap();
                rows.get(frame.frame())
                    .and_then(|receipt| receipt.number)
                    .unwrap_or(rows.len() as i64 + 1)
            };
            let content_hash = sinex_transport::producer::content_hash(&frame).unwrap();
            let ack = Acknowledgment {
                number: Some(number),
                material_extents: vec![],
                frame: if frame.frame().is_empty() || frame.frame().contains('\0') {
                    format!("malformed/{}", blake3::hash(&message.payload).to_hex())
                } else {
                    frame.frame().to_owned()
                },
                material,
                admitted,
                outcome: if refusal.is_some() {
                    json!({"refusal_kind": if matches!(frame, Ingress::Chunk { .. }) { FrameRefusal::Bytes } else { refusal_kind }, "failures": 1, "frame_content_hash": content_hash})
                } else {
                    json!({"frame_content_hash": content_hash})
                },
                refusal,
                commit_seq,
            };
            recorded
                .lock()
                .unwrap()
                .insert(ack.frame.clone(), ack.clone());
            seen.lock().unwrap().push(frame);
            acker
                .publish(
                    ack_subject.clone(),
                    serde_json::to_vec(&sinex_protocol::ReceiptHint {
                        number: ack.number.unwrap(),
                        frame_hash: blake3::hash(ack.frame.as_bytes()).to_hex().to_string(),
                    })
                    .unwrap()
                    .into(),
                )
                .await
                .unwrap();
            message.ack().await.unwrap();
        }
    });
    received
}
