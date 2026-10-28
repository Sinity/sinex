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
            declarer
                .publish(
                    message.reply.unwrap(),
                    serde_json::to_vec(&sinex_protocol::ProducerWelcome { refusal: None })
                        .unwrap()
                        .into(),
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
            let frames: Vec<_> = {
                let answers = answers.lock().unwrap();
                query
                    .frames
                    .iter()
                    .filter_map(|frame| answers.get(frame).cloned())
                    .collect()
            };
            responder
                .publish(
                    message.reply.unwrap(),
                    serde_json::to_vec(&frames).unwrap().into(),
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
                        serde_json::to_vec(&ack).unwrap().into(),
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
            let ack = Acknowledgment {
                frame: if refusal.is_some() && refusal_kind == FrameRefusal::Malformed {
                    format!("malformed/{}", blake3::hash(&message.payload).to_hex())
                } else {
                    frame.frame().to_owned()
                },
                material,
                admitted,
                outcome: if refusal.is_some() {
                    json!({"refusal_kind": if matches!(frame, Ingress::Chunk { .. }) { FrameRefusal::Bytes } else { refusal_kind }, "failures": 1})
                } else {
                    json!({})
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
                    serde_json::to_vec(&ack).unwrap().into(),
                )
                .await
                .unwrap();
            message.ack().await.unwrap();
        }
    });
    received
}
