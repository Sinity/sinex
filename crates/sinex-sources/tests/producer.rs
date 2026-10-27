use async_nats::jetstream;
use futures::StreamExt;
use serde_json::json;
use sinex_protocol::{Acknowledgment, Ingress, Subjects};
use sinex_source_core::drivers::producer::{Position, Producer, Store};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
async fn standin(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
) -> Arc<Mutex<Vec<Ingress>>> {
    standin_with_refusal(nats, subjects, producer, None).await
}

async fn standin_with_refusal(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
    mut refuse_offset: Option<u64>,
) -> Arc<Mutex<Vec<Ingress>>> {
    use async_nats::jetstream::{consumer, stream};
    let client = async_nats::connect(nats.url()).await.unwrap();
    let mut declarations = client.subscribe(subjects.connect(producer)).await.unwrap();
    let declarer = client.clone();
    tokio::spawn(async move {
        while let Some(message) = declarations.next().await {
            declarer.publish(message.reply.unwrap(), serde_json::to_vec(&sinex_protocol::ProducerWelcome { refusal: None }).unwrap().into()).await.unwrap();
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
    let (seen, acker, ack_subject) = (received.clone(), client, subjects.ack(producer));
    tokio::spawn(async move {
        let mut messages = consumer.messages().await.unwrap();
        while let Some(Ok(message)) = messages.next().await {
            let frame: Ingress = serde_json::from_slice(&message.payload).unwrap();
            let (material, admitted) = match &frame {
                Ingress::Material { material, .. } => (Some(material.id), vec![]),
                Ingress::Events { events, .. } => (
                    None,
                    events.iter().map(|_| sinex_model::EventId::new()).collect(),
                ),
                Ingress::Chunk { .. } | Ingress::LargeFrame { .. } => (None, vec![]),
            };
            let refusal = match &frame {
                Ingress::Chunk { offset, .. } if refuse_offset == Some(*offset) => {
                    refuse_offset = None;
                    Some("synthetic interruption".into())
                }
                _ => None,
            };
            let ack = Acknowledgment {
                frame: frame.frame().to_owned(),
                material,
                admitted,
                outcome: if refusal.is_some() { json!({"refusal_kind": if matches!(frame, Ingress::Chunk { .. }) { "bytes" } else { "content" }, "failures": 1}) } else { json!({}) },
                refusal,
                commit_seq: Some(1),
            };
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

/// Produce a Hyprland spool against the stand-in: material first, once.
#[devkit::test(multi_thread)]
async fn a_spool_is_produced_once_material_first() {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter, capture_lines};
    use sinex_sources::sources::hyprland;

    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "session-test").await;

    let dir = std::env::temp_dir().join(format!("sinex-prod-{}", uuid::Uuid::now_v7()));
    let spool = dir.join("spool");
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    capture_lines(
        &b"activewindow>>kitty,t\nactivewindowv2>>55aa\nclosewindow>>55aa\n"[..],
        &mut writer,
        Duration::ZERO,
    )
    .unwrap();
    let store = Store::open(&dir.join("store")).unwrap();
    let positions = dir.join("positions");
    let driver = hyprland::driver(spool, "h".into());
    let source = hyprland::source();
    let mut producer = Producer::connect(nats.url(), None, subjects, "session-test", &sinex_protocol::ProducerHello::default())
        .await
        .unwrap();

    let poll = |producer_positions: &Path| {
        let p = Position::load(producer_positions, "hyprland@h").unwrap();
        driver.poll(&p.cursor).unwrap()
    };
    let sent = producer
        .produce(&source, "hyprland@h", poll(&positions), &store, &positions, &driver.spool)
        .await
        .unwrap();
    assert_eq!(
        (sent.materials, sent.events, sent.admitted, sent.unread),
        (1, 2, 2, 0)
    );
    let again = producer
        .produce(&source, "hyprland@h", poll(&positions), &store, &positions, &driver.spool)
        .await
        .unwrap();
    assert_eq!(
        again.events, 0,
        "the position advanced; nothing is sent twice"
    );

    let frames = received.lock().unwrap().clone();
    assert!(
        matches!(frames.first(), Some(Ingress::Material { .. })),
        "material first"
    );
    let Some(Ingress::Events { events, .. }) = frames.get(1) else {
        panic!("events after the material: {frames:?}")
    };
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(types, vec!["window.focused", "window.closed"]);
    assert!(
        events.iter().all(|e| e.location.is_some()),
        "events cite their material"
    );
}
