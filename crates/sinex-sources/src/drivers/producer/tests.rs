use super::*;
use serde_json::json;

fn event(n: usize) -> ProducerEvent {
    ProducerEvent {
        event_type: "window.focused".into(),
        payload: json!({"title": "x".repeat(n)}),
        location: None,
        ordinal: 0,
        parents: vec![],
        reading_order: 0,
        class: Default::default(),
    }
}

#[test]
fn frames_fit_one_message_and_a_large_event_travels_alone() {
    let frames = split(vec![
        event(300_000),
        event(300_000),
        event(10),
        event(2_000_000),
    ]);
    assert_eq!(
        frames.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 2, 1]
    );
}

use std::sync::{Arc, Mutex};

/// A stand-in for `sinexd`'s ingress that records the frames it receives
/// and acknowledges each one.
async fn standin(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
) -> Arc<Mutex<Vec<Ingress>>> {
    use async_nats::jetstream::{consumer, stream};
    let client = async_nats::connect(nats.url()).await.unwrap();
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
                Ingress::Chunk { .. } | Ingress::Indirect { .. } => (None, vec![]),
            };
            let ack = Acknowledgment {
                frame: frame.frame().to_owned(),
                material,
                admitted,
                outcome: json!({}),
                refusal: None,
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

/// A batch left pending by a stop before its position was saved is sent
/// again on the next start, with its bytes uploaded in chunks; a frame
/// larger than one message travels indirectly.
#[tokio::test(flavor = "multi_thread")]
async fn a_pending_batch_is_resent_with_uploaded_bytes() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "node-test").await;
    let dir = std::env::temp_dir().join(format!("sinex-pend-{}", uuid::Uuid::now_v7()));
    let state = dir.join("positions");
    let store = Store::open(&dir.join("store")).unwrap();
    let bytes = vec![b'x'; 3 * CHUNK_BYTES / 2];
    let blob = store.put(&bytes).unwrap();
    let material = sinex_model::MaterialId::new();
    let frames = vec![
        Ingress::Material {
            frame: "b/material".into(),
            material: Box::new(MaterialFrame {
                id: material,
                input: sinex_model::InputRef {
                    kind: "file".into(),
                    locator: "/var/log/x".into(),
                    host: "node".into(),
                },
                blob: blob.clone(),
                length: bytes.len() as u64,
                parser: "p".into(),
                parser_version: 1,
                part_index: 0,
                previous_part: None,
                manifest: json!({}),
            }),
        },
        Ingress::Events {
            frame: "b/events".into(),
            events: vec![event(10)],
            failures: vec![],
        },
    ];
    Pending {
        frames,
        position: Position {
            cursor: json!({"segment": 7}),
            carry: json!(null),
        },
    }
    .save(&state, "b")
    .unwrap();

    let mut producer = Producer::connect(nats.url(), subjects, "node-test")
        .await
        .unwrap()
        .upload_bytes(Store::open(&dir.join("store")).unwrap());
    let position = producer.position("b", &state).await.unwrap();
    assert_eq!(position.cursor, json!({"segment": 7}));
    assert!(Pending::load(&state, "b").unwrap().is_none());
    let kinds = |frames: &[Ingress]| -> Vec<String> {
        frames
            .iter()
            .map(|f| match f {
                Ingress::Chunk { offset, .. } => format!("chunk@{offset}"),
                other => other.frame().to_owned(),
            })
            .collect()
    };
    assert_eq!(
        kinds(&received.lock().unwrap()),
        [
            "chunk@0".to_owned(),
            format!("chunk@{CHUNK_BYTES}"),
            "b/material".into(),
            "b/events".into()
        ]
    );

    // A frame too big for one message is stored, uploaded and named.
    received.lock().unwrap().clear();
    let big = Ingress::Events {
        frame: "b/big".into(),
        events: vec![event(2 * producer.max_message)],
        failures: vec![],
    };
    let prepared = producer.prepare(big, &store).unwrap();
    assert!(matches!(&prepared, Ingress::Indirect { frame, .. } if frame == "b/big/indirect"));
    producer.deliver(prepared).await.unwrap();
    let got = kinds(&received.lock().unwrap());
    assert_eq!(got.last().map(String::as_str), Some("b/big/indirect"));
    assert!(got.len() >= 3, "uploaded in chunks first: {got:?}");
    let _ = std::fs::remove_dir_all(dir);
}

/// Produce a Hyprland spool against the stand-in: material first, once.
#[tokio::test(flavor = "multi_thread")]
async fn a_spool_is_produced_once_material_first() {
    use crate::drivers::live_stream::{Rotation, SpoolWriter, capture_lines};
    use crate::sources::hyprland;

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
    let mut producer = Producer::connect(nats.url(), subjects, "session-test")
        .await
        .unwrap();

    let poll = |producer_positions: &Path| {
        let p = Position::load(producer_positions, "hyprland@h").unwrap();
        driver.poll(&p.cursor).unwrap()
    };
    let sent = producer
        .produce(&source, "hyprland@h", poll(&positions), &store, &positions)
        .await
        .unwrap();
    assert_eq!(
        (sent.materials, sent.events, sent.admitted, sent.unread),
        (1, 2, 2, 0)
    );
    let again = producer
        .produce(&source, "hyprland@h", poll(&positions), &store, &positions)
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

#[test]
fn position_survives_a_restart() {
    let dir = std::env::temp_dir().join(format!("sinex-pos-{}", uuid::Uuid::now_v7()));
    assert!(Position::load(&dir, "hyprland@h").unwrap().cursor.is_null());
    Position {
        cursor: json!({"segment": 3}),
        carry: json!(null),
    }
    .save(&dir, "hyprland@h")
    .unwrap();
    assert_eq!(
        Position::load(&dir, "hyprland@h").unwrap().cursor,
        json!({"segment": 3})
    );
}
