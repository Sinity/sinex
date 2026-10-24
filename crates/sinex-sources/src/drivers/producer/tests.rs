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
                Ingress::Chunk { .. } | Ingress::Indirect { .. } | Ingress::Reserve { .. } => {
                    (None, vec![])
                }
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
                outcome: if matches!(frame, Ingress::Reserve { .. }) {
                    json!({"producer_bytes":1,"complete":null})
                } else {
                    json!({})
                },
                refusal,
                commit_seq: Some(1),
            };
            if !matches!(frame, Ingress::Reserve { .. }) {
                seen.lock().unwrap().push(frame);
            }
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
    let received =
        standin_with_refusal(&nats, &subjects, "node-test", Some(CHUNK_BYTES as u64)).await;
    let dir = std::env::temp_dir().join(format!("sinex-pend-{}", uuid::Uuid::now_v7()));
    let state = dir.join("positions");
    let store = Store::open(&dir.join("store")).unwrap();
    let bytes: Vec<u8> = (0..3 * CHUNK_BYTES / 2).map(|i| (i % 251) as u8).collect();
    let file = dir.join("material");
    std::fs::write(&file, &bytes).unwrap();
    let snapshots = Snapshots::new().unwrap();
    let blob = snapshots.put_file(&file).unwrap();
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
    let declaration = Publication {
        version: 1,
        frame: "b/material".into(),
        class: Default::default(),
        material: match &frames[0] {
            Ingress::Material { material, .. } => Some(Material::from_frame(material)),
            _ => unreachable!(),
        },
        envelope: None,
    };
    spool::stage(
        &store.0,
        "node-test",
        &declaration,
        Role::Material,
        bytes.as_slice(),
    )
    .unwrap();
    Pending {
        version: 1,
        publications: vec![Some(declaration), None],
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
    assert!(matches!(
        producer.position("b", &state, &store).await,
        Err(Error::Refused { .. })
    ));
    assert!(Pending::load(&state, "b").unwrap().is_some());
    assert!(Position::load(&state, "b").unwrap().cursor.is_null());
    let first_attempt = received.lock().unwrap().clone();
    assert_eq!(first_attempt.len(), 2);
    assert_chunks(&first_attempt, &bytes);
    received.lock().unwrap().clear();
    let position = producer.position("b", &state, &store).await.unwrap();
    let second_attempt = received.lock().unwrap().clone();
    assert_ne!(first_attempt[0].frame(), second_attempt[0].frame());
    assert_chunks(&second_attempt[..2], &bytes);
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
    let (prepared, publication) = producer
        .prepare(big, &snapshots, &store, Default::default())
        .unwrap();
    assert!(matches!(&prepared, Ingress::Indirect { frame, .. } if frame == "b/big/indirect"));
    producer
        .deliver(prepared, publication.as_ref(), &store)
        .await
        .unwrap();
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

fn assert_chunks(frames: &[Ingress], expected: &[u8]) {
    let mut bytes = Vec::new();
    for frame in frames {
        let Ingress::Chunk {
            offset,
            total,
            data,
            ..
        } = frame
        else {
            panic!("not a chunk")
        };
        assert_eq!(*offset, bytes.len() as u64);
        assert_eq!(*total, expected.len() as u64);
        let chunk = base64::engine::general_purpose::STANDARD
            .decode(data)
            .unwrap();
        assert!(chunk.len() <= CHUNK_BYTES);
        bytes.extend(chunk);
    }
    assert_eq!(bytes, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn streamed_upload_preserves_empty_and_chunk_boundary_materials() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "boundaries").await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut producer = Producer::connect(nats.url(), subjects, "boundaries")
        .await
        .unwrap()
        .upload_bytes(Store::open(dir.path()).unwrap());
    for length in [
        0,
        1,
        CHUNK_BYTES,
        2 * CHUNK_BYTES,
        2 * CHUNK_BYTES + 19,
        9 * CHUNK_BYTES + 19,
    ] {
        let bytes: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
        let path = dir.path().join("input");
        std::fs::write(&path, &bytes).unwrap();
        let blob = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let declaration = spooled(&store, "boundaries", &bytes, &format!("upload-{length}"));
        assert_eq!(blob, format!("blake3:{}", blake3::hash(&bytes).to_hex()));
        received.lock().unwrap().clear();
        producer
            .upload_role(&declaration, Role::Envelope, dir.path())
            .await
            .unwrap();
        let frames = received.lock().unwrap();
        assert_eq!(frames.len(), length.div_ceil(CHUNK_BYTES).max(1));
        assert_chunks(&frames, &bytes);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn damaged_producer_material_sends_no_chunks() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "damaged").await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let declaration = spooled(
        &store,
        "damaged",
        b"synthetic retained material",
        "damaged-frame",
    );
    let path = spool::directory(dir.path(), "damaged", &declaration).join("envelope");
    std::fs::write(&path, b"damaged").unwrap();
    let mut producer = Producer::connect(nats.url(), subjects, "damaged")
        .await
        .unwrap()
        .upload_bytes(store);
    assert!(
        producer
            .upload_role(&declaration, Role::Envelope, dir.path())
            .await
            .is_err()
    );
    assert!(received.lock().unwrap().is_empty());
}

#[test]
fn malformed_producer_blob_names_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for name in [
        format!("blake3:xé{}", "a".repeat(61)),
        format!("blake3:{}", "g".repeat(64)),
        "blake3:ab".into(),
        format!("sha256:{}", "a".repeat(64)),
    ] {
        let declaration = Publication {
            version: 1,
            frame: "invalid".into(),
            class: Default::default(),
            material: None,
            envelope: Some(Bytes {
                blob: name.clone(),
                length: 1,
            }),
        };
        assert!(
            spool::snapshot(&store.0, "invalid", &declaration, Role::Envelope).is_err(),
            "{name}"
        );
    }
}

fn spooled(store: &Store, producer: &str, bytes: &[u8], frame: &str) -> Publication {
    let publication = Publication {
        version: 1,
        frame: frame.into(),
        class: Default::default(),
        material: None,
        envelope: Some(Bytes {
            blob: format!("blake3:{}", blake3::hash(bytes).to_hex()),
            length: bytes.len() as u64,
        }),
    };
    spool::stage(&store.0, producer, &publication, Role::Envelope, bytes).unwrap();
    publication
}
