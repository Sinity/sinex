use super::*;
use serde_json::json;
use std::collections::HashMap;

fn event(n: usize) -> ProducerEvent {
    ProducerEvent {
        event_type: "window.focused".into(),
        payload: json!({"title": "x".repeat(n)}),
        location: None,
        output_index: 0,
        parents: vec![],
        reading_order: 0,
        class: Default::default(),
    }
}

#[devkit::test]
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
    standin_with_refusal(nats, subjects, producer, None, false).await
}

async fn standin_with_refusal(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
    mut refuse_offset: Option<u64>,
    mut refuse_events: bool,
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
            let (material, mut admitted) = match &frame {
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
                Ingress::Events { .. } | Ingress::LargeFrame { .. } if refuse_events => {
                    refuse_events = false;
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
                frame: frame.frame().to_owned(),
                material,
                admitted,
                outcome: json!({}),
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

/// A batch a stop left unacknowledged before its position was saved is sent
/// again on the next start, with its bytes uploaded in chunks; a frame larger
/// than one message travels as a large frame.
#[devkit::test(multi_thread)]
async fn an_unacknowledged_batch_is_resent_with_uploaded_bytes() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin_with_refusal(
        &nats,
        &subjects,
        "node-test",
        Some(CHUNK_BYTES as u64),
        false,
    )
    .await;
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
                manifest: sinex_model::Manifest::from_json(json!({})),
            }),
        },
        Ingress::Events {
            frame: "b/events".into(),
            events: vec![event(10)],
            failures: vec![],
        },
    ];
    frame_bytes::write(
        &store.0,
        "node-test",
        "b/material",
        &blob,
        bytes.len() as u64,
        bytes.as_slice(),
    )
    .unwrap();
    UnacknowledgedFrames {
        frames,
        position: Position {
            cursor: json!({"segment": 7}),
            carry: json!(null),
        },
    }
    .save(&state, "b")
    .unwrap();

    let mut producer = Producer::connect(nats.url(), None, subjects, "node-test")
        .await
        .unwrap()
        .upload_bytes(Store::open(&dir.join("store")).unwrap());
    assert!(matches!(
        producer.position("b", &state, &store).await,
        Err(Error::Refused { .. })
    ));
    assert!(UnacknowledgedFrames::load(&state, "b").unwrap().is_some());
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
    assert!(UnacknowledgedFrames::load(&state, "b").unwrap().is_none());
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
    let prepared = producer.prepare(big, &snapshots, &store).unwrap();
    assert!(matches!(&prepared, Ingress::LargeFrame { frame, .. } if frame == "b/big/large"));
    producer.deliver(prepared, &store).await.unwrap();
    let got = kinds(&received.lock().unwrap());
    assert_eq!(got.last().map(String::as_str), Some("b/big/large"));
    assert!(got.len() >= 3, "uploaded in chunks first: {got:?}");
    let _ = std::fs::remove_dir_all(dir);
}

#[devkit::test]
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

#[devkit::test(multi_thread)]
async fn streamed_upload_preserves_empty_and_chunk_boundary_materials() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "boundaries").await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut producer = Producer::connect(nats.url(), None, subjects, "boundaries")
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
        let frame = format!("upload-{length}");
        spooled(&store, "boundaries", &bytes, &frame);
        received.lock().unwrap().clear();
        producer
            .upload(&frame, &blob, bytes.len() as u64, dir.path())
            .await
            .unwrap();
        let frames = received.lock().unwrap();
        assert_eq!(frames.len(), length.div_ceil(CHUNK_BYTES).max(1));
        assert_chunks(&frames, &bytes);
    }
}

#[devkit::test(multi_thread)]
async fn damaged_producer_material_sends_no_chunks() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "damaged").await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let bytes = b"synthetic stored material";
    let blob = spooled(&store, "damaged", bytes, "damaged-frame");
    let path = frame_bytes::directory(dir.path(), "damaged", "damaged-frame").join(&blob[7..]);
    std::fs::write(&path, b"damaged").unwrap();
    let mut producer = Producer::connect(nats.url(), None, subjects, "damaged")
        .await
        .unwrap()
        .upload_bytes(store);
    assert!(
        producer
            .upload("damaged-frame", &blob, bytes.len() as u64, dir.path())
            .await
            .is_err()
    );
    assert!(received.lock().unwrap().is_empty());
}

#[devkit::test]
fn malformed_producer_blob_names_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    for name in [
        format!("blake3:xé{}", "a".repeat(61)),
        format!("blake3:{}", "g".repeat(64)),
        "blake3:ab".into(),
        format!("sha256:{}", "a".repeat(64)),
    ] {
        assert!(
            frame_bytes::snapshot(&store.0, "invalid", "frame", &name, 1).is_err(),
            "{name}"
        );
    }
}

/// Write `bytes` to the spool of `frame` and return their name.
fn spooled(store: &Store, producer: &str, bytes: &[u8], frame: &str) -> String {
    let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
    frame_bytes::write(&store.0, producer, frame, &blob, bytes.len() as u64, bytes).unwrap();
    blob
}

#[devkit::test(multi_thread)]
async fn a_recorded_invalid_frame_is_skipped_and_the_next_frame_is_sent() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin_with_refusal(&nats, &subjects, "invalid", None, true).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let state = dir.path().join("positions");
    let mut producer = Producer::connect(nats.url(), None, subjects, "invalid")
        .await
        .unwrap();
    let snapshots = Snapshots::new().unwrap();
    let invalid = producer
        .prepare(
            Ingress::Events {
                frame: "invalid".into(),
                events: vec![event(2 * producer.max_message)],
                failures: vec![],
            },
            &snapshots,
            &store,
        )
        .unwrap();
    assert!(matches!(invalid, Ingress::LargeFrame { .. }));
    UnacknowledgedFrames {
        frames: vec![
            invalid,
            Ingress::Events {
                frame: "valid".into(),
                events: vec![event(1)],
                failures: vec![],
            },
        ],
        position: Position {
            cursor: json!({"segment":3}),
            carry: Value::Null,
        },
    }
    .save(&state, "binding")
    .unwrap();
    assert_eq!(
        Producer::unacknowledged_frames("binding", &state).unwrap(),
        2
    );
    assert_eq!(
        producer
            .position("binding", &state, &store)
            .await
            .unwrap()
            .cursor,
        json!({"segment":3})
    );
    assert_eq!(
        Producer::unacknowledged_frames("binding", &state).unwrap(),
        0
    );
    assert!(
        UnacknowledgedFrames::load(&state, "binding")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        received
            .lock()
            .unwrap()
            .iter()
            .map(Ingress::frame)
            .collect::<Vec<_>>(),
        vec!["invalid/large", "valid"]
    );
}

#[devkit::test(multi_thread)]
async fn a_refused_large_material_keeps_its_bytes_and_skips_dependent_events() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin_with_refusal(&nats, &subjects, "invalid-material", None, true).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let state = dir.path().join("positions");
    let mut producer = Producer::connect(nats.url(), None, subjects, "invalid-material")
        .await
        .unwrap();
    let snapshots = Snapshots::new().unwrap();
    let bytes = b"synthetic captured material";
    let blob = snapshots.put(bytes).unwrap();
    let mut manifest = sinex_model::Manifest::default();
    manifest.details.insert(
        "synthetic".into(),
        json!("x".repeat(2 * producer.max_message)),
    );
    let frame = producer
        .prepare(
            Ingress::Material {
                frame: "invalid-material".into(),
                material: Box::new(MaterialFrame {
                    id: sinex_model::MaterialId::new(),
                    input: sinex_model::InputRef {
                        kind: "file".into(),
                        locator: "synthetic".into(),
                        host: "synthetic".into(),
                    },
                    blob: blob.clone(),
                    length: bytes.len() as u64,
                    parser: "synthetic".into(),
                    parser_version: 1,
                    part_index: 0,
                    previous_part: None,
                    manifest,
                }),
            },
            &snapshots,
            &store,
        )
        .unwrap();
    assert!(matches!(frame, Ingress::LargeFrame { .. }));
    let frame_id = frame.frame().to_owned();
    UnacknowledgedFrames {
        frames: vec![
            frame,
            Ingress::Events {
                frame: "dependent".into(),
                events: vec![event(1)],
                failures: vec![],
            },
        ],
        position: Position {
            cursor: json!({"segment":3}),
            carry: Value::Null,
        },
    }
    .save(&state, "binding")
    .unwrap();
    producer.position("binding", &state, &store).await.unwrap();
    assert_eq!(received.lock().unwrap().len(), 1);
    assert!(
        frame_bytes::snapshot(
            &store.0,
            "invalid-material",
            &frame_id,
            &blob,
            bytes.len() as u64
        )
        .is_ok()
    );
    let saved = std::fs::read_dir(state.join("refused"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let refused: UnacknowledgedFrames =
        serde_json::from_slice(&std::fs::read(saved).unwrap()).unwrap();
    assert_eq!(refused.frames.len(), 2);
}
