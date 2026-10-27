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
    standin_with_refusal(nats, subjects, producer, None, 0, FrameRefusal::Content).await
}

async fn standin_with_refusal(
    nats: &devkit::nats::TestNats,
    subjects: &Subjects,
    producer: &str,
    mut refuse_offset: Option<u64>,
    mut refuse_events: usize,
    refusal_kind: FrameRefusal,
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
                Ingress::Events { .. } | Ingress::LargeFrame { .. }
                    if refuse_events > 0 && frame.frame() != "first" =>
                {
                    refuse_events -= 1;
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
        0,
        FrameRefusal::Content,
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
            capture_failures: Vec::new(),
        },
        sources: vec![CapturedRange {
            path: file.clone(),
            offset: 0,
            length: bytes.len() as u64,
            blob: blob.clone(),
        }],
        mismatch: None,
        skip_position: None,
        records: 0,
    }
    .save(&state, "b")
    .unwrap();

    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "node-test",
        &sinex_protocol::ProducerHello::default(),
    )
    .await
    .unwrap()
    .upload_bytes(Store::open(&dir.join("store")).unwrap());
    let position = producer.position("b", &state, &store).await.unwrap();
    assert_eq!(position.cursor, json!({"segment": 7}));
    assert!(position.capture_failures.is_empty());
    assert!(UnacknowledgedFrames::load(&state, "b").unwrap().is_none());
    let received_frames = received.lock().unwrap().clone();
    assert_chunks(&received_frames[..2], &bytes);
    assert_chunks(&received_frames[2..4], &bytes);
    assert_ne!(received_frames[0].frame(), received_frames[2].frame());
    let kinds = |frames: &[Ingress]| -> Vec<String> {
        frames
            .iter()
            .map(|f| match f {
                Ingress::Chunk { offset, .. } => format!("chunk@{offset}"),
                other => other.frame().to_owned(),
            })
            .collect()
    };
    assert!(matches!(&received_frames[4], Ingress::Material { .. }));
    assert_eq!(received_frames[5].frame(), "b/events");

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
        capture_failures: Vec::new(),
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
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "boundaries",
        &sinex_protocol::ProducerHello::default(),
    )
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
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "damaged",
        &sinex_protocol::ProducerHello::default(),
    )
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
    let received =
        standin_with_refusal(&nats, &subjects, "invalid", None, 1, FrameRefusal::Content).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let state = dir.path().join("positions");
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "invalid",
        &sinex_protocol::ProducerHello::default(),
    )
    .await
    .unwrap();
    let invalid = Ingress::Events {
        frame: "invalid".into(),
        events: vec![event(2 * producer.max_message)],
        failures: vec![],
    };
    UnacknowledgedFrames {
        frames: vec![
            Ingress::Events {
                frame: "first".into(),
                events: vec![event(1)],
                failures: vec![],
            },
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
            capture_failures: Vec::new(),
        },
        sources: Vec::new(),
        mismatch: None,
        skip_position: None,
        records: 0,
    }
    .save(&state, "binding")
    .unwrap();
    assert_eq!(
        Producer::unacknowledged_frames("binding", &state).unwrap(),
        3
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
        vec!["first", "invalid/large", "valid"]
    );
}

#[devkit::test(multi_thread)]
async fn three_identical_byte_refusals_keep_the_spool_and_advance_without_a_part_link() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received =
        standin_with_refusal(&nats, &subjects, "bytes", None, 3, FrameRefusal::Bytes).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let state = dir.path().join("positions");
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "bytes",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    let path = dir.path().join("000000000000.ndjson");
    let bytes = b"synthetic spool range\n";
    std::fs::write(&path, bytes).unwrap();
    let material = sinex_model::MaterialId::new();
    let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
    let logical = Ingress::Material {
        frame: "material".into(),
        material: Box::new(MaterialFrame {
            id: material,
            input: sinex_model::InputRef {
                kind: "socket".into(),
                locator: "synthetic".into(),
                host: "test".into(),
            },
            blob: blob.clone(),
            length: bytes.len() as u64,
            parser: "synthetic".into(),
            parser_version: 1,
            part_index: 0,
            previous_part: None,
            manifest: Default::default(),
        }),
    };
    // This material is sent as a large frame, so the same refusal test also
    // covers byte failures in its JSON transfer.
    producer.max_message = 1;
    UnacknowledgedFrames {
        frames: vec![logical, Ingress::Events { frame: "dependent".into(), events: vec![event(1)], failures: vec![] }],
        position: Position { cursor: json!({"segment":0,"last_material":material,"part_base":0,"inner":{"offset":4,"material":material,"part_index":0,"through":4}}), ..Position::default() },
        skip_position: Some(Position { cursor: json!({"segment":0,"last_material":material,"part_base":0,"inner":{"offset":bytes.len(),"material":material,"part_index":0,"through":bytes.len()}}), carry: json!({"after":"whole material"}), ..Position::default() }),
        records: 2,
        sources: vec![CapturedRange { path: path.clone(), offset: 0, length: bytes.len() as u64, blob }], mismatch: None,
    }.save(&state, "binding").unwrap();
    let position = producer.position("binding", &state, &store).await.unwrap();
    assert_eq!(position.capture_failures.len(), 1);
    assert_eq!(position.capture_failures[0].ranges[0].path, path);
    assert_eq!(position.cursor["inner"]["offset"], bytes.len());
    assert_eq!(position.carry, json!({"after":"whole material"}));
    assert!(position.cursor["inner"]["material"].is_null());
    assert!(position.cursor["last_material"].is_null());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(received.lock().unwrap().len(), 3);
    assert!(
        received
            .lock()
            .unwrap()
            .iter()
            .all(|frame| matches!(frame, Ingress::LargeFrame { .. }))
    );
    assert!(
        UnacknowledgedFrames::load(&state, "binding")
            .unwrap()
            .is_none()
    );
    // A new producer reads the same ordinary position and its failure.
    assert_eq!(
        Position::load(&state, "binding")
            .unwrap()
            .capture_failures
            .len(),
        1
    );
    assert!(!state.join("refused").exists());
}

#[devkit::test(multi_thread)]
async fn a_byte_refusal_rebuilds_a_large_frame_and_succeeds_on_the_next_attempt() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received =
        standin_with_refusal(&nats, &subjects, "repair", None, 1, FrameRefusal::Bytes).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let state = dir.path().join("positions");
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "repair",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    producer.max_message = 1;
    UnacknowledgedFrames {
        frames: vec![Ingress::Events {
            frame: "events".into(),
            events: vec![event(1)],
            failures: vec![],
        }],
        position: Position {
            cursor: json!({"segment":1}),
            ..Position::default()
        },
        sources: vec![],
        mismatch: None,
        skip_position: None,
        records: 0,
    }
    .save(&state, "binding")
    .unwrap();
    let position = producer.position("binding", &state, &store).await.unwrap();
    assert!(position.capture_failures.is_empty());
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 2);
    assert_ne!(received[0].frame(), received[1].frame());
    let json_bytes = |frame: &Ingress| {
        let Ingress::LargeFrame { blob, length, .. } = frame else {
            panic!("large frame")
        };
        (blob.clone(), *length)
    };
    // Both attempts were rebuilt from the same logical frame. Only its name changes.
    assert_ne!(json_bytes(&received[0]).0, json_bytes(&received[1]).0);
    assert_eq!(position.cursor, json!({"segment":1}));
}

#[devkit::test(multi_thread)]
async fn a_malformed_frame_stays_an_error_without_advancing_the_binding() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin_with_refusal(
        &nats,
        &subjects,
        "malformed",
        None,
        1,
        FrameRefusal::Malformed,
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let state = dir.path().join("positions");
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "malformed",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    UnacknowledgedFrames {
        frames: vec![Ingress::Events {
            frame: "broken".into(),
            events: vec![event(1)],
            failures: vec![],
        }],
        position: Position {
            cursor: json!({"segment":1}),
            ..Position::default()
        },
        sources: vec![],
        mismatch: None,
        skip_position: None,
        records: 0,
    }
    .save(&state, "binding")
    .unwrap();
    assert!(matches!(
        producer.position("binding", &state, &store).await,
        Err(Error::Refused {
            kind: FrameRefusal::Malformed,
            recorded: true,
            ..
        })
    ));
    assert!(Position::load(&state, "binding").unwrap().cursor.is_null());
    assert_eq!(received.lock().unwrap().len(), 1);
}

#[devkit::test(multi_thread)]
async fn a_stale_malformed_acknowledgment_does_not_refuse_another_frame() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "delayed").await;
    let mut producer = Producer::connect(nats.url(), None, subjects.clone(), "delayed", &ProducerHello::default()).await.unwrap();
    let client = async_nats::connect(nats.url()).await.unwrap();
    let stale = Acknowledgment {
        frame: "malformed/another-frame".into(), material: None,
        admitted: vec![], outcome: json!({"refusal_kind": FrameRefusal::Malformed}),
        refusal: Some("a delayed malformed frame".into()), commit_seq: None,
    };
    client.publish(subjects.ack("delayed"), serde_json::to_vec(&stale).unwrap().into()).await.unwrap();
    client.flush().await.unwrap();
    let ack = producer.send(Ingress::Events { frame: "valid".into(), events: vec![event(1)], failures: vec![] }).await.unwrap();
    assert_eq!(ack.frame, "valid");
    assert_eq!(ack.admitted.len(), 1);
    assert_eq!(received.lock().unwrap().len(), 1);
}

#[devkit::test]
fn failed_spool_segments_survive_cleanup_and_native_paths_survive_position_restart() {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join(std::ffi::OsString::from_vec(b"failed-\xff.ndjson".to_vec()));
    let position = Position {
        capture_failures: vec![CaptureFailure {
            ranges: vec![CapturedRange {
                path: path.clone(),
                offset: 3,
                length: 4,
                blob: "synthetic".into(),
            }],
            reason: "three byte refusals".into(),
        }],
        ..Position::default()
    };
    position.save(dir.path(), "binding").unwrap();
    assert_eq!(
        Position::load(dir.path(), "binding")
            .unwrap()
            .capture_failures[0]
            .ranges[0]
            .path,
        path
    );
    let failed = dir.path().join("000000000000.ndjson");
    let accepted = dir.path().join("000000000001.ndjson");
    std::fs::write(&failed, b"failed captured bytes\n").unwrap();
    std::fs::write(&accepted, b"stored captured bytes\n").unwrap();
    let driver = super::super::live_stream::LiveStream {
        spool: dir.path().to_owned(),
        input: sinex_model::InputRef {
            kind: "socket".into(),
            locator: "synthetic".into(),
            host: "test".into(),
        },
    };
    let cursor = serde_json::to_value(super::super::live_stream::Cursor {
        segment: 2,
        ..Default::default()
    })
    .unwrap();
    let mut protected = cursor;
    protected["keep_files"] = json!([failed.as_os_str().as_bytes()]);
    assert_eq!(driver.remove_passed(&protected).unwrap(), 1);
    assert!(failed.exists());
    assert!(!accepted.exists());
}

#[devkit::test(multi_thread)]
async fn byte_mismatch_count_survives_a_producer_restart() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received =
        standin_with_refusal(&nats, &subjects, "restart", None, 1, FrameRefusal::Bytes).await;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let state = dir.path().join("positions");
    let spool = dir.path().join("spool.ndjson");
    std::fs::write(&spool, b"synthetic spool range\n").unwrap();
    let sources = vec![CapturedRange {
        path: spool.clone(),
        offset: 0,
        length: 22,
        blob: "synthetic".into(),
    }];
    let mismatch = ByteMismatch {
        fingerprint: fingerprint(&sources).unwrap(),
        reason: "synthetic invalid event".into(),
        attempts: 2,
    };
    UnacknowledgedFrames {
        frames: vec![Ingress::Events {
            frame: "events".into(),
            events: vec![event(1)],
            failures: vec![],
        }],
        position: Position {
            cursor: json!({"segment":1}),
            ..Default::default()
        },
        sources,
        mismatch: Some(mismatch),
        skip_position: None,
        records: 0,
    }
    .save(&state, "binding")
    .unwrap();
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "restart",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    let position = producer.position("binding", &state, &store).await.unwrap();
    assert_eq!(received.lock().unwrap().len(), 1);
    assert_eq!(position.capture_failures.len(), 1);
    assert_eq!(position.capture_failures[0].ranges[0].path, spool);
}
