use super::*;
use crate::Reader;
use serde_json::json;

fn event(n: usize) -> ProducerEvent {
    ProducerEvent {
        kind: sinex_model::KindRef {
            name: "window.focused".into(),
            version: 2,
        },
        payload: json!({"title": "x".repeat(n)}),
        location: None,
        output_index: 0,
        parents: vec![],
        reading_order: Some(0),
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

#[path = "../../../tests/support/producer.rs"]
mod support;
use support::{refusing, standin, standin_with_refusal};

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
        refusal: None,
        reporting_failure: false,
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
        refusal: None,
        reporting_failure: false,
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
        refusal: None,
        reporting_failure: false,
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
        refusal: None,
        reporting_failure: false,
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
async fn a_restart_after_a_fix_retries_malformed_and_material_content_refusals() {
    for (kind, material_frame, large) in [
        (FrameRefusal::Malformed, false, false),
        (FrameRefusal::Malformed, false, true),
        (FrameRefusal::Content, true, false),
        (FrameRefusal::Configuration, false, false),
    ] {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let received = refusing(&nats, &subjects, "restart", None, 1, kind, material_frame).await;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let state = dir.path().join("positions");
        let bytes = b"synthetic source bytes";
        let source = dir.path().join("source");
        std::fs::write(&source, bytes).unwrap();
        let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
        let frame = if material_frame {
            Ingress::Material {
                frame: "broken".into(),
                material: Box::new(MaterialFrame {
                    id: sinex_model::MaterialId::new(),
                    input: sinex_model::InputRef {
                        kind: "file".into(),
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
            }
        } else {
            Ingress::Events {
                frame: "broken".into(),
                events: vec![event(1)],
                failures: vec![],
            }
        };
        UnacknowledgedFrames {
            frames: vec![frame],
            position: Position {
                cursor: json!({"segment":1}),
                ..Default::default()
            },
            sources: vec![CapturedRange {
                path: source,
                offset: 0,
                length: bytes.len() as u64,
                blob,
            }],
            mismatch: None,
            skip_position: None,
            records: 1,
            refusal: None,
            reporting_failure: false,
        }
        .save(&state, "binding")
        .unwrap();
        let mut producer = Producer::connect(
            nats.url(),
            None,
            subjects.clone(),
            "restart",
            &ProducerHello::default(),
        )
        .await
        .unwrap();
        if large {
            producer.max_message = 1;
        }
        assert!(matches!(
            producer.position("binding", &state, &store).await,
            Err(Error::Refused { recorded: true, .. })
        ));
        assert!(Position::load(&state, "binding").unwrap().cursor.is_null());
        if kind == FrameRefusal::Malformed && !large {
            // A crash can happen after the daemon records the refusal and before the local save.
            let mut interrupted = UnacknowledgedFrames::load(&state, "binding")
                .unwrap()
                .unwrap();
            interrupted.refusal = None;
            interrupted.save(&state, "binding").unwrap();
        }
        drop(producer);
        let mut producer = Producer::connect(
            nats.url(),
            None,
            subjects,
            "restart",
            &ProducerHello::default(),
        )
        .await
        .unwrap();
        if large {
            producer.max_message = 1;
        }
        let position = producer.position("binding", &state, &store).await.unwrap();
        assert_eq!(position.cursor, json!({"segment":1}));
        assert!(
            UnacknowledgedFrames::load(&state, "binding")
                .unwrap()
                .is_none()
        );
        let received = received.lock().unwrap();
        assert_eq!(received.len(), 2);
        assert!(received[1].frame().contains("/retry/"));
        assert_ne!(received[0].frame(), received[1].frame());
    }
}

#[devkit::test]
fn old_positions_open_and_old_unacknowledged_frames_require_draining() {
    let dir = tempfile::tempdir().unwrap();
    let position = Position::path(dir.path(), "binding");
    let bytes = br#"{"cursor":{"segment":4},"carry":{"partial":"synthetic"}}"#;
    std::fs::write(&position, bytes).unwrap();
    let loaded = Position::load(dir.path(), "binding").unwrap();
    assert_eq!(loaded.cursor, json!({"segment":4}));
    assert_eq!(loaded.carry, json!({"partial":"synthetic"}));
    assert!(loaded.capture_failures.is_empty());
    assert_eq!(std::fs::read(position).unwrap(), bytes);
    let path = UnacknowledgedFrames::path(dir.path(), "binding");
    let bytes = br#"{"frames":[{"kind":"large_frame","frame":"saved","blob":"synthetic","length":12}],"position":{"cursor":{},"carry":null}}"#;
    std::fs::write(&path, bytes).unwrap();
    let error = Producer::unacknowledged_position("binding", dir.path())
        .unwrap_err()
        .to_string();
    assert!(error.contains(path.to_str().unwrap()), "{error}");
    assert!(error.contains("drain the old producer first"), "{error}");
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[devkit::test(multi_thread)]
async fn a_stale_malformed_acknowledgment_does_not_refuse_another_frame() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "delayed").await;
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects.clone(),
        "delayed",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    let client = async_nats::connect(nats.url()).await.unwrap();
    let stale = Acknowledgment {
        material_extents: vec![],
        frame: "malformed/another-frame".into(),
        material: None,
        admitted: vec![],
        outcome: json!({"refusal_kind": FrameRefusal::Malformed}),
        refusal: Some("a delayed malformed frame".into()),
        commit_seq: None,
    };
    client
        .publish(
            subjects.ack("delayed"),
            serde_json::to_vec(&stale).unwrap().into(),
        )
        .await
        .unwrap();
    client.flush().await.unwrap();
    let ack = producer
        .send(Ingress::Events {
            frame: "valid".into(),
            events: vec![event(1)],
            failures: vec![],
        })
        .await
        .unwrap();
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
        standin_with_refusal(&nats, &subjects, "restart", None, 4, FrameRefusal::Bytes).await;
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
    let material = sinex_model::MaterialId::new();
    let mut events = vec![event(1), event(2)];
    for (index, event) in events.iter_mut().enumerate() {
        event.location = Some(sinex_model::RecordLocation {
            material,
            anchor: index as u64,
        });
    }
    UnacknowledgedFrames {
        frames: vec![Ingress::Events {
            frame: "events".into(),
            events,
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
        refusal: None,
        reporting_failure: false,
    }
    .save(&state, "binding")
    .unwrap();
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects.clone(),
        "restart",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    assert!(matches!(
        producer.position("binding", &state, &store).await,
        Err(Error::Refused {
            kind: FrameRefusal::Bytes,
            ..
        })
    ));
    assert!(Position::load(&state, "binding").unwrap().cursor.is_null());
    let pending = UnacknowledgedFrames::load(&state, "binding")
        .unwrap()
        .unwrap();
    assert!(pending.reporting_failure);
    assert_eq!(pending.position.capture_failures.len(), 1);
    drop(producer);
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
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 5);
    let Ingress::Events {
        events, failures, ..
    } = &received[4]
    else {
        panic!("failure frame")
    };
    assert!(events.is_empty());
    assert_eq!(failures.len(), 2);
    assert!(
        failures
            .iter()
            .all(|failure| failure.location.material == material
                && failure.reason.contains("three identical byte refusals"))
    );
    assert_eq!(
        Position::load(&state, "binding").unwrap().cursor,
        json!({"segment":1})
    );
    assert_eq!(position.capture_failures.len(), 1);
    assert_eq!(position.capture_failures[0].ranges[0].path, spool);
}

#[devkit::test(multi_thread)]
async fn a_saved_invalid_name_restarts_under_a_valid_attempt_name() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "restart-name").await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("positions");
    let store = Store::open(&dir.path().join("store")).unwrap();
    let material = sinex_model::MaterialId::new();
    let prefix = format!("synthetic:/spool/\0@host/events/{material}/0/0");
    UnacknowledgedFrames {
        frames: vec![Ingress::Events {
            frame: prefix,
            events: vec![event(1)],
            failures: vec![],
        }],
        position: Position {
            cursor: json!({"segment":1}),
            ..Default::default()
        },
        sources: vec![],
        mismatch: None,
        skip_position: None,
        records: 1,
        refusal: Some(FrameRefusal::Malformed),
        reporting_failure: false,
    }
    .save(&state, "binding")
    .unwrap();
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "restart-name",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    let position = producer.position("binding", &state, &store).await.unwrap();
    assert_eq!(position.cursor, json!({"segment":1}));
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    let mut frame = received[0].clone();
    assert!(!frame.frame().contains('\0'));
    assert!(frame.frame().starts_with(&format!(
        "synthetic:/spool/\\0@host/events/{material}/0/0/retry/"
    )));
    let first_name = frame.frame().to_owned();
    retry(&mut frame);
    assert_ne!(frame.frame(), first_name);
    assert_eq!(frame.frame().len(), first_name.len());
}

#[devkit::test(multi_thread)]
async fn a_missing_spool_requires_verified_retained_material_bytes() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    standin(&nats, &subjects, "retained").await;
    let producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "retained",
        &ProducerHello::default(),
    )
    .await
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let bytes = b"synthetic retained bytes";
    let blob = format!("blake3:{}", blake3::hash(bytes));
    let frame = Ingress::Material {
        frame: "retained/material".into(),
        material: Box::new(MaterialFrame {
            id: sinex_model::MaterialId::new(),
            input: sinex_model::InputRef {
                kind: "file".into(),
                locator: "/synthetic".into(),
                host: "fixture".into(),
            },
            blob: blob.clone(),
            length: bytes.len() as u64,
            parser: "fixture".into(),
            parser_version: 1,
            part_index: 0,
            previous_part: None,
            manifest: sinex_model::Manifest::from_json(json!({})),
        }),
    };
    let ranges = [CapturedRange {
        path: root.path().join("missing-spool"),
        offset: 7,
        length: bytes.len() as u64,
        blob: blob.clone(),
    }];
    frame_bytes::write(
        &store.0,
        "retained",
        frame.frame(),
        &blob,
        bytes.len() as u64,
        bytes.as_slice(),
    )
    .unwrap();
    assert!(producer.rebuild(&frame, &ranges, &store).is_ok());
    let retained = frame_bytes::directory(&store.0, "retained", frame.frame())
        .join(blob.strip_prefix("blake3:").unwrap());
    // A same-length replacement must fail the hash check; truncation must fail the length check.
    for replacement in [vec![b'x'; bytes.len()], vec![]] {
        std::fs::write(&retained, replacement).unwrap();
        assert!(matches!(
            producer.rebuild(&frame, &ranges, &store),
            Err(Error::Local(_))
        ));
    }
    assert!(!ranges[0].path.exists());
}
