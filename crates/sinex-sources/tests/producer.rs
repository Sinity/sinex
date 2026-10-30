use sinex_protocol::{Ingress, Subjects};
use sinex_source_core::Reader;
use sinex_source_core::drivers::producer::{Position, Producer, Store};
use std::{path::Path, time::Duration};
#[path = "../../sinex-source-core/tests/support/producer.rs"]
mod support;
use support::standin;
#[path = "../../sinex-source-core/tests/support/hello.rs"]
mod hello;

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
    let mut driver = hyprland::driver(spool, "h".into());
    let source = hyprland::source();
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "session-test",
        &sinex_protocol::ProducerHello::default(),
    )
    .await
    .unwrap();

    let spool = driver.spool.clone();
    let mut poll = |producer_positions: &Path| {
        let p = Position::load(producer_positions, "hyprland@h").unwrap();
        driver.poll(&p.cursor).unwrap()
    };
    let sent = producer
        .produce(
            &source,
            "hyprland@h",
            poll(&positions),
            &store,
            &positions,
            &spool,
        )
        .await
        .unwrap();
    assert_eq!(
        (sent.materials, sent.events, sent.admitted, sent.unread),
        (1, 2, 2, 0)
    );
    let again = producer
        .produce(
            &source,
            "hyprland@h",
            poll(&positions),
            &store,
            &positions,
            &spool,
        )
        .await
        .unwrap();
    assert_eq!(
        again.events, 0,
        "the position advanced; nothing is sent twice"
    );

    let frames: Vec<_> = received
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| !matches!(frame, Ingress::Chunk { .. }))
        .cloned()
        .collect();
    assert!(
        matches!(frames.first(), Some(Ingress::Material { .. })),
        "material first"
    );
    let Some(Ingress::Events { events, .. }) = frames.get(1) else {
        panic!("events after the material: {frames:?}")
    };
    let types: Vec<&str> = events.iter().map(|e| e.kind.name.as_str()).collect();
    assert_eq!(types, vec!["window.focused", "window.closed"]);
    assert!(
        events.iter().all(|e| e.location.is_some()),
        "events cite their material"
    );
}

#[devkit::test(multi_thread)]
async fn draining_after_moving_session_storage_keeps_names_and_positions() {
    use base64::Engine;
    use sinex_model::Timestamp;
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
    use sinex_source_core::drivers::{append_file, live_stream};
    use sinex_sources::{discovery, live};
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::symlink;
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "session-test").await;
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let spool = state.join("spool/hyprland");
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    for line in [
        &b"activewindow>>kitty,synthetic"[..],
        &b"activewindowv2>>55aa"[..],
    ] {
        writer.append(sinex_model::Timestamp::now(), line).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);
    let segment = spool.join("000000000000.ndjson");
    let captured = std::fs::read(&segment).unwrap();
    let records: Vec<serde_json::Value> = captured
        .split_inclusive(|byte| *byte == b'\n')
        .map(|record| serde_json::from_slice(record).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["line"], "activewindow>>kitty,synthetic");
    assert_eq!(records[1]["line"], "activewindowv2>>55aa");
    let metadata = std::fs::metadata(&segment).unwrap();
    let changed = (i128::from(metadata.mtime()) * 1_000_000_000
        + i128::from(metadata.mtime_nsec()))
    .max(i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec()));
    let context = discovery::DiscoveryContext {
        host: "h".into(),
        spool: state.join("spool"),
        ..Default::default()
    };
    let bindings = discovery::spools(&context, discovery::Owner::Session).unwrap();
    let name = bindings
        .iter()
        .find(|(_, binding)| binding.source == "desktop.hyprland")
        .unwrap()
        .1
        .name
        .clone();
    let store = Store::open(&root.path().join("store")).unwrap();
    let positions = state.join("positions");
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "session-test",
        &Default::default(),
    )
    .await
    .unwrap();
    let first_started = Timestamp::now().unix_ns();
    live::drain(&mut producer, &store, &positions, bindings)
        .await
        .unwrap();
    let first_ended = Timestamp::now().unix_ns();
    let admitted = Position::load(&positions, &name).unwrap();
    let first_cursor: live_stream::Cursor =
        serde_json::from_value(admitted.cursor.clone()).unwrap();
    let first_inner: append_file::Cursor =
        serde_json::from_value(first_cursor.inner.clone()).unwrap();
    assert!((first_started..=first_ended).contains(&first_inner.looked_ns));
    assert_eq!(first_cursor.segment, 0);
    assert_eq!(first_cursor.part_base, 0);
    assert_eq!(first_inner.part_index, 0);
    assert_eq!(first_inner.offset, captured.len() as u64);
    assert_eq!(first_inner.size, captured.len() as u64);
    assert_eq!(first_inner.through, captured.len());
    assert_eq!(first_inner.inode, metadata.ino());
    assert_eq!(
        first_inner.mtime_ns,
        i128::from(metadata.mtime()) * 1_000_000_000 + i128::from(metadata.mtime_nsec())
    );
    assert_eq!(
        first_inner.ctime_ns,
        i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec())
    );
    assert_eq!(
        first_inner.window,
        Some(blake3::hash(&captured).to_hex().to_string())
    );
    assert_eq!(first_cursor.last_material, first_inner.material);

    let offered = received.lock().unwrap().clone();
    let frames = offered.len();
    let materials: Vec<_> = offered
        .iter()
        .filter_map(|frame| match frame {
            Ingress::Material { frame, material } => Some((frame, material)),
            _ => None,
        })
        .collect();
    assert_eq!(materials.len(), 1);
    let (material_frame, material) = materials[0];
    assert_eq!(Some(material.id), first_inner.material);
    assert_eq!(material.source.name, "desktop.hyprland");
    assert_eq!(
        material.source.version,
        sinex_sources::sources::hyprland::SPEC.parser_version
    );
    assert_eq!(material.parser, "hyprland-socket2");
    assert_eq!(material.parser_version, material.source.version);
    assert_eq!(material.input.kind, "socket");
    assert_eq!(material.input.locator, "hyprland:socket2");
    assert_eq!(material.input.host, "h");
    assert_eq!(material.part_index, 0);
    assert_eq!(material.previous_part, None);
    assert_eq!(
        material.manifest.path.as_deref(),
        Some(segment.to_str().unwrap())
    );
    assert_eq!(material.manifest.media_type.as_deref(), Some("text/plain"));
    assert_eq!(material.manifest.file_offset, Some(0));
    assert_eq!(material.manifest.order_base, Some(0));
    assert_eq!(material.manifest.detail("host").as_str(), Some("h"));
    assert_eq!(material.manifest.detail("spool_segment").as_u64(), Some(0));
    assert_eq!(
        material.blob,
        format!("blake3:{}", blake3::hash(&captured).to_hex())
    );
    assert_eq!(material.length, captured.len() as u64);
    let receiver = root.path().join("receiver");
    let mut transferred = false;
    for frame in &offered {
        if let Ingress::Chunk {
            for_frame,
            blob,
            descriptor,
            offset,
            total,
            data,
            ..
        } = frame
        {
            assert_eq!(for_frame, material_frame);
            assert_eq!(blob, &material.blob);
            let descriptor = hashed_blobs::EncodedBlob::decode(descriptor).unwrap();
            assert_eq!(descriptor.hash.to_string(), material.blob);
            assert_eq!(descriptor.decoded_len, material.length);
            assert_eq!(*total, descriptor.stored_len);
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .unwrap();
            transferred = sinex_transport::frame_bytes::receive(
                &receiver,
                "session-test",
                for_frame,
                &descriptor,
                *offset,
                &bytes,
            )
            .unwrap();
        }
    }
    assert!(transferred, "complete encoded custody reached the receiver");
    let mut decoded = Vec::new();
    sinex_transport::frame_bytes::reader(
        &receiver,
        "session-test",
        material_frame,
        &material.blob,
        material.length,
    )
    .unwrap()
    .read_to_end(&mut decoded)
    .unwrap();
    assert_eq!(
        decoded, captured,
        "receiver retained the actual captured spool bytes"
    );
    let second_record = captured.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    let events: Vec<_> = offered
        .iter()
        .flat_map(|frame| match frame {
            Ingress::Events { source, events, .. } => {
                assert_eq!(source.as_ref(), Some(&material.source));
                events.as_slice()
            }
            _ => &[],
        })
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind.name, "window.focused");
    assert_eq!(
        events[0].location,
        Some(sinex_model::RecordLocation {
            material: material.id,
            anchor: second_record as u64,
        })
    );
    assert_eq!(
        received
            .lock()
            .unwrap()
            .iter()
            .flat_map(|frame| match frame {
                Ingress::Events { events, .. } => events.as_slice(),
                _ => &[],
            })
            .filter(|event| event.kind.name == "window.focused")
            .count(),
        1
    );
    let destination = root.path().join("relocated-state");
    std::fs::rename(&state, &destination).unwrap();
    symlink(&destination, &state).unwrap();
    let bindings = discovery::spools(&context, discovery::Owner::Session).unwrap();
    assert!(bindings.iter().any(|(_, binding)| binding.name == name));
    let second_started = Timestamp::now().unix_ns();
    live::drain(&mut producer, &store, &positions, bindings)
        .await
        .unwrap();
    let second_ended = Timestamp::now().unix_ns();
    assert_eq!(
        received.lock().unwrap().len(),
        frames,
        "admitted records are not sent again"
    );
    let after = Position::load(&positions, &name).unwrap();
    let second_cursor: live_stream::Cursor = serde_json::from_value(after.cursor.clone()).unwrap();
    let second_inner: append_file::Cursor = serde_json::from_value(second_cursor.inner).unwrap();
    if second_inner.looked_ns != first_inner.looked_ns {
        assert!(
            second_inner.looked_ns > first_inner.looked_ns,
            "refreshed acquisition look is monotone"
        );
        assert!((second_started..=second_ended).contains(&second_inner.looked_ns));
        assert!(
            (0..append_file::RACY_NS).contains(&(first_inner.looked_ns - changed)),
            "only a prior look within the existing racy-file window permits refresh"
        );
    }
    // An unchanged look can retain the first interval's valid acquisition
    // basis; a refreshed look was independently proved above. Compare every
    // other cursor and Position field exactly, including generated IDs.
    let mut actual = serde_json::to_value(after).unwrap();
    let expected = serde_json::to_value(admitted).unwrap();
    actual["cursor"]["inner"]["looked_ns"] = expected["cursor"]["inner"]["looked_ns"].clone();
    assert_eq!(actual, expected);
    assert_eq!(std::fs::read(&segment).unwrap(), captured);

    assert!(destination.join("positions").is_dir());
}

#[devkit::test(multi_thread)]
async fn drain_retries_pending_frames_then_requires_the_missing_spool() {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
    use sinex_sources::{discovery, live};
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let hello = hello::hello(&nats, &subjects, "session-test").await;
    let root = tempfile::tempdir().unwrap();
    let spool_root = root.path().join("spool");
    let spool = spool_root.join("hyprland");
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    for line in [
        &b"activewindow>>kitty,synthetic retry"[..],
        &b"activewindowv2>>55aa"[..],
    ] {
        writer.append(sinex_model::Timestamp::now(), line).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);
    let context = discovery::DiscoveryContext {
        host: "h".into(),
        spool: spool_root,
        ..Default::default()
    };
    let store = Store::open(&root.path().join("store")).unwrap();
    let positions = root.path().join("positions");
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects.clone(),
        "session-test",
        &Default::default(),
    )
    .await
    .unwrap();
    let error = live::drain(
        &mut producer,
        &store,
        &positions,
        discovery::spools(&context, discovery::Owner::Session).unwrap(),
    )
    .await
    .unwrap_err();
    assert!(
        error.contains("no stream found for given subject"),
        "{error}"
    );
    drop(producer);
    std::fs::rename(&spool, root.path().join("kept-spool")).unwrap();
    hello.abort();
    let _ = hello.await;
    let received = standin(&nats, &subjects, "session-test").await;
    let mut producer = Producer::connect(
        nats.url(),
        None,
        subjects,
        "session-test",
        &Default::default(),
    )
    .await
    .unwrap();
    let missing = live::drain(
        &mut producer,
        &store,
        &positions,
        discovery::spools(&context, discovery::Owner::Session).unwrap(),
    )
    .await
    .unwrap_err();
    assert!(
        missing.contains("is absent after saved progress"),
        "{missing}"
    );
    assert!(
        received
            .lock()
            .unwrap()
            .iter()
            .any(|frame| matches!(frame, Ingress::Events { .. }))
    );
    assert!(std::fs::read_dir(&positions).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("unacknowledged")
    }));
    let frames = received.lock().unwrap().len();
    assert_eq!(
        received
            .lock()
            .unwrap()
            .iter()
            .flat_map(|frame| match frame {
                Ingress::Events { events, .. } => events.as_slice(),
                _ => &[],
            })
            .filter(|event| event.kind.name == "window.focused")
            .count(),
        1
    );
    std::fs::rename(root.path().join("kept-spool"), &spool).unwrap();
    live::drain(
        &mut producer,
        &store,
        &positions,
        discovery::spools(&context, discovery::Owner::Session).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        received.lock().unwrap().len(),
        frames,
        "acknowledged records are not sent again"
    );
    assert!(spool.exists());
}
