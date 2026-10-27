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
                outcome: json!({}),
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
    let mut producer = Producer::connect(nats.url(), None, subjects, "session-test")
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

#[devkit::test(multi_thread)]
async fn draining_after_moving_session_storage_keeps_names_and_positions() {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
    use sinex_sources::{discovery, live};
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
    let mut producer = Producer::connect(nats.url(), None, subjects, "session-test")
        .await
        .unwrap();
    live::drain(&mut producer, &store, &positions, bindings)
        .await
        .unwrap();
    let admitted = Position::load(&positions, &name).unwrap();
    let frames = received.lock().unwrap().len();
    assert!(frames > 0);
    assert_eq!(
        received
            .lock()
            .unwrap()
            .iter()
            .flat_map(|frame| match frame {
                Ingress::Events { events, .. } => events.as_slice(),
                _ => &[],
            })
            .filter(|event| event.event_type == "window.focused")
            .count(),
        1
    );
    let destination = root.path().join("relocated-state");
    std::fs::rename(&state, &destination).unwrap();
    symlink(&destination, &state).unwrap();
    let bindings = discovery::spools(&context, discovery::Owner::Session).unwrap();
    assert!(bindings.iter().any(|(_, binding)| binding.name == name));
    live::drain(&mut producer, &store, &positions, bindings)
        .await
        .unwrap();
    assert_eq!(
        received.lock().unwrap().len(),
        frames,
        "admitted records are not sent again"
    );
    assert_eq!(
        Position::load(&positions, &name).unwrap().cursor,
        admitted.cursor
    );
    assert!(destination.join("positions").is_dir());
}

#[devkit::test(multi_thread)]
async fn drain_retries_pending_frames_then_requires_the_missing_spool() {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
    use sinex_sources::{discovery, live};
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
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
    let mut producer = Producer::connect(nats.url(), None, subjects.clone(), "session-test")
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
    let received = standin(&nats, &subjects, "session-test").await;
    let mut producer = Producer::connect(nats.url(), None, subjects, "session-test")
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
            .filter(|event| event.event_type == "window.focused")
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
