use sinex_protocol::{Ingress, Subjects};
use sinex_source_core::drivers::producer::{Position, Producer, Store};
use std::{path::Path, time::Duration};
#[path = "../../sinex-source-core/tests/support/producer.rs"]
mod support;
use support::standin;

/// Produce a Hyprland spool against the stand-in: material first, once.
#[devkit::test(multi_thread)]
async fn a_spool_is_produced_once_material_first() {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter, capture_lines};
    use sinex_sources::sources::hyprland;

    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let received = standin(&nats, &subjects, "session-test", None).await;

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
