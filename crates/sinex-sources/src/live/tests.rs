use super::*;

#[devkit::test]
fn options_keep_numbers_and_collect_lists() {
    let o = options(&[
        "every=60".into(),
        "kind=screen".into(),
        "follow=true".into(),
        "roots=/a".into(),
        "roots=/b".into(),
    ])
    .unwrap();
    assert_eq!(
        o,
        serde_json::json!({"every": 60, "kind": "screen", "follow": true, "roots": ["/a", "/b"]})
    );
    assert_eq!(options(&[]).unwrap(), Value::Null);
    assert!(options(&["novalue".into()]).is_err());
}

#[devkit::test]
fn live_bindings_come_from_the_kinds_that_capture() {
    let (kind, b) = binding(
        "clipboard",
        Path::new("/spool/primary"),
        "h",
        serde_json::json!({"selection": "primary"}),
    )
    .unwrap();
    assert_eq!(kind.name, "clipboard");
    assert!(b.name.contains("#primary@h"), "{}", b.name);
    // An export is not a live source.
    assert!(binding("spotify", Path::new("/x"), "h", Value::Null).is_err());
}

use futures::StreamExt;
use serde_json::json;
use sinex_protocol::{Acknowledgment, Ingress, Subjects};
use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

static SYNTHETIC: InputFormat = InputFormat {
    capture: Some(|binding, stop| {
        let mode = sinex_transport::private_mode::PrivateMode::new(binding.input.join("private"));
        let mut writer = SpoolWriter::open(&binding.input, Rotation::default())
            .map_err(|e| e.to_string())?
            .with_private_mode(mode);
        let mut count = 0;
        while !stop.stopped() {
            if !binding.input.join("finished").exists() {
                count += 1;
                writer
                    .append(
                        sinex_model::Timestamp::now(),
                        format!("workspacev2>>{count},synthetic").as_bytes(),
                    )
                    .map_err(|e| e.to_string())?;
                writer.sync().map_err(|e| e.to_string())?;
            }
            stop.wait(Duration::from_millis(100));
        }
        Ok(count)
    }),
    ..sources::hyprland::FORMAT
};

async fn fake_ingress(
    url: &str,
    subjects: &Subjects,
    available: Arc<AtomicBool>,
    timeout: Arc<tokio::sync::Notify>,
) -> (
    Arc<Mutex<HashMap<String, Ingress>>>,
    tokio::task::JoinHandle<()>,
) {
    let client = async_nats::connect(url).await.unwrap();
    jetstream_stream(&client, subjects).await;
    let mut incoming = client
        .subscribe(subjects.ingress("synthetic"))
        .await
        .unwrap();
    let mut queries = client
        .subscribe(subjects.recorded("synthetic"))
        .await
        .unwrap();
    client.flush().await.unwrap();
    let received = Arc::new(Mutex::new(HashMap::<String, Ingress>::new()));
    let frames = received.clone();
    let ack_subject = subjects.ack("synthetic");
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(message) = incoming.next() => {
                    if !available.load(Ordering::SeqCst) { continue; }
                    let frame: Ingress = serde_json::from_slice(&message.payload).unwrap();
                    frames.lock().unwrap().entry(frame.frame().to_string()).or_insert_with(|| frame.clone());
                    let ack = Acknowledgment { frame: frame.frame().to_string(), material: None, admitted: vec![], outcome: json!({}), refusal: None, commit_seq: Some(1) };
                    client.publish(ack_subject.clone(), serde_json::to_vec(&ack).unwrap().into()).await.unwrap();
                }
                Some(message) = queries.next() => {
                    // Frames dropped while the ingress was unavailable have
                    // no record. The producer must resend their original IDs.
                    client.publish(message.reply.unwrap(), b"[]".as_slice().into()).await.unwrap();
                    timeout.notify_one();
                }
                else => break,
            }
        }
    });
    (received, task)
}

async fn jetstream_stream(client: &async_nats::Client, subjects: &Subjects) {
    async_nats::jetstream::new(client.clone())
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: subjects.ingress_stream(),
            subjects: vec![subjects.ingress_all()],
            ..Default::default()
        })
        .await
        .unwrap();
}

#[devkit::test(multi_thread)]
async fn acknowledgment_timeout_keeps_capture_writing_and_recovery_drains_without_duplicates() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let available = Arc::new(AtomicBool::new(false));
    let timeout = Arc::new(tokio::sync::Notify::new());
    let (received, ingress) =
        fake_ingress(nats.url(), &subjects, available.clone(), timeout.clone()).await;
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let positions = dir.path().join("positions");
    let (_, binding) = binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let connection = Connection {
        url: nats.url().into(),
        password_file: None,
        subjects,
        producer: "synthetic".into(),
        upload: false,
    };
    let task_positions = positions.clone();
    let run = tokio::spawn(async move {
        run(
            connection,
            &store,
            &task_positions,
            vec![(&SYNTHETIC, binding)],
            Duration::from_millis(20),
            Some(Instant::now() + Duration::from_secs(38)),
        )
        .await
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = std::fs::metadata(spool.join("000000000000.ndjson"))
        .unwrap()
        .len();
    // The production acknowledgment wait is 30 seconds. Cross that boundary
    // with the fake ingress down and verify the capture lifetime survives it.
    tokio::time::timeout(Duration::from_secs(45), timeout.notified())
        .await
        .unwrap();
    assert!(!run.is_finished());
    assert!(
        std::fs::metadata(spool.join("000000000000.ndjson"))
            .unwrap()
            .len()
            > before
    );
    let health: Value =
        serde_json::from_slice(&std::fs::read(positions.join("health.json")).unwrap()).unwrap();
    assert!(
        health["spools"][0]["unpublished"]["frames"]
            .as_u64()
            .unwrap()
            > 20
    );
    std::fs::write(spool.join("finished"), b"").unwrap();
    available.store(true, Ordering::SeqCst);
    run.await.unwrap().unwrap();
    let health: Value =
        serde_json::from_slice(&std::fs::read(positions.join("health.json")).unwrap()).unwrap();
    assert_eq!(health["spools"][0]["unpublished"]["frames"], 0);
    let captured = std::fs::read_to_string(spool.join("000000000000.ndjson"))
        .unwrap()
        .lines()
        .count();
    let frames = received.lock().unwrap();
    let events: Vec<_> = frames
        .values()
        .filter_map(|frame| match frame {
            Ingress::Events { events, .. } => Some(events),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(events.len(), captured);
    let workspaces: std::collections::HashSet<_> = events
        .iter()
        .map(|event| event.payload.to_string())
        .collect();
    assert_eq!(workspaces.len(), captured);
    ingress.abort();
}

#[devkit::test(multi_thread)]
async fn unavailable_connection_does_not_delay_capture_start() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("nats://{}", listener.local_addr().unwrap());
    drop(listener);
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let (_, binding) = binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    run(
        Connection {
            url,
            password_file: None,
            subjects: Subjects::sinex(),
            producer: "synthetic".into(),
            upload: false,
        },
        &store,
        &dir.path().join("positions"),
        vec![(&SYNTHETIC, binding)],
        Duration::from_millis(20),
        Some(Instant::now() + Duration::from_secs(2)),
    )
    .await
    .unwrap();
    assert!(
        std::fs::read_to_string(spool.join("000000000000.ndjson"))
            .unwrap()
            .lines()
            .count()
            >= 5
    );
}

#[devkit::test]
fn retry_delay_doubles_to_thirty_seconds_and_resets_after_recovery() {
    let mut retry = Retry::default();
    for seconds in [1, 2, 4, 8, 16, 30, 30] {
        retry.failed("unavailable", &"synthetic failure");
        assert_eq!(retry.delay, Duration::from_secs(seconds));
        assert!(!retry.ready());
    }
    retry.recovered();
    assert!(retry.ready());
    retry.failed("unavailable", &"synthetic failure");
    assert_eq!(retry.delay, Duration::from_secs(1));
}

static INTERRUPTED: InputFormat = InputFormat {
    capture: Some(|binding, stop| {
        let marker = binding.input.join("first-attempt");
        if !marker.exists() {
            std::fs::write(marker, b"").map_err(|e| e.to_string())?;
            return Err("synthetic source disconnected".into());
        }
        (SYNTHETIC.capture.unwrap())(binding, stop)
    }),
    ..sources::hyprland::FORMAT
};

#[devkit::test(multi_thread)]
async fn a_source_failure_restarts_capture_while_the_bus_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let (_, binding) = binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("nats://{}", listener.local_addr().unwrap());
    drop(listener);
    let store = Store::open(&dir.path().join("store")).unwrap();
    run(
        Connection {
            url,
            password_file: None,
            subjects: Subjects::sinex(),
            producer: "synthetic".into(),
            upload: false,
        },
        &store,
        &dir.path().join("positions"),
        vec![(&INTERRUPTED, binding)],
        Duration::from_millis(20),
        Some(Instant::now() + Duration::from_secs(3)),
    )
    .await
    .unwrap();
    assert!(
        std::fs::read_to_string(spool.join("000000000000.ndjson"))
            .unwrap()
            .lines()
            .count()
            >= 5
    );
}

static BROKEN_SPOOL: InputFormat = InputFormat {
    capture: Some(|binding, _| {
        std::fs::remove_dir(&binding.input).map_err(|e| e.to_string())?;
        std::fs::write(&binding.input, b"not a spool directory").map_err(|e| e.to_string())?;
        Err("synthetic local storage failure".into())
    }),
    ..sources::hyprland::FORMAT
};

#[devkit::test(multi_thread)]
async fn an_unwritable_spool_is_a_local_fault_that_ends_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let (_, binding) = binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let result = run(
        Connection {
            url: "nats://127.0.0.1:1".into(),
            password_file: None,
            subjects: Subjects::sinex(),
            producer: "synthetic".into(),
            upload: false,
        },
        &store,
        &dir.path().join("positions"),
        vec![(&BROKEN_SPOOL, binding)],
        Duration::from_millis(20),
        Some(Instant::now() + Duration::from_secs(3)),
    )
    .await;
    assert!(result.unwrap_err().contains("spool"));
}
