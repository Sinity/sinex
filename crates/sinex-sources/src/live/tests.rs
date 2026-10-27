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
use std::sync::Mutex;

static SYNTHETIC: InputFormat = InputFormat {
    capture: Some(|binding, stop| {
        let mode = sinex_transport::private_mode::PrivateMode::new(binding.input.join("private"));
        let mut writer = SpoolWriter::open(&binding.input, Rotation::default())
            .map_err(|e| e.to_string())?
            .with_private_mode(mode);
        let mut count = 0;
        while !stop.stopped() && !binding.input.join("finished").exists() {
            count += 1;
            writer
                .append(
                    sinex_model::Timestamp::now(),
                    format!("workspacev2>>{count},synthetic").as_bytes(),
                )
                .map_err(|e| e.to_string())?;
            writer.sync().map_err(|e| e.to_string())?;
            stop.wait(Duration::from_millis(100));
        }
        if !stop.stopped() {
            std::fs::write(binding.input.join("paused"), b"").map_err(|e| e.to_string())?;
            while !stop.wait(Duration::from_millis(100)) {}
        }
        Ok(count)
    }),
    ..sources::hyprland::FORMAT
};

type ReceivedFrames = Arc<Mutex<HashMap<String, Ingress>>>;
type ReceivedOrder = Arc<Mutex<Vec<Ingress>>>;

async fn fake_ingress(
    url: &str,
    subjects: &Subjects,
    mut available: tokio::sync::watch::Receiver<bool>,
    timeout: Arc<tokio::sync::Notify>,
) -> (ReceivedFrames, ReceivedOrder, tokio::task::JoinHandle<()>) {
    let client = async_nats::connect(url).await.unwrap();
    let mut declarations = client.subscribe(subjects.connect("synthetic")).await.unwrap();
    let declarer = client.clone();
    tokio::spawn(async move {
        while let Some(message) = declarations.next().await {
            declarer.publish(message.reply.unwrap(), serde_json::to_vec(&sinex_protocol::ProducerWelcome { refusal: None }).unwrap().into()).await.unwrap();
        }
    });
    jetstream_stream(&client, subjects).await;
    let stream = async_nats::jetstream::new(client.clone())
        .get_stream(subjects.ingress_stream())
        .await
        .unwrap();
    let consumer = stream
        .get_or_create_consumer(
            "synthetic",
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("synthetic".into()),
                ack_wait: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut incoming = consumer.messages().await.unwrap();
    let mut queries = client
        .subscribe(subjects.recorded("synthetic"))
        .await
        .unwrap();
    client.flush().await.unwrap();
    let received = Arc::new(Mutex::new(HashMap::<String, Ingress>::new()));
    let frames = received.clone();
    let order = Arc::new(Mutex::new(Vec::new()));
    let received_order = order.clone();
    let ack_subject = subjects.ack("synthetic");
    let task = tokio::spawn(async move {
        let mut pending = Vec::new();
        let mut recorded = HashMap::<String, Acknowledgment>::new();
        loop {
            tokio::select! {
                Some(message) = incoming.next() => {
                    if let Ok(message) = message {
                        let frame = serde_json::from_slice::<Ingress>(&message.payload).unwrap();
                        pending.push((frame, message));
                    }
                }
                Some(message) = queries.next() => {
                    let query: sinex_protocol::RecordedQuery = serde_json::from_slice(&message.payload).unwrap();
                    let replies: Vec<_> = query.frames.iter().filter_map(|id| recorded.get(id).cloned()).collect();
                    client.publish(message.reply.unwrap(), serde_json::to_vec(&replies).unwrap().into()).await.unwrap();
                    timeout.notify_one();
                }
                changed = available.changed() => {
                    if changed.is_err() { break; }
                }
                else => break,
            }
            // Ingress messages stay queued while the receiver is unavailable.
            let receiving = *available.borrow();
            if receiving {
                for (frame, message) in pending.drain(..) {
                    let id = frame.frame().to_string();
                    let ack = recorded
                        .entry(id.clone())
                        .or_insert_with(|| {
                            let (material, admitted) = match &frame {
                                Ingress::Material { material, .. } => (Some(material.id), vec![]),
                                Ingress::Events { events, .. } => (
                                    None,
                                    events.iter().map(|_| sinex_model::EventId::new()).collect(),
                                ),
                                _ => (None, vec![]),
                            };
                            let commit_seq = if admitted.is_empty() { None } else { Some(1) };
                            Acknowledgment {
                                frame: id.clone(),
                                material,
                                admitted,
                                outcome: json!({}),
                                refusal: None,
                                commit_seq,
                            }
                        })
                        .clone();
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        frames.lock().unwrap().entry(id)
                    {
                        received_order.lock().unwrap().push(frame.clone());
                        entry.insert(frame);
                    }
                    message.ack().await.unwrap();
                    client
                        .publish(
                            ack_subject.clone(),
                            serde_json::to_vec(&ack).unwrap().into(),
                        )
                        .await
                        .unwrap();
                }
            }
        }
    });
    (received, order, task)
}

struct Running(tokio::task::JoinHandle<Result<(), String>>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn wait_for(running: &mut Running, mut ready: impl FnMut() -> bool) {
    let observed = async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(90), observed) => result.expect("live capture did not make progress"),
        result = &mut running.0 => panic!("live publishing stopped unexpectedly: {result:?}"),
    }
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
    let (available, availability) = tokio::sync::watch::channel(false);
    let timeout = Arc::new(tokio::sync::Notify::new());
    let (received, _, ingress) =
        fake_ingress(nats.url(), &subjects, availability, timeout.clone()).await;
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
    let private_mode = sinex_transport::private_mode::PrivateMode::new(spool.join("private"));
    let mut running = Running(tokio::spawn(async move {
        run_with_private_mode(
            connection,
            &store,
            &task_positions,
            vec![(&SYNTHETIC, binding)],
            Duration::from_millis(20),
            None,
            private_mode,
        )
        .await
    }));
    let segment = spool.join("000000000000.ndjson");
    wait_for(&mut running, || {
        std::fs::metadata(&segment).is_ok_and(|file| file.len() > 0)
    })
    .await;
    tokio::select! {
        _ = timeout.notified() => {},
        result = &mut running.0 => panic!("publishing stopped before the acknowledgment timeout: {result:?}"),
    }
    for _ in 0..2 {
        let before = std::fs::metadata(&segment).unwrap().len();
        wait_for(&mut running, || {
            std::fs::metadata(&segment).unwrap().len() > before
        })
        .await;
    }
    std::fs::write(spool.join("finished"), b"").unwrap();
    wait_for(&mut running, || spool.join("paused").exists()).await;
    let captured = std::fs::read_to_string(&segment).unwrap().lines().count();
    let captured_bytes = std::fs::metadata(&segment).unwrap().len();
    let report = positions.parent().unwrap().join("health.json");
    wait_for(&mut running, || {
        std::fs::read(&report)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .is_some_and(|health| {
                health["spools"][0]["unpublished"]["records"] == captured as u64
                    && health["spools"][0]["unpublished"]["bytes"] == captured_bytes
                    && health["spools"][0]["unpublished"]["frames"]
                        .as_u64()
                        .is_some_and(|frames| frames > 0)
            })
    })
    .await;
    available.send(true).unwrap();
    wait_for(&mut running, || {
        let health: Value = serde_json::from_slice(&std::fs::read(&report).unwrap()).unwrap();
        health["spools"][0]["unpublished"]["records"] == 0
            && health["spools"][0]["unpublished"]["frames"] == 0
    })
    .await;
    running.0.abort();
    assert!((&mut running.0).await.unwrap_err().is_cancelled());
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
    run_with_private_mode(
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
        sinex_transport::private_mode::PrivateMode::new(spool.join("private")),
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
        retry.failed("synthetic", "unavailable", &"synthetic failure");
        assert_eq!(retry.delay, Duration::from_secs(seconds));
        assert!(!retry.ready());
    }
    retry.recovered("synthetic");
    assert!(retry.ready());
    retry.failed("synthetic", "unavailable", &"synthetic failure");
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
    run_with_private_mode(
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
        sinex_transport::private_mode::PrivateMode::new(spool.join("private")),
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
    let result = run_with_private_mode(
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
        sinex_transport::private_mode::PrivateMode::new(spool.join("private")),
    )
    .await;
    assert!(result.unwrap_err().contains("spool"));
}

static PAUSED: InputFormat = InputFormat {
    capture: Some(|_, stop| {
        while !stop.wait(Duration::from_millis(100)) {}
        Ok(0)
    }),
    ..sources::hyprland::FORMAT
};

#[devkit::test(multi_thread)]
async fn backlogged_bindings_publish_materials_in_alternating_order() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let (available, availability) = tokio::sync::watch::channel(true);
    let (_, order, ingress) =
        fake_ingress(nats.url(), &subjects, availability, Arc::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let mut bindings = Vec::new();
    for name in ["first", "second"] {
        let spool = dir.path().join(name);
        let mut writer = SpoolWriter::open(
            &spool,
            Rotation {
                max_bytes: 1,
                ..Default::default()
            },
        )
        .unwrap()
        .with_private_mode(sinex_transport::private_mode::PrivateMode::new(
            dir.path().join("private"),
        ));
        for count in 0..4 {
            writer
                .append(
                    sinex_model::Timestamp::now(),
                    format!("workspacev2>>{count},synthetic").as_bytes(),
                )
                .unwrap();
        }
        writer.sync().unwrap();
        let (_, binding) = binding("hyprland", &spool, name, Value::Null).unwrap();
        bindings.push((&PAUSED, binding));
    }
    let store = Store::open(&dir.path().join("store")).unwrap();
    let positions = dir.path().join("positions");
    let private = sinex_transport::private_mode::PrivateMode::new(dir.path().join("private"));
    let mut running = Running(tokio::spawn(async move {
        run_with_private_mode(
            Connection {
                url: nats.url().into(),
                password_file: None,
                subjects,
                producer: "synthetic".into(),
                upload: false,
            },
            &store,
            &positions,
            bindings,
            Duration::from_secs(60),
            None,
            private,
        )
        .await
    }));
    wait_for(&mut running, || {
        order
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| matches!(frame, Ingress::Material { .. }))
            .count()
            == 8
    })
    .await;
    let hosts: Vec<_> = order
        .lock()
        .unwrap()
        .iter()
        .filter_map(|frame| match frame {
            Ingress::Material { material, .. } => Some(material.input.host.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        hosts,
        [
            "first", "second", "first", "second", "first", "second", "first", "second"
        ]
    );
    drop(available);
    ingress.abort();
}

struct RestartingNats {
    port: u16,
    dir: tempfile::TempDir,
    child: Option<std::process::Child>,
}

impl RestartingNats {
    fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        Self {
            port,
            dir: tempfile::tempdir().unwrap(),
            child: None,
        }
    }

    fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    async fn start(&mut self) {
        let log = std::fs::File::create(self.dir.path().join("nats.log")).unwrap();
        self.child = Some(
            std::process::Command::new("nats-server")
                .args([
                    "-a",
                    "127.0.0.1",
                    "-p",
                    &self.port.to_string(),
                    "-js",
                    "-sd",
                ])
                .arg(self.dir.path().join("jetstream"))
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if async_nats::connect(self.url()).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("NATS did not start");
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
}

impl Drop for RestartingNats {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn capture_recovers_when_nats_returns(initially_unavailable: bool) {
    let mut nats = RestartingNats::new();
    let subjects = Subjects::new("reconnect");
    let (available, availability) = tokio::sync::watch::channel(true);
    let mut responder = None;
    if !initially_unavailable {
        nats.start().await;
        responder =
            Some(fake_ingress(&nats.url(), &subjects, availability.clone(), Arc::default()).await);
    }
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    let (_, binding) = binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let positions = dir.path().join("positions");
    let connection = Connection {
        url: nats.url(),
        password_file: None,
        subjects: subjects.clone(),
        producer: "synthetic".into(),
        upload: false,
    };
    let private = sinex_transport::private_mode::PrivateMode::new(spool.join("private"));
    let mut running = Running(tokio::spawn(async move {
        run_with_private_mode(
            connection,
            &store,
            &positions,
            vec![(&SYNTHETIC, binding)],
            Duration::from_millis(20),
            None,
            private,
        )
        .await
    }));
    let segment = spool.join("000000000000.ndjson");
    if let Some((frames, _, _)) = &responder {
        wait_for(&mut running, || {
            frames
                .lock()
                .unwrap()
                .values()
                .any(|frame| matches!(frame, Ingress::Events { .. }))
        })
        .await;
        nats.stop();
    }
    wait_for(&mut running, || segment.exists()).await;
    let before = std::fs::metadata(&segment).unwrap().len();
    wait_for(&mut running, || {
        std::fs::metadata(&segment).unwrap().len() > before + 500
    })
    .await;
    std::fs::write(spool.join("finished"), b"").unwrap();
    wait_for(&mut running, || spool.join("paused").exists()).await;
    let captured: std::collections::HashSet<_> = std::fs::read_to_string(&segment)
        .unwrap()
        .lines()
        .map(|line| {
            let frame: Value = serde_json::from_str(line).unwrap();
            frame["line"].as_str().unwrap().to_owned()
        })
        .collect();
    let report = dir.path().join("health.json");
    wait_for(&mut running, || {
        std::fs::read(&report)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .is_some_and(|health| {
                health["spools"][0]["unpublished"]["records"]
                    .as_u64()
                    .is_some_and(|records| records > 0)
            })
    })
    .await;
    nats.start().await;
    let (received, _, ingress) = match responder {
        Some(responder) => responder,
        None => fake_ingress(&nats.url(), &subjects, availability, Arc::default()).await,
    };
    let report = dir.path().join("health.json");
    wait_for(&mut running, || {
        std::fs::read(&report)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .is_some_and(|health| {
                health["spools"][0]["unpublished"]["records"] == 0
                    && health["spools"][0]["unpublished"]["frames"] == 0
            })
    })
    .await;
    // Check every captured event across both server lifetimes.
    let frames = received.lock().unwrap();
    let events: Vec<_> = frames
        .values()
        .filter_map(|frame| match frame {
            Ingress::Events { events, .. } => Some(events),
            _ => None,
        })
        .flatten()
        .collect();
    let payloads: std::collections::HashSet<_> = events
        .iter()
        .map(|event| event.payload.to_string())
        .collect();
    assert_eq!(payloads.len(), events.len());
    assert_eq!(events.len(), captured.len());
    assert!(!events.is_empty());
    drop(available);
    ingress.abort();
}

#[devkit::test(multi_thread)]
async fn capture_keeps_writing_when_nats_drops_and_drains_after_restart() {
    capture_recovers_when_nats_returns(false).await;
}

#[devkit::test(multi_thread)]
async fn capture_drains_after_the_first_connection_fails_and_nats_starts() {
    capture_recovers_when_nats_returns(true).await;
}

#[devkit::test(multi_thread)]
async fn a_connect_refusal_is_reported_once_while_capture_continues() {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let client = async_nats::connect(nats.url()).await.unwrap();
    let mut declarations = client.subscribe(subjects.connect("synthetic")).await.unwrap();
    client.flush().await.unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let received = count.clone();
    let responder = tokio::spawn(async move {
        while let Some(message) = declarations.next().await {
            received.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            client.publish(message.reply.unwrap(), serde_json::to_vec(&sinex_protocol::ProducerWelcome { refusal: Some("producer bytes cannot be stored sealed".into()) }).unwrap().into()).await.unwrap();
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let positions = dir.path().join("positions");
    let spool = dir.path().join("spool");
    let (_, binding) = super::binding("hyprland", &spool, "synthetic", Value::Null).unwrap();
    let store = Store::open(&dir.path().join("store")).unwrap();
    let mode = sinex_transport::private_mode::PrivateMode::new(dir.path().join("private"));
    run_with_private_mode(Connection { url: nats.url().into(), password_file: None, subjects, producer: "synthetic".into(), upload: false }, &store, &positions, vec![(&SYNTHETIC, binding)], Duration::from_millis(50), Some(Instant::now() + Duration::from_secs(3)), mode).await.unwrap();
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    let report: Value = serde_json::from_slice(&std::fs::read(dir.path().join("health.json")).unwrap()).unwrap();
    assert_eq!(report["spools"][0]["state"], "error: producer bytes cannot be stored sealed");
    assert!(report["spools"][0]["unpublished"]["records"].as_u64().unwrap() >= 10);
    responder.abort();
}
