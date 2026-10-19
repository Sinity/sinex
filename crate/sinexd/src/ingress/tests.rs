use super::*;
use crate::writer;
use serde_json::json;
use sinex_model::{InputRef, MaterialId, RecordLocation};
use sinex_protocol::Confirmed;
use sqlx::Connection;
use std::sync::Arc;

/// A daemon slice for tests: NATS, a database, the writer, the ingress task
/// and a content store.
struct Harness {
    nats: devkit::nats::TestNats,
    subjects: Subjects,
    client: async_nats::Client,
    url: String,
    blobs: Blobs,
    store_dir: std::path::PathBuf,
    stop: watch::Sender<bool>,
    _db: devkit::pg::TestDatabase,
}

impl Harness {
    async fn start() -> Self {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let client = async_nats::connect(nats.url()).await.unwrap();
        let (db, url, connection) = crate::testdb::fresh().await;
        let (handle, queues) = writer::channels();
        tokio::spawn(writer::run(
            url.clone(),
            connection,
            queues,
            Arc::new(sinex_store::NoDerivations),
        ));
        let store_dir = std::env::temp_dir().join(format!("sinex-ingress-{}", Uuid::now_v7()));
        let blobs = Blobs::open(&store_dir).unwrap();
        let ingest = Ingest::new(
            subjects.clone(),
            handle,
            PgConnection::connect(&url).await.unwrap(),
            blobs.clone(),
            "test-host".into(),
        )
        .unwrap();
        let (stop, stopped) = watch::channel(false);
        let task_client = client.clone();
        tokio::spawn(async move { run(task_client, ingest, stopped).await.unwrap() });
        let harness = Self {
            nats,
            subjects,
            client,
            url,
            blobs,
            store_dir,
            stop,
            _db: db,
        };
        // The ingress stream exists once the task has started.
        harness.wait_for_stream().await;
        harness
    }

    async fn wait_for_stream(&self) {
        let js = jetstream::new(self.client.clone());
        for _ in 0..500 {
            if js.get_stream(self.subjects.ingress_stream()).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("ingress stream was not created");
    }

    /// Send a frame as `producer` and wait for its acknowledgment.
    async fn send(&self, producer: &str, frame: &Ingress) -> Acknowledgment {
        self.send_raw(producer, serde_json::to_vec(frame).unwrap())
            .await
    }

    async fn send_raw(&self, producer: &str, body: Vec<u8>) -> Acknowledgment {
        let mut acks = self
            .client
            .subscribe(self.subjects.ack(producer))
            .await
            .unwrap();
        self.client.flush().await.unwrap();
        let js = jetstream::new(self.client.clone());
        js.publish(self.subjects.ingress(producer), body.into())
            .await
            .unwrap()
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(20), acks.next())
            .await
            .expect("an acknowledgment")
            .unwrap();
        serde_json::from_slice(&message.payload).unwrap()
    }

    async fn have(&self, producer: &str, frames: &[&str]) -> Vec<Acknowledgment> {
        let request = Have {
            frames: frames.iter().map(|s| s.to_string()).collect(),
        };
        let reply = self
            .client
            .request(
                self.subjects.have(producer),
                serde_json::to_vec(&request).unwrap().into(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&reply.payload).unwrap()
    }

    fn material_frame(&self, frame: &str, bytes: &[u8]) -> (MaterialId, Ingress) {
        let blob = self.blobs.put(bytes).unwrap();
        let id = MaterialId::new();
        (
            id,
            Ingress::Material {
                frame: frame.into(),
                material: Box::new(MaterialFrame {
                    id,
                    input: InputRef {
                        kind: "file".into(),
                        locator: "/home/op/.zsh_history".into(),
                        host: "laptop".into(),
                    },
                    blob,
                    length: bytes.len() as u64,
                    parser: "zsh".into(),
                    parser_version: 1,
                    part_index: 0,
                    previous_part: None,
                    manifest: json!({"media_type": "text/plain"}),
                }),
            },
        )
    }

    async fn count(&self, sql: &str) -> i64 {
        let mut conn = PgConnection::connect(&self.url).await.unwrap();
        sqlx::query_scalar(sql).fetch_one(&mut conn).await.unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        let _ = std::fs::remove_dir_all(&self.store_dir);
    }
}

fn imported(material: MaterialId, anchor: u64, command: &str) -> ProducerEvent {
    ProducerEvent {
        event_type: "command.imported".into(),
        payload: json!({
            "command": command,
            "hostname": "laptop",
            "started_at": "2026-10-18T12:00:00Z",
            "header_elapsed": 0
        }),
        location: Some(RecordLocation { material, anchor }),
        ordinal: 0,
        parents: vec![],
        reading_order: 0,
        class: sinex_model::Class::Open,
    }
}

#[tokio::test]
async fn materials_then_events_are_admitted_and_acknowledged_once() {
    let h = Harness::start().await;
    let bytes = b": 1760788800:0;ls\n: 1760788801:0;pwd\n";
    let (material, frame) = h.material_frame("m1", bytes);
    let ack = h.send("kitty", &frame).await;
    assert_eq!(ack.refusal, None);
    assert_eq!(ack.material, Some(material));

    let events = Ingress::Events {
        frame: "e1".into(),
        events: vec![imported(material, 0, "ls"), imported(material, 18, "pwd")],
    };
    let ack = h.send("kitty", &events).await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.admitted.len(), 2);
    assert_eq!(ack.outcome["admitted"], 2);
    assert!(ack.commit_seq.is_some());

    // The same frame again: the recorded acknowledgment, nothing admitted twice.
    let again = h.send("kitty", &events).await;
    assert_eq!(again, ack);
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE source = 'kitty'")
            .await,
        2
    );
    // The same events in a new frame are repeats.
    let resent = Ingress::Events {
        frame: "e2".into(),
        events: vec![imported(material, 0, "ls")],
    };
    let ack2 = h.send("kitty", &resent).await;
    assert_eq!(ack2.admitted, vec![]);
    assert_eq!(ack2.outcome["repeats"], 1);

    let have = h.have("kitty", &["m1", "e1", "never-sent"]).await;
    let names: Vec<&str> = have.iter().map(|a| a.frame.as_str()).collect();
    assert_eq!(names, ["m1", "e1"]);
    // Another producer's frames are not visible under this producer.
    assert!(h.have("phone", &["e1"]).await.is_empty());
}

/// Polylogue's side of the boundary (architecture §9): a unit of an
/// archive's rows arrives as a record-batch material and one `polylogue.rows`
/// event at its start. Sinex keys units by archive and position, so a unit
/// sent again in a new frame is a repeat, and the next position is a new
/// unit. Polylogue's own rebuild proof drives the same frames through a real
/// `sinexd` (polylogue `crates/sinex/tests/proof.rs`).
#[tokio::test]
async fn polylogue_units_are_admitted_once_by_archive_and_position() {
    let h = Harness::start().await;
    let rows = b"{\"table\":\"note\",\"row\":{\"id\":\"n1\",\"local_seq\":4}}\n";
    let (batch, frame) = h.material_frame("batch:a1:1", rows);
    assert_eq!(h.send("polylogue", &frame).await.refusal, None);
    let unit = |position: u64| ProducerEvent {
        event_type: "polylogue.rows".into(),
        payload: json!({
            "archive": "a1",
            "position": position,
            "local_seq": 4,
            "unit": "note",
            "key": "n1",
            "batch": format!("blake3:{}", blake3::hash(rows).to_hex()),
            "rows": 1,
            "blobs": []
        }),
        location: Some(RecordLocation {
            material: batch,
            anchor: 0,
        }),
        ordinal: 0,
        parents: vec![],
        reading_order: position as i64,
        class: sinex_model::Class::Open,
    };
    let first = Ingress::Events {
        frame: "rows:a1:1".into(),
        events: vec![unit(1)],
    };
    let ack = h.send("polylogue", &first).await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.outcome["admitted"], 1);
    let again = Ingress::Events {
        frame: "rows:a1:1-again".into(),
        events: vec![unit(1)],
    };
    assert_eq!(h.send("polylogue", &again).await.outcome["repeats"], 1);
    let next = Ingress::Events {
        frame: "rows:a1:2".into(),
        events: vec![unit(2)],
    };
    assert_eq!(h.send("polylogue", &next).await.outcome["admitted"], 1);
    let have = h
        .have(
            "polylogue",
            &["batch:a1:1", "rows:a1:1", "rows:a1:2", "rows:a1:3"],
        )
        .await;
    assert_eq!(have.len(), 3, "the unsent position is missing");
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type = 'polylogue.rows'")
            .await,
        2
    );
}

#[tokio::test]
async fn invalid_frames_are_refused_and_recorded() {
    let h = Harness::start().await;
    let (material, frame) = h.material_frame("m1", b"x\n");
    h.send("kitty", &frame).await;

    let mut wrong_type = imported(material, 0, "ls");
    wrong_type.event_type = "no.such.type".into();
    let mut missing_field = imported(material, 0, "ls");
    missing_field.payload = json!({"command": "ls"});
    let mut no_origin = imported(material, 0, "ls");
    no_origin.location = None;
    let unregistered = imported(MaterialId::new(), 0, "ls");

    for (name, event, expected) in [
        ("f-type", wrong_type, "not a registered event type"),
        ("f-schema", missing_field, "hostname"),
        ("f-origin", no_origin, "record location or parent"),
        ("f-material", unregistered, "not registered"),
    ] {
        let ack = h
            .send(
                "kitty",
                &Ingress::Events {
                    frame: name.into(),
                    events: vec![event],
                },
            )
            .await;
        let reason = ack.refusal.unwrap_or_default();
        assert!(reason.contains(expected), "{name}: {reason}");
        assert!(ack.admitted.is_empty());
    }
    // Bytes the producer never placed.
    let missing_bytes = Ingress::Material {
        frame: "m-missing".into(),
        material: Box::new(MaterialFrame {
            blob: format!("blake3:{}", blake3::hash(b"absent").to_hex()),
            ..match h.material_frame("unused", b"y").1 {
                Ingress::Material { material, .. } => *material,
                Ingress::Events { .. } => unreachable!(),
            }
        }),
    };
    let ack = h.send("kitty", &missing_bytes).await;
    assert!(ack.refusal.unwrap().contains("not in the content store"));

    // Refusals are recorded: asking again gets the same answer.
    let have = h.have("kitty", &["f-type", "m-missing"]).await;
    assert_eq!(have.len(), 2);
    assert!(have.iter().all(|a| a.refusal.is_some()));
    assert_eq!(
        h.count("SELECT count(*) FROM core.events").await,
        0,
        "no refused frame admitted anything"
    );

    let garbage = h.send_raw("kitty", b"{not json".to_vec()).await;
    assert!(garbage.refusal.unwrap().starts_with("not a frame"));
}

#[tokio::test]
async fn producers_are_limited_to_their_own_subjects() {
    let subjects = Subjects::new("sx");
    let config = subjects.authorization(("daemon", "dpw"), &[("kitty", "kpw"), ("phone", "ppw")]);
    let server = devkit::nats::Server::start_with(&devkit::nats::Options { config }).unwrap();
    let address = server.url().trim_start_matches("nats://").to_owned();
    let connect = |user: &str, password: &str| {
        async_nats::ConnectOptions::with_user_and_password(user.into(), password.into())
            .connect(address.clone())
    };
    let daemon = connect("daemon", "dpw").await.unwrap();
    let mut seen = daemon.subscribe("sx.ingress.*").await.unwrap();
    daemon.flush().await.unwrap();

    let kitty = connect("kitty", "kpw").await.unwrap();
    kitty
        .publish("sx.ingress.phone", "forged".into())
        .await
        .unwrap();
    kitty
        .publish("sx.ingress.kitty", "mine".into())
        .await
        .unwrap();
    kitty.flush().await.unwrap();

    let first = tokio::time::timeout(Duration::from_secs(5), seen.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.subject.as_str(), "sx.ingress.kitty");
    assert_eq!(&first.payload[..], b"mine");
    assert!(
        tokio::time::timeout(Duration::from_millis(300), seen.next())
            .await
            .is_err(),
        "the forged message on another producer's subject was not delivered"
    );
    // Nor can a producer read another producer's acknowledgments.
    assert!(kitty.subscribe("sx.ack.phone").await.is_ok());
    let mut probe = kitty.subscribe("sx.ack.phone").await.unwrap();
    daemon
        .publish("sx.ack.phone", "secret".into())
        .await
        .unwrap();
    daemon.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), probe.next())
            .await
            .map_or(true, |m| m.is_none()),
        "subscription to another producer's acknowledgments delivers nothing"
    );
}

#[tokio::test]
async fn ingress_events_reach_the_confirmed_stream() {
    let h = Harness::start().await;
    let (material, frame) = h.material_frame("m1", b"ls\n");
    h.send("kitty", &frame).await;
    h.send(
        "kitty",
        &Ingress::Events {
            frame: "e1".into(),
            events: vec![imported(material, 0, "ls")],
        },
    )
    .await;
    let js = jetstream::new(h.client.clone());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    crate::publisher::ensure_stream(&js, &h.subjects)
        .await
        .unwrap();
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let stream = js.get_stream(h.subjects.confirmed_stream()).await.unwrap();
    let message = stream
        .get_last_raw_message_by_subject(&h.subjects.confirmed_event("command.imported"))
        .await
        .unwrap();
    let confirmed: Confirmed = serde_json::from_slice(&message.payload).unwrap();
    let Confirmed::Event(event) = confirmed else {
        panic!("event")
    };
    assert_eq!(event.source, "kitty");
    assert_eq!(event.payload.unwrap()["command"], "ls");
    let _ = &h.nats;
}
