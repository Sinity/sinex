use super::*;
use crate::writer;
use serde_json::json;
use sinex_model::{InputRef, MaterialId, RecordLocation};
use sinex_protocol::Confirmed;
use sinex_transport::frame_bytes;
use sqlx::Connection;
use std::sync::Arc;

/// A daemon slice for tests: NATS, a database, the writer, the ingress task
/// and a content store.
struct Harness {
    reads: sqlx::PgPool,
    writer: WriterHandle,
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
    async fn start(db: devkit::pg::TestDatabase) -> Self {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let client = async_nats::connect(nats.url()).await.unwrap();
        let url = db.url(None);
        let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .unwrap();
        let store_dir = std::env::temp_dir().join(format!("sinex-ingress-{}", Uuid::now_v7()));
        let blobs = Blobs::open(&store_dir).unwrap();
        let (handle, queues) = writer::channels();
        tokio::spawn(writer::run(
            url.clone(),
            connection,
            queues,
            Arc::new(sinex_store::NoDerivations),
            Some(blobs.clone()),
        ));
        // One connection for requests, so a test can hold it to make a
        // request wait.
        let reads = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let requests = Requests {
            subjects: subjects.clone(),
            writer: handle.clone(),
            reads: reads.clone(),
            blobs: blobs.clone(),
        };
        let ingest = Ingest::new(
            subjects.clone(),
            handle.clone(),
            PgConnection::connect(&url).await.unwrap(),
            blobs.clone(),
            "test-host".into(),
        )
        .unwrap();
        let (stop, stopped) = watch::channel(false);
        let task_client = client.clone();
        tokio::spawn(async move { run(task_client, ingest, requests, stopped).await.unwrap() });
        let harness = Self {
            reads,
            writer: handle,
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

    /// Send a frame as `producer` and wait for its acknowledgment, after
    /// writing the fixture bytes it names to its spool directory, as a
    /// producer on this host does.
    async fn send(&self, producer: &str, frame: &Ingress) -> Acknowledgment {
        let mut spooled = Vec::new();
        match frame {
            Ingress::Material { material, .. } => spooled.push(material.blob.clone()),
            Ingress::LargeFrame { blob, .. } => {
                spooled.push(blob.clone());
                if let Ok(bytes) = std::fs::read(self.fixture_path(blob))
                    && let Ok(Ingress::Material { material, .. }) = serde_json::from_slice(&bytes)
                {
                    spooled.push(material.blob.clone());
                }
            }
            _ => {}
        }
        for blob in spooled {
            let Ok(bytes) = std::fs::read(self.fixture_path(&blob)) else {
                continue;
            };
            if let Err(reason) = frame_bytes::write(
                &self.store_dir,
                producer,
                frame.frame(),
                &blob,
                bytes.len() as u64,
                bytes.as_slice(),
            ) {
                return Acknowledgment {
                    frame: frame.frame().into(),
                    refusal: Some(reason.to_string()),
                    ..refused_unrecorded("")
                };
            }
        }
        self.send_raw(producer, serde_json::to_vec(frame).unwrap())
            .await
    }

    fn fixture_path(&self, blob: &str) -> std::path::PathBuf {
        self.store_dir
            .join("fixtures")
            .join(blake3::hash(blob.as_bytes()).to_hex().as_str())
    }
    fn fixture_blob(&self, bytes: &[u8]) -> String {
        let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
        let path = self.fixture_path(&blob);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
        blob
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

    async fn recorded(&self, producer: &str, frames: &[&str]) -> Vec<Acknowledgment> {
        let request = RecordedQuery {
            frames: frames.iter().map(|s| s.to_string()).collect(),
        };
        let reply = self
            .client
            .request(
                self.subjects.recorded(producer),
                serde_json::to_vec(&request).unwrap().into(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&reply.payload).unwrap()
    }

    async fn materials(&self, producer: &str, after: i64, limit: u32) -> Vec<ProducerMaterial> {
        let reply = self
            .client
            .request(
                self.subjects.materials(producer),
                serde_json::to_vec(&Materials { after, limit })
                    .unwrap()
                    .into(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&reply.payload).unwrap()
    }

    fn material_frame(&self, frame: &str, bytes: &[u8]) -> (MaterialId, Ingress) {
        let blob = self.fixture_blob(bytes);
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
                    manifest: sinex_model::Manifest::from_json(json!({"media_type": "text/plain"})),
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
            "header_elapsed": 0,
            // Distinct per place, so the tests' records stay distinct.
            "n": anchor
        }),
        location: Some(RecordLocation { material, anchor }),
        output_index: 0,
        parents: vec![],
        reading_order: 0,
        class: sinex_model::Class::Open,
    }
}

#[devkit::test]
async fn materials_then_events_are_admitted_and_acknowledged_once(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let bytes = b": 1760788800:0;ls\n: 1760788801:0;pwd\n";
    let (material, frame) = h.material_frame("m1", bytes);
    let ack = h.send("kitty", &frame).await;
    assert_eq!(ack.refusal, None);
    assert_eq!(ack.material, Some(material));

    let events = Ingress::Events {
        frame: "e1".into(),
        events: vec![imported(material, 0, "ls"), imported(material, 18, "pwd")],
        failures: vec![],
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
        failures: vec![],
    };
    let ack2 = h.send("kitty", &resent).await;
    assert_eq!(ack2.admitted, vec![]);
    assert_eq!(ack2.outcome["repeats"], 1);

    let have = h.recorded("kitty", &["m1", "e1", "never-sent"]).await;
    let names: Vec<&str> = have.iter().map(|a| a.frame.as_str()).collect();
    assert_eq!(names, ["m1", "e1"]);
    // Another producer's frames are not visible under this producer.
    assert!(h.recorded("phone", &["e1"]).await.is_empty());
}

/// A synthetic Claude Code session as Polylogue sends it: the session file as
/// a material of its real input, then the session, its message and tool call
/// and the turn, keyed by Polylogue's IDs and anchored at their records. The
/// same reading sent again is a repeat; a newer parser's reading replaces the
/// message; the session answers Sinex's own query language; and the
/// materials request lists the file, as Polylogue reads it to rebuild.
#[devkit::test]
async fn an_ai_session_from_polylogue_is_ordinary_events(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let file = concat!(
        r#"{"type":"user","uuid":"u1","sessionId":"s1","cwd":"/home/op/morrow","message":{"content":"run the tests"}}"#,
        "\n",
        r#"{"type":"assistant","uuid":"a1","sessionId":"s1","message":{"content":[{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"cargo test"}}]}}"#,
        "\n"
    );
    let blob = h.fixture_blob(file.as_bytes());
    let material = MaterialId::new();
    let frame = Ingress::Material {
        frame: "input:s1".into(),
        material: Box::new(MaterialFrame {
            id: material,
            input: InputRef {
                kind: "claude-code".into(),
                locator: "/home/op/.claude/projects/-home-op-morrow/s1.jsonl".into(),
                host: "laptop".into(),
            },
            blob,
            length: file.len() as u64,
            parser: "polylogue-claude-code".into(),
            parser_version: 40,
            part_index: 0,
            previous_part: None,
            manifest: sinex_model::Manifest::from_json(json!({"media_type": "application/jsonl"})),
        }),
    };
    assert_eq!(h.send("polylogue", &frame).await.refusal, None);
    let second = file.find('\n').unwrap() as u64 + 1;
    let at = |anchor: u64| Some(RecordLocation { material, anchor });
    let event =
        |event_type: &str, payload: serde_json::Value, anchor: u64, order: i64| ProducerEvent {
            event_type: event_type.into(),
            payload,
            location: at(anchor),
            output_index: 0,
            parents: vec![],
            reading_order: order,
            class: sinex_model::Class::Open,
        };
    let session = json!({
        "session": "claude-code:s1", "origin": "claude-code", "kind": "main", "title": null,
        "cwd": ["/home/op/morrow"], "repo": null, "branch": null, "tool_version": null,
        "mode": null, "machine": null,
        "started": "2026-10-03T09:00:00Z", "last_activity": "2026-10-03T09:00:05Z", "links": []
    });
    let message = |text: &str| {
        json!({
            "message": "claude-code:u1", "session": "claude-code:s1", "parent": null, "role": "user",
            "written_by": "human", "prompt_kind": "prose", "at": "2026-10-03T09:00:00Z",
            "model": null, "text": text, "usage": {"input": null, "cached_input": null,
            "cache_write": null, "output": null, "reasoning": null}, "cost_nusd": null,
            "stop_reason": null, "duration_ms": null, "attachments": [], "copies": []
        })
    };
    let call = json!({
        "call": "tool:claude-code:s1#t1", "session": "claude-code:s1", "message": "claude-code:a1",
        "tool": "Bash", "action": "shell", "command": "cargo test", "path": null,
        "outcome": "unknown", "outcome_reason": "no_result", "exit_code": null,
        "at": "2026-10-03T09:00:05Z", "result": null
    });
    let turn = json!({
        "turn": "claude-code:u1", "session": "claude-code:s1", "start": "2026-10-03T09:00:00Z",
        "end": "2026-10-03T09:00:05Z", "prompts": ["claude-code:u1"], "answer": "claude-code:a1",
        "slash_command": false, "tool_calls": 1, "errors": 0, "input_tokens": null,
        "output_tokens": null, "cost_nusd": null, "abandoned": false
    });
    let events = |text: &str, order: i64, name: &str| Ingress::Events {
        frame: name.into(),
        events: vec![
            event("polylogue.session", session.clone(), 0, order),
            event("polylogue.message", message(text), 0, order),
            event("polylogue.tool_call", call.clone(), second, order),
            event("polylogue.turn", turn.clone(), 0, order),
        ],
        failures: vec![],
    };
    let ack = h
        .send("polylogue", &events("run the tests", 1, "events:1"))
        .await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.outcome["admitted"], 4);
    // The same reading again, under a new frame: repeats.
    let again = h
        .send("polylogue", &events("run the tests", 1, "events:2"))
        .await;
    assert_eq!(again.outcome["repeats"], 4, "{again:?}");
    // A newer parser reads the prompt differently: only the message changes,
    // and it replaces the live reading.
    let newer = h
        .send("polylogue", &events("run the tests, please", 2, "events:3"))
        .await;
    assert_eq!(newer.outcome["admitted"], 1, "{newer:?}");
    assert_eq!(newer.outcome["repeats"], 3, "{newer:?}");
    assert_eq!(
        h.count(
            "SELECT count(*) FROM core.events e WHERE e.event_type = 'polylogue.message' \
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)"
        )
        .await,
        1
    );
    // The message text is guarded although the frame asked for open.
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type = 'polylogue.message' AND class = 'guarded'")
            .await,
        2
    );

    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let text = "events where event_type = polylogue.session and payload.cwd ~ morrow";
    let ctx = sinex_model::query::Context::new(
        jiff::Timestamp::now(),
        jiff::tz::TimeZone::UTC,
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let bound = sinex_model::query::bind(text, &ctx).unwrap();
    let answer = sinex_store::query::run(
        &mut conn,
        text,
        &bound,
        &ctx,
        &sinex_store::query::Page {
            size: Some(10),
            cursor: None,
        },
    )
    .await
    .unwrap();
    let rows = serde_json::to_value(answer).unwrap()["rows"].clone();
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows}");

    let page = h.materials("polylogue", 0, 10).await;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].frame, "input:s1");
    assert_eq!(page[0].material.id, material);
    assert_eq!(page[0].material.input.kind, "claude-code");
    assert!(
        h.materials("polylogue", page[0].number, 10)
            .await
            .is_empty()
    );
    assert!(h.materials("kitty", 0, 10).await.is_empty());
}

#[devkit::test]
async fn invalid_frames_are_refused_and_recorded(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
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
                    failures: vec![],
                },
            )
            .await;
        let reason = ack.refusal.unwrap_or_default();
        assert!(reason.contains(expected), "{name}: {reason}");
        assert!(ack.admitted.is_empty());
    }
    // Refusals are recorded: asking again gets the same answer.
    let have = h.recorded("kitty", &["f-type"]).await;
    assert_eq!(have.len(), 1);
    assert!(have.iter().all(|a| a.refusal.is_some()));
    assert_eq!(
        h.count("SELECT count(*) FROM core.events").await,
        0,
        "no refused frame admitted anything"
    );

    let garbage = h.send_raw("kitty", b"{not json".to_vec()).await;
    assert!(garbage.refusal.unwrap().starts_with("not a frame"));
}

#[devkit::test]
async fn producers_are_limited_to_their_own_subjects() {
    let subjects = Subjects::new("sx");
    // The server holds only the passwords' hashes.
    let hash = |password: &str| crate::admin::password_hash(password).unwrap();
    let config = subjects.authorization(
        ("daemon", &hash("dpw")),
        &[("kitty", &hash("kpw")), ("phone", &hash("ppw"))],
    );
    assert!(!config.contains("dpw") && !config.contains("kpw"));
    let server = devkit::nats::Server::start_with(&devkit::nats::Options { config }).unwrap();
    let address = server.url().trim_start_matches("nats://").to_owned();
    let connect = |user: &str, password: &str| {
        async_nats::ConnectOptions::with_user_and_password(user.into(), password.into())
            .connect(address.clone())
    };
    assert!(connect("daemon", "wrong").await.is_err());
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

#[devkit::test]
async fn ingress_events_reach_the_confirmed_stream(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let (material, frame) = h.material_frame("m1", b"ls\n");
    h.send("kitty", &frame).await;
    h.send(
        "kitty",
        &Ingress::Events {
            frame: "e1".into(),
            events: vec![imported(material, 0, "ls")],
            failures: vec![],
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

#[devkit::test]
async fn bytes_sent_in_chunks_register_a_material_and_failures_are_recorded(
    db: devkit::pg::TestDatabase,
) {
    use base64::Engine;
    let h = Harness::start(db).await;
    // A producer on another host: its bytes are not in this content store.
    let bytes: Vec<u8> = b": 1760788800:0;ls\n: 1760788801:0;pwd\n".repeat(50);
    let blob = format!("blake3:{}", blake3::hash(&bytes).to_hex());
    let total = bytes.len() as u64;
    let id = MaterialId::new();
    let mut frame = h.material_frame("m-up", b"unused").1;
    if let Ingress::Material { material, .. } = &mut frame {
        material.id = id;
        material.blob = blob.clone();
        material.length = total;
    }
    let chunk = |attempt: &str, offset: usize, end: usize| Ingress::Chunk {
        for_frame: "m-up".into(),
        frame: format!("{blob}/{attempt}/{offset}"),
        blob: blob.clone(),
        offset: offset as u64,
        total,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes[offset..end]),
    };
    // Out of order is refused and recorded; in order is accepted.
    let ack = h.send("node", &chunk("a", 1_000, bytes.len())).await;
    assert!(ack.refusal.unwrap().contains("ordered prefix"));
    for (offset, end) in [(0, 1_000), (1_000, bytes.len())] {
        let ack = h.send("node", &chunk("b", offset, end)).await;
        assert_eq!(ack.refusal, None, "{ack:?}");
    }
    assert!(
        !h.blobs.contains(&blob).unwrap(),
        "chunks wait in the spool until their material frame"
    );

    let ack = h.send("node", &frame).await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert!(h.blobs.contains(&blob).unwrap());
    assert!(
        !frame_bytes::directory(&h.store_dir, "node", "m-up").exists(),
        "a recorded frame's spool is removed"
    );

    let ack = h
        .send(
            "node",
            &Ingress::Events {
                frame: "e-up".into(),
                events: vec![imported(id, 0, "ls")],
                failures: vec![ProducerFailure {
                    location: RecordLocation {
                        material: id,
                        anchor: 18,
                    },
                    reason: "unreadable".into(),
                }],
            },
        )
        .await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.admitted.len(), 1);
    assert_eq!(
        h.count("SELECT count(*) FROM raw.failures WHERE reason = 'unreadable'")
            .await,
        1
    );
}

#[devkit::test]
async fn an_indirect_frame_is_handled_as_the_frame_it_names(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let (material, frame) = h.material_frame("m1", b"ls\n");
    h.send("kitty", &frame).await;
    let inner = Ingress::Events {
        frame: "inner-name-ignored".into(),
        events: vec![imported(material, 0, "ls")],
        failures: vec![],
    };
    let body = serde_json::to_vec(&inner).unwrap();
    let blob = h.fixture_blob(&body);
    let outer = Ingress::LargeFrame {
        frame: "e-big".into(),
        blob: blob.clone(),
        length: body.len() as u64,
    };
    let ack = h.send("kitty", &outer).await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.frame, "e-big");
    assert_eq!(ack.admitted.len(), 1);
    // A large frame cannot stand for another large frame.
    let nested_body = serde_json::to_vec(&outer).unwrap();
    let nested = h.fixture_blob(&nested_body);
    let ack = h
        .send(
            "kitty",
            &Ingress::LargeFrame {
                frame: "e-nested".into(),
                blob: nested,
                length: nested_body.len() as u64,
            },
        )
        .await;
    assert!(ack.refusal.unwrap().contains("material or events frame"));
}

impl Harness {
    async fn excise(&self, producer: &str, materials: &[MaterialId]) -> ExciseReply {
        let request = Excise {
            materials: materials.to_vec(),
        };
        let reply = self
            .client
            .request(
                self.subjects.excise(producer),
                serde_json::to_vec(&request).unwrap().into(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&reply.payload).unwrap()
    }

    async fn stored(&self, material: MaterialId) -> Vec<u8> {
        let mut conn = PgConnection::connect(&self.url).await.unwrap();
        let blob: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id = $1")
            .bind(material.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        self.blobs.read(&blob).unwrap()
    }
}

/// A producer excises whole materials it registered: their events, bytes,
/// frames and input go, and sending the same material again is refused. A
/// request naming a material another producer registered is refused, and
/// nothing changes.
#[devkit::test]
async fn a_producer_excises_only_its_own_whole_materials(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let (mine, mine_frame) = h.material_frame("m1", b": 1760788800:0;ls\n");
    assert_eq!(h.send("kitty", &mine_frame).await.refusal, None);
    let (other, frame) = h.material_frame("m2", b": 1760788801:0;pwd\n");
    assert_eq!(h.send("phone", &frame).await.refusal, None);
    for (producer, material) in [("kitty", mine), ("phone", other)] {
        let events = Ingress::Events {
            frame: format!("e-{producer}"),
            events: vec![imported(material, 0, "ls")],
            failures: vec![],
        };
        assert_eq!(h.send(producer, &events).await.refusal, None);
    }

    let refused = h.excise("kitty", &[mine, other]).await;
    assert!(
        refused
            .refusal
            .as_deref()
            .is_some_and(|r| r.contains("not registered by kitty")),
        "{refused:?}"
    );
    assert_eq!(h.count("SELECT count(*) FROM core.events").await, 2);
    assert_eq!(h.stored(mine).await, b": 1760788800:0;ls\n");

    let done = h.excise("kitty", &[mine]).await;
    assert_eq!(
        done,
        ExciseReply {
            materials: 1,
            events: 1,
            refusal: None
        }
    );
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE source = 'kitty'")
            .await,
        0
    );
    assert_eq!(
        h.count(&format!(
            "SELECT count(*) FROM raw.materials WHERE id = '{}'",
            mine.0
        ))
        .await,
        0
    );
    assert_eq!(
        h.count("SELECT count(*) FROM raw.inputs WHERE binding = 'ingress:kitty'")
            .await,
        0,
        "no excised path stays"
    );
    assert_eq!(
        h.count("SELECT count(*) FROM core.ingress_frames WHERE producer = 'kitty'")
            .await,
        0
    );
    assert!(
        h.materials("kitty", 0, 10).await.is_empty(),
        "an excised material is no longer listed"
    );
    assert_eq!(h.materials("phone", 0, 10).await.len(), 1);
    assert_eq!(
        h.stored(other).await,
        b": 1760788801:0;pwd\n",
        "another producer's material is untouched"
    );
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE source = 'phone'")
            .await,
        1
    );
    let again = h.send("kitty", &mine_frame).await;
    assert!(
        again
            .refusal
            .as_deref()
            .is_some_and(|r| r.contains("an excision removed")),
        "{again:?}"
    );
}

/// A slow producer request (here one waiting for a database connection)
/// leaves the frame consumer reading: frames are acknowledged meanwhile, and
/// the request is answered once it can run.
#[devkit::test]
async fn frames_flow_while_a_request_waits(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let read_permit = h.reads.acquire().await.unwrap();
    let client = h.client.clone();
    let subject = h.subjects.recorded("kitty");
    let request = tokio::spawn(async move {
        let body = serde_json::to_vec(&RecordedQuery {
            frames: vec!["m1".into()],
        })
        .unwrap();
        client.request(subject, body.into()).await.unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !request.is_finished(),
        "the request waits for its connection"
    );

    let (_, frame) = h.material_frame("m1", b"ls\n");
    assert_eq!(h.send("kitty", &frame).await.refusal, None);
    assert!(!request.is_finished());

    drop(read_permit);
    let reply = tokio::time::timeout(Duration::from_secs(10), request)
        .await
        .unwrap()
        .unwrap();
    let acks: Vec<Acknowledgment> = serde_json::from_slice(&reply.payload).unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].frame, "m1");
}

#[devkit::test]
async fn storage_faults_stay_unavailable_and_invalid_bytes_are_refused(
    db: devkit::pg::TestDatabase,
) {
    let root = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(root.path()).unwrap();
    let (writer, _queues) = writer::channels();
    let ingest = Ingest::new(
        Subjects::sinex(),
        writer,
        PgConnection::connect(&db.url(None)).await.unwrap(),
        blobs,
        "synthetic".into(),
    )
    .unwrap();
    let bytes = b"synthetic";
    let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
    let material = MaterialFrame {
        id: MaterialId::new(),
        input: InputRef {
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
        manifest: Default::default(),
    };
    assert!(matches!(
        ingest.store_material("node", "missing", &material).await,
        Err(Check::Unavailable(_))
    ));
    let large = Ingress::LargeFrame {
        frame: "missing".into(),
        blob: blob.clone(),
        length: bytes.len() as u64,
    };
    assert!(matches!(
        ingest.unwrap_large_frame("node", large).await,
        Err(Check::Unavailable(_))
    ));
    frame_bytes::write(
        root.path(),
        "node",
        "wrong-hash",
        &blob,
        bytes.len() as u64,
        bytes.as_slice(),
    )
    .unwrap();
    let path = frame_bytes::directory(root.path(), "node", "wrong-hash").join(&blob[7..]);
    std::fs::write(path, b"different").unwrap();
    assert!(matches!(
        ingest.store_material("node", "wrong-hash", &material).await,
        Err(Check::Bytes(_))
    ));
    // A content store fault must not be recorded as a permanent refusal.
    std::fs::remove_dir_all(root.path().join("incoming")).unwrap();
    std::fs::write(root.path().join("incoming"), b"blocked directory").unwrap();
    let mut chunk = Ingress::Chunk {
        frame: "chunk".into(),
        for_frame: "material".into(),
        blob,
        offset: 0,
        total: bytes.len() as u64,
        data: base64::engine::general_purpose::STANDARD.encode(bytes),
    };
    assert!(matches!(
        ingest.receive_chunk("node", &chunk).await,
        Err(Check::Unavailable(_))
    ));
    if let Ingress::Chunk { data, .. } = &mut chunk {
        *data = "not base64!".into();
    }
    assert!(matches!(
        ingest.receive_chunk("node", &chunk).await,
        Err(Check::Bytes(_))
    ));
}

#[devkit::test]
async fn a_recorded_refusal_keeps_the_frame_bytes_on_redelivery(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let bytes = b"not JSON";
    let blob = format!("blake3:{}", blake3::hash(bytes).to_hex());
    frame_bytes::write(
        &h.store_dir,
        "node",
        "invalid",
        &blob,
        bytes.len() as u64,
        bytes.as_slice(),
    )
    .unwrap();
    let frame = Ingress::LargeFrame {
        frame: "invalid".into(),
        blob: blob.clone(),
        length: bytes.len() as u64,
    };
    for _ in 0..2 {
        let ack = h
            .send_raw("node", serde_json::to_vec(&frame).unwrap())
            .await;
        assert!(ack.refusal.is_some());
        assert!(
            frame_bytes::snapshot(&h.store_dir, "node", "invalid", &blob, bytes.len() as u64)
                .is_ok()
        );
    }
    assert_eq!(h.recorded("node", &["invalid"]).await.len(), 1);
}

#[devkit::test]
async fn a_refused_event_between_good_frames_is_a_queryable_material_failure_and_replays(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start(db).await;
    let lines = [
        ": 1760788800:0;first\n",
        ": 1760788801:0;second\n",
        ": 1760788802:0;third\n",
    ];
    let bytes = lines.concat();
    let (material, frame) = h.material_frame("material", bytes.as_bytes());
    assert!(h.send("session", &frame).await.refusal.is_none());
    let mut offset = 0;
    for (index, command) in ["first", "second", "third"].iter().enumerate() {
        let mut event = imported(material, offset, command);
        if index == 1 {
            event.event_type = "not.declared".into();
        }
        let frame = Ingress::Events {
            frame: format!("events-{index}"),
            events: vec![event],
            failures: vec![],
        };
        let ack = h.send("session", &frame).await;
        if index == 1 {
            assert_eq!(ack.refusal_kind(), Some(FrameRefusal::Content));
            assert_eq!(ack.outcome["failures"], 1);
            assert_eq!(h.send("session", &frame).await, ack);
        } else {
            assert_eq!(ack.admitted.len(), 1);
        }
        offset += lines[index].len() as u64;
    }
    assert_eq!(h.count("SELECT count(*) FROM raw.failures").await, 1);
    assert_eq!(sinex_store::diagnostics::positions(&mut PgConnection::connect(&h.url).await.unwrap()).await.unwrap().failures_24h, 1);
    assert_eq!(h.stored(material).await, bytes.as_bytes());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let context = sinex_model::query::Context::new(
        jiff::Timestamp::now(),
        jiff::tz::TimeZone::UTC,
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let bound = sinex_model::query::bind("failures", &context).unwrap();
    let answer = sinex_store::query::run(
        &mut conn,
        "failures",
        &bound,
        &context,
        &sinex_store::query::Page {
            size: None,
            cursor: None,
        },
    )
    .await
    .unwrap();
    let rows = serde_json::to_value(answer).unwrap()["rows"].clone();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert!(rows.to_string().contains("not.declared"));
    // The current parser emits the corrected declared type from the kept bytes.
    let report = crate::replay::run(
        &mut conn,
        &h.blobs,
        &h.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                materials: vec![material.0],
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.admitted, 3);
    assert_eq!(report.failures, 0);
    assert_eq!(h.count("SELECT count(*) FROM raw.failures").await, 0);
    assert_eq!(h.stored(material).await, bytes.as_bytes());
}

#[devkit::test]
async fn admission_content_errors_and_missing_locations_are_record_failures(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start(db).await;
    let (material, frame) = h.material_frame("material", b": 1760788800:0;synthetic\n");
    h.send("session", &frame).await;
    for (name, event) in [
        ("nul", imported(material, 0, "a\0b")),
        (
            "unknown-material",
            imported(MaterialId::new(), 0, "synthetic"),
        ),
    ] {
        let ack = h
            .send(
                "session",
                &Ingress::Events {
                    frame: name.into(),
                    events: vec![event],
                    failures: vec![],
                },
            )
            .await;
        assert_eq!(ack.refusal_kind(), Some(FrameRefusal::Content), "{ack:?}");
        assert_eq!(ack.outcome["failures"], 1, "{ack:?}");
    }
    assert_eq!(h.count("SELECT count(*) FROM raw.failures").await, 2);
    assert_eq!(h.count("SELECT count(*) FROM core.events").await, 0);
}

#[devkit::test]
async fn producer_configuration_is_refused_at_connect_before_any_frame(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start(db).await;
    let mut hello = ProducerHello::default();
    hello.version += 1;
    let unknown_version = hello.clone();
    hello = ProducerHello::default();
    hello.class = sinex_model::Class::Sealed;
    let sealed = hello.clone();
    hello = ProducerHello::default();
    hello.event_types = vec!["not.declared".into()];
    for declaration in [unknown_version, sealed, hello] {
        let connected = sinex_source_core::drivers::producer::Producer::connect(
            h.nats.url(),
            None,
            h.subjects.clone(),
            "session",
            &declaration,
        )
        .await;
        assert!(matches!(
            connected,
            Err(sinex_source_core::drivers::producer::Error::Configuration(
                _
            ))
        ));
    }
    assert_eq!(h.count("SELECT count(*) FROM core.ingress_frames").await, 0);
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 0);
}

#[devkit::test]
async fn malformed_frames_with_no_name_are_recorded_once(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    for bytes in [
        b"{not json".as_slice(),
        br#"{"kind":"events","frame":"","events":[]}"#.as_slice(),
    ] {
        let ack = h.send_raw("session", bytes.to_vec()).await;
        assert_eq!(ack.refusal_kind(), Some(FrameRefusal::Malformed));
        assert_eq!(h.send_raw("session", bytes.to_vec()).await, ack);
        assert_eq!(h.recorded("session", &[&ack.frame]).await, vec![ack]);
    }
    assert_eq!(h.count("SELECT count(*) FROM core.ingress_frames").await, 2);
}

#[devkit::test]
async fn malformed_large_frames_are_errors_and_byte_refusals_remove_transfer_bytes(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    for (name, inner) in [
        ("no-inner-name", json!({"kind":"events","frame":"","events":[]})),
        ("wrong-inner-kind", json!({"kind":"chunk","frame":"chunk","for_frame":"m","blob":"invalid","offset":0,"total":0,"data":""})),
    ] {
        let bytes = serde_json::to_vec(&inner).unwrap();
        let blob = h.fixture_blob(&bytes);
        let ack = h.send("session", &Ingress::LargeFrame { frame: name.into(), blob, length: bytes.len() as u64 }).await;
        assert_eq!(ack.refusal_kind(), Some(FrameRefusal::Malformed));
    }
    let (_, material) = h.material_frame("damaged", b"synthetic");
    let Ingress::Material { material: m, .. } = &material else { unreachable!() };
    frame_bytes::write(&h.store_dir, "session", "damaged", &m.blob, m.length, b"synthetic".as_slice()).unwrap();
    let dir = frame_bytes::directory(&h.store_dir, "session", "damaged");
    std::fs::write(dir.join(&m.blob[7..]), b"different").unwrap();
    let ack = h.send_raw("session", serde_json::to_vec(&material).unwrap()).await;
    assert_eq!(ack.refusal_kind(), Some(FrameRefusal::Bytes));
    assert!(!dir.exists());
    assert_eq!(h.recorded("session", &["damaged"]).await, vec![ack]);
}
