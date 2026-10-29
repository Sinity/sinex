//! The confirmed stream across a NATS outage: once NATS is back, every
//! committed event is on the stream once, in commit order, equal to the stored
//! row, and a consumer filtering by event type reads the same sequence as the
//! commit log.
use super::*;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};

/// The test owns the broker, its port and its JetStream directory. Restarting
/// retains the same store, so acknowledged messages must survive the process.
struct Broker {
    root: tempfile::TempDir,
    address: SocketAddr,
    url: String,
    child: Option<Child>,
}

impl Broker {
    fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::fs::write(
            root.path().join("nats.conf"),
            format!(
                "listen: \"{address}\"\nhttp: 0\njetstream {{ store_dir: \"{}\" }}\n",
                root.path().join("jetstream").display(),
            ),
        )
        .unwrap();
        let mut broker = Self {
            root,
            address,
            url: format!("nats://{address}"),
            child: None,
        };
        drop(listener);
        broker.restore();
        broker
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.root.path().join("server.log")).unwrap()
    }

    fn restore(&mut self) {
        if self.child.is_some() {
            return;
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join("server.log"))
            .unwrap();
        self.child = Some(
            Command::new("nats-server")
                .arg("-c")
                .arg(self.root.path().join("nats.conf"))
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            assert!(
                self.child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "broker stopped: {}",
                self.log()
            );
            if let Ok(stream) = TcpStream::connect_timeout(&self.address, Duration::from_millis(20))
            {
                stream
                    .set_read_timeout(Some(Duration::from_millis(100)))
                    .unwrap();
                let mut line = String::new();
                if BufReader::new(stream).read_line(&mut line).is_ok() && line.starts_with("INFO ")
                {
                    return;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "broker never became ready: {}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn cut(&mut self) {
        let mut child = self.child.take().expect("the broker was running");
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success(), "the broker was killed during an outage");
        assert!(
            TcpStream::connect_timeout(&self.address, Duration::from_millis(100)).is_err(),
            "the broker port remains open after reaping its process"
        );
    }
}
impl Drop for Broker {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            if let Err(error) = child.kill() {
                eprintln!("killing owned NATS: {error}");
            }
            if let Err(error) = child.wait() {
                eprintln!("reaping owned NATS: {error}");
            }
        }
    }
}

fn imported(command: &str, n: i64) -> Event {
    Event::build(
        &sinex_model::event_types::CommandImported {
            command: command.into(),
            hostname: "h".into(),
            started_at: Some(Timestamp::from_unix_secs(1_700_000_000 + n).unwrap()),
            header_elapsed: Some(0),
            paths: vec![],
            n: 0,
        },
        "zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId(uuid::Uuid::from_u128(1_000 + n as u128)),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

/// Every message on `subjects`, in stream order.
async fn read_filtered(
    js: &jetstream::Context,
    stream: &str,
    filters: Vec<String>,
) -> Vec<Confirmed> {
    let stream = js.get_stream(stream).await.unwrap();
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            filter_subjects: filters,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut out = Vec::new();
    loop {
        let mut batch = consumer
            .fetch()
            .max_messages(100)
            .expires(Duration::from_millis(300))
            .messages()
            .await
            .unwrap();
        let mut got = 0;
        while let Some(m) = batch.next().await {
            let m = m.unwrap();
            out.push(serde_json::from_slice(&m.payload).unwrap());
            m.ack().await.unwrap();
            got += 1;
        }
        if got == 0 {
            return out;
        }
    }
}

async fn wait_published(conn: &mut PgConnection) {
    let head: i64 = sqlx::query_scalar("SELECT max(commit_seq) FROM core.commit_log")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (position, _) = sinex_store::commit_log::consumer_position(conn, "publisher")
                .await
                .unwrap();
            if position >= head {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("publisher did not cover committed work");
}

#[devkit::test(multi_thread)]
async fn the_stream_equals_the_commit_log_across_an_outage(db: devkit::pg::TestDatabase) {
    let mut broker = Broker::start();
    let subjects = Subjects::new(format!("outage{}", uuid::Uuid::now_v7().simple()));
    let direct = context(async_nats::connect(broker.url.as_str()).await.unwrap());
    ensure_stream(&direct, &subjects).await.unwrap();
    let publishing = context(
        async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(broker.url.clone())
            .await
            .unwrap(),
    );
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    let (commits, commits_seen) = watch::channel(0u64);
    let (stop, stopped) = watch::channel(false);
    let publisher = tokio::spawn(run(
        PgConnection::connect(&url).await.unwrap(),
        publishing,
        subjects.clone(),
        commits_seen,
        stopped,
    ));

    // Before, during and after outages of different lengths: new events of
    // two types, a newer reading and a hide.
    let first = executed("1", "ls", 1, Class::Open);
    let first_id = first.id;
    for (round, outage_ms) in [(0, 0u64), (1, 300), (2, 1_500), (3, 0)] {
        if outage_ms > 0 {
            broker.cut();
        }
        let mut events = vec![
            imported(&format!("z{round}a"), round * 10),
            executed(
                &format!("e{round}"),
                &format!("run {round}"),
                1,
                Class::Open,
            ),
            imported(&format!("z{round}b"), round * 10 + 1),
        ];
        if round == 0 {
            events.push(first.clone());
        }
        let mut archive = vec![];
        if round == 1 {
            events.push(executed("1", "ls -la", 2, Class::Open));
        }
        if round == 2 {
            archive.push(ArchiveRequest::Hide {
                target: events[0].id,
                operation: OperationId::new(),
            });
        }
        admit(&mut conn, batch(events, archive)).await;
        commits.send_modify(|n| *n += 1);
        if round == 0 {
            // The first broker is killed only after it acknowledged real events.
            wait_published(&mut conn).await;
        }
        tokio::time::sleep(Duration::from_millis(outage_ms)).await;
        broker.restore();
    }

    wait_published(&mut conn).await;
    stop.send_replace(true);
    publisher.await.unwrap().unwrap();

    // Per type filter: the stream's sequence is the commit log's.
    let stream = subjects.confirmed_stream();
    for types in [
        vec!["command.imported"],
        vec!["command.executed"],
        vec!["command.imported", "command.executed"],
    ] {
        let filters = types.iter().map(|t| subjects.confirmed_event(t)).collect();
        let published: Vec<(i64, uuid::Uuid)> = read_filtered(&direct, &stream, filters)
            .await
            .into_iter()
            .map(|m| match m {
                Confirmed::Event(e) => (e.commit_seq, e.id.0),
                other => panic!("an event subject carried {other:?}"),
            })
            .collect();
        let committed: Vec<(i64, uuid::Uuid)> = sqlx::query_as(
            "SELECT l.commit_seq, l.event_id FROM core.commit_log l
             WHERE l.event_type = ANY($1) ORDER BY l.commit_seq",
        )
        .bind(types.iter().map(|t| (*t).to_owned()).collect::<Vec<_>>())
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(published, committed, "{types:?}");
    }
    // Archive changes, once each, in the order they were made.
    let changes: Vec<(uuid::Uuid, sinex_model::ArchiveReason)> =
        read_filtered(&direct, &stream, vec![subjects.confirmed_archive()])
            .await
            .into_iter()
            .filter_map(|m| match m {
                Confirmed::Archived(a) => Some((a.event.0, a.reason)),
                _ => None,
            })
            .collect();
    assert_eq!(changes.len(), 2, "{changes:?}");
    assert_eq!(
        changes[0],
        (first_id.0, sinex_model::ArchiveReason::NewerReading)
    );
    assert_eq!(changes[1].1, sinex_model::ArchiveReason::Hide);

    // Each message equals its stored row, record hash included.
    let all = read_filtered(&direct, &stream, vec![subjects.confirmed_all()]).await;
    let mut ids = BTreeSet::new();
    for message in &all {
        let Confirmed::Event(e) = message else {
            continue;
        };
        assert!(ids.insert(e.id), "{} published twice", e.id);
        let stored: (String, serde_json::Value, i64) = sqlx::query_as(
            "SELECT encode(record_hash, 'hex'), payload, reading_order FROM core.events WHERE id = $1",
        )
        .bind(e.id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            (&e.record_hash, e.payload.as_ref(), e.reading_order),
            (&stored.0, Some(&stored.1), stored.2)
        );
    }
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.commit_log WHERE event_id IS NOT NULL")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        i64::try_from(ids.len()).unwrap(),
        events,
        "every event once"
    );
}
