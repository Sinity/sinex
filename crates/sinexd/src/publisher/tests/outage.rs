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

    /// A copy of the broker's store, taken while it is stopped.
    fn copy_store(&mut self) -> tempfile::TempDir {
        self.cut();
        let copy = tempfile::tempdir().unwrap();
        copy_tree(&self.root.path().join("jetstream"), copy.path());
        self.restore();
        copy
    }

    /// Restart from an earlier copy of the store: what a host crash leaves when
    /// JetStream had not yet synced its newest messages. The stream keeps its
    /// name and creation time.
    fn lose_newest(&mut self, copy: &tempfile::TempDir) {
        self.cut();
        let store = self.root.path().join("jetstream");
        std::fs::remove_dir_all(&store).unwrap();
        copy_tree(copy.path(), &store);
        self.restore();
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

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
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
    let head: i64 = sqlx::query_scalar("SELECT value FROM core.counters WHERE name = 'commit_seq'")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let position:i64=sqlx::query_scalar("SELECT coalesce(max(commit_seq),0) FROM core.consumer_positions WHERE consumer LIKE 'publisher/%'")
                .fetch_one(&mut *conn).await.unwrap();
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
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
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
    // two types, a newer reading and an archive.
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
        if round == 1 {
            events.push(executed("1", "ls -la", 2, Class::Open));
        }
        let archive_target = events[0].id;
        admit(&mut conn, batch(events)).await;
        if round == 2 {
            lifecycle(&mut conn, Act::Archive, vec![archive_target]).await;
        }
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
        let filters = types
            .iter()
            .map(|t| subjects.confirmed_event("open", t))
            .collect();
        let published: Vec<(i64, uuid::Uuid)> = read_filtered(&direct, &stream, filters)
            .await
            .into_iter()
            .map(|m| match m {
                Confirmed::EventReference { commit_seq, event } => (commit_seq, event.0),
                other => panic!("an event subject carried {other:?}"),
            })
            .collect();
        let committed: Vec<(i64, uuid::Uuid)> = sqlx::query_as(
            "SELECT e.commit_seq, e.id FROM core.events e
             WHERE e.event_type = ANY($1) ORDER BY e.commit_seq",
        )
        .bind(types.iter().map(|t| (*t).to_owned()).collect::<Vec<_>>())
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(published, committed, "{types:?}");
    }
    // Archive changes, once each, in the order they were made.
    let changes: Vec<(uuid::Uuid, sinex_model::ArchiveReason)> = read_filtered(
        &direct,
        &stream,
        vec![subjects.confirmed_lifecycle("*", ">")],
    )
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
    assert_eq!(changes[1].1, sinex_model::ArchiveReason::Archive);

    // References retain exact original positions and IDs, with no inline
    // payload, key or other source metadata surviving a later rule edit.
    let all = read_filtered(&direct, &stream, vec![subjects.confirmed_all()]).await;
    let mut ids = BTreeSet::new();
    for message in &all {
        let Confirmed::EventReference { commit_seq, event } = message else {
            continue;
        };
        assert!(ids.insert(*event), "{event} published twice");
        let stored: i64 = sqlx::query_scalar("SELECT commit_seq FROM core.events WHERE id=$1")
            .bind(event.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(*commit_seq, stored);
    }
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        i64::try_from(ids.len()).unwrap(),
        events,
        "every event once"
    );
}

/// Events A are on the stream when its store is copied; events B are published
/// afterwards and lost when the broker restarts from the copy; event C is
/// committed after that restart, so the old code would publish it at a lost
/// sequence number. Returns the broker, a connection and the three groups.
async fn stream_lost_its_newest_messages(
    db: &devkit::pg::TestDatabase,
    subjects: &Subjects,
) -> (
    Broker,
    sinex_store::AdmissionContext,
    PgConnection,
    [Vec<EventId>; 3],
) {
    let mut broker = Broker::start();
    let js = context(async_nats::connect(broker.url.as_str()).await.unwrap());
    ensure_stream(&js, subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let mut publisher = PgConnection::connect(&url).await.unwrap();
    let a = vec![imported("a0", 0), imported("a1", 1)];
    let b = vec![imported("b0", 2), imported("b1", 3)];
    let c = vec![imported("c0", 4)];
    let ids = |events: &[Event]| events.iter().map(|e| e.id).collect::<Vec<_>>();
    let groups = [ids(&a), ids(&b), ids(&c)];

    admit(&mut conn, batch(a)).await;
    publish_pending(&mut publisher, &js, subjects)
        .await
        .unwrap();
    let copy = broker.copy_store();
    let js = context(async_nats::connect(broker.url.as_str()).await.unwrap());
    admit(&mut conn, batch(b)).await;
    publish_pending(&mut publisher, &js, subjects)
        .await
        .unwrap();
    broker.lose_newest(&copy);
    admit(&mut conn, batch(c)).await;
    (broker, conn, publisher, groups)
}

/// Every recorded stream position holds the reference to its own event.
async fn recorded_positions_hold_their_events(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) {
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    let last = stream.cached_info().state.last_sequence as i64;
    let recorded: Vec<(uuid::Uuid, i64)> =
        sqlx::query_as("SELECT event_id, stream_seq FROM core.published ORDER BY stream_seq")
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    assert!(!recorded.is_empty());
    for (event, seq) in recorded {
        assert!(
            seq <= last,
            "{event} recorded at {seq}, past the stream's last {last}"
        );
        let message = stream.get_raw_message(seq as u64).await.unwrap();
        assert!(
            matches!(
                serde_json::from_slice(&message.payload).unwrap(),
                Confirmed::EventReference { event: found, .. } if found.0 == event
            ),
            "{event} recorded at {seq}, which holds another message"
        );
    }
}

#[devkit::test(multi_thread)]
async fn publisher_rewinds_to_last_marker_after_stream_truncation(db: devkit::pg::TestDatabase) {
    let subjects = Subjects::new(format!("lost{}", uuid::Uuid::now_v7().simple()));
    let (broker, mut conn, mut publisher, groups) =
        stream_lost_its_newest_messages(&db, &subjects).await;
    let js = context(async_nats::connect(broker.url.as_str()).await.unwrap());
    publish_pending(&mut publisher, &js, &subjects)
        .await
        .unwrap();

    let on_stream: Vec<EventId> = read_filtered(
        &js,
        &subjects.confirmed_stream(),
        vec![subjects.confirmed_all()],
    )
    .await
    .into_iter()
    .filter_map(|m| match m {
        Confirmed::EventReference { event, .. } => Some(event),
        _ => None,
    })
    .collect();
    let committed: Vec<EventId> = groups.concat();
    assert_eq!(
        on_stream.iter().collect::<BTreeSet<_>>(),
        committed.iter().collect::<BTreeSet<_>>(),
        "every committed event is on the stream"
    );
    assert_eq!(on_stream.len(), committed.len(), "each event once");
    recorded_positions_hold_their_events(&mut conn, &js, &subjects).await;
}

#[devkit::test(multi_thread)]
async fn erasure_never_deletes_a_reused_sequence(db: devkit::pg::TestDatabase) {
    let subjects = Subjects::new(format!("reuse{}", uuid::Uuid::now_v7().simple()));
    let (broker, mut conn, mut publisher, [_, lost, after]) =
        stream_lost_its_newest_messages(&db, &subjects).await;
    let js = context(async_nats::connect(broker.url.as_str()).await.unwrap());
    publish_pending(&mut publisher, &js, &subjects)
        .await
        .unwrap();

    lifecycle(&mut conn, Act::Archive, vec![lost[0]]).await;
    lifecycle(&mut conn, Act::Tombstone, vec![lost[0]]).await;
    let done = publish_pending(&mut publisher, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(done.tombstoned, 1);

    let events: Vec<EventId> = read_filtered(
        &js,
        &subjects.confirmed_stream(),
        vec![subjects.confirmed_all()],
    )
    .await
    .into_iter()
    .filter_map(|m| match m {
        Confirmed::EventReference { event, .. } => Some(event),
        _ => None,
    })
    .collect();
    assert!(!events.contains(&lost[0]), "the tombstoned event is erased");
    for kept in [lost[1], after[0]] {
        assert!(
            events.contains(&kept),
            "{kept} survives another event's erasure"
        );
    }
    recorded_positions_hold_their_events(&mut conn, &js, &subjects).await;
}
