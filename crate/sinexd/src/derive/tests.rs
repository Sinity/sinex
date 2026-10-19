//! The driver against a real database: keys dirtied by admission, outputs
//! replacing the key's previous set, waiting keys, the comparison lane.
use super::*;
use crate::keys::Registered as Keys;
use crate::writer;
use serde_json::json;
use sinex_automata::{Computed, KeyState, Keyed};
use sinex_model::kinds::{
    CommandExecuted, CommandFused, CommandImported, ShellSession, StateInterval,
};
use sinex_model::{ArchiveRequest, Class, MaterialId, OperationId, Origin, RecordLocation};
use sqlx::Connection;

const T: i64 = 1_718_438_400; // 2024-06-15 10:00 CEST

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn location(anchor: u64) -> Origin {
    Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor,
        },
        ordinal: 0,
    }
}

fn atuin(id: &str, command: &str, secs: i64, exit: i64, order: i64) -> Event {
    Event::build(
        &CommandExecuted {
            native_id: id.into(),
            command: command.into(),
            cwd: "/p".into(),
            session: "s".into(),
            hostname: "box:u".into(),
            started_at: at(secs),
            duration_ns: Some(2_000_000_000),
            exit_code: Some(exit),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin",
        location(0),
        ReadingOrder(order),
        *blake3::hash(format!("{id}{command}{exit}{order}").as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn zsh(command: &str, secs: i64, anchor: u64) -> Event {
    Event::build(
        &CommandImported {
            command: command.into(),
            hostname: "box".into(),
            started_at: at(secs),
            header_elapsed: 0,
            n: 0,
        },
        "terminal.zsh",
        location(anchor),
        ReadingOrder(0),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn focus(window: &str, secs: i64) -> Event {
    Event::build(
        &sinex_model::kinds::desktop::WindowFocused {
            received_at: at(secs),
            window: Some(window.into()),
            class: Some("kitty".into()),
            title: Some(window.into()),
        },
        "wm.hyprland",
        location(u64::try_from(secs).unwrap()),
        ReadingOrder(0),
        *blake3::hash(format!("{window}{secs}").as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

struct World {
    _db: devkit::pg::TestDatabase,
    url: String,
    handle: writer::WriterHandle,
    _writer: tokio::task::JoinHandle<()>,
}

impl World {
    async fn new() -> Self {
        Self::with_keys(sinex_automata::keyed()).await
    }

    /// A world whose writer marks the keys of `keyed` dirty.
    async fn with_keys(keyed: Vec<Box<dyn AnyKeyed>>) -> Self {
        let (db, url, connection) = crate::testdb::fresh().await;
        let (handle, queues) = writer::channels();
        let task = tokio::spawn(writer::run(
            url.clone(),
            connection,
            queues,
            Arc::new(Keys::new(keyed)),
        ));
        Self {
            _db: db,
            url,
            handle,
            _writer: task,
        }
    }

    async fn admit(&self, events: Vec<Event>) {
        self.batch(events, vec![]).await;
    }

    async fn batch(&self, events: Vec<Event>, archive: Vec<ArchiveRequest>) {
        let batch = Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "test".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events,
            }],
            archive,
        };
        self.handle.submit(Priority::Live, batch).await.unwrap();
    }

    async fn driver(&self) -> Driver {
        self.driver_with(sinex_automata::keyed(), vec![]).await
    }

    async fn driver_with(
        &self,
        keyed: Vec<Box<dyn AnyKeyed>>,
        per_event: Vec<Box<dyn AnyPerEvent>>,
    ) -> Driver {
        let read = PgConnection::connect(&self.url).await.unwrap();
        Driver::start(read, self.handle.clone(), keyed, per_event, "test")
            .await
            .unwrap()
    }

    async fn live<P: sinex_model::EventPayload>(&self) -> Vec<P> {
        let mut conn = PgConnection::connect(&self.url).await.unwrap();
        let rows: Vec<Value> = sqlx::query_scalar(
            "SELECT payload FROM core.events e WHERE event_type = $1
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id) ORDER BY ts_orig",
        )
        .bind(P::EVENT_TYPE)
        .fetch_all(&mut conn)
        .await
        .unwrap();
        rows.into_iter()
            .map(|v| serde_json::from_value(v).unwrap())
            .collect()
    }

    async fn count(&self, sql: &str) -> i64 {
        let mut conn = PgConnection::connect(&self.url).await.unwrap();
        sqlx::query_scalar(sql).fetch_one(&mut conn).await.unwrap()
    }
}

#[tokio::test]
async fn commands_fuse_into_sessions_and_summaries() {
    let w = World::new().await;
    w.admit(vec![
        atuin("1", "make", 0, 0, 1),
        zsh("make", 1, 10),
        atuin("2", "cargo test", 60, 101, 1),
        zsh("cargo test", 60, 20),
        zsh("ls", 2000, 30),
    ])
    .await;
    let mut d = w.driver().await;
    d.settle().await.unwrap();

    let fused: Vec<CommandFused> = w.live().await;
    assert_eq!(fused.len(), 3, "{fused:?}");
    assert_eq!(fused[0].observed_by, vec!["terminal.atuin", "terminal.zsh"]);
    let sessions: Vec<ShellSession> = w.live().await;
    assert_eq!(sessions.len(), 2, "{sessions:?}");
    assert_eq!(sessions[0].commands, 2);
    assert_eq!(sessions[0].failed, 1);
    let daily = w
        .count(
            "SELECT count(*) FROM core.events e WHERE event_type = 'activity.summary' AND payload->>'grain' = 'day'
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)",
        )
        .await;
    assert_eq!(daily, 1);
    // Metric days stay dirty for the analysis maintainer, not the driver.
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.dirty WHERE derivation NOT LIKE 'metric:%'")
            .await,
        0,
        "every derivation settled"
    );
    // Every derived event cites parents.
    assert_eq!(
        w.count("SELECT count(*) FROM core.events WHERE parents IS NULL AND source IN ('command.fusion', 'shell.sessions')")
            .await,
        0
    );
}

#[tokio::test]
async fn a_changed_input_replaces_outputs_downstream() {
    let w = World::new().await;
    w.admit(vec![atuin("1", "make", 0, 0, 1)]).await;
    let mut d = w.driver().await;
    d.settle().await.unwrap();
    assert_eq!(w.live::<ShellSession>().await[0].failed, 0);

    // Atuin rewrote the row: the command failed after all.
    w.admit(vec![atuin("1", "make", 0, 2, 2)]).await;
    d.settle().await.unwrap();
    let fused: Vec<CommandFused> = w.live().await;
    assert_eq!(fused.len(), 1, "replaced, not duplicated");
    assert_eq!(fused[0].exit_code, Some(2));
    assert_eq!(w.live::<ShellSession>().await[0].failed, 1);
}

#[tokio::test]
async fn an_output_no_longer_produced_is_archived() {
    let w = World::new().await;
    let a = atuin("1", "make", 0, 0, 1);
    let a_id = a.id;
    w.admit(vec![a, zsh("make", 1, 10)]).await;
    let mut d = w.driver().await;
    d.settle().await.unwrap();
    assert_eq!(w.live::<CommandFused>().await[0].observed_by.len(), 2);

    // Hiding the Atuin reading leaves zsh alone: the two-source fused command
    // goes, a zsh-only one takes its place.
    w.batch(
        vec![],
        vec![ArchiveRequest::Hide {
            target: a_id,
            operation: OperationId::new(),
        }],
    )
    .await;
    d.settle().await.unwrap();
    let fused: Vec<CommandFused> = w.live().await;
    assert_eq!(fused.len(), 1);
    assert_eq!(fused[0].observed_by, vec!["terminal.zsh"]);
    assert!(
        w.count(
            "SELECT count(*) FROM audit.archived WHERE reason IN ('recomputed', 'newer_reading')"
        )
        .await
            >= 1
    );
}

#[tokio::test]
async fn an_open_interval_waits_and_closes_on_the_next_transition() {
    let w = World::new().await;
    w.admit(vec![focus("0xa", 0)]).await;
    let mut d = w.driver().await;
    d.settle().await.unwrap();
    let open: Vec<StateInterval> = w.live().await;
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].ended_at, None);
    assert_eq!(w.count("SELECT count(*) FROM derivation.waiting").await, 1);

    // The next focus arrives three days later: a different day's key, which
    // runs the derivation and so recomputes the waiting one.
    w.admit(vec![focus("0xb", 3 * 86_400)]).await;
    d.settle().await.unwrap();
    let intervals: Vec<StateInterval> = w.live().await;
    assert_eq!(intervals.len(), 2, "{intervals:?}");
    assert_eq!(intervals[0].ended_at, Some(at(3 * 86_400)));
    assert_eq!(intervals[1].ended_at, None);
}

/// A derivation with one key per command, each always waiting for more
/// evidence.
struct Waiting;

impl Keyed for Waiting {
    const NAME: &'static str = "test.waiting";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        vec![input.payload["command"].clone()]
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let mut q = InputQuery::window(Self::INPUTS, at(-86_400), at(86_400));
        q.equal.push(("command", key.clone()));
        Some(q)
    }
    fn compute(&self, key: &Value, inputs: &[Event], _: &Env) -> Computed {
        let record = key.as_str().unwrap_or_default().to_owned();
        let payload = CommandFused {
            record: record.clone(),
            command: record,
            host: "box".into(),
            started_at: at(0),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            observed_by: vec![],
        };
        Computed {
            outputs: vec![sinex_automata::output::derived(
                &payload,
                "test.waiting",
                inputs,
            )],
            waiting: true,
        }
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Open
    }
}

#[tokio::test]
async fn a_waiting_key_stops_waiting_past_the_horizon() {
    let w = World::with_keys(vec![Box::new(Waiting)]).await;
    let waiting = "SELECT count(*) FROM derivation.waiting";
    w.admit(vec![zsh("a", 0, 1)]).await;
    let mut d = w.driver_with(vec![Box::new(Waiting)], vec![]).await;
    d.settle().await.unwrap();
    assert_eq!(w.count(waiting).await, 1);

    // An old window alone does not stop a wait: the next run, which
    // refreshes waiting keys, keeps "a" waiting.
    w.admit(vec![zsh("b", 10, 2)]).await;
    d.settle().await.unwrap();
    assert_eq!(w.count(waiting).await, 2);

    // Once they have waited past the horizon, the next run leaves their last
    // outputs standing and stops waiting; the new key waits as usual.
    d.clock = Some(Timestamp(
        Timestamp::now().0 + WAITING_HORIZON + time::Duration::days(1),
    ));
    w.admit(vec![zsh("c", 20, 3)]).await;
    d.settle().await.unwrap();
    assert_eq!(w.count(waiting).await, 1);
    let out: Vec<CommandFused> = w.live().await;
    assert_eq!(out.len(), 3, "{out:?}");
}

#[test]
fn derivations_rank_after_the_derivations_they_read() {
    let catalog: Vec<Registered> = sinex_automata::keyed()
        .iter()
        .map(|d| Registered {
            name: d.name().to_owned(),
            kind: "keyed",
            version: 1,
            inputs: d.inputs_types().iter().map(|s| (*s).to_owned()).collect(),
            outputs: d.output_types().iter().map(|s| (*s).to_owned()).collect(),
        })
        .collect();
    let r = ranks(&catalog);
    let fusion = r["command.fusion"];
    for (name, rank) in &r {
        let d = catalog.iter().find(|d| d.name == *name).unwrap();
        for u in &catalog {
            if u.name != d.name && u.outputs.iter().any(|o| d.inputs.contains(o)) {
                assert!(rank > &r[u.name.as_str()], "{name} after {}", u.name);
            }
        }
    }
    assert!(r.values().any(|&rank| rank > fusion), "{r:?}");
}

#[tokio::test]
async fn a_key_dirtied_again_stays_dirty() {
    let w = World::new().await;
    w.admit(vec![atuin("1", "make", 0, 0, 1)]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let dirty = "SELECT count(*) FROM derivation.dirty WHERE derivation = 'command.fusion'";
    let before = w.count(dirty).await;
    let key = sqlx::query_scalar::<_, Value>(
        "SELECT key FROM derivation.dirty WHERE derivation = 'command.fusion' LIMIT 1",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    // A settle computed before the latest dirt (dirty_seq 0) leaves the mark.
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Derived {
            derivation: "command.fusion".into(),
            scope: DerivedScope::Key {
                key: key.clone(),
                dirty_seq: 0,
                waiting: false,
            }
            .to_value(),
        },
        records: vec![],
        archive: vec![],
    };
    w.handle.submit(Priority::Derived, batch).await.unwrap();
    assert_eq!(w.count(dirty).await, before);
}

/// A keyed derivation whose output depends on a version, to exercise the
/// comparison lane and version changes.
struct Count<const V: u32>;

impl<const V: u32> Keyed for Count<V> {
    const NAME: &'static str = "test.count";
    const VERSION: u32 = V;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn keys_for(&self, _: &KeyView<'_>, _: &Env) -> Vec<Value> {
        vec![json!("all")]
    }
    fn inputs(&self, _: &Value, _: &Env) -> Option<InputQuery> {
        Some(InputQuery::window(Self::INPUTS, at(-86_400), at(86_400)))
    }
    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        let payload = CommandFused {
            record: "all".into(),
            command: format!("v{V}:{}", inputs.len()),
            host: "box".into(),
            started_at: at(0),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            observed_by: vec![],
        };
        Computed {
            outputs: vec![sinex_automata::output::derived(
                &payload,
                "test.count",
                inputs,
            )],
            waiting: false,
        }
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
}

#[tokio::test]
async fn compare_shows_a_version_change_before_switching() {
    let w = World::new().await;
    w.admit(vec![zsh("a", 0, 1), zsh("b", 10, 2)]).await;
    let mut v1 = w.driver_with(vec![Box::new(Count::<1>)], vec![]).await;
    v1.settle().await.unwrap();
    let same = v1.compare("test.count", None, 3).await.unwrap();
    assert_eq!((same.keys, same.same, same.changed), (1, 1, 0));

    // Version 2 computes a different output; compare writes nothing.
    let mut v2 = w.driver_with(vec![Box::new(Count::<2>)], vec![]).await;
    let cmp = v2.compare("test.count", None, 3).await.unwrap();
    assert_eq!((cmp.changed, cmp.added, cmp.removed), (1, 0, 0), "{cmp}");
    assert!(cmp.to_string().contains("v2:2"));
    // The comparison is kept, newest first, with its examples.
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let kept = derived::comparisons(&mut conn, Some("test.count"), 10)
        .await
        .unwrap();
    assert_eq!(kept.len(), 2);
    assert_eq!(Some(kept[0].id), cmp.id);
    assert_eq!((kept[0].version, kept[0].changed), (2, 1));
    assert_eq!(kept[0].examples[0]["kind"], "changed");
    // Starting version 2 queued its recompute; settling switches.
    v2.settle().await.unwrap();
    let out: Vec<CommandFused> = w.live().await;
    assert!(out.iter().any(|f| f.command == "v2:2"), "{out:?}");
    assert!(!out.iter().any(|f| f.command == "v1:2"));
}

struct Echo;
impl sinex_automata::PerEvent for Echo {
    const NAME: &'static str = "test.echo";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn derive(&self, input: sinex_automata::Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let input = input.event;
        let c: CommandImported = input.payload_as().unwrap();
        Ok(vec![sinex_automata::output::derived(
            &CommandFused {
                record: sinex_automata::output::record_identity(input),
                command: c.command.to_uppercase(),
                host: c.hostname,
                started_at: c.started_at,
                cwd: None,
                session: None,
                duration_ns: None,
                exit_code: None,
                observed_by: vec![],
            },
            "test.echo",
            [input],
        )])
    }
}

#[tokio::test]
async fn per_event_derivations_read_in_commit_order_and_keep_their_place() {
    let w = World::new().await;
    w.admit(vec![zsh("a", 0, 1), zsh("b", 10, 2)]).await;
    let mut d = w.driver_with(vec![], vec![Box::new(Echo)]).await;
    d.settle().await.unwrap();
    w.admit(vec![zsh("c", 20, 3)]).await;
    d.settle().await.unwrap();
    let out: Vec<CommandFused> = w.live().await;
    let commands: Vec<&str> = out.iter().map(|f| f.command.as_str()).collect();
    assert_eq!(commands, vec!["A", "B", "C"]);
    assert_eq!(
        w.count("SELECT commit_seq FROM derivation.positions WHERE derivation = 'test.echo'")
            .await,
        w.count(
            "SELECT max(commit_seq) FROM core.commit_log WHERE event_type = 'command.imported'"
        )
        .await
    );
}
