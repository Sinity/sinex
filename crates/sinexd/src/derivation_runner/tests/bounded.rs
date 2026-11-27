//! The runner's memory and the runtime it shares with the writer: a long
//! computation leaves the writer running, and a pass's outputs reach the
//! writer in bounded batches.
use super::*;

/// With one worker thread, a computation that ran on the worker would leave
/// the writer's task no thread to run on: the live batch below would wait for
/// the computation to end. The computation runs off the worker, so the batch
/// is admitted while the computation is still paused.
#[devkit::test]
fn a_running_computation_leaves_the_writer_admitting(db: devkit::pg::TestDatabase) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let w = World::with_keys(db, vec![Box::new(Paused { pause: None })]).await;
        w.admit(vec![zsh("make", 0, 1)]).await;
        let (paused, mut pauses) =
            tokio::sync::mpsc::unbounded_channel::<std::sync::mpsc::Sender<()>>();
        let pause: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let (resume, resumed) = std::sync::mpsc::channel();
            if paused.send(resume).is_ok() {
                resumed.recv().unwrap();
            }
        });
        let mut d = w
            .driver_with(vec![Box::new(Paused { pause: Some(pause) })], vec![])
            .await;
        let runner = tokio::spawn(async move { d.catch_up().await });
        let resume = pauses.recv().await.unwrap();
        // Only the first computation pauses.
        drop(pauses);
        // The deadline is a plain thread: with the only worker held, the
        // runtime's own timer would not fire either.
        let (expire, expired) = tokio::sync::oneshot::channel::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(30));
            let _ = expire.send(());
        });
        let admitted = tokio::select! {
            () = w.admit(vec![zsh("ls", 10, 2)]) => true,
            _ = expired => false,
        };
        resume.send(()).unwrap();
        assert!(
            admitted,
            "the writer admits a live batch while a derivation computes"
        );
        runner.await.unwrap().unwrap();
        let outputs: Vec<CommandFused> = w.live().await;
        assert_eq!(
            outputs
                .iter()
                .map(|o| o.command.as_str())
                .collect::<Vec<_>>(),
            ["2"]
        );
    });
}

/// Each input becomes one output carrying a 12 MiB text, so four inputs carry
/// more than one writer batch's bytes.
struct Large;
const LARGE_TEXT: usize = 12 << 20;

impl sinex_derivations::PerEvent for Large {
    const NAME: &'static str = "test.large";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn derive(
        &self,
        input: sinex_derivations::Input<'_>,
        _: &Env,
    ) -> Result<Vec<Event>, sinex_derivations::ComputationError> {
        let input = input.event;
        let c: CommandImported = input.payload_as().unwrap();
        Ok(vec![sinex_derivations::derived_event::derived(
            &CommandFused {
                record: sinex_model::record::identity(input),
                command: format!("{} {}", c.command, "x".repeat(LARGE_TEXT)),
                host: c.hostname,
                started_at: c.started_at.expect("fixture has a zsh header"),
                cwd: None,
                session: None,
                duration_ns: None,
                exit_code: None,
                execution: None,
                members: vec![],
                basis: sinex_model::event_types::fused::Basis::Rule,
                ambiguous: 1,
                competitors: vec![],
                rule_version: 1,
            },
            Self::NAME,
            [input],
        )?])
    }
}

/// A pass whose outputs exceed one batch's bytes submits them in several
/// batches, each its own transaction, instead of holding the whole pass; every
/// output is admitted once and the position reaches the last input.
#[devkit::test]
async fn a_per_event_pass_submits_large_outputs_in_bounded_batches(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    w.admit(vec![
        zsh("a", 0, 1),
        zsh("b", 10, 2),
        zsh("c", 20, 3),
        zsh("d", 30, 4),
    ])
    .await;
    let mut d = w.driver_with(vec![], vec![Box::new(Large)]).await;
    d.catch_up().await.unwrap();
    assert_eq!(
        w.count("SELECT count(*) FROM core.events WHERE source = 'test.large'")
            .await,
        4
    );
    // Three outputs reach the batch's bytes; the fourth goes in the next
    // batch. Each batch is its own transaction, so its rows share an xmin.
    assert_eq!(
        w.count("SELECT count(DISTINCT xmin::text) FROM core.events WHERE source = 'test.large'")
            .await,
        2
    );
    assert_eq!(
        w.count("SELECT commit_seq FROM derivation.positions WHERE derivation = 'test.large'")
            .await,
        w.count("SELECT max(commit_seq) FROM core.events WHERE event_type = 'command.imported'")
            .await
    );
}

fn computed_commands(
    keys: &[(&str, &[&str])],
    parent: &Event,
    position: i64,
    read: ReadMark,
) -> Segment {
    let mut scopes = Vec::new();
    let mut records = Vec::new();
    let mut outputs = 0;
    let mut bytes = 0;
    for &(key, names) in keys {
        let events: Vec<_> = names
            .iter()
            .map(|name| {
                fused_output(
                    "test.paused",
                    name,
                    (*name).into(),
                    std::slice::from_ref(parent),
                )
            })
            .collect();
        outputs += events.len();
        bytes += events
            .iter()
            .map(|event| payload_bytes(&event.payload))
            .sum::<usize>();
        scopes.push(ComputedKey {
            key: json!(key),
            dirty_seq: position,
            waiting: false,
            refresh_at: None,
            woken_by: None,
        });
        records.push(RecordOutcome::Events {
            location: None,
            events,
        });
    }
    Segment {
        computed: (0..keys.len()).collect(),
        scopes,
        records,
        outputs,
        bytes,
        read,
        position,
        refused: Vec::new(),
    }
}

async fn live_commands(w: &World) -> Vec<String> {
    let mut names: Vec<_> = w
        .live::<CommandFused>()
        .await
        .into_iter()
        .map(|row| row.command)
        .collect();
    names.sort_unstable();
    names
}

#[devkit::test]
async fn complete_keys_yield_the_writer_to_live_admission(db: devkit::pg::TestDatabase) {
    // The test runtime uses one thread. Polling both senders queues their jobs before the writer runs.
    let w = World::with_keys(db, vec![Box::new(Paused { pause: None })]).await;
    let parent = zsh("parent", 0, 1);
    w.admit(vec![parent.clone()]).await;
    let mut runner = w
        .driver_with(vec![Box::new(Paused { pause: None })], vec![])
        .await;
    let run = runner.run_of("test.paused").unwrap();
    let read = ReadMark::now(&mut runner.read).await.unwrap();
    let head = "SELECT value FROM core.counters WHERE name='commit_seq'";
    let mut counts = Counts::default();
    let derivation = Paused { pause: None };
    let mut compute = KeyComputation {
        read: &mut runner.read,
        writer: &w.handle,
        derivation: &derivation,
        time_settings: runner.time_settings.clone(),
        run,
        counts: &mut counts,
    };
    let seed = computed_commands(
        &[("first", &["old-first"]), ("all", &["old-all"])],
        &parent,
        w.count(head).await,
        read,
    );
    // Reaching either budget exactly still fits in one transaction.
    let bytes = seed.bytes;
    compute.submit(seed, 2, bytes).await.unwrap();
    assert_eq!(
        w.count("SELECT count(DISTINCT xmin::text) FROM core.events WHERE source='test.paused'")
            .await,
        1
    );
    let read = ReadMark::now(&mut *compute.read).await.unwrap();
    let replacement = computed_commands(
        &[("first", &["new-first"]), ("all", &["a", "b", "c"])],
        &parent,
        w.count(head).await,
        read,
    );
    let mut writing = Box::pin(compute.submit(replacement, 2, usize::MAX));
    assert!(futures::poll!(tokio::task::unconstrained(writing.as_mut())).is_pending());
    // A Bulk acknowledgement follows the first queued Derived write. The
    // submitting future is parked until we poll it for the next key.
    w.handle
        .submit(
            Priority::Bulk,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "test".into(),
                },
                records: vec![],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(live_commands(&w).await, ["new-first", "old-all"]);
    assert!(futures::poll!(tokio::task::unconstrained(writing.as_mut())).is_pending());
    let incoming = zsh("incoming", 10, 2);
    let mut live = Box::pin(w.admit(vec![incoming]));
    assert!(futures::poll!(tokio::task::unconstrained(live.as_mut())).is_pending());
    let (written, ()) = tokio::join!(writing, live);
    written.unwrap();
    assert_eq!(live_commands(&w).await, ["a", "b", "c", "new-first"]);
    let (first, live, tail, transactions): (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT max(commit_seq) FILTER (WHERE payload->>'command'='new-first'),
                    max(commit_seq) FILTER (WHERE payload->>'command'='incoming'),
                    min(commit_seq) FILTER (WHERE payload->>'command' IN ('a','b','c')),
                    count(DISTINCT xmin::text) FILTER (WHERE payload->>'command' IN ('a','b','c'))
             FROM core.events",
    )
    .fetch_one(&mut *compute.read)
    .await
    .unwrap();
    assert!(first < live && live < tail, "{first} < {live} < {tail}");
    assert_eq!(transactions, 1, "the oversized key committed whole");
    assert_eq!(w.count("SELECT last_dirty_seq FROM derivation.dirty WHERE derivation='test.paused' AND key='\"all\"'::jsonb").await, live);

    // A byte overflow alone also starts a separate transaction.
    let read = ReadMark::now(&mut *compute.read).await.unwrap();
    let by_bytes = computed_commands(
        &[("first", &["byte-first"]), ("all", &["byte-last"])],
        &parent,
        w.count(head).await,
        read,
    );
    let bytes = by_bytes.bytes - 1;
    compute.submit(by_bytes, usize::MAX, bytes).await.unwrap();
    assert_eq!(w.count("SELECT count(DISTINCT xmin::text) FROM core.events WHERE payload->>'command' IN ('byte-first','byte-last')").await, 2);
}

/// A key that no longer produces most of its outputs has them archived in
/// transactions of their own before its batch, so the writer admits a live
/// batch between two of them instead of after the whole replacement. Until
/// the batch commits the key stays dirty, so a stop in between computes it
/// again.
#[devkit::test]
async fn a_large_replacement_archives_in_transactions_with_live_admission_between(
    db: devkit::pg::TestDatabase,
) {
    // The test runtime uses one thread: a submitting future queues its next
    // job only when polled, and a Bulk acknowledgement follows the Derived
    // job queued before it.
    let w = World::with_keys(db, vec![Box::new(Paused { pause: None })]).await;
    let parent = zsh("parent", 0, 1);
    w.admit(vec![parent.clone()]).await;
    let mut runner = w
        .driver_with(vec![Box::new(Paused { pause: None })], vec![])
        .await;
    let run = runner.run_of("test.paused").unwrap();
    let head = "SELECT value FROM core.counters WHERE name='commit_seq'";
    let dirty_mark = "SELECT last_dirty_seq FROM derivation.dirty WHERE derivation='test.paused' AND key='\"all\"'::jsonb";
    let mut counts = Counts::default();
    let derivation = Paused { pause: None };
    let mut compute = KeyComputation {
        read: &mut runner.read,
        writer: &w.handle,
        derivation: &derivation,
        time_settings: runner.time_settings.clone(),
        run,
        counts: &mut counts,
    };
    let read = ReadMark::now(&mut *compute.read).await.unwrap();
    let seed = computed_commands(
        &[(
            "all",
            &["old-1", "old-2", "old-3", "old-4", "old-5", "old-6"],
        )],
        &parent,
        w.count(head).await,
        read,
    );
    compute.submit(seed, usize::MAX, usize::MAX).await.unwrap();
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.dirty WHERE derivation='test.paused'")
            .await,
        0
    );

    let after_queued_work = || async {
        w.handle
            .submit(
                Priority::Bulk,
                Batch {
                    run: RunId::new(),
                    kind: BatchKind::Api {
                        actor: "test".into(),
                    },
                    records: vec![],
                    archive: vec![],
                },
            )
            .await
            .unwrap();
    };
    let read = ReadMark::now(&mut *compute.read).await.unwrap();
    let computed_at = w.count(head).await;
    let replacement = computed_commands(&[("all", &["new"])], &parent, computed_at, read);
    // One transaction archives at most two previous outputs here.
    let mut writing = Box::pin(compute.submit(replacement, 2, usize::MAX));
    assert!(futures::poll!(tokio::task::unconstrained(writing.as_mut())).is_pending());
    // The batch is refused whole: nothing of the key changed.
    after_queued_work().await;
    assert_eq!(live_commands(&w).await.len(), 6);
    assert!(futures::poll!(tokio::task::unconstrained(writing.as_mut())).is_pending());
    // The first two outputs are archived; the key waits to be completed.
    after_queued_work().await;
    assert_eq!(live_commands(&w).await.len(), 4);
    assert_eq!(w.count(dirty_mark).await, computed_at);
    assert!(futures::poll!(tokio::task::unconstrained(writing.as_mut())).is_pending());
    let mut live = Box::pin(w.admit(vec![zsh("incoming", 10, 2)]));
    assert!(futures::poll!(tokio::task::unconstrained(live.as_mut())).is_pending());
    let (written, ()) = tokio::join!(writing, live);
    written.unwrap();
    assert_eq!(live_commands(&w).await, ["new"]);
    let (first, last, transactions): (i64, i64, i64) = sqlx::query_as(
        "SELECT min(a.archived_seq), max(a.archived_seq), count(DISTINCT a.archived_seq)
         FROM audit.archived a JOIN core.events e ON e.id = a.event_id
         WHERE e.source = 'test.paused' AND a.reason = 'recomputed'",
    )
    .fetch_one(&mut *compute.read)
    .await
    .unwrap();
    let incoming = w
        .count("SELECT commit_seq FROM core.events WHERE payload->>'command'='incoming'")
        .await;
    let new = w
        .count("SELECT commit_seq FROM core.events WHERE payload->>'command'='new'")
        .await;
    assert_eq!(transactions, 3, "six outputs, two in each transaction");
    assert!(
        first < incoming && incoming < last && last < new,
        "{first} < {incoming} < {last} < {new}"
    );
    // The live input dirtied the key after it was computed; that mark stays.
    assert_eq!(w.count(dirty_mark).await, incoming);
}
