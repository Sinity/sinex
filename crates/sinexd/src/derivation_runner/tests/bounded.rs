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

    fn derive(&self, input: sinex_derivations::Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
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
        )])
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
