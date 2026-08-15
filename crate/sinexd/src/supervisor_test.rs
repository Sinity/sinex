use super::{
    DEFAULT_AUTOMATA_ENABLED, RuntimeRetrySchedule, automata_enabled_arg, jittered_runtime_backoff,
    runtime_startup_delay,
};
use crate::automata::registry::{AutomatonRuntimeContract, AutomatonSpec};
use crate::runtime::systemd_notify::{HostedReadiness, HostedReadinessStatus, HostedWorkerId};
use futures::future::BoxFuture;
use sinex_primitives::error::{Result, SinexError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::timeout;
use xtask::sandbox::prelude::sinex_test;

static SUPERVISOR_FAILURE_RUNS: AtomicUsize = AtomicUsize::new(0);

fn failing_automaton_run() -> BoxFuture<'static, Result<()>> {
    SUPERVISOR_FAILURE_RUNS.fetch_add(1, Ordering::Relaxed);
    Box::pin(async { Err(SinexError::processing("synthetic startup failure")) })
}

fn test_automaton_contract() -> AutomatonRuntimeContract {
    AutomatonRuntimeContract {
        supports_continuous: true,
        supports_historical: false,
        manages_own_continuous_loop: false,
        manages_own_checkpoints: false,
    }
}

static FAILING_AUTOMATON: AutomatonSpec = AutomatonSpec {
    name: "test-failing-worker",
    run: failing_automaton_run,
    contract: test_automaton_contract,
    outputs: &[],
};

#[sinex_test]
async fn automata_enabled_arg_distinguishes_unset_from_empty() -> xtask::sandbox::TestResult<()> {
    assert_eq!(automata_enabled_arg(None), Some(DEFAULT_AUTOMATA_ENABLED));
    assert_eq!(automata_enabled_arg(Some("")), None);
    assert_eq!(automata_enabled_arg(Some("   ")), None);
    assert_eq!(
        automata_enabled_arg(Some("interval-lift")),
        Some("interval-lift")
    );
    assert_eq!(automata_enabled_arg(Some("all")), Some("all"));
    Ok(())
}

#[sinex_test]
async fn runtime_startup_stagger_is_bounded_and_seeded() -> xtask::sandbox::TestResult<()> {
    let first = runtime_startup_delay(1);
    let second = runtime_startup_delay(2);
    assert!(first < Duration::from_secs(2));
    assert!(second < Duration::from_secs(2));
    assert_ne!(first, second);
    Ok(())
}

#[sinex_test]
async fn runtime_retry_backoff_has_bounded_jitter() -> xtask::sandbox::TestResult<()> {
    let base = Duration::from_secs(8);
    let first = jittered_runtime_backoff(base, 1);
    let second = jittered_runtime_backoff(base, u64::MAX);
    assert!(first >= Duration::from_secs(4));
    assert!(second >= Duration::from_secs(4));
    assert!(first <= Duration::from_secs(12));
    assert!(second <= Duration::from_secs(12));
    assert_ne!(first, second);
    Ok(())
}

#[sinex_test]
async fn runtime_retry_backoff_stays_capped_at_the_ladder_max()
-> xtask::sandbox::TestResult<()> {
    let first = jittered_runtime_backoff(Duration::from_secs(30), 0);
    let second = jittered_runtime_backoff(Duration::from_secs(30), u64::MAX);
    assert!(first >= Duration::from_secs(15));
    assert!(second <= Duration::from_secs(45));
    assert_ne!(first, second);
    Ok(())
}

#[sinex_test]
async fn runtime_retry_schedule_jitters_capped_retries_and_resets_after_stability()
-> xtask::sandbox::TestResult<()> {
    let mut schedule = RuntimeRetrySchedule::default();
    let initial = schedule.next_delay(Duration::ZERO, 0);
    assert!(initial >= Duration::from_millis(500));
    assert!(initial <= Duration::from_millis(1500));
    let second = schedule.next_delay(Duration::ZERO, 2_000);
    assert!(second >= Duration::from_secs(1));
    assert!(second <= Duration::from_secs(3));

    let mut capped_schedule = RuntimeRetrySchedule {
        delay: Duration::from_secs(30),
    };
    let first_capped = capped_schedule.next_delay(Duration::ZERO, 0);
    let second_capped = capped_schedule.next_delay(Duration::ZERO, 1);
    assert_eq!(first_capped, Duration::from_secs(15));
    assert!(second_capped >= Duration::from_secs(15));
    assert!(second_capped <= Duration::from_secs(30));

    let reset = capped_schedule.next_delay(Duration::from_secs(60), 1);
    assert!(reset >= Duration::from_millis(500));
    assert!(reset <= Duration::from_millis(1500));
    Ok(())
}

#[sinex_test]
async fn supervisor_configures_worker_before_spawn_and_stops_pre_ready_retry()
-> xtask::sandbox::TestResult<()> {
    SUPERVISOR_FAILURE_RUNS.store(0, Ordering::Relaxed);
    let readiness = HostedReadiness::configured([HostedWorkerId::from(
        "automaton:test-failing-worker",
    )])
    .map_err(|error| color_eyre::eyre::eyre!(error))?;
    let worker = readiness
        .worker("automaton:test-failing-worker")
        .expect("worker identity must be configured before spawn");
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = super::spawn_automaton(&FAILING_AUTOMATON, shutdown_rx.clone(), worker);

    let status = timeout(Duration::from_secs(4), readiness.wait(shutdown_rx)).await?;
    let HostedReadinessStatus::Failed { worker_id, reason } = status else {
        panic!("pre-ready worker failure must be reported explicitly");
    };
    assert_eq!(
        worker_id,
        HostedWorkerId::from("automaton:test-failing-worker")
    );
    assert!(
        reason.contains("synthetic startup failure"),
        "failure reason should retain the worker error context: {reason}"
    );
    timeout(Duration::from_secs(1), handle).await??;
    assert_eq!(
        SUPERVISOR_FAILURE_RUNS.load(Ordering::Relaxed),
        1,
        "a pre-ready failure must not be retried through the systemd startup timeout"
    );
    Ok(())
}

/// sinex-ijz6: `SINEX_AUTOMATA_ENABLED` unset must select the 2026-07-08
/// ratified default-enabled set (canonicalizer, health, analytics,
/// attention-stream, interval-lift, session-detector, hourly-summarizer,
/// daily-summarizer -- 8 of 16), not "all".
#[sinex_test]
async fn unset_automata_enabled_selects_the_ratified_default_set_not_all()
-> xtask::sandbox::TestResult<()> {
    let effective = automata_enabled_arg(None);
    let selected = crate::automata::registry::parse_enabled(effective)
        .map_err(|e| color_eyre::eyre::eyre!("parse_enabled: {e}"))?;
    let mut names: Vec<&str> = selected.iter().map(|spec| spec.name).collect();
    names.sort_unstable();

    let mut ratified = vec![
        "canonicalizer",
        "analytics",
        "session",
        "hourly",
        "daily",
        "health",
        "attention-stream",
        "interval-lift",
    ];
    ratified.sort_unstable();

    assert_eq!(
        names,
        ratified,
        "SINEX_AUTOMATA_ENABLED unset selected {} automata instead of the ratified \
         8-automaton default set -- entity-extractor/resolver/enricher, \
         relation-extractor, tag-applier, instruction-reconciler, document-parser, \
         and embedding-producer must \
         stay default-off per the 2026-07-08 retire-until-needed ruling",
        names.len()
    );
    Ok(())
}
