use super::*;
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceBuildImpact, SourceContract,
    SourceCriticality, SourceRecoveryPolicy, SourceRuntimeBinding, SubjectRef,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use xtask::sandbox::prelude::sinex_test;

/// Marker env var used by the concurrent-binding regression test. Picked
/// to be specific enough that no other code in the process sets it.
const TEST_KEY: &str = "SINEX_BINDINGS_TEST_DISPLAY_RACE";

// ---------------------------------------------------------------------------
// sinex-sn6s: criticality-declaration enforcement fixture
// ---------------------------------------------------------------------------
//
// A permanent (proposed = false, so it is treated as "active") test-only
// source registered at link time into the same global `inventory`
// collections `validate_bindings` reads at real sinexd startup. It
// deliberately never calls `.criticality(..)` on the runtime binding, so
// `validate_bindings` must reject a manifest that enables it. This exercises
// the actual production validation path — not a reimplementation — so
// reverting the criticality check inside `validate_bindings` (or reverting
// the `criticality: Option<SourceCriticality>` field/enforcement itself)
// makes `enabling_binding_without_criticality_fails_loudly` below fail.
const CRITICALITY_FIXTURE_SOURCE_ID: &str = "sinex.fixture.criticality-undeclared";
const INVALID_RECOVERY_FIXTURE_SOURCE_ID: &str = "sinex.fixture.recovery-policy-invalid";

sinex_primitives::register_source_contract! {
    SourceContract {
        id: CRITICALITY_FIXTURE_SOURCE_ID,
        namespace: "test",
        event_types: &[("sinex.fixture", "criticality.undeclared")],
        source_role: sinex_primitives::sources::SourceRole::Reflection,
        privacy_tier: PrivacyTier::Public,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Natural,
        access_scope: AccessScope::Internal,
    }
}

sinex_primitives::register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:sinex.fixture.criticality-undeclared"),
        CRITICALITY_FIXTURE_SOURCE_ID,
        "test",
    )
    .implementation("sinexd::test-fixture")
    .adapter("test_adapter")
    .output_event_type("criticality.undeclared")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EmbeddedEmitter)
    .source_id(CRITICALITY_FIXTURE_SOURCE_ID)
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .recovery_policy(SourceRecoveryPolicy::APPEND_STREAM)
    // Deliberately no `.criticality(..)` call — this is the missing
    // declaration the test asserts startup rejects.
    .build()
}

sinex_primitives::register_source_contract! {
    SourceContract {
        id: INVALID_RECOVERY_FIXTURE_SOURCE_ID,
        namespace: "test",
        event_types: &[("sinex.fixture", "recovery-policy.invalid")],
        source_role: sinex_primitives::sources::SourceRole::Reflection,
        privacy_tier: PrivacyTier::Public,
        horizons: &[Horizon::Continuous],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Natural,
        access_scope: AccessScope::Internal,
    }
}

sinex_primitives::register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:sinex.fixture.recovery-policy-invalid"),
        INVALID_RECOVERY_FIXTURE_SOURCE_ID,
        "test",
    )
    .implementation("sinexd::test-fixture")
    .adapter("test_adapter")
    .output_event_type("recovery-policy.invalid")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EmbeddedEmitter)
    .source_id(INVALID_RECOVERY_FIXTURE_SOURCE_ID)
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .build_impact(SourceBuildImpact::ZERO)
    .recovery_policy(SourceRecoveryPolicy::live_observation("", "sinex-r6d.8"))
    .criticality(SourceCriticality::Reconstructable)
    .build()
}

#[sinex_test]
async fn enabling_binding_without_criticality_fails_loudly() -> xtask::sandbox::TestResult<()> {
    let manifest_binding = SourceBinding {
        source_id: CRITICALITY_FIXTURE_SOURCE_ID.to_string(),
        instance_idx: 1,
        service_name: None,
        runtime_config: None,
        extra_args: Vec::new(),
        extra_env: HashMap::new(),
    };

    let result = validate_bindings(std::slice::from_ref(&manifest_binding));

    let error = result.expect_err(
        "validate_bindings must fail loudly when an enabled binding has no \
         declared SourceCriticality",
    );
    let message = error.to_string();
    assert!(
        message.contains(CRITICALITY_FIXTURE_SOURCE_ID),
        "error must name the offending source id, got: {message}"
    );
    assert!(
        message.to_lowercase().contains("criticality"),
        "error must explain the missing SourceCriticality declaration, got: {message}"
    );
    Ok(())
}

#[sinex_test]
async fn enabling_binding_with_invalid_recovery_policy_fails_loudly()
-> xtask::sandbox::TestResult<()> {
    let manifest_binding = SourceBinding {
        source_id: INVALID_RECOVERY_FIXTURE_SOURCE_ID.to_string(),
        instance_idx: 1,
        service_name: None,
        runtime_config: None,
        extra_args: Vec::new(),
        extra_env: HashMap::new(),
    };

    let error = validate_bindings(std::slice::from_ref(&manifest_binding)).expect_err(
        "validate_bindings must reject an enabled binding with an invalid SourceRecoveryPolicy",
    );
    assert!(
        error.to_string().contains("invalid SourceRecoveryPolicy"),
        "error must identify the invalid policy, got: {error}"
    );
    Ok(())
}

#[sinex_test]
async fn binding_defaults_control_identity_to_source_id() -> xtask::sandbox::TestResult<()> {
    let config = source_binding_runtime_config_with_identity(
        None,
        "source-driver-fs-watcher-1",
        "fs-watcher",
    )
    .expect("default runtime config must be present");

    assert_eq!(
        config["checkpoint_identity"], "source-driver-fs-watcher-1",
        "checkpoint state remains per binding instance"
    );
    assert_eq!(
        config["control_identity"], "fs-watcher",
        "replay/control subjects must address the logical source id"
    );
    Ok(())
}

#[sinex_test]
async fn binding_preserves_explicit_control_identity() -> xtask::sandbox::TestResult<()> {
    let config = source_binding_runtime_config_with_identity(
        Some(serde_json::json!({
            "control_identity": "custom-control",
            "checkpoint_identity": "custom-checkpoint"
        })),
        "source-driver-fs-watcher-1",
        "fs-watcher",
    )
    .expect("object runtime config must be present");

    assert_eq!(config["checkpoint_identity"], "custom-checkpoint");
    assert_eq!(config["control_identity"], "custom-control");
    Ok(())
}

/// Two concurrent bindings with conflicting `extra_env` values for the
/// same key must each observe their own value while the lock is held.
/// Pre-fix, the second binding's `set_var` would clobber the first's
/// value; the global `BINDING_ENV_LOCK` plus `EnvGuard` save/restore
/// guarantees serialized, isolated env mutation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_bindings_see_their_own_env_values() {
    // Ensure no stale value leaks in from a prior test run.
    // SAFETY: test is single-threaded at this point; no readers yet.
    unsafe {
        std::env::remove_var(TEST_KEY);
    }

    let saw_a = Arc::new(AtomicBool::new(false));
    let saw_b = Arc::new(AtomicBool::new(false));

    let make_task = |key: &'static str, expected: &'static str, flag: Arc<AtomicBool>| {
        let mut env = HashMap::new();
        env.insert(key.to_string(), expected.to_string());
        async move {
            let _lock = BINDING_ENV_LOCK.lock().await;
            let _guard = EnvGuard::install(&env);
            // Hold the lock long enough that the other task is blocked
            // and forced to wait for restore.
            let observed = std::env::var(key).unwrap_or_default();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let observed_after_sleep = std::env::var(key).unwrap_or_default();
            if observed == expected && observed_after_sleep == expected {
                flag.store(true, Ordering::SeqCst);
            }
        }
    };

    let task_a = tokio::spawn(make_task(TEST_KEY, ":0", saw_a.clone()));
    let task_b = tokio::spawn(make_task(TEST_KEY, ":1", saw_b.clone()));

    task_a.await.unwrap();
    task_b.await.unwrap();

    assert!(
        saw_a.load(Ordering::SeqCst),
        "binding A did not see its own value"
    );
    assert!(
        saw_b.load(Ordering::SeqCst),
        "binding B did not see its own value"
    );

    // Both EnvGuards have dropped and restored; the key should now be
    // unset (it was unset before the test).
    assert!(
        std::env::var(TEST_KEY).is_err(),
        "EnvGuard did not restore unset state after drop"
    );
}

/// Regression for the deadlock introduced by holding `BINDING_ENV_LOCK`
/// across the full factory lifetime: continuous factories never resolve,
/// so binding A would block binding B forever.
///
/// `EnvLockedFactory` drops the lock + `EnvGuard` after the first poll,
/// so two never-resolving env-mutating factories must both reach their
/// pending state without one starving the other.
///
/// This test only asserts the no-deadlock property. It does not assert
/// env isolation across the full factory lifetime — that contract is
/// limited to the first poll (factory sync prefix) per
/// [`BINDING_ENV_LOCK`]'s docs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn env_locked_factory_releases_lock_after_first_poll() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    const KEY_A: &str = "SINEX_BINDINGS_TEST_DEADLOCK_A";
    const KEY_B: &str = "SINEX_BINDINGS_TEST_DEADLOCK_B";

    // Track how many distinct factories observed their env during the
    // sync prefix (the first poll). Both must hit poll-1 even though
    // neither inner future ever resolves.
    let observed = Arc::new(AtomicUsize::new(0));

    fn make_factory(
        key: &'static str,
        expected: &'static str,
        observed: Arc<AtomicUsize>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        // The factory's sync prefix snapshots env (mirroring
        // `baseline_adapter_config`), then yields forever.
        let snapshot = std::env::var(key).ok();
        if snapshot.as_deref() == Some(expected) {
            observed.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(std::future::poll_fn(|_cx| Poll::<()>::Pending))
    }

    let run_one = |key: &'static str, value: &'static str, observed: Arc<AtomicUsize>| {
        let mut env = HashMap::new();
        env.insert(key.to_string(), value.to_string());
        async move {
            let env_lock = Arc::clone(&BINDING_ENV_LOCK).lock_owned().await;
            let env_guard = EnvGuard::install(&env);
            // Construct the factory *after* installing env so the sync
            // prefix sees it. (run_binding calls factory(argv) after
            // env install for the same reason.)
            let inner = make_factory(key, value, Arc::clone(&observed));
            let locked = EnvLockedFactory {
                inner,
                lock_state: Some((env_lock, env_guard)),
            };
            locked.await;
        }
    };

    // Each factory is Pending forever; we must time them out, not await
    // resolution.
    let timeout = std::time::Duration::from_secs(2);
    let a = tokio::spawn(run_one(KEY_A, "alpha", Arc::clone(&observed)));
    let b = tokio::spawn(run_one(KEY_B, "beta", Arc::clone(&observed)));

    // Give both tasks time to reach their first poll. If the lock were
    // still held across the full future, only one would observe its env
    // and `observed` would stick at 1 until the test times out.
    let _ = tokio::time::timeout(timeout, async {
        loop {
            if observed.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both factories must reach first poll within 2s (deadlock regression)");

    a.abort();
    b.abort();
    let _ = a.await;
    let _ = b.await;

    // Cleanup: keys should have been restored to unset by the
    // EnvGuards dropped at poll-1.
    // SAFETY: test is the only thread mutating env at this point.
    unsafe {
        std::env::remove_var(KEY_A);
        std::env::remove_var(KEY_B);
    }
}

/// Installing an `EnvGuard` over an empty map is a no-op and does not
/// touch the environment.
#[sinex_test]
async fn env_guard_empty_is_noop() -> xtask::sandbox::TestResult<()> {
    let key = "SINEX_BINDINGS_TEST_EMPTY_NOOP";
    // SAFETY: scoped to a key no one else uses.
    unsafe {
        std::env::remove_var(key);
    }
    let empty: HashMap<String, String> = HashMap::new();
    {
        let _guard = EnvGuard::install(&empty);
        assert!(std::env::var(key).is_err());
    }
    assert!(std::env::var(key).is_err());
    Ok(())
}

/// Installing an `EnvGuard` over a key that had a prior value restores
/// the prior value on drop, not unsets it.
#[sinex_test]
async fn env_guard_restores_prior_value() -> xtask::sandbox::TestResult<()> {
    let key = "SINEX_BINDINGS_TEST_RESTORE";
    // SAFETY: scoped to a unique key; not in scope for the multi-thread
    // race test above.
    unsafe {
        std::env::set_var(key, "prior");
    }
    let mut new_env = HashMap::new();
    new_env.insert(key.to_string(), "overridden".to_string());
    {
        let _guard = EnvGuard::install(&new_env);
        assert_eq!(std::env::var(key).unwrap(), "overridden");
    }
    assert_eq!(
        std::env::var(key).unwrap(),
        "prior",
        "EnvGuard drop must restore the prior value"
    );
    // Cleanup.
    unsafe {
        std::env::remove_var(key);
    }
    Ok(())
}
