//! Source-binding manifest loader and in-process spawner.
//!
//! Replaces the old per-binding source systemd fleet with
//! one tokio task per binding under the `sinexd` supervisor. The supervisor
//! reads `SINEX_SOURCE_BINDINGS_PATH`, deserializes the manifest, and
//! dispatches each enabled binding through the source-factory
//! registry in-process.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::parser::SourceId;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{info, warn};

use crate::sources::registry::SourceContractRegistry;
use crate::sources::source_factory;

/// Process-global mutex guarding bindings that mutate environment variables.
///
/// `std::env::set_var` is process-global; concurrent invocation from multiple
/// tokio tasks lets each binding clobber the others' DISPLAY/XAUTHORITY/etc.
/// values, and edition 2024 classifies it as UB whenever any other thread is
/// reading the environment. The lock serializes the *first-poll window* of
/// the factory future so each binding observes its own env during the brief
/// startup window when adapters call `env::var`. After the first poll the
/// lock is released and the `EnvRestore` guard restores the prior env.
///
/// The lock is held only across the **first poll** of the factory future, not
/// for its entire lifetime. Source factories are continuous and never
/// resolve under normal operation, so a lock held for the full future would
/// permanently block every later env-mutating binding (e.g. a second display
/// scope using `DISPLAY=:1`). Releasing the lock after the first poll lets
/// concurrent bindings each install their env, snapshot it during their sync
/// prefix, and proceed without blocking each other.
///
/// Bindings with empty `extra_env` (the common case — RUST_LOG-only bindings
/// and source contracts that don't read env) take the fast path: no mutation, no
/// serialization, no lock contention.
///
/// Residual hazard: a factory that reads `std::env::var` *after* its first
/// `.await` observes a non-deterministic value, because the lock and
/// `EnvGuard` have already been dropped by then. Adapter authors are
/// responsible for snapshotting env early (in the sync prefix or any
/// `baseline_adapter_config` invoked before the runtime yields). A fully
/// sound fix threads per-binding env through the factory as data and removes
/// `std::env::var` from adapters; that is tracked as a follow-up because it
/// requires extending `MaterialParser::baseline_adapter_config` on the runtime
/// trait, which ripples through every source.
static BINDING_ENV_LOCK: LazyLock<Arc<Mutex<()>>> = LazyLock::new(|| Arc::new(Mutex::new(())));

/// One row in the manifest file at `SINEX_SOURCE_BINDINGS_PATH`.
///
/// The NixOS module generates this from the enabled source binding options at
/// activation time and writes it into the Nix store. The Rust side
/// validates the manifest structure but defers source-id validity
/// checking to the source contract registry (so an unknown source fails
/// loudly at startup with a list of registered alternatives).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBinding {
    /// Source id (e.g. `terminal.atuin-history`). Must be registered
    /// in the source contract registry via `register_source_contract!`.
    pub source_id: String,

    /// 1-based instance index used to derive a stable per-binding service
    /// label.
    #[serde(default = "default_instance_idx")]
    pub instance_idx: u32,

    /// Optional runtime label override. Defaults to
    /// `source-driver-<id>-<idx>` when absent.
    #[serde(default)]
    pub service_name: Option<String>,

    /// JSON object passed verbatim to the source via `--runtime-config`.
    /// Empty / null skips the flag.
    #[serde(default)]
    pub runtime_config: Option<serde_json::Value>,

    /// Extra CLI arguments. In continuous mode (empty `extra_args`) the
    /// `service` subcommand is appended automatically. When non-empty,
    /// the first element is the subcommand (e.g. `"scan"`) and the rest
    /// are its flags.
    #[serde(default)]
    pub extra_args: Vec<String>,

    /// Environment variables injected into the binding's process scope.
    /// Replaces the per-unit `EnvironmentFile` overlays that existed when
    /// each source was a separate systemd unit. Keys set here
    /// override the daemon's inherited environment for the duration of
    /// the binding's lifecycle.
    #[serde(default)]
    pub extra_env: HashMap<String, String>,
}

fn default_instance_idx() -> u32 {
    1
}

/// Manifest envelope. Single `bindings` array; extra fields tolerated for
/// forward compatibility with NixOS module evolution.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceBindingsManifest {
    #[serde(default)]
    pub bindings: Vec<SourceBinding>,
}

impl SourceBindingsManifest {
    /// Load and parse a manifest from disk.
    pub fn load(path: &Utf8PathBuf) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|error| {
            SinexError::configuration(format!("failed to read source-bindings manifest at {path}"))
                .with_std_error(&error)
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            SinexError::configuration(format!(
                "failed to parse source-bindings manifest at {path}"
            ))
            .with_std_error(&error)
        })
    }
}

/// Validate every binding's `source_id` against the descriptor
/// registry, returning an error listing every offending entry. Fail-fast
/// on startup so a malformed manifest cannot silently shrink the active
/// capture surface.
pub fn validate_bindings(bindings: &[SourceBinding]) -> Result<()> {
    let registry = SourceContractRegistry::from_inventory();
    let mut errors = Vec::new();
    for binding in bindings {
        let unit_id = SourceId::new(&binding.source_id).map_err(|error| {
            SinexError::configuration(format!(
                "binding has invalid source_id '{}': {error}",
                binding.source_id
            ))
        })?;
        if let Err(error) = registry.validate(&unit_id) {
            errors.push(format!("{}: {error}", binding.source_id));
            continue;
        }
        if source_factory::find_source_factory(&unit_id).is_none() {
            errors.push(format!(
                "{}: source contract registered but no source factory \
                 (missing register_source! factory call)",
                binding.source_id
            ));
        }
    }
    if !errors.is_empty() {
        return Err(SinexError::configuration(format!(
            "{} source-binding(s) failed validation:\n  - {}",
            errors.len(),
            errors.join("\n  - ")
        )));
    }
    Ok(())
}

/// Drive one binding through the standard runtime lifecycle.
///
/// Look up the source factory for the source id, then call it with a
/// synthesized argv equivalent to the old per-unit `ExecStart`.
pub async fn run_binding(binding: SourceBinding) -> Result<()> {
    let unit_id = SourceId::new(&binding.source_id).map_err(|error| {
        SinexError::configuration(format!(
            "invalid source_id '{}': {error}",
            binding.source_id
        ))
    })?;
    let factory = source_factory::find_source_factory(&unit_id).ok_or_else(|| {
        SinexError::configuration(format!(
            "no source factory registered for source '{}'",
            binding.source_id
        ))
    })?;

    let service_name = binding.service_name.clone().unwrap_or_else(|| {
        format!(
            "source-driver-{}-{}",
            binding.source_id, binding.instance_idx
        )
    });

    let mut argv: Vec<std::ffi::OsString> = vec![
        std::ffi::OsString::from("sinexd-source"),
        std::ffi::OsString::from("--source"),
        std::ffi::OsString::from(&binding.source_id),
        std::ffi::OsString::from("--service-name"),
        std::ffi::OsString::from(&service_name),
    ];
    if let Some(config) = &binding.runtime_config {
        // Skip if explicitly null or an empty object — clap rejects empty
        // values and an empty {} is operationally identical to "use defaults".
        let is_empty_object = config.as_object().is_some_and(serde_json::Map::is_empty);
        if !config.is_null() && !is_empty_object {
            let encoded = serde_json::to_string(config).map_err(|error| {
                SinexError::configuration(format!(
                    "failed to encode runtime_config for '{}'",
                    binding.source_id
                ))
                .with_std_error(&error)
            })?;
            argv.push(std::ffi::OsString::from("--runtime-config"));
            argv.push(std::ffi::OsString::from(encoded));
        }
    }
    for arg in &binding.extra_args {
        argv.push(std::ffi::OsString::from(arg));
    }
    if binding.extra_args.is_empty() {
        argv.push(std::ffi::OsString::from("service"));
    }

    info!(
        source = %binding.source_id,
        instance_idx = binding.instance_idx,
        service_name = %service_name,
        "starting in-process source binding"
    );

    // Per-binding environment variables (#1562 item 1).
    //
    // The fast path (empty `extra_env`) is the common case and never touches
    // the process environment; the original `tokio::spawn` parallelism is
    // preserved unchanged.
    //
    // The slow path (non-empty `extra_env`) acquires the global
    // `BINDING_ENV_LOCK` and installs an `EnvGuard`, then drives the factory
    // future through `EnvLockedFactory`. The lock + guard are released after
    // the factory's first poll completes — long enough for the factory's
    // sync prefix (CLI parsing, runtime construction, the first synchronous
    // configuration hop) to observe the binding's env, but short enough that
    // a second env-mutating binding can proceed without deadlocking against
    // the first one's never-resolving continuous lifecycle.
    if binding.extra_env.is_empty() {
        factory(argv).await.map_err(|error| {
            SinexError::service(format!(
                "source binding '{}' exited with error: {error}",
                binding.source_id
            ))
        })
    } else {
        let env_lock = Arc::clone(&BINDING_ENV_LOCK).lock_owned().await;
        let env_guard = EnvGuard::install(&binding.extra_env);
        let locked = EnvLockedFactory {
            inner: factory(argv),
            lock_state: Some((env_lock, env_guard)),
        };
        locked.await.map_err(|error| {
            SinexError::service(format!(
                "source binding '{}' exited with error: {error}",
                binding.source_id
            ))
        })
    }
}

/// Future wrapper that holds the env lock + `EnvGuard` across exactly the
/// first poll of the inner factory future, then drops both so concurrent
/// env-mutating bindings can proceed.
///
/// Continuous source factories never resolve under normal operation;
/// holding the lock for the full future lifetime would deadlock every
/// subsequent env-mutating binding. Restricting the lock to the first poll
/// gives each binding's sync prefix a chance to snapshot env values into
/// owned configuration, which is the contract documented on
/// [`BINDING_ENV_LOCK`].
struct EnvLockedFactory<F> {
    inner: F,
    lock_state: Option<(OwnedMutexGuard<()>, EnvGuard)>,
}

impl<F> Future for EnvLockedFactory<F>
where
    F: Future + Unpin,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let result = Pin::new(&mut me.inner).poll(cx);
        // Release lock + restore env after the first poll regardless of
        // outcome. If the factory resolved synchronously the cleanup happens
        // before we return; if it returned Pending we drop the lock so other
        // bindings can advance their own sync prefixes.
        me.lock_state.take();
        result
    }
}

/// Save/restore guard for env mutations performed under `BINDING_ENV_LOCK`.
struct EnvGuard {
    saved: Vec<(String, String)>,
    keys: Vec<String>,
}

impl EnvGuard {
    fn install(env: &HashMap<String, String>) -> Self {
        let saved: Vec<_> = env
            .keys()
            .filter_map(|k| std::env::var(k).ok().map(|v| (k.clone(), v)))
            .collect();
        for (k, v) in env {
            // SAFETY: caller holds `BINDING_ENV_LOCK`; no other binding is
            // concurrently mutating env.
            unsafe {
                std::env::set_var(k, v);
            }
        }
        Self {
            saved,
            keys: env.keys().cloned().collect(),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in &self.saved {
            // SAFETY: still under `BINDING_ENV_LOCK`.
            unsafe {
                std::env::set_var(k, v);
            }
        }
        let previously_set: Vec<_> = self.saved.iter().map(|(k, _)| k.as_str()).collect();
        for k in &self.keys {
            if !previously_set.contains(&k.as_str()) {
                unsafe {
                    std::env::remove_var(k);
                }
            }
        }
    }
}

/// Resolve `SINEX_SOURCE_BINDINGS_PATH` into a parsed manifest.
///
/// Returns `Ok(None)` when the env var is unset or empty (no bindings to
/// host). Any parse / IO failure propagates so the supervisor refuses to
/// start with an unreadable manifest rather than silently capturing
/// nothing.
pub fn load_from_env(env_var: &str) -> Result<Option<SourceBindingsManifest>> {
    let raw = std::env::var(env_var).ok();
    let Some(path_str) = raw.map(|s| s.trim().to_owned()).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let path = Utf8PathBuf::from(path_str);
    let manifest = SourceBindingsManifest::load(&path)?;
    if !manifest.bindings.is_empty() {
        warn!(
            path = %path,
            count = manifest.bindings.len(),
            "loaded source-bindings manifest"
        );
    }
    Ok(Some(manifest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Marker env var used by the concurrent-binding regression test. Picked
    /// to be specific enough that no other code in the process sets it.
    const TEST_KEY: &str = "SINEX_BINDINGS_TEST_DISPLAY_RACE";

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
    #[test]
    fn env_guard_empty_is_noop() {
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
    }

    /// Installing an `EnvGuard` over a key that had a prior value restores
    /// the prior value on drop, not unsets it.
    #[test]
    fn env_guard_restores_prior_value() {
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
    }
}
