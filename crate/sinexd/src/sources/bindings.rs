//! Source-binding manifest loader and in-process spawner.
//!
//! Replaces the per-binding `sinex-source-worker` systemd unit fleet with
//! one tokio task per binding under the `sinexd` supervisor. The supervisor
//! reads `SINEX_SOURCE_BINDINGS_PATH`, deserializes the manifest, and
//! dispatches each enabled binding through the existing node-factory
//! registry — the same factory the deleted `sinex-source-worker` binary
//! used, just invoked in-process.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use sinex_primitives::error::{Result, SinexError};
use sinex_primitives::parser::SourceUnitId;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::sources::node_factory;
use crate::sources::registry::SourceUnitRegistry;

/// Process-global mutex guarding bindings that mutate environment variables.
///
/// `std::env::set_var` is process-global; concurrent invocation from multiple
/// tokio tasks lets each binding clobber the others' DISPLAY/XAUTHORITY/etc.
/// values, and edition 2024 classifies it as UB whenever any other thread is
/// reading the environment. The lock serializes the *first-poll window* of
/// the factory future so each binding observes its own env during the brief
/// startup window when adapters call `env::var`. After the first poll the
/// lock is released and the EnvRestore guard restores the prior env.
///
/// Residual hazard: any adapter that reads env::var AFTER its first `.await`
/// observes a non-deterministic value. Bounded-impact today (only Hyprland
/// reads env, in `baseline_adapter_config` which is called from
/// `AdapterBackedIngestor::initialize` after several `.await` points). A
/// fully sound fix threads per-binding env through the factory as data and
/// removes `std::env::var` from adapters; tracked as follow-up.
static BINDING_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());


/// One row in the manifest file at `SINEX_SOURCE_BINDINGS_PATH`.
///
/// The NixOS module generates this from `services.sinex.generatedBindings.*`
/// at activation time and writes it into the Nix store. The Rust side
/// validates the manifest structure but defers source-unit-id validity
/// checking to the descriptor registry (so an unknown source unit fails
/// loudly at startup with a list of registered alternatives).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBinding {
    /// Source-unit id (e.g. `terminal.atuin-history`). Must be registered
    /// in the descriptor registry via `register_source_unit!`.
    pub source_unit_id: String,

    /// 1-based instance index. Preserved from the historical
    /// `sinex-source-worker-<id>-<idx>.service` unit name convention so
    /// log output and checkpoint identities stay comparable across the
    /// collapse.
    #[serde(default = "default_instance_idx")]
    pub instance_idx: u32,

    /// Optional service-name override. Defaults to
    /// `sinex-source-worker-<id>-<idx>` when absent.
    #[serde(default)]
    pub service_name: Option<String>,

    /// JSON object passed verbatim to the source unit via `--node-config`.
    /// Empty / null skips the flag.
    #[serde(default)]
    pub node_config: Option<serde_json::Value>,

    /// Extra CLI arguments. In continuous mode (empty extra_args) the
    /// `service` subcommand is appended automatically. When non-empty,
    /// the first element is the subcommand (e.g. `"scan"`) and the rest
    /// are its flags.
    #[serde(default)]
    pub extra_args: Vec<String>,

    /// Environment variables injected into the binding's process scope.
    /// Replaces the per-unit `EnvironmentFile` overlays that existed when
    /// each source-worker was a separate systemd unit. Keys set here
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

/// Validate every binding's `source_unit_id` against the descriptor
/// registry, returning an error listing every offending entry. Fail-fast
/// on startup so a malformed manifest cannot silently shrink the active
/// capture surface.
pub fn validate_bindings(bindings: &[SourceBinding]) -> Result<()> {
    let registry = SourceUnitRegistry::from_inventory();
    let mut errors = Vec::new();
    for binding in bindings {
        let unit_id = SourceUnitId::new(&binding.source_unit_id).map_err(|error| {
            SinexError::configuration(format!(
                "binding has invalid source_unit_id '{}': {error}",
                binding.source_unit_id
            ))
        })?;
        if let Err(error) = registry.validate(&unit_id) {
            errors.push(format!("{}: {error}", binding.source_unit_id));
            continue;
        }
        if node_factory::find_node_factory(&unit_id).is_none() {
            errors.push(format!(
                "{}: descriptor registered but no node factory \
                 (missing register_node_factory! / register_adapter_ingestor! call)",
                binding.source_unit_id
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

/// Drive one binding through the standard SDK lifecycle.
///
/// Mirrors the deleted `sinex-source-worker` trampoline: look up the
/// node factory for the source-unit id, then call it with a synthesized
/// argv equivalent to the old systemd `ExecStart`.
pub async fn run_binding(binding: SourceBinding) -> Result<()> {
    let unit_id = SourceUnitId::new(&binding.source_unit_id).map_err(|error| {
        SinexError::configuration(format!(
            "invalid source_unit_id '{}': {error}",
            binding.source_unit_id
        ))
    })?;
    let factory = node_factory::find_node_factory(&unit_id).ok_or_else(|| {
        SinexError::configuration(format!(
            "no node factory registered for source unit '{}'",
            binding.source_unit_id
        ))
    })?;

    let service_name = binding.service_name.clone().unwrap_or_else(|| {
        format!(
            "sinex-source-worker-{}-{}",
            binding.source_unit_id, binding.instance_idx
        )
    });

    let mut argv: Vec<std::ffi::OsString> = vec![
        std::ffi::OsString::from("sinexd-source-worker"),
        std::ffi::OsString::from("--source-unit"),
        std::ffi::OsString::from(&binding.source_unit_id),
        std::ffi::OsString::from("--service-name"),
        std::ffi::OsString::from(&service_name),
    ];
    if let Some(config) = &binding.node_config {
        // Skip if explicitly null or an empty object — clap rejects empty
        // values and an empty {} is operationally identical to "use defaults".
        let is_empty_object = config
            .as_object()
            .map(serde_json::Map::is_empty)
            .unwrap_or(false);
        if !config.is_null() && !is_empty_object {
            let encoded = serde_json::to_string(config).map_err(|error| {
                SinexError::configuration(format!(
                    "failed to encode node_config for '{}'",
                    binding.source_unit_id
                ))
                .with_std_error(&error)
            })?;
            argv.push(std::ffi::OsString::from("--node-config"));
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
        source_unit = %binding.source_unit_id,
        instance_idx = binding.instance_idx,
        service_name = %service_name,
        "starting in-process source-worker binding"
    );

    // Per-binding environment variables (#1562 item 1).
    //
    // The fast path (empty `extra_env`) is the common case and never touches
    // the process environment; the original `tokio::spawn` parallelism is
    // preserved unchanged.
    //
    // The slow path (non-empty `extra_env`) acquires the global
    // `BINDING_ENV_LOCK` so concurrent env-mutating bindings cannot clobber
    // each other's DISPLAY/XAUTHORITY values. Refined in a follow-up commit
    // to release the lock after the factory's first poll.
    if binding.extra_env.is_empty() {
        factory(argv).await.map_err(|error| {
            SinexError::service(format!(
                "source-worker binding '{}' exited with error: {error}",
                binding.source_unit_id
            ))
        })
    } else {
        let _env_lock = BINDING_ENV_LOCK.lock().await;
        let _env_guard = EnvGuard::install(&binding.extra_env);
        factory(argv).await.map_err(|error| {
            SinexError::service(format!(
                "source-worker binding '{}' exited with error: {error}",
                binding.source_unit_id
            ))
        })
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
            unsafe { std::env::set_var(k, v); }
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
            unsafe { std::env::set_var(k, v); }
        }
        let previously_set: Vec<_> = self.saved.iter().map(|(k, _)| k.as_str()).collect();
        for k in &self.keys {
            if !previously_set.contains(&k.as_str()) {
                unsafe { std::env::remove_var(k); }
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
        unsafe { std::env::remove_var(TEST_KEY); }

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

        assert!(saw_a.load(Ordering::SeqCst), "binding A did not see its own value");
        assert!(saw_b.load(Ordering::SeqCst), "binding B did not see its own value");

        // Both EnvGuards have dropped and restored; the key should now be
        // unset (it was unset before the test).
        assert!(
            std::env::var(TEST_KEY).is_err(),
            "EnvGuard did not restore unset state after drop"
        );
    }

    /// Installing an `EnvGuard` over an empty map is a no-op and does not
    /// touch the environment.
    #[test]
    fn env_guard_empty_is_noop() {
        let key = "SINEX_BINDINGS_TEST_EMPTY_NOOP";
        // SAFETY: scoped to a key no one else uses.
        unsafe { std::env::remove_var(key); }
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
        unsafe { std::env::set_var(key, "prior"); }
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
        unsafe { std::env::remove_var(key); }
    }
}
