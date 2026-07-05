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
    /// Replaces the per-source `EnvironmentFile` overlays that existed when
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
/// synthesized argv equivalent to the old per-source `ExecStart`.
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
    let runtime_config = source_binding_runtime_config_with_identity(
        binding.runtime_config.clone(),
        &service_name,
        &binding.source_id,
    );

    let mut argv: Vec<std::ffi::OsString> = vec![
        std::ffi::OsString::from("sinexd-source"),
        std::ffi::OsString::from("--source"),
        std::ffi::OsString::from(&binding.source_id),
        std::ffi::OsString::from("--service-name"),
        std::ffi::OsString::from(&service_name),
    ];
    if let Some(config) = &runtime_config {
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

fn source_binding_runtime_config_with_identity(
    config: Option<serde_json::Value>,
    service_name: &str,
    source_id: &str,
) -> Option<serde_json::Value> {
    match config {
        Some(serde_json::Value::Object(mut object)) => {
            object
                .entry("checkpoint_identity".to_string())
                .or_insert_with(|| serde_json::json!(service_name));
            object
                .entry("control_identity".to_string())
                .or_insert_with(|| serde_json::json!(source_id));
            Some(serde_json::Value::Object(object))
        }
        Some(value) => Some(value),
        None => Some(serde_json::json!({
            "checkpoint_identity": service_name,
            "control_identity": source_id,
        })),
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
#[path = "bindings_test.rs"]
mod tests;
