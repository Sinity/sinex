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

/// Restores environment variables to their previous values on drop.
struct EnvRestore(Vec<(String, Option<String>)>);

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (key, old_value) in self.0.drain(..).rev() {
            match old_value {
                Some(v) => unsafe { std::env::set_var(&key, v) },
                None => unsafe { std::env::remove_var(&key) },
            }
        }
    }
}

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

    // Per-binding env injection: save current values, set overrides,
    // restore on scope exit. Replaces per-unit EnvironmentFile overlays
    // from the pre-collapse multi-systemd-unit architecture.
    let _env_guard = {
        let mut saved: Vec<(String, Option<String>)> = Vec::new();
        for (key, value) in &binding.extra_env {
            saved.push((key.clone(), std::env::var(key).ok()));
            // SAFETY: single-process daemon; these writes are scoped to
            // the binding's lifetime and restored before the next binding
            // or supervisor tick.
            unsafe { std::env::set_var(key, value); }
        }
        EnvRestore(saved)
    };

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

    factory(argv).await.map_err(|error| {
        SinexError::service(format!(
            "source-worker binding '{}' exited with error: {error}",
            binding.source_unit_id
        ))
    })
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
