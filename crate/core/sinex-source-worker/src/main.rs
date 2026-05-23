//! sinex-source-worker — consolidated source-unit ingestor host.
//!
//! Hosts source-unit ingestors in a single binary. The source unit to run
//! is selected by the `--source-unit <name>` argument (or `SINEX_SOURCE_UNIT`
//! env).
//!
//! # Usage
//!
//! ```text
//! sinex-source-worker --source-unit <name> [node-sdk args] service
//! sinex-source-worker --source-unit <name> [node-sdk args] scan
//! ```
//!
//! # Source unit names
//!
//! Source units are registered at compile time via [`register_node_factory!`].
//! No match arms — discovery is fully registry-driven.
//!
//! See `crate::node_factory` for the registration protocol.

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_primitives::parser::SourceUnitId;
use sinex_source_worker::node_factory;
use sinex_source_worker::registry::SourceUnitRegistry;

/// Extract `--source-unit <name>` (or `SINEX_SOURCE_UNIT`) from raw argv and
/// return both the source unit name and the filtered argv (without the
/// `--source-unit` flag).
///
/// The `NodeCli` parser does not know about `--source-unit` as a
/// dispatch selector — it only sees it as an optional identity field. We strip
/// the selector form before forwarding the remaining args, and the identity is
/// already carried through `NodeCli`'s `--source-unit` identity field.
fn extract_source_unit(args: Vec<std::ffi::OsString>) -> (SourceUnitId, Vec<std::ffi::OsString>) {
    // Read env as the fallback; CLI must override (standard CLI precedence).
    let env_val = std::env::var("SINEX_SOURCE_UNIT")
        .ok()
        .filter(|v| !v.trim().is_empty());

    let mut cli_value: Option<String> = None;
    let mut filtered: Vec<std::ffi::OsString> = Vec::with_capacity(args.len());
    let mut skip_next = false;

    for (i, arg) in args.iter().enumerate() {
        if i == 0 {
            // Keep argv[0] (program name).
            filtered.push(arg.clone());
            continue;
        }
        if skip_next {
            skip_next = false;
            continue;
        }
        let s = arg.to_string_lossy();
        if s == "--source-unit" {
            skip_next = true;
            if let Some(val) = args.get(i + 1) {
                let val_str = val.to_string_lossy().into_owned();
                cli_value = Some(val_str);
            }
            // Forward both `--source-unit` and its value to NodeCli so
            // `NodeCli::source_unit` is populated for downstream wiring
            // (default_service_name, source_unit_id config injection).
            filtered.push(arg.clone());
            if let Some(val) = args.get(i + 1) {
                filtered.push(val.clone());
            }
        } else if let Some(val) = s.strip_prefix("--source-unit=") {
            cli_value = Some(val.to_owned());
            filtered.push(arg.clone());
        } else {
            filtered.push(arg.clone());
        }
    }

    // CLI takes precedence over env. Without this ordering, an env value
    // set in the systemd service template silently overrides any explicit
    // operator `--source-unit` override on the command line.
    let source_unit = cli_value.or(env_val);

    let name_str = source_unit.unwrap_or_else(|| {
        let list = registered_factory_ids_for_display();
        eprintln!(
            "error: --source-unit <name> is required (or set SINEX_SOURCE_UNIT).\n\
             Registered source units: {list}"
        );
        std::process::exit(1);
    });

    let name = SourceUnitId::new(&name_str).unwrap_or_else(|e| {
        eprintln!("error: invalid --source-unit value '{name_str}': {e}");
        std::process::exit(1);
    });

    (name, filtered)
}

/// Format the registered node-factory ids for display in CLI error messages.
fn registered_factory_ids_for_display() -> String {
    let registered = node_factory::registered_node_factory_ids();
    if registered.is_empty() {
        "(none registered)".to_string()
    } else {
        registered
            .iter()
            .map(sinex_primitives::parser::SourceUnitId::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    human_panic::setup_panic!();

    let raw_args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let (source_unit_name, filtered_args) = extract_source_unit(raw_args);

    // Validate the source unit exists in the descriptor registry before
    // attempting dispatch. This gives a clear error listing available units.
    let registry = SourceUnitRegistry::from_inventory();
    if let Err(e) = registry.validate(&source_unit_name) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }

    // Dispatch to the source unit's factory — registry-driven, no match arms.
    if let Some(factory) = node_factory::find_node_factory(&source_unit_name) {
        factory(filtered_args).await
    } else {
        let list = registered_factory_ids_for_display();
        eprintln!(
            "error: source unit '{source_unit_name}' is in the descriptor registry \
             but has no node factory registered.\n\
             Source units with factories: {list}\n\
             Register a factory with register_node_factory!(\"{source_unit_name}\", YourNode)."
        );
        std::process::exit(1);
    }
}
