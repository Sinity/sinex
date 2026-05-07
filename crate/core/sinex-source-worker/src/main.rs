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
//! - `noop` — template/test source unit (emits no events)
//!
//! Additional source units are added by:
//! 1. Adding the ingestor crate as a dependency
//! 2. Adding `pub use` of the ingestor type in `lib.rs`
//! 3. Adding a match arm below

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_source_worker::{NoopSourceUnit, registry::SourceUnitRegistry};

/// Extract `--source-unit <name>` (or `SINEX_SOURCE_UNIT`) from raw argv and
/// return both the source unit name and the filtered argv (without the
/// `--source-unit` flag).
///
/// The NodeCli parser does not know about `--source-unit` as a
/// dispatch selector — it only sees it as an optional identity field. We strip
/// the selector form before forwarding the remaining args, and the identity is
/// already carried through NodeCli's `--source-unit` identity field.
fn extract_source_unit(
    args: Vec<std::ffi::OsString>,
) -> (String, Vec<std::ffi::OsString>) {
    // Check env first.
    let env_val = std::env::var("SINEX_SOURCE_UNIT")
        .ok()
        .filter(|v| !v.trim().is_empty());

    let mut source_unit: Option<String> = env_val;
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
            // Next arg is the value; record it (if not already set from env).
            skip_next = true;
            if source_unit.is_none() {
                if let Some(val) = args.get(i + 1) {
                    source_unit = Some(val.to_string_lossy().into_owned());
                }
            }
        } else if let Some(val) = s.strip_prefix("--source-unit=") {
            if source_unit.is_none() {
                source_unit = Some(val.to_owned());
            }
        } else {
            filtered.push(arg.clone());
        }
    }

    let name = source_unit.unwrap_or_else(|| {
        eprintln!(
            "error: --source-unit <name> is required (or set SINEX_SOURCE_UNIT).\n\
             Valid values: noop"
        );
        std::process::exit(1);
    });

    (name, filtered)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    human_panic::setup_panic!();

    let raw_args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let (source_unit_name, filtered_args) = extract_source_unit(raw_args);

    // Validate the source unit exists in the registry before attempting
    // dispatch. This gives a clear error message listing available units.
    let registry = SourceUnitRegistry::from_inventory();
    if let Err(e) = registry.validate(&source_unit_name) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }

    // Dispatch to the source unit's IngestorNode implementation.
    // Pattern follows sinex-process/src/main.rs exactly.
    match source_unit_name.as_str() {
        "noop" => {
            run_source_unit::<NoopSourceUnit>(filtered_args).await
        }
        // Future source units — add a match arm per source unit id:
        // "terminal.atuin" => run_source_unit::<TerminalNode>(filtered_args).await,
        // "fs.watcher" => run_source_unit::<FsWatcherNode>(filtered_args).await,
        // "desktop.focus" => run_source_unit::<DesktopFocusNode>(filtered_args).await,
        // "system.journal" => run_source_unit::<SystemJournalNode>(filtered_args).await,
        // "browser.history" => run_source_unit::<BrowserHistoryNode>(filtered_args).await,
        // "document.watcher" => run_source_unit::<DocumentWatcherNode>(filtered_args).await,
        other => {
            eprintln!(
                "error: unknown source unit '{other}'.\n\
                 Valid values: noop"
            );
            std::process::exit(1);
        }
    }
}

/// Run a source-unit ingestor through the standard SDK lifecycle.
///
/// This is the generic dispatch function. Each source unit provides its
/// own `IngestorNode` implementation, which is wrapped in an
/// `IngestorNodeAdapter` that provides `Node`, `ExplorationProvider`,
/// checkpoint persistence, and health reporting via the SDK.
async fn run_source_unit<I>(
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: sinex_node_sdk::IngestorNode + Default + 'static,
{
    use clap::Parser;
    use sinex_node_sdk::IngestorNodeAdapter;
    use sinex_node_sdk::node_cli::{NodeCli, NodeCliRunner};

    let parsed = NodeCli::parse_from(args);
    let node = IngestorNodeAdapter::new(I::default());
    let mut runner = NodeCliRunner::new(node);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}
