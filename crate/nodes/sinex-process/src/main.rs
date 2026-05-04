//! sinex-process — consolidated automata binary.
//!
//! Hosts nine derived-node automata in a single binary. The automaton to run
//! is selected by the `--automaton <name>` argument (or `SINEX_AUTOMATON` env).
//!
//! # Usage
//!
//! ```text
//! sinex-process --automaton <name> [node-sdk args] service
//! sinex-process --automaton <name> [node-sdk args] scan
//! ```
//!
//! # Automaton names
//!
//! - `canonicalizer`      — terminal command canonicalizer (TransducerNode)
//! - `analytics`          — activity window analytics (WindowedNode)
//! - `health`             — health aggregator (ScopeReconcilerNode)
//! - `session`            — session detector (WindowedNode)
//! - `hourly`             — hourly activity summarizer (WindowedNode)
//! - `daily`              — daily activity summarizer (WindowedNode)
//! - `entity-resolver`    — entity resolver (WindowedNode)
//! - `relation-extractor` — relation extractor (ScopeReconcilerNode)
//! - `entity-enricher`    — entity enricher (ScopeReconcilerNode)

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_process::{
    AnalyticsAutomatonNode, DailySummarizerNode, EntityEnricherNode, EntityResolverNode,
    HealthAggregatorNode, HourlySummarizerNode, RelationExtractorNode, SessionDetectorNode,
    TerminalCommandCanonicalizerNode,
};

/// Extract `--automaton <name>` (or `SINEX_AUTOMATON`) from raw argv and return
/// both the automaton name and the filtered argv (without the `--automaton` flag).
///
/// The NodeCli parser does not know about `--automaton`, so we strip it before
/// forwarding the remaining args to it.
fn extract_automaton(
    args: Vec<std::ffi::OsString>,
) -> (String, Vec<std::ffi::OsString>) {
    // Check env first.
    let env_val = std::env::var("SINEX_AUTOMATON").ok().filter(|v| !v.trim().is_empty());

    let mut automaton: Option<String> = env_val;
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
        if s == "--automaton" {
            // Next arg is the value; record it (if not already set from env).
            skip_next = true;
            if automaton.is_none() {
                if let Some(val) = args.get(i + 1) {
                    automaton = Some(val.to_string_lossy().into_owned());
                }
            }
        } else if let Some(val) = s.strip_prefix("--automaton=") {
            if automaton.is_none() {
                automaton = Some(val.to_owned());
            }
        } else {
            filtered.push(arg.clone());
        }
    }

    let name = automaton.unwrap_or_else(|| {
        eprintln!(
            "error: --automaton <name> is required (or set SINEX_AUTOMATON).\n\
             Valid values: canonicalizer | analytics | health | session | hourly | daily | entity-resolver | relation-extractor | entity-enricher"
        );
        std::process::exit(1);
    });

    (name, filtered)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    human_panic::setup_panic!();

    let raw_args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    let (automaton_name, filtered_args) = extract_automaton(raw_args);

    // Override process argv so NodeCli::parse() picks up the filtered args.
    // Safety: single-threaded at this point (tokio hasn't spawned yet).
    // We use a trampoline approach: set up a fake ARGV via try_parse_from.
    match automaton_name.as_str() {
        "canonicalizer" => run_node::<TerminalCommandCanonicalizerNode>(filtered_args).await,
        "analytics" => run_node::<AnalyticsAutomatonNode>(filtered_args).await,
        "health" => run_node::<HealthAggregatorNode>(filtered_args).await,
        "session" => run_node::<SessionDetectorNode>(filtered_args).await,
        "hourly" => run_node::<HourlySummarizerNode>(filtered_args).await,
        "daily" => run_node::<DailySummarizerNode>(filtered_args).await,
        "entity-resolver" => run_node::<EntityResolverNode>(filtered_args).await,
        "relation-extractor" => run_node::<RelationExtractorNode>(filtered_args).await,
        "entity-enricher" => run_node::<EntityEnricherNode>(filtered_args).await,
        other => {
            eprintln!(
                "error: unknown automaton '{other}'.\n\
                 Valid values: canonicalizer | analytics | health | session | hourly | daily | entity-resolver | relation-extractor | entity-enricher"
            );
            std::process::exit(1);
        }
    }
}

async fn run_node<T>(
    args: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: sinex_node_sdk::runtime::stream::Node
        + sinex_node_sdk::ExplorationProvider
        + Default
        + 'static,
{
    use clap::Parser;
    use sinex_node_sdk::node_cli::{NodeCli, NodeCliRunner};
    let parsed = NodeCli::parse_from(args);
    let node = T::default();
    let mut runner = NodeCliRunner::new(node);
    runner.run(parsed).await.map_err(std::convert::Into::into)
}
