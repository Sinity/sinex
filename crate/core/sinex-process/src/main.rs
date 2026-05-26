//! sinex-process — consolidated automata binary.
//!
//! Hosts thirteen derived-node automata in a single binary. The automaton to run
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
//! - `canonicalizer`      — terminal command canonicalizer (`Transducer`)
//! - `analytics`          — activity window analytics (`Windowed`)
//! - `health`             — health aggregator (`ScopeReconciler`)
//! - `session`            — session detector (`Windowed`)
//! - `hourly`             — hourly activity summarizer (`Windowed`)
//! - `daily`              — daily activity summarizer (`Windowed`)
//! - `entity-resolver`    — entity resolver (`Windowed`)
//! - `relation-extractor` — relation extractor (`ScopeReconciler`)
//! - `entity-enricher`    — entity enricher (`ScopeReconciler`)
//! - `entity-extractor`   — entity extractor (`Transducer`)
//! - `tag-applier`       — rule-based tag applier (`Transducer`)
//! - `document-parser`    — document parser (`MultiOutputTransducerNode`)
//! - `instruction-reconciler` — instruction expectation reconciler (`ScopeReconciler`)

#[cfg(not(target_env = "msvc"))]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sinex_process::{
    AnalyticsAutomatonNode, DailySummarizerNode, DocumentParserNodeAdapter, EntityEnricherNode,
    EntityExtractorNode, EntityResolverNode, HealthAggregatorNode, HourlySummarizerNode,
    InstructionExpectationReconcilerNode, RelationExtractorNode, SessionDetectorNode,
    TagApplierNode, TerminalCommandCanonicalizerNode,
};

/// Extract `--automaton <name>` (or `SINEX_AUTOMATON`) from raw argv and return
/// both the automaton name and the filtered argv (without the `--automaton` flag).
///
/// The `NodeCli` parser does not know about `--automaton`, so we strip it before
/// forwarding the remaining args to it.
fn extract_automaton(args: Vec<std::ffi::OsString>) -> (String, Vec<std::ffi::OsString>) {
    // Check env first.
    let env_val = std::env::var("SINEX_AUTOMATON")
        .ok()
        .filter(|v| !v.trim().is_empty());

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
            if automaton.is_none()
                && let Some(val) = args.get(i + 1)
            {
                automaton = Some(val.to_string_lossy().into_owned());
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
             Valid values: canonicalizer | analytics | health | session | hourly | daily | entity-extractor | tag-applier | entity-resolver | relation-extractor | entity-enricher | document-parser | instruction-reconciler"
        );
        std::process::exit(1);
    });

    (name, filtered)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        "entity-extractor" => run_node::<EntityExtractorNode>(filtered_args).await,
        "entity-resolver" => run_node::<EntityResolverNode>(filtered_args).await,
        "relation-extractor" => run_node::<RelationExtractorNode>(filtered_args).await,
        "entity-enricher" => run_node::<EntityEnricherNode>(filtered_args).await,
        "tag-applier" => run_node::<TagApplierNode>(filtered_args).await,
        "document-parser" => run_node::<DocumentParserNodeAdapter>(filtered_args).await,
        "instruction-reconciler" => {
            run_node::<InstructionExpectationReconcilerNode>(filtered_args).await
        }
        other => {
            eprintln!(
                "error: unknown automaton '{other}'.\n\
                 Valid values: canonicalizer | analytics | health | session | hourly | daily | entity-extractor | tag-applier | entity-resolver | relation-extractor | entity-enricher | document-parser | instruction-reconciler"
            );
            std::process::exit(1);
        }
    }
}

async fn run_node<T>(args: Vec<std::ffi::OsString>) -> Result<(), Box<dyn std::error::Error>>
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
