//! Static catalog of derived-node automata hosted in `sinexd`.
//!
//! The supervisor reads `SINEX_AUTOMATA_ENABLED` (comma-separated list of
//! automaton names, or the literal `all`) and dispatches each enabled entry
//! through [`spawn`] into the supervisor task tree. Each spawn synthesizes a
//! `NodeCli` argv with `--service-name sinex-<name>` and the `service`
//! subcommand, then drives it through the standard
//! [`NodeCliRunner`](crate::node_sdk::node_cli::NodeCliRunner) lifecycle —
//! the same lifecycle the deleted `sinex-process` per-automaton binary
//! used, just in-process.
//!
//! Adding a new automaton is two lines below: import the node alias and add
//! an `AutomatonSpec` entry.

use futures::future::BoxFuture;
use sinex_primitives::error::{Result, SinexError};
use tracing::info;

use crate::automata::{
    AnalyticsAutomatonNode, DailySummarizerNode, DocumentParserNodeAdapter, EmbeddingProducerNode,
    EntityEnricherNode, EntityExtractorNode, EntityResolverNode, HealthAggregatorNode,
    HourlySummarizerNode, InstructionExpectationReconcilerNode, RelationExtractorNode,
    SessionDetectorNode, TagApplierNode, TerminalCommandCanonicalizerNode,
};

/// Type-erased automaton runner. Returns `Ok(())` when the automaton exits
/// cleanly; `Err` is logged by the supervisor without aborting siblings.
pub type AutomatonRunFn = fn() -> BoxFuture<'static, Result<()>>;

/// Static catalog entry for one hosted automaton.
pub struct AutomatonSpec {
    /// CLI-friendly name. Matches the historical `--automaton <name>`
    /// selector for `sinex-process` so operator-facing tooling and
    /// `SINEX_AUTOMATA_ENABLED` lists stay stable across the collapse.
    pub name: &'static str,
    /// Spawner that constructs and drives the automaton's `NodeCliRunner`.
    pub run: AutomatonRunFn,
}

/// All automata hosted by `sinexd`. Order is preserved in `all` selection.
pub const AUTOMATA: &[AutomatonSpec] = &[
    AutomatonSpec {
        name: "canonicalizer",
        run: || Box::pin(run_one::<TerminalCommandCanonicalizerNode>("canonicalizer")),
    },
    AutomatonSpec {
        name: "analytics",
        run: || Box::pin(run_one::<AnalyticsAutomatonNode>("analytics")),
    },
    AutomatonSpec {
        name: "health",
        run: || Box::pin(run_one::<HealthAggregatorNode>("health")),
    },
    AutomatonSpec {
        name: "session",
        run: || Box::pin(run_one::<SessionDetectorNode>("session")),
    },
    AutomatonSpec {
        name: "hourly",
        run: || Box::pin(run_one::<HourlySummarizerNode>("hourly")),
    },
    AutomatonSpec {
        name: "daily",
        run: || Box::pin(run_one::<DailySummarizerNode>("daily")),
    },
    AutomatonSpec {
        name: "entity-extractor",
        run: || Box::pin(run_one::<EntityExtractorNode>("entity-extractor")),
    },
    AutomatonSpec {
        name: "entity-resolver",
        run: || Box::pin(run_one::<EntityResolverNode>("entity-resolver")),
    },
    AutomatonSpec {
        name: "relation-extractor",
        run: || Box::pin(run_one::<RelationExtractorNode>("relation-extractor")),
    },
    AutomatonSpec {
        name: "entity-enricher",
        run: || Box::pin(run_one::<EntityEnricherNode>("entity-enricher")),
    },
    AutomatonSpec {
        name: "tag-applier",
        run: || Box::pin(run_one::<TagApplierNode>("tag-applier")),
    },
    AutomatonSpec {
        name: "embedding-producer",
        run: || Box::pin(run_one::<EmbeddingProducerNode>("embedding-producer")),
    },
    AutomatonSpec {
        name: "document-parser",
        run: || Box::pin(run_one::<DocumentParserNodeAdapter>("document-parser")),
    },
    AutomatonSpec {
        name: "instruction-reconciler",
        run: || {
            Box::pin(run_one::<InstructionExpectationReconcilerNode>(
                "instruction-reconciler",
            ))
        },
    },
];

/// Look up an automaton by name.
#[must_use]
pub fn find(name: &str) -> Option<&'static AutomatonSpec> {
    AUTOMATA.iter().find(|spec| spec.name == name)
}

/// Comma-separated list of registered automaton names.
#[must_use]
pub fn registered_names_for_display() -> String {
    AUTOMATA
        .iter()
        .map(|spec| spec.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve `SINEX_AUTOMATA_ENABLED` into a vector of matched specs.
///
/// Accepts:
/// - Unset / empty / whitespace-only → no automata (supervisor logs `disabled`).
/// - `all` (case-insensitive) → every registered automaton in catalog order.
/// - Comma-separated names → each looked up; unknown names raise an error.
pub fn parse_enabled(raw: Option<&str>) -> Result<Vec<&'static AutomatonSpec>> {
    let Some(value) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(Vec::new());
    };

    if value.eq_ignore_ascii_case("all") {
        return Ok(AUTOMATA.iter().collect());
    }

    let mut selected = Vec::new();
    for token in value.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let spec = find(token).ok_or_else(|| {
            SinexError::configuration(format!(
                "unknown automaton '{token}' in SINEX_AUTOMATA_ENABLED. \
                 Registered: {}",
                registered_names_for_display()
            ))
        })?;
        selected.push(spec);
    }
    Ok(selected)
}

/// Construct and drive one automaton through its standard lifecycle.
///
/// Builds an in-process argv equivalent to:
/// ```text
/// sinex-process --automaton <name> --service-name sinex-<name> service
/// ```
/// then dispatches through [`NodeCliRunner`] just as the deleted
/// `sinex-process` trampoline did. Env-var fallbacks for NATS/TLS/database
/// remain effective because `clap`'s `env` attribute reads the process
/// environment at parse time.
async fn run_one<T>(name: &'static str) -> Result<()>
where
    T: crate::node_sdk::runtime::stream::Node
        + crate::node_sdk::ExplorationProvider
        + Default
        + 'static,
{
    use crate::node_sdk::node_cli::{NodeCli, NodeCliRunner};
    use clap::Parser;

    let service_name = format!("sinex-{name}");
    info!(automaton = name, service_name = %service_name, "starting in-process automaton");

    let argv: Vec<std::ffi::OsString> = vec![
        std::ffi::OsString::from("sinexd-automaton"),
        std::ffi::OsString::from("--service-name"),
        std::ffi::OsString::from(&service_name),
        std::ffi::OsString::from("service"),
    ];

    let parsed = NodeCli::try_parse_from(argv).map_err(|error| {
        SinexError::configuration(format!(
            "failed to construct NodeCli for automaton '{name}': {error}"
        ))
    })?;
    let mut runner = NodeCliRunner::new(T::default());
    runner.run(parsed).await.map_err(|error| {
        SinexError::service(format!("automaton '{name}' exited with error")).with_std_error(&error)
    })
}
