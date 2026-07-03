//! Static catalog of automata hosted in `sinexd`.
//!
//! The supervisor reads `SINEX_AUTOMATA_ENABLED` (comma-separated list of
//! automaton names, or the literal `all`) and dispatches each enabled entry
//! through [`spawn`] into the supervisor task tree. Each spawn synthesizes a
//! `RuntimeCli` argv with `--service-name sinex-<name>` and the `service`
//! subcommand, then drives it through the standard
//! [`RuntimeCliRunner`](crate::runtime::runtime_cli::RuntimeCliRunner) lifecycle.
//!
//! Adding a new automaton is two lines below: import the adapter alias and add
//! an `AutomatonSpec` entry.

use futures::future::BoxFuture;
use sinex_primitives::error::{Result, SinexError};
use tracing::info;

use crate::automata::{
    AnalyticsAutomatonRuntime, DailySummarizerRuntime, DocumentParserRuntime,
    EmbeddingProducerRuntime, EntityEnricherRuntime, EntityExtractorRuntime, EntityResolverRuntime,
    HealthAggregatorRuntime, HourlySummarizerRuntime, InstructionExpectationReconcilerRuntime,
    RelationExtractorRuntime, SessionDetectorRuntime, TagApplierRuntime,
    TerminalCommandCanonicalizerRuntime,
};

/// Type-erased automaton runner. Returns `Ok(())` when the automaton exits
/// cleanly; `Err` is logged by the supervisor without aborting siblings.
pub type AutomatonRunFn = fn() -> BoxFuture<'static, Result<()>>;

/// Type-erased runtime contract probe for one registered automaton.
///
/// The supervisor only needs [`AutomatonRunFn`], but durability tests need to
/// prove that the concrete registered runtime shape is compatible with the
/// confirmed-event bridge. Keep this side-channel cheap and derived from the
/// same concrete type used by `run`.
pub type AutomatonContractFn = fn() -> AutomatonRuntimeContract;

/// Runtime contract bits that determine confirmed-event bridge safety.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutomatonRuntimeContract {
    pub supports_continuous: bool,
    pub supports_historical: bool,
    pub manages_own_continuous_loop: bool,
    pub manages_own_checkpoints: bool,
}

/// Static catalog entry for one hosted automaton.
pub struct AutomatonSpec {
    /// CLI-friendly name used by `SINEX_AUTOMATA_ENABLED` and status surfaces.
    pub name: &'static str,
    /// Spawner that constructs and drives the automaton's `RuntimeCliRunner`.
    pub run: AutomatonRunFn,
    /// Probe for the same concrete runtime type's bridge-safety contract.
    pub contract: AutomatonContractFn,
}

/// All automata hosted by `sinexd`. Order is preserved in `all` selection.
pub const AUTOMATA: &[AutomatonSpec] = &[
    AutomatonSpec {
        name: "canonicalizer",
        run: || {
            Box::pin(run_one::<TerminalCommandCanonicalizerRuntime>(
                "canonicalizer",
            ))
        },
        contract: contract_for::<TerminalCommandCanonicalizerRuntime>,
    },
    AutomatonSpec {
        name: "analytics",
        run: || Box::pin(run_one::<AnalyticsAutomatonRuntime>("analytics")),
        contract: contract_for::<AnalyticsAutomatonRuntime>,
    },
    AutomatonSpec {
        name: "health",
        run: || Box::pin(run_one::<HealthAggregatorRuntime>("health")),
        contract: contract_for::<HealthAggregatorRuntime>,
    },
    AutomatonSpec {
        name: "session",
        run: || Box::pin(run_one::<SessionDetectorRuntime>("session")),
        contract: contract_for::<SessionDetectorRuntime>,
    },
    AutomatonSpec {
        name: "hourly",
        run: || Box::pin(run_one::<HourlySummarizerRuntime>("hourly")),
        contract: contract_for::<HourlySummarizerRuntime>,
    },
    AutomatonSpec {
        name: "daily",
        run: || Box::pin(run_one::<DailySummarizerRuntime>("daily")),
        contract: contract_for::<DailySummarizerRuntime>,
    },
    AutomatonSpec {
        name: "entity-extractor",
        run: || Box::pin(run_one::<EntityExtractorRuntime>("entity-extractor")),
        contract: contract_for::<EntityExtractorRuntime>,
    },
    AutomatonSpec {
        name: "entity-resolver",
        run: || Box::pin(run_one::<EntityResolverRuntime>("entity-resolver")),
        contract: contract_for::<EntityResolverRuntime>,
    },
    AutomatonSpec {
        name: "relation-extractor",
        run: || Box::pin(run_one::<RelationExtractorRuntime>("relation-extractor")),
        contract: contract_for::<RelationExtractorRuntime>,
    },
    AutomatonSpec {
        name: "entity-enricher",
        run: || Box::pin(run_one::<EntityEnricherRuntime>("entity-enricher")),
        contract: contract_for::<EntityEnricherRuntime>,
    },
    AutomatonSpec {
        name: "tag-applier",
        run: || Box::pin(run_one::<TagApplierRuntime>("tag-applier")),
        contract: contract_for::<TagApplierRuntime>,
    },
    AutomatonSpec {
        name: "embedding-producer",
        run: || Box::pin(run_one::<EmbeddingProducerRuntime>("embedding-producer")),
        contract: contract_for::<EmbeddingProducerRuntime>,
    },
    AutomatonSpec {
        name: "document-parser",
        run: || Box::pin(run_one::<DocumentParserRuntime>("document-parser")),
        contract: contract_for::<DocumentParserRuntime>,
    },
    AutomatonSpec {
        name: "instruction-reconciler",
        run: || {
            Box::pin(run_one::<InstructionExpectationReconcilerRuntime>(
                "instruction-reconciler",
            ))
        },
        contract: contract_for::<InstructionExpectationReconcilerRuntime>,
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
/// sinexd <automaton runtime args> --service-name sinex-<name> service
/// ```
/// then dispatches through [`RuntimeCliRunner`]. Env-var fallbacks for
/// NATS/TLS/database remain effective because `clap`'s `env` attribute reads
/// the process environment at parse time.
async fn run_one<T>(name: &'static str) -> Result<()>
where
    T: crate::runtime::stream::RuntimeModule
        + crate::runtime::ExplorationProvider
        + Default
        + 'static,
{
    use crate::runtime::runtime_cli::{RuntimeCli, RuntimeCliRunner};
    use clap::Parser;

    let service_name = format!("sinex-{name}");
    info!(automaton = name, service_name = %service_name, "starting in-process automaton");

    let argv: Vec<std::ffi::OsString> = vec![
        std::ffi::OsString::from("sinexd-automaton"),
        std::ffi::OsString::from("--service-name"),
        std::ffi::OsString::from(&service_name),
        std::ffi::OsString::from("service"),
    ];

    let parsed = RuntimeCli::try_parse_from(argv).map_err(|error| {
        SinexError::configuration(format!(
            "failed to construct RuntimeCli for automaton '{name}': {error}"
        ))
    })?;
    let mut runner = RuntimeCliRunner::new(T::default());
    runner.run(parsed).await.map_err(|error| {
        SinexError::service(format!("automaton '{name}' exited with error")).with_std_error(&error)
    })
}

fn contract_for<T>() -> AutomatonRuntimeContract
where
    T: crate::runtime::stream::RuntimeModule + Default,
{
    let capabilities = T::default().capabilities();
    AutomatonRuntimeContract {
        supports_continuous: capabilities.supports_continuous,
        supports_historical: capabilities.supports_historical,
        manages_own_continuous_loop: capabilities.manages_own_continuous_loop,
        manages_own_checkpoints: capabilities.manages_own_checkpoints,
    }
}

#[cfg(test)]
#[path = "registry_test.rs"]
mod registry_test;
