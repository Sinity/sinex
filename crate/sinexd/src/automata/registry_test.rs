use super::*;
use std::collections::HashSet;
use xtask::sandbox::prelude::*;

/// sinex-0vx.1 AC: every registered automaton declares at least one
/// derivation-control-plane output, no (output_source, output_event_type,
/// product_class, semantics_version) tuple is duplicated across the
/// registry (mirrors the `derivation.product_declarations` unique index
/// sketched in the blueprint), and every declaration's shape invariants
/// hold (`DerivationOutputDeclaration::validate()`).
#[sinex_test]
async fn automata_derivation_declarations_cover_registry() -> TestResult<()> {
    assert_eq!(
        AUTOMATA.len(),
        16,
        "expected exactly 16 registered automata (blueprint census) — update this test \
         deliberately if the registry population changed"
    );

    let mut seen_tuples: HashSet<(
        &'static str,
        Option<&'static str>,
        Option<&'static str>,
        sinex_primitives::derivation::DerivedProductClass,
        &'static str,
    )> = HashSet::new();
    let mut seen_declaration_ids = HashSet::new();

    for spec in AUTOMATA {
        assert!(
            !spec.outputs.is_empty(),
            "{} has no derivation output declarations — every registered automaton must \
             declare at least one (sinex-0vx.1 AC)",
            spec.name
        );

        for declaration in spec.outputs {
            declaration.validate().map_err(|error| {
                color_eyre::eyre::eyre!(
                    "{}: declaration {} failed shape validation: {error}",
                    spec.name,
                    declaration.declaration_id
                )
            })?;

            assert!(
                seen_declaration_ids.insert(declaration.declaration_id),
                "duplicate declaration_id '{}' (owner {})",
                declaration.declaration_id,
                declaration.owner
            );

            let tuple = (
                spec.name,
                declaration.output_source,
                declaration.output_event_type,
                declaration.product_class,
                declaration.semantics_version,
            );
            assert!(
                seen_tuples.insert(tuple),
                "{}: duplicate (output_source, output_event_type, product_class, \
                 semantics_version) tuple for declaration {}",
                spec.name,
                declaration.declaration_id
            );

            assert_eq!(
                declaration.owner, spec.name,
                "{}: declaration {} owner does not match its registry entry",
                spec.name, declaration.declaration_id
            );
        }
    }

    Ok(())
}

/// Cross-checks each automaton's static `OUTPUT_DECLARATIONS` (surfaced via
/// `AutomatonSpec.outputs`) against the SAME automaton's own
/// `output_event_source()`/`output_event_type()` (or `output_event_types()`
/// for `MultiOutputTransducer`) trait methods — the two are independent
/// surfaces (registry data vs. runtime trait dispatch) that must agree, or
/// the adapter's undeclared-output rejection (sinex-0vx.2) would reject
/// every real event this automaton emits.
#[sinex_test]
async fn automata_declared_event_types_match_runtime_output() -> TestResult<()> {
    use crate::runtime::{MultiOutputTransducer, ScopeReconciler, Transducer, Windowed};

    fn assert_single_output_matches(
        automaton_name: &'static str,
        runtime_source: &'static str,
        runtime_type: &'static str,
        declarations: &'static [sinex_primitives::derivation::DerivationOutputDeclaration],
    ) {
        assert!(
            declarations
                .iter()
                .any(|d| d.output_source == Some(runtime_source)
                    && d.output_event_type == Some(runtime_type)),
            "{automaton_name}: no declaration matches runtime \
             output_event_source()={runtime_source:?} / output_event_type()={runtime_type:?}"
        );
    }

    let canonicalizer = crate::automata::canonicalizer::TerminalCommandCanonicalizer;
    assert_single_output_matches(
        "canonicalizer",
        canonicalizer.output_event_source(),
        canonicalizer.output_event_type(),
        crate::automata::canonicalizer::CANONICALIZER_OUTPUT_DECLARATIONS,
    );

    let analytics = crate::automata::analytics::AnalyticsAutomaton::default();
    assert_single_output_matches(
        "analytics",
        analytics.output_event_source(),
        analytics.output_event_type(),
        crate::automata::analytics::ANALYTICS_OUTPUT_DECLARATIONS,
    );

    let attention = crate::automata::attention::AttentionStream;
    assert_single_output_matches(
        "attention-stream",
        attention.output_event_source(),
        attention.output_event_type(),
        crate::automata::attention::ATTENTION_STREAM_OUTPUT_DECLARATIONS,
    );

    let health = crate::automata::health::HealthAggregator::default();
    assert_single_output_matches(
        "health",
        health.output_event_source(),
        health.output_event_type(),
        crate::automata::health::HEALTH_OUTPUT_DECLARATIONS,
    );

    let session = crate::automata::session::SessionDetector;
    assert_single_output_matches(
        "session",
        session.output_event_source(),
        session.output_event_type(),
        crate::automata::session::SESSION_OUTPUT_DECLARATIONS,
    );

    let hourly = crate::automata::hourly::HourlySummarizer;
    assert_single_output_matches(
        "hourly",
        hourly.output_event_source(),
        hourly.output_event_type(),
        crate::automata::hourly::HOURLY_OUTPUT_DECLARATIONS,
    );

    let daily = crate::automata::daily::DailySummarizer;
    assert_single_output_matches(
        "daily",
        daily.output_event_source(),
        daily.output_event_type(),
        crate::automata::daily::DAILY_OUTPUT_DECLARATIONS,
    );

    let entity_extractor = crate::automata::entity_extractor::EntityExtractor;
    assert_single_output_matches(
        "entity-extractor",
        entity_extractor.name(),
        entity_extractor.output_event_type(),
        crate::automata::entity_extractor::ENTITY_EXTRACTOR_OUTPUT_DECLARATIONS,
    );

    let entity_resolver = crate::automata::entity_resolver::EntityResolver;
    assert_single_output_matches(
        "entity-resolver",
        entity_resolver.output_event_source(),
        entity_resolver.output_event_type(),
        crate::automata::entity_resolver::ENTITY_RESOLVER_OUTPUT_DECLARATIONS,
    );

    let relation_extractor = crate::automata::relation_extractor::RelationExtractor;
    assert_single_output_matches(
        "relation-extractor",
        relation_extractor.output_event_source(),
        relation_extractor.output_event_type(),
        crate::automata::relation_extractor::RELATION_EXTRACTOR_OUTPUT_DECLARATIONS,
    );

    let entity_enricher = crate::automata::entity_enricher::EntityEnricher::default();
    assert_single_output_matches(
        "entity-enricher",
        entity_enricher.output_event_source(),
        entity_enricher.output_event_type(),
        crate::automata::entity_enricher::ENTITY_ENRICHER_OUTPUT_DECLARATIONS,
    );

    let tag_applier = crate::automata::tag_applier::TagApplier;
    assert_single_output_matches(
        "tag-applier",
        tag_applier.output_event_source(),
        tag_applier.output_event_type(),
        crate::automata::tag_applier::TAG_APPLIER_OUTPUT_DECLARATIONS,
    );

    let embedding_producer = crate::automata::embedding_producer::EmbeddingProducer;
    assert_single_output_matches(
        "embedding-producer",
        embedding_producer.output_event_source(),
        embedding_producer.output_event_type(),
        crate::automata::embedding_producer::EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS,
    );

    let instruction_reconciler =
        crate::automata::instruction_reconciler::InstructionExpectationReconciler;
    assert_single_output_matches(
        "instruction-reconciler",
        instruction_reconciler.output_event_source(),
        instruction_reconciler.output_event_type(),
        crate::automata::instruction_reconciler::INSTRUCTION_RECONCILER_OUTPUT_DECLARATIONS,
    );

    // Multi-output automata: every runtime-declared event type must have a
    // matching static declaration with the same source.
    let interval_lift = crate::automata::interval_lift::IntervalLift;
    for event_type in interval_lift.output_event_types() {
        assert_single_output_matches(
            "interval-lift",
            interval_lift.output_event_source(),
            event_type,
            crate::automata::interval_lift::INTERVAL_LIFT_OUTPUT_DECLARATIONS,
        );
    }

    let document_parser = crate::automata::document_parser::DocumentParserAutomaton::default();
    for event_type in document_parser.output_event_types() {
        assert_single_output_matches(
            "document-parser",
            document_parser.name(),
            event_type,
            crate::automata::document_parser::DOCUMENT_PARSER_OUTPUT_DECLARATIONS,
        );
    }

    Ok(())
}

#[sinex_test]
async fn registered_automata_have_unique_names() -> TestResult<()> {
    let mut seen = HashSet::new();
    for spec in AUTOMATA {
        assert!(
            seen.insert(spec.name),
            "duplicate automaton registry name: {}",
            spec.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn registered_automata_are_bridge_repairable() -> TestResult<()> {
    let mut checked = Vec::new();
    for spec in AUTOMATA {
        let contract = (spec.contract)();
        assert!(
            contract.supports_continuous,
            "{} must be a continuous runtime to use the confirmed-event bridge",
            spec.name
        );
        assert!(
            contract.supports_historical,
            "{} must support historical catch-up before consuming the confirmed-event tail",
            spec.name
        );
        assert!(
            !contract.manages_own_continuous_loop,
            "{} bypasses the generic bridge; add a dedicated loss-window proof before registering it here",
            spec.name
        );
        checked.push(spec.name);
    }

    assert_eq!(checked.len(), AUTOMATA.len());
    assert!(!checked.is_empty(), "automata registry must not be empty");
    Ok(())
}
