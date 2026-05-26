//! sinex-process — consolidated automata pack.
//!
//! This crate hosts all twelve derived-node automata in a single binary.
//! The binary is dispatched by `--automaton <name>` to the appropriate
//! [`sinex_node_sdk::node_entrypoint`] equivalent.
//!
//! # Hosted automata
//!
//! | Name | Selector | Node type |
//! |------|----------|-----------|
//! | `canonicalizer` | `--automaton canonicalizer` | [`Transducer`] |
//! | `analytics` | `--automaton analytics` | [`Windowed`] |
//! | `health` | `--automaton health` | [`ScopeReconciler`] |
//! | `session` | `--automaton session` | [`Windowed`] |
//! | `hourly` | `--automaton hourly` | [`Windowed`] |
//! | `daily` | `--automaton daily` | [`Windowed`] |
//! | `entity-resolver` | `--automaton entity-resolver` | [`Windowed`] |
//! | `relation-extractor` | `--automaton relation-extractor` | [`ScopeReconciler`] |
//! | `entity-enricher` | `--automaton entity-enricher` | [`ScopeReconciler`] |
//! | `entity-extractor` | `--automaton entity-extractor` | [`Transducer`] |
//! | `tag-applier` | `--automaton tag-applier` | [`Transducer`] |
//! | `document-parser` | `--automaton document-parser` | [`MultiOutputTransducerNode`] |
//! | `instruction-reconciler` | `--automaton instruction-reconciler` | [`ScopeReconciler`] |
//!
//! All thirteen [`SourceUnitDescriptor`](sinex_primitives::proof::SourceUnitDescriptor)s are
//! registered at program load via the `register_source_unit!` macro in each submodule.

pub mod automata {
    pub mod analytics;
    pub mod canonicalizer;
    pub mod daily;
    pub mod document_parser;
    pub mod entity_enricher;
    pub mod entity_extractor;
    pub mod entity_resolver;
    pub mod health;
    pub mod hourly;
    pub mod instruction_reconciler;
    pub mod relation_extractor;
    pub mod session;
    pub mod tag_applier;
}

pub use automata::analytics::AnalyticsAutomatonNode;
pub use automata::canonicalizer::TerminalCommandCanonicalizerNode;
pub use automata::daily::DailySummarizerNode;
pub use automata::document_parser::DocumentParserNode;
pub use automata::document_parser::DocumentParserNodeAdapter;
pub use automata::entity_enricher::EntityEnricherNode;
pub use automata::entity_extractor::EntityExtractorNode;
pub use automata::entity_resolver::EntityResolverNode;
pub use automata::health::HealthAggregatorNode;
pub use automata::hourly::HourlySummarizerNode;
pub use automata::instruction_reconciler::InstructionExpectationReconcilerNode;
pub use automata::relation_extractor::RelationExtractorNode;
pub use automata::session::SessionDetectorNode;
pub use automata::tag_applier::TagApplierNode;
