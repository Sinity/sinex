//! Derived-node automata.
//!
//! Implementations are registered at program load via
//! `register_source_unit!` in each submodule. The
//! [`SourceUnitDescriptor`](sinex_primitives::proof::SourceUnitDescriptor)
//! catalog discovers them through `inventory`.

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

pub use analytics::AnalyticsAutomatonNode;
pub use canonicalizer::TerminalCommandCanonicalizerNode;
pub use daily::DailySummarizerNode;
pub use document_parser::{DocumentParserNode, DocumentParserNodeAdapter};
pub use entity_enricher::EntityEnricherNode;
pub use entity_extractor::EntityExtractorNode;
pub use entity_resolver::EntityResolverNode;
pub use health::HealthAggregatorNode;
pub use hourly::HourlySummarizerNode;
pub use instruction_reconciler::InstructionExpectationReconcilerNode;
pub use relation_extractor::RelationExtractorNode;
pub use session::SessionDetectorNode;
pub use tag_applier::TagApplierNode;
