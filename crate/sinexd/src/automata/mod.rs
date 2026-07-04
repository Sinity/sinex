//! Automata hosted inside `sinexd`.
//!
//! Implementations are registered at program load via
//! `register_source_contract!` in each submodule. The
//! [`SourceContract`](sinex_primitives::source_contracts::SourceContract)
//! catalog discovers them through `inventory`.

pub mod analytics;
pub mod attention;
pub mod canonicalizer;
pub mod daily;
pub mod document_parser;
pub mod embedding_producer;
pub mod entity_enricher;
pub mod entity_extractor;
pub mod entity_resolver;
pub mod health;
pub mod hourly;
pub mod instruction_reconciler;
pub mod registry;
pub mod relation_extractor;
pub mod session;
pub mod tag_applier;

pub use analytics::AnalyticsAutomatonRuntime;
pub use attention::AttentionStreamRuntime;
pub use canonicalizer::TerminalCommandCanonicalizerRuntime;
pub use daily::DailySummarizerRuntime;
pub use document_parser::{DocumentParserAutomaton, DocumentParserRuntime};
pub use embedding_producer::EmbeddingProducerRuntime;
pub use entity_enricher::EntityEnricherRuntime;
pub use entity_extractor::EntityExtractorRuntime;
pub use entity_resolver::EntityResolverRuntime;
pub use health::HealthAggregatorRuntime;
pub use hourly::HourlySummarizerRuntime;
pub use instruction_reconciler::InstructionExpectationReconcilerRuntime;
pub use relation_extractor::RelationExtractorRuntime;
pub use session::SessionDetectorRuntime;
pub use tag_applier::TagApplierRuntime;
