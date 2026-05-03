//! sinex-process — consolidated automata pack.
//!
//! This crate hosts all six derived-node automata in a single binary.
//! The binary is dispatched by `--automaton <name>` to the appropriate
//! [`sinex_node_sdk::node_entrypoint`] equivalent.
//!
//! # Hosted automata
//!
//! | Name | Selector | Node type |
//! |------|----------|-----------|
//! | `canonicalizer` | `--automaton canonicalizer` | [`TransducerNode`] |
//! | `analytics` | `--automaton analytics` | [`WindowedNode`] |
//! | `health` | `--automaton health` | [`ScopeReconcilerNode`] |
//! | `session` | `--automaton session` | [`WindowedNode`] |
//! | `hourly` | `--automaton hourly` | [`WindowedNode`] |
//! | `daily` | `--automaton daily` | [`WindowedNode`] |
//!
//! All six [`SourceUnitDescriptor`](sinex_primitives::source_unit::SourceUnitDescriptor)s are
//! registered at program load via the `register_source_unit!` macro in each submodule.

pub mod automata {
    pub mod analytics;
    pub mod canonicalizer;
    pub mod daily;
    pub mod health;
    pub mod hourly;
    pub mod session;
}

pub use automata::analytics::AnalyticsAutomatonNode;
pub use automata::canonicalizer::TerminalCommandCanonicalizerNode;
pub use automata::daily::DailySummarizerNode;
pub use automata::health::HealthAggregatorNode;
pub use automata::hourly::HourlySummarizerNode;
pub use automata::session::SessionDetectorNode;
