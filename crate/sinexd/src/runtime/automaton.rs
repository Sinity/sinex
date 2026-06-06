//! # Derived RuntimeModule Model Family
//!
//! Four explicit processing models:
//!
//! - [`Transducer`] — 1:1 event transform with deterministic `ts_orig` inheritance
//! - [`MultiOutputTransducer`] — 1:N event transform, each output with its own event type
//! - [`Windowed`] — accumulate events in a window, emit on completion
//! - [`ScopeReconciler`] — scope-keyed working set reconciliation
//!
//! All four share [`AutomatonContext`] and [`DerivedOutput`], which carry
//! the synthetic metadata required for replay-correct provenance chains.

mod adapter;
mod context;
pub mod histograms;
pub mod invalidation;
mod output;
pub mod traits;

pub use adapter::{
    AutomatonRuntime, MultiOutputTransducerAdapter, ScopeReconcilerAdapter, TransducerAdapter,
    WindowedAdapter,
};
pub use context::AutomatonContext;
pub use invalidation::{DerivedScopeInvalidation, INVALIDATION_SUBJECT};
pub use output::{DerivedAggregationMeta, DerivedOutput};
pub use traits::{
    Automaton, AutomatonAdapterConfig, InputProvenanceFilter, MultiOutputTransducer,
    MultiOutputTransducerWrapper, ScopeReconciler, ScopeReconcilerWrapper, Transducer,
    TransducerWrapper, Windowed, WindowedWrapper,
};
