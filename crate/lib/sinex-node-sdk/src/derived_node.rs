//! # Derived Node Model Family
//!
//! Four explicit processing models:
//!
//! - [`TransducerNode`] — 1:1 event transform with deterministic `ts_orig` inheritance
//! - [`MultiOutputTransducerNode`] — 1:N event transform, each output with its own event type
//! - [`WindowedNode`] — accumulate events in a window, emit on completion
//! - [`ScopeReconcilerNode`] — scope-keyed working set reconciliation
//!
//! All four share [`DerivedTriggerContext`] and [`DerivedOutput`], which carry
//! the synthetic metadata required for replay-correct provenance chains.

mod adapter;
mod context;
pub mod histograms;
pub mod invalidation;
mod output;
pub mod traits;

pub use adapter::{
    DerivedNodeAdapter, MultiOutputTransducerNodeAdapter, ScopeReconcilerNodeAdapter,
    TransducerNodeAdapter, WindowedNodeAdapter,
};
pub use context::DerivedTriggerContext;
pub use invalidation::{DerivedScopeInvalidation, INVALIDATION_SUBJECT};
pub use output::{DerivedAggregationMeta, DerivedOutput};
pub use traits::{
    DerivedNodeConfig, DerivedNodeImpl, InputProvenanceFilter, MultiOutputTransducerNode,
    MultiOutputTransducerWrapper, ScopeReconcilerNode, ScopeReconcilerWrapper, TransducerNode,
    TransducerWrapper, WindowedNode, WindowedWrapper,
};
