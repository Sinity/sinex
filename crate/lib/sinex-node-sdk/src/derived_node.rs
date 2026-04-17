//! # Derived Node Model Family
//!
//! Three explicit processing models:
//!
//! - [`TransducerNode`] — 1:1 event transform with deterministic `ts_orig` inheritance
//! - [`WindowedNode`] — accumulate events in a window, emit on completion
//! - [`ScopeReconcilerNode`] — scope-keyed working set reconciliation
//!
//! All three share [`DerivedTriggerContext`] and [`DerivedOutput`], which carry
//! the synthetic metadata required for replay-correct provenance chains.

mod adapter;
mod context;
pub mod invalidation;
mod output;
pub mod traits;

pub use adapter::{
    DerivedNodeAdapter, ScopeReconcilerNodeAdapter, TransducerNodeAdapter, WindowedNodeAdapter,
};
pub use context::DerivedTriggerContext;
pub use invalidation::{DerivedScopeInvalidation, INVALIDATION_SUBJECT};
pub use output::DerivedOutput;
pub use traits::{
    DerivedNodeConfig, DerivedNodeImpl, InputProvenanceFilter, ScopeReconcilerNode,
    ScopeReconcilerWrapper, TransducerNode, TransducerWrapper, WindowedNode, WindowedWrapper,
};
