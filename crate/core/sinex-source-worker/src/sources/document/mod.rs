//! Document source units.
//!
//! `staging.rs` carries the descriptor, binding, and parser dispatch.
//! `node.rs` carries the imperative `DocumentNode` runtime (moved verbatim
//! from the legacy `sinex-document-ingestor` crate during the Wave-B fold).

pub mod node;
pub mod staging;
