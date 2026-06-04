//! Schema-drift detection — re-exported from `sinex-primitives`.
//!
//! The canonical types live in `sinex_primitives::parser::fingerprint`. This
//! module exists so that `sinexd::node_sdk::parser::SourceRecordFingerprint` and
//! friends still resolve without every call site being updated.

pub use sinex_primitives::parser::fingerprint::*;
