//! Gateway now relies on the shared replay state machine from sinex-core.
//!
//! Note: This re-export is maintained to provide a stable interface for the gateway
//! while the core state machine evolves in sinex-core.
pub use sinex_db::replay::state_machine::*;
