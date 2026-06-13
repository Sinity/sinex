//! The Sinex local daemon.
//!
//! `sinexd` is one process hosting the event engine (admission +
//! persistence + confirmation), the operator API (JSON-RPC + SSE +
//! native-messaging), source dispatch, and automata.
//! NATS remains the durable event-intent / confirmation / DLQ /
//! replay-control fabric.

use sinexd_nonexistent_crate::BrokenType; // DELIBERATE_BREAK_FOR_CI_PROOF
pub mod api;
pub mod automata;
pub mod event_engine;
pub mod runtime;
pub mod sources;
pub mod supervisor;
