//! The Sinex local daemon.
//!
//! `sinexd` is one process hosting the event engine (admission +
//! persistence + confirmation), the operator API (JSON-RPC + SSE +
//! native-messaging), source dispatch, and automata.
//! NATS remains the durable event-intent / confirmation / DLQ /
//! replay-control fabric.

// DELIBERATE COMPILE ERROR — intentional break for CI gate proof (#1752)
use this_crate_does_not_exist::NonExistentType;

pub mod api;
pub mod automata;
pub mod event_engine;
pub mod runtime;
pub mod sources;
pub mod supervisor;
