//! The Sinex local daemon.
//!
//! `sinexd` is one process hosting the event engine (admission +
//! persistence + confirmation), the operator API (JSON-RPC + SSE +
//! native-messaging), source dispatch, and derived-node automata.
//! NATS remains the durable event-intent / confirmation / DLQ /
//! replay-control fabric.

pub mod api;
pub mod automata;
pub mod event_engine;
pub mod node_sdk;
pub mod sources;
pub mod supervisor;
