//! The Sinex local daemon.
//!
//! `sinexd` is a single local process hosting the event engine (admission +
//! persistence + confirmation), the operator API (JSON-RPC + SSE +
//! native-messaging), the source-unit dispatch and drain machinery, and the
//! derived-node automata runtime. NATS remains the durable event-intent /
//! confirmation / DLQ / replay-control fabric — the collapse changes process
//! topology, not data flow.
//!
//! See issue #1054 for the architecture contract and the staged migration
//! from the prior four-binary arrangement.

pub mod api;
pub mod automata;
pub mod event_engine;
pub mod runtime;
pub mod sources;
pub mod supervisor;
