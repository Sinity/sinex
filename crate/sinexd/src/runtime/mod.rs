//! Runtime substrate — dissolved from `sinex-node-sdk` into sinexd.
//!
//! The implementations live under `crate::node_sdk`. This module
//! provides a flat re-export surface preserving the old import paths
//! so call sites don't need to change.

pub use crate::node_sdk::content_store;
pub use crate::node_sdk::derived_node;
pub use crate::node_sdk::deterministic_event_id;
pub use crate::node_sdk::error_helpers;
pub use crate::node_sdk::heartbeat;
pub use crate::node_sdk::ingestor_node;
pub use crate::node_sdk::node_cli;
pub use crate::node_sdk::parser;
pub use crate::node_sdk::runtime::stream;
pub use crate::node_sdk::self_observation;
pub use crate::node_sdk::service_runtime;
pub use crate::node_sdk::shutdown;
pub use crate::node_sdk::systemd_notify;
pub use crate::node_sdk::wait_for_shutdown_signal_bool;
pub use crate::node_sdk::{SelfObservationError, SelfObserver, SelfObserverConfig};
