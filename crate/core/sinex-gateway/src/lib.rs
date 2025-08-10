//! Sinex Host Library
//!
//! Provides the service container and related functionality for the Sinex Host

// Expose modules for testing and external use
pub mod handlers;
pub mod rpc_server;
pub mod service_container;

// Replay system modules
pub mod cascade_analyzer;
pub mod replay_state_machine;
