//! Sinex CLI library
//!
//! This library provides the core logic for the `sinexctl` command-line tool.
//! It handles RPC communication with the Sinex gateway and formatting of output.

pub mod auth;
pub mod client;
pub mod commands;
pub mod fmt;
pub mod model;

pub use client::GatewayClient;
pub use model::{NodeRole, OutputFormat};

/// Result type for CLI operations
pub type Result<T> = std::result::Result<T, color_eyre::Report>;
