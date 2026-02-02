//! Sinex CLI library
//!
//! This library provides the core logic for the `sinexctl` command-line tool.
//! It handles RPC communication with the Sinex gateway and formatting of output.

// TODO: Enable strict clippy after cleanup
#![allow(clippy::all)]
#![allow(clippy::pedantic)]
#![allow(clippy::nursery)]

pub mod auth;
pub mod client;
pub mod commands;
pub mod config;
pub mod error;
pub mod fmt;
pub mod model;
pub mod util;
pub mod validation;

pub use client::GatewayClient;
pub use config::{default_rpc_url, Config};
pub use model::{NodeRole, OutputFormat};

/// Result type for CLI operations
pub type Result<T> = std::result::Result<T, color_eyre::Report>;
