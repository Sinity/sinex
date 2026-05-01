//! Sinex CLI library
//!
//! This library provides the core logic for the `sinexctl` command-line tool.
//! It handles RPC communication with the Sinex gateway and formatting of output.

// CLI application code — allow unwrap/expect (errors surface to the user, not a library)
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod auth;
pub mod client;
pub mod commands;
pub mod config;
pub mod error;
pub mod fmt;
pub mod model;
pub mod parse;
pub mod prompt;
pub mod validation;

pub use client::GatewayClient;
pub use color_eyre::Result;
pub use config::{Config, default_rpc_url};
pub use model::format_registry::{
    registry as format_registry, render_format_matrix_terminal, validate_format,
};
pub use model::{FormatCapability, NodeRole, OutputFormat};
