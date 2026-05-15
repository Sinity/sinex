//! Sinex CLI library
//!
//! This library provides the core logic for the `sinexctl` command-line tool.
//! It handles RPC communication with the Sinex gateway and formatting of output.

pub mod admin;
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

pub use admin::AdminCommands;
pub use client::GatewayClient;
pub use color_eyre::Result;
pub use config::{Config, default_rpc_url};
pub use model::format_registry::{
    registry as format_registry, render_format_matrix_terminal, validate_format,
};
pub use model::{FormatCapability, NodeRole, OutputFormat};
