//! Command implementations for xtask.
//!
//! This module contains all xtask command implementations following
//! the `XtaskCommand` trait pattern.
//!
//! # Organization
//!
//! Commands are organized into separate files by category:
//! - Simple commands: check, lint, completions
//! - Subcommand groups: db, schema, tls, deps, graph, bench, history
//! - Complex commands: test, ci, doctor
//!
//! # Adding New Commands
//!
//! To add a new command:
//! 1. Create a new file in this directory (e.g., `mycommand.rs`)
//! 2. Implement the `XtaskCommand` trait
//! 3. Add the module declaration here
//! 4. Update the dispatch function in main.rs
//!
//! # Example
//!
//! ```no_run
//! // mycommand.rs
//! use xtask::command::{XtaskCommand, CommandContext, CommandResult};
//! use anyhow::Result;
//!
//! pub struct MyCommand {
//!     pub verbose: bool,
//! }
//!
//! impl XtaskCommand for MyCommand {
//!     fn name(&self) -> &str {
//!         "mycommand"
//!     }
//!
//!     fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
//!         Ok(CommandResult::success())
//!     }
//! }
//! ```

// Command modules - incrementally extracted from main.rs
pub mod check;
pub mod ci;
pub mod ci_preflight;
pub mod completions;
pub mod coverage;
pub mod db;
pub mod dev;
pub mod doctor;
pub mod fuzz;
pub mod history;
pub mod jobs;
pub mod lint;
pub mod lint_forbidden;
pub mod logs;
pub mod mutants;
pub mod schema;
pub mod sqlx;
pub mod status;
pub mod test;
pub mod up;

// Re-export command structs for convenience
pub use check::CheckCommand;
pub use ci::{CiCommand, CiSubcommand};
pub use ci_preflight::CiPreflightCommand;
pub use completions::CompletionsCommand;
pub use coverage::{CoverageCommand, CoverageSubcommand};
pub use db::{DbCommand, DbSubcommand};
pub use dev::{DevCommand, DevSubcommand};
pub use doctor::DoctorCommand;
pub use fuzz::{FuzzCommand, FuzzSubcommand};
pub use history::{HistoryCommand, HistorySubcommand, HistoryTestsSubcommand};
pub use jobs::{JobsCommand, JobsSubcommand};
pub use lint::LintCommand;
pub use lint_forbidden::LintForbiddenCommand;
pub use logs::LogsCommand;
pub use mutants::MutantsCommand;
pub use schema::{SchemaCommand, SchemaSubcommand};
pub use sqlx::{SqlxCommand, SqlxSubcommand};
pub use status::StatusCommand;
pub use test::TestCommand;
pub use up::UpCommand;
