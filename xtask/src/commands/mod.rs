//! Command implementations for xtask.

// Components
pub mod analyze;
pub mod check;
pub mod ci;
pub mod completions;
pub mod coverage;
pub mod db;
pub mod dev;
pub mod fuzz;
pub mod history;
pub mod jobs;
pub mod lint;
pub mod lint_forbidden;
pub mod mutants;
pub mod qa;
pub mod schema;
pub mod stack;
pub mod test;
pub mod vm;

// Aggregates
pub use analyze::AnalyzeCommand;
pub use ci::CiCommand;
pub use completions::CompletionsCommand;
pub use db::DbCommand;
pub use dev::DevCommand;
pub use jobs::JobsCommand;
pub use qa::QaCommand;
pub use stack::StackCommand;
