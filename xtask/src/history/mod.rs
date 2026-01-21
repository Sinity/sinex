//! Persistent build/test history stored in SQLite.
//!
//! Provides queryable history of xtask invocations, test results, and build diagnostics.

mod db;
mod tests;

pub use db::{CommandStats, HistoryDb, Invocation, InvocationStatus};
pub use tests::{parse_nextest_output, TestResult, TestStatus};
