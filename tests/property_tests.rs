//! Workspace-level property test harness.
//!
//! Property suites that still span multiple crates (automation flows, checkpoint
//! coordination, queue mechanics) live under `tests/property/`. Tests that focus
//! on a single crate have been relocated beside that crate.

mod property;

// The `#[sinex_test]` functions live inside the individual modules; simply
// including the module tree here makes them part of the workspace test target.
