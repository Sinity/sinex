//! Production-path obligation tests for media capture packages (#1043).
//!
//! These cases exercise accepted staged media parser modes through the shared
//! source host obligation harness rather than only parser-local unit tests.

#[cfg(test)]
#[path = "media_test.rs"]
mod tests;
