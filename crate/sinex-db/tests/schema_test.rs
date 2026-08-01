//! Top-level nextest entry point for `tests/schema/`.
//!
//! Cargo only auto-discovers `tests/*.rs` files directly under `tests/` as
//! integration-test binary targets; nested directories such as
//! `tests/schema/` are never compiled unless explicitly wired in via
//! `#[path]` mod includes from a top-level file. `schema/schema_tests.rs`
//! and `schema/validation_tests.rs` already wire their own sibling files
//! this way, but nothing wired *them* (or their standalone neighbors) into
//! a top-level target, so the entire `tests/schema/` directory silently
//! never compiled or ran (sinex-xjx8).
//!
//! `#[path]` resolution is relative to the directory of the file containing
//! the attribute, so each nested module's own further `#[path]` includes
//! (e.g. `schema_tests.rs` -> `schema_tests_constraint_tests.rs`) keep
//! resolving correctly from `tests/schema/` once wired in from here.

#[path = "schema/schema_tests.rs"]
mod schema_tests;

#[path = "schema/migration_chain_test.rs"]
mod migration_chain_test;

#[path = "schema/strict_diff_test.rs"]
mod strict_diff_test;

#[path = "schema/validation_tests.rs"]
mod validation_tests;

#[path = "schema/schema_registry_test.rs"]
mod schema_registry_test;
