//! Security regression suites (path validation, Annex hardening, etc.).
//!
//! The real tests live under `tests/security/`. This file wires them into a
//! dedicated integration binary so they actually compile and run.

#![cfg(test)]

#[path = "security/path_validation_test.rs"]
mod path_validation_test;
