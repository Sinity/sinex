// # Sinex Test Suite
//
// Comprehensive test suite organized by scope, complexity, and resource requirements.
// Each category serves a specific testing purpose and has different runtime characteristics.
//
// ## Test Categories
//
// ### 🏃‍♂️ Unit Tests (`unit/`)
// - **Scope**: Individual functions and components in isolation
// - **Speed**: Fast (< 1s per test)
// - **Dependencies**: Minimal external dependencies
// - **Purpose**: Verify correctness of individual components
// - **Run**: `cargo test --test unit`
//
// ### 🔗 Integration Tests (`integration/`)
// - **Scope**: Component interactions within the system
// - **Speed**: Medium (1-10s per test)
// - **Dependencies**: Database, some external services
// - **Purpose**: Verify components work together correctly
// - **Run**: `cargo test --test integration`
//
// ### 🌍 System Tests (`system/`)
// - **Scope**: Complete system validation
// - **Speed**: Slow (10s+ per test)
// - **Dependencies**: Full system, external services
// - **Purpose**: End-to-end system behavior validation
// - **Run**: `cargo test --test system`
//
// ### 🎯 Property Tests (`property/`)
// - **Scope**: Behavior validation across input ranges
// - **Speed**: Variable (depends on iterations)
// - **Dependencies**: Proptest framework
// - **Purpose**: Verify properties hold across many inputs
// - **Run**: `cargo test --test property`
//
// ### ⚔️ Adversarial Tests (`adversarial/`)
// - **Scope**: Edge cases, attacks, stress scenarios
// - **Speed**: Variable (often slow due to stress testing)
// - **Dependencies**: Full system setup
// - **Purpose**: System robustness under hostile conditions
// - **Run**: `cargo test --test adversarial`
//
// ## Test Infrastructure
//
// All tests use the unified `#[sinex_test]` infrastructure providing:
// - **Automatic Database Setup**: Shared pool with transaction isolation
// - **Standard Error Handling**: `TestResult`
// - **Timing Utilities**: Deterministic waits instead of arbitrary sleeps
// - **Test Context**: `TestContext` parameter for consistent resource access
// - **Cleanup**: Automatic transaction rollback for perfect test isolation
//
// ## Quick Reference
//
// ```rust
// use crate::common::prelude::*;
//
// #[sinex_test]
// async fn my_test(ctx: TestContext) -> TestResult {
//     let pool = ctx.pool();
//     // Test implementation
//     Ok(())
// }
// ```

use crate::common::prelude::*;

// Common test infrastructure (always available)
mod common;

// Test categories organized by scope and resource requirements
#[cfg(test)]
mod unit;

#[cfg(test)]
mod integration;

#[cfg(test)]
mod system;

#[cfg(test)]
mod property;

#[cfg(test)]
mod adversarial;
