# Sinex Test Suite

Comprehensive test suite organized by scope, complexity, and resource requirements.
Each category serves a specific testing purpose and has different runtime characteristics.

## Test Organization

### 🏃‍♂️ Unit Tests (`unit_tests.rs`)
- **Scope**: Individual functions and components in isolation
- **Speed**: Fast (< 1s per test)
- **Dependencies**: Minimal external dependencies
- **Purpose**: Verify correctness of individual components
- **Features**: Generic ID system, ULID properties, domain types, event builders
- **Run**: `cargo test unit_tests`

### 🔗 Integration Tests (`integration_tests.rs`)
- **Scope**: Component interactions within the system
- **Speed**: Medium (1-10s per test)
- **Dependencies**: Database, some external services
- **Purpose**: Verify components work together correctly
- **Features**: Database operations, repository pattern, schema validation, concurrent operations
- **Run**: `cargo test integration_tests`

### 🎯 Property Tests (`property_tests.rs`)
- **Scope**: Behavior validation across input ranges
- **Speed**: Variable (depends on iterations)
- **Dependencies**: Proptest framework
- **Purpose**: Verify properties hold across many inputs
- **Features**: Randomized testing, edge case discovery, invariant validation
- **Run**: `cargo test property_tests`

### ⚡ Simple Tests (`simple_tests.rs`)
- **Scope**: Basic functionality verification
- **Speed**: Very fast (< 100ms per test)
- **Dependencies**: Minimal
- **Purpose**: Smoke tests and basic validation
- **Run**: `cargo test simple_tests`

## Specialized Test Categories

### 🌍 System Tests (`system/`)
- **Scope**: Complete system validation
- **Speed**: Slow (10s+ per test)
- **Dependencies**: Full system, external services
- **Purpose**: End-to-end system behavior validation
- **Run**: `cargo test --test system`

### ⚔️ Adversarial Tests (`adversarial/`)
- **Scope**: Edge cases, attacks, stress scenarios
- **Speed**: Variable (often slow due to stress testing)
- **Dependencies**: Full system setup
- **Purpose**: System robustness under hostile conditions
- **Run**: `cargo test --test adversarial`

### 🚀 Performance Tests (`performance/`)
- **Scope**: Performance characteristics and benchmarks
- **Speed**: Variable (can be slow for stress tests)
- **Dependencies**: Full system, performance monitoring
- **Purpose**: Performance regression detection and optimization
- **Run**: `cargo test --test performance`

### 🔒 Security Tests (`security/`)
- **Scope**: Security-focused test scenarios
- **Speed**: Medium to slow
- **Dependencies**: Full system setup
- **Purpose**: Security validation and vulnerability testing
- **Run**: `cargo test --test security`

### 🔄 Concurrency Tests (`concurrency/`)
- **Scope**: Multi-threaded and async behavior validation
- **Speed**: Medium to slow
- **Dependencies**: Full system, thread management
- **Purpose**: Race condition and synchronization testing
- **Run**: `cargo test --test concurrency`

### 📚 Examples (`examples/`)
- **Scope**: Code examples and usage demonstrations
- **Purpose**: Documentation and API usage examples
- **Run**: Files in this directory demonstrate modern test patterns

### 🛠️ Scripts (`scripts/`)
- **Scope**: Test automation and utilities
- **Purpose**: Test conversion scripts and utilities

### 💻 VM Tests (`nixos-vm/`)
- **Scope**: Full NixOS deployment testing
- **Speed**: Very slow (5-15min)
- **Dependencies**: NixOS, VM infrastructure
- **Purpose**: Complete system deployment validation

## Test Infrastructure

All tests use the unified `#[sinex_test]` infrastructure providing:
- **Automatic Database Setup**: Shared pool with transaction isolation
- **Standard Error Handling**: `color_eyre::eyre::Result<()>`
- **Modern Test Stack**: rstest, insta, tracing-test, similar-asserts
- **Test Context**: `TestContext` parameter for consistent resource access
- **Cleanup**: Automatic transaction rollback for perfect test isolation

## Quick Reference

```rust
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn my_test(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let pool = ctx.pool().clone();
    // Test implementation
    Ok(())
}
```

## Running Tests

```bash
# All tests
cargo test

# Specific test files
cargo test unit_tests
cargo test integration_tests
cargo test property_tests
cargo test simple_tests

# Specific test categories
cargo test --test performance
cargo test --test adversarial
cargo test --test security
cargo test --test system
cargo test --test concurrency

# Using just commands
just test              # Unit + property tests (~30s)
just test-all         # Complete test suite
just test-integration # Integration tests only
just test-performance # Performance tests only
```

## Performance Expectations

- **Unit Tests**: 1-5 seconds total
- **Integration Tests**: 30 seconds - 2 minutes
- **Property Tests**: 1-2 minutes
- **Performance Tests**: 2-10 minutes
- **System Tests**: 5-15 minutes
- **VM Tests**: 15-30 minutes

## Test Development Guidelines

1. **Use appropriate test category**: Choose the right test file/directory for your test scope
2. **Follow naming conventions**: Use descriptive test names that explain what's being tested
3. **Use modern infrastructure**: Leverage `#[sinex_test]`, rstest, and insta where appropriate
4. **Maintain isolation**: Tests should not depend on each other
5. **Performance awareness**: Keep unit tests fast, use integration tests for database operations
6. **Documentation**: Include docstrings for complex test scenarios