// # Unit Tests
//
// Fast, isolated tests that verify individual components without external dependencies.
// Unit tests focus on correctness of individual functions, data structures, and algorithms.
//
// ## Scope & Purpose
//
// **Unit tests verify:**
// - Individual function correctness
// - Data structure behavior
// - Algorithm implementation
// - Error handling in isolation
// - Edge cases and boundary conditions
//
// **Unit tests are:**
// - **Fast**: < 1 second per test
// - **Isolated**: No external dependencies (database, filesystem, network)
// - **Deterministic**: Same input always produces same output
// - **Focused**: Test one thing at a time
//
// ## Test Organization
//
// Unit tests are organized by the Rust crate they test, mirroring the `crate/` directory structure:
//
// ### 🎨 Core (`core/`)
// Tests for core types and utilities:
// - Event processing and validation
// - Event type system and builders
// - Core data structures and utilities
// - Configuration parsing and validation
//
// ### �� Database (`db/`)
// Tests for `sinex-db` crate:
// - Database model correctness
// - Query builder functionality
// - Connection pool behavior
// - Migration utilities
//
// ### 🏧 ULID (`ulid/`)
// Tests for `sinex-ulid` crate:
// - ULID generation and parsing
// - UUID conversion correctness
// - Ordering and comparison behavior
// - Concurrent generation safety
//
// ### 📊 Model (`model/`)
// Tests for data model structures:
// - Event serialization/deserialization
// - Data validation rules
// - Schema compatibility
// - Type conversions
//
// ### 📥 Ingestor (`ingestor/`)
// Tests for event ingestion logic:
// - Event parsing and validation
// - Batch processing algorithms
// - Error handling and retry logic
// - Rate limiting and backpressure
//
// ## Running Unit Tests
//
// ```bash
// cargo test --test unit           # All unit tests
// cargo test --test unit::core     # Core crate only
// cargo test --test unit::db       # Database crate only
// just test-unit                   # Via just command
// ```
//
// ## Performance Expectations
//
// - **Individual tests**: < 1 second
// - **Full suite**: 1-5 minutes
// - **Resource usage**: Minimal (< 100MB RAM)
// - **Dependencies**: None (pure computation)
//
// ## Test Infrastructure
//
// Unit tests use minimal infrastructure and avoid the full `#[sinex_test]` macro
// when external resources aren't needed. Use standard `#[test]` for pure unit tests.

use sinex_test_utils::prelude::*;

// === Consolidated Unit Tests ===

/// Tests for core types and utilities
pub mod core_test;

/// Tests for API layer functionality
pub mod api_test;

/// Tests for event type system (replaces EventRegistry tests)
pub mod event_type_system_test;

/// Consolidated database unit tests (includes db, model, ingestor, preflight)
pub mod database_test;

/// Tests for preflight verification
pub mod preflight_test;

/// ULID comprehensive tests
pub mod ulid_test;

/// Tests for typed clipboard events
pub mod typed_clipboard_test;

/// Comprehensive error path testing for production code
pub mod error_paths_test;

// Infrastructure tests are in test/common/ directory
