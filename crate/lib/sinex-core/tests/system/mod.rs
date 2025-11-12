// # System Tests
//
// Complete system validation tests that verify end-to-end behavior, external integrations,
// performance characteristics, and system reliability under realistic conditions.
//
// ## Scope & Purpose
//
// **System tests verify:**
// - Complete end-to-end workflows
// - Integration with external systems (Git Annex, PostgreSQL, etc.)
// - System performance under realistic loads
// - Regression prevention for complex scenarios
// - System reliability and fault tolerance
// - Production-like deployment scenarios
//
// **System tests are:**
// - **Slow**: Often 10+ seconds per test
// - **Resource intensive**: May require significant CPU/memory/disk
// - **Comprehensive**: Test complete system behavior
// - **Realistic**: Use production-like data volumes and scenarios
//
// ## Test Categories
//
// ### 🌍 End-to-End (`end_to_end/`)
// Complete workflow validation from event ingestion to query results.
// Tests the entire pipeline: Satellite → Ingestd → Database → Automaton → Query.
//
// ### ⚡ Performance (`performance/`)
// System performance validation:
// - Load testing with realistic data volumes
// - Throughput and latency measurements
// - Resource usage profiling
// - Scaling behavior validation
//
// ### 🚪 Regression (`regression/`)
// Tests that prevent specific bugs from reoccurring:
// - Previously fixed issues
// - Complex interaction bugs
// - Performance regression detection
// - Configuration edge cases
//
// ### 🏗️ Reliability (`reliability/`)
// System behavior under adverse conditions:
// - Network partitions and reconnection
// - Disk full scenarios
// - High load sustained operation
// - Graceful degradation verification
//
// ### 💪 Stress (`stress/`)
// Extreme load and concurrency testing:
// - High concurrency scenarios
// - Resource exhaustion testing
// - Deadlock and race condition detection
// - System limits discovery
//
// ## Running System Tests
//
// ```bash
// cargo nextest run --test system                          # All system tests
// cargo nextest run --test system -E 'test(end_to_end::)'  # End-to-end only
// cargo nextest run --test system -E 'test(performance::)' # Performance only
// ```
//
// ## Performance Expectations
//
// - **Individual tests**: 10-300 seconds
// - **Full suite**: 10-30 minutes
// - **Resource usage**: Up to 4GB RAM, significant disk I/O
// - **External dependencies**: PostgreSQL, Git Annex, filesystem access
//
// ## Test Infrastructure
//
// System tests use dedicated test databases and may create temporary files.
// Tests are designed to be idempotent and clean up after themselves.

// === Complete System Validation ===

mod performance_test;
mod reliability_test;
mod stress_test;
mod temporal_chaos_test;
