//! Failure mode tests for Sinex
//!
//! These tests verify system behavior under various failure conditions:
//! - Resource exhaustion (memory, connections, disk)
//! - Network issues (timeouts, disconnections)
//! - Component failures (crashes, hangs)
//! - Performance degradation
//!
//! Each test is designed to verify graceful degradation and recovery.
//!
//! Failure mode tests have been consolidated into ../failure_modes_test.rs