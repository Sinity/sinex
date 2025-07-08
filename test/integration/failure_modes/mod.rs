//! Failure mode tests for Sinex
//!
//! These tests verify system behavior under various failure conditions:
//! - Resource exhaustion (memory, connections, disk)
//! - Network issues (timeouts, disconnections)
//! - Component failures (crashes, hangs)
//! - Performance degradation
//!
//! Each test is designed to verify graceful degradation and recovery.

pub mod channel_backpressure_test;
pub mod config_reload_test;
pub mod connection_pool_test;
pub mod database_failures_test;
pub mod filesystem_failures_test;
pub mod network_timeout_test;
pub mod performance_degradation_test;
pub mod worker_orphan_test;
