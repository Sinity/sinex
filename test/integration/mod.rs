//! Integration tests for Sinex components
//! 
//! These tests verify that different components work together correctly
//! without testing the entire system end-to-end.

pub mod database;
pub mod collector; 
pub mod worker;
pub mod event_sources;
pub mod failure_modes;

// Query interface tests
pub mod query_interface_test;

// Phase 7-9 comprehensive integration tests
pub mod full_system_startup_test;
pub mod failure_recovery_integration_test;
pub mod health_monitoring_integration_test;
pub mod git_annex_full_integration_test;
pub mod configuration_validation_integration_test;