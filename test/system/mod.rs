//! System-level tests for Sinex
//! 
//! These tests verify the complete system behavior including:
//! - End-to-end functionality
//! - External system integration
//! - Performance under load
//! - Regression testing

pub mod end_to_end;
pub mod external;
pub mod performance;
pub mod regression;