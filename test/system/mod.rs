//! System-level tests for Sinex
//! 
//! These tests verify the complete system behavior including:
//! - End-to-end functionality
//! - External system integration
//! - Performance under load
//! - Regression testing
//! - Infrastructure deployment
//! - System reliability

pub mod end_to_end;
pub mod external;
pub mod performance;
pub mod regression;
// pub mod nixos_vm; // TODO: Re-enable when nixos_vm tests are available
pub mod stress;
// pub mod reliability; // TODO: Re-enable when reliability tests are available