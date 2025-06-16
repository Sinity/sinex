//! Integration tests for Sinex components
//! 
//! These tests verify that different components work together correctly
//! without testing the entire system end-to-end.

pub mod database;
pub mod collector; 
pub mod worker;
pub mod event_sources;