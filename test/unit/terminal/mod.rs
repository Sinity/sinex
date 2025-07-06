//! # Terminal Event Source Unit Tests
//!
//! Tests for terminal-related event sources in the `sinex-events-terminal` crate.
//!
//! ## Test Coverage
//!
//! ### Kitty Terminal Integration (`kitty_integration_test.rs`)
//! - Event source initialization and configuration
//! - Event type registration and constants
//! - Payload serialization/deserialization
//! - New event types: tab lifecycle, process changes, config changes
//!
//! ### Scrollback Chunking (`scrollback_chunking_test.rs`)  
//! - FastCDC chunking for large scrollback content
//! - Chunking threshold configuration
//! - Small vs large content handling
//! - Chunk reconstruction and data integrity
//! - Performance implications of chunking
//!
//! ## Test Organization
//!
//! - **Fast unit tests**: Pure logic without external dependencies
//! - **Configuration validation**: Ensure configs serialize properly
//! - **Data integrity**: Verify chunking preserves content
//! - **Edge cases**: Threshold boundaries, malformed data

pub mod kitty_integration_test;
pub mod scrollback_chunking_test;