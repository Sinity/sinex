//! Common test utilities for sinex-cli tests
//!
//! Provides mock client infrastructure and test helpers.

#![allow(unused_imports)]

pub mod fixtures;
pub mod mock_client;

pub use fixtures::{ConfigFixture, TestDir, TlsFixture, TokenFixture};
pub use mock_client::{MockGatewayClient, MockResponse};
