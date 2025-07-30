//! Verification module for preflight and integration testing
//!
//! This module provides types and queries for system verification,
//! integration testing, and preflight checks.

use serde_json::Value as JsonValue;
use sqlx::FromRow;

/// Record type for event ID results
#[derive(Debug, FromRow)]
pub struct EventIdRecord {
    pub id: sqlx::types::Uuid,
}

/// Record type for test event results
#[derive(Debug, FromRow)]
pub struct TestEventRecord {
    pub id: sqlx::types::Uuid,
    pub source: String,
    pub event_type: String,
    pub payload: JsonValue,
}

/// Record type for count results
#[derive(Debug, FromRow)]
pub struct CountRecord {
    pub count: Option<i64>,
}

/// Record type for checkpoint ID results
#[derive(Debug, FromRow)]
pub struct CheckpointIdRecord {
    pub id: sqlx::types::Uuid,
}
