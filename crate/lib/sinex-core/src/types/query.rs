use crate::types::error::SinexError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Shared limit/offset helper that clamps inputs and centralizes defaults.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pagination {
    limit: i64,
    offset: i64,
}

impl Pagination {
    /// Default limit applied when callers omit or pass invalid input.
    pub const DEFAULT_LIMIT: i64 = 100;
    /// Global maximum limit enforced across services unless overridden.
    pub const MAX_LIMIT: i64 = 1000;

    /// Construct pagination using standard defaults.
    pub fn new(limit: Option<i64>, offset: Option<i64>) -> Self {
        Self::with_bounds(limit, offset, Self::DEFAULT_LIMIT, Self::MAX_LIMIT)
    }

    /// Construct pagination with a custom default limit but global max.
    pub fn with_default(limit: Option<i64>, offset: Option<i64>, default_limit: i64) -> Self {
        Self::with_bounds(limit, offset, default_limit, Self::MAX_LIMIT)
    }

    /// Construct pagination with fully custom defaults and max limit.
    pub fn with_bounds(
        limit: Option<i64>,
        offset: Option<i64>,
        default_limit: i64,
        max_limit: i64,
    ) -> Self {
        assert!(
            default_limit > 0,
            "default pagination limit must be positive"
        );
        assert!(
            max_limit >= default_limit,
            "max pagination limit must be >= default limit"
        );

        let limit = limit.unwrap_or(default_limit);
        let limit = if limit <= 0 { default_limit } else { limit };
        let limit = limit.min(max_limit);

        let offset = offset.unwrap_or(0);
        let offset = offset.max(0);

        Self { limit, offset }
    }

    /// Apply a tighter max limit post-construction (useful for endpoints with stricter caps).
    pub fn clamp_max(self, max_limit: i64) -> Self {
        assert!(max_limit > 0, "max pagination limit must be positive");
        let limit = self.limit.min(max_limit);
        Self {
            limit,
            offset: self.offset,
        }
    }

    pub fn limit(&self) -> i64 {
        self.limit
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn as_tuple(&self) -> (i64, i64) {
        (self.limit, self.offset)
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            limit: Self::DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

/// Helper for validating optional `(start, end)` timestamps.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeRange {
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
}

impl TimeRange {
    pub fn new(
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Self, SinexError> {
        if let (Some(start), Some(end)) = (start, end) {
            if start > end {
                return Err(
                    SinexError::validation("start_time must be earlier than end_time")
                        .with_context("start_time", start)
                        .with_context("end_time", end),
                );
            }
        }

        Ok(Self { start, end })
    }

    pub fn start(&self) -> Option<DateTime<Utc>> {
        self.start
    }

    pub fn end(&self) -> Option<DateTime<Utc>> {
        self.end
    }

    pub fn contains(&self, ts: DateTime<Utc>) -> bool {
        if let Some(start) = self.start {
            if ts < start {
                return false;
            }
        }
        if let Some(end) = self.end {
            if ts > end {
                return false;
            }
        }
        true
    }
}
