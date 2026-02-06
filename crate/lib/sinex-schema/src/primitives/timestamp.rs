//! Timestamp wrapper around `time::OffsetDateTime` with additional trait implementations.

use serde::{Deserialize, Serialize};
use std::fmt;
use time::OffsetDateTime;

/// A wrapper around `OffsetDateTime` that implements necessary traits like `JsonSchema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(#[serde(with = "time::serde::rfc3339")] OffsetDateTime);

impl Timestamp {
    /// Returns the current time in UTC.
    #[must_use]
    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc())
    }

    /// Create from inner `OffsetDateTime`
    #[must_use]
    pub fn new(dt: OffsetDateTime) -> Self {
        Self(dt)
    }

    /// Get the inner `OffsetDateTime`
    #[must_use]
    pub fn inner(&self) -> OffsetDateTime {
        self.0
    }

    /// Create from Unix timestamp in seconds.
    pub fn from_unix_timestamp(secs: i64) -> Option<Self> {
        OffsetDateTime::from_unix_timestamp(secs).ok().map(Self)
    }

    /// Create from Unix timestamp in milliseconds.
    pub fn from_unix_timestamp_millis(ms: i64) -> Option<Self> {
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
            .ok()
            .map(Self)
    }

    /// Create from Unix timestamp in nanoseconds.
    pub fn from_unix_timestamp_nanos(ns: i128) -> Option<Self> {
        OffsetDateTime::from_unix_timestamp_nanos(ns).ok().map(Self)
    }

    /// Parse from an RFC3339 string.
    pub fn parse_rfc3339(s: &str) -> Result<Self, time::error::Parse> {
        use time::format_description::well_known::Rfc3339;
        OffsetDateTime::parse(s, &Rfc3339).map(Self)
    }

    /// Format as an RFC3339 string.
    #[must_use]
    pub fn format_rfc3339(&self) -> String {
        use time::format_description::well_known::Rfc3339;
        self.0
            .format(&Rfc3339)
            .unwrap_or_else(|_| "invalid_time".to_string())
    }

    /// Get the sub-microsecond component (0-999 nanoseconds).
    /// `PostgreSQL`'s timestamptz has microsecond precision; this captures the remaining resolution.
    #[must_use]
    pub fn subnano(&self) -> i32 {
        (self.0.nanosecond() % 1_000) as i32
    }

    /// Reconstruct a high-precision timestamp from a Postgres timestamp (microsecond precision)
    /// and a sub-microsecond nanosecond remainder (0-999).
    #[must_use]
    pub fn from_postgres_timestamp(base: OffsetDateTime, sub_nanos: i32) -> Self {
        let nanos = base.nanosecond();
        // Ensure the base doesn't already have the sub-nanos (if it came from a source that preserved them)
        // Checks if base is microsecond-aligned. If not, we trust base?
        // Actually, the contract is: base is what we got from DB (microsecond precision), sub_nanos is the extra.
        // We act defensively: take base truncated to micros, add sub_nanos.
        let micros = nanos / 1_000;
        let new_nanos = (micros * 1_000) + (sub_nanos as u32);

        // Safety: new_nanos will be < 2_000_000_000, replace only fails if out of range
        let dt = base.replace_nanosecond(new_nanos).unwrap_or(base);
        Self(dt)
    }

    /// Split into a Postgres-compatible timestamp (truncated to microseconds)
    /// and the sub-microsecond nanosecond remainder (0-999).
    ///
    /// This ensures that (`ts_db`, `sub_nano`) stored in the database can be perfectly
    /// reconstructed into the original nanosecond-precision timestamp.
    #[must_use]
    pub fn to_postgres_parts(&self) -> (OffsetDateTime, i32) {
        let full_nanos = self.0.nanosecond();
        let sub_nano = (full_nanos % 1_000) as i32;
        let truncated_nanos = (full_nanos / 1_000) * 1_000;

        let pg_ts = self.0.replace_nanosecond(truncated_nanos).unwrap_or(self.0);
        (pg_ts, sub_nano)
    }
}

impl From<OffsetDateTime> for Timestamp {
    fn from(dt: OffsetDateTime) -> Self {
        Self(dt)
    }
}

impl From<std::time::SystemTime> for Timestamp {
    fn from(st: std::time::SystemTime) -> Self {
        // Convert SystemTime to OffsetDateTime (fallible, but we use a fallback)
        let dt = OffsetDateTime::from(st);
        Self(dt)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Timestamp> for OffsetDateTime {
    fn from(t: Timestamp) -> Self {
        t.0
    }
}

impl std::ops::Sub<time::Duration> for Timestamp {
    type Output = Timestamp;
    fn sub(self, rhs: time::Duration) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl std::ops::Add<time::Duration> for Timestamp {
    type Output = Timestamp;
    fn add(self, rhs: time::Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl std::ops::Sub<Timestamp> for Timestamp {
    type Output = time::Duration;
    fn sub(self, rhs: Timestamp) -> Self::Output {
        self.0 - rhs.0
    }
}

impl std::ops::Deref for Timestamp {
    type Target = OffsetDateTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "json-schema")]
impl schemars::JsonSchema for Timestamp {
    fn schema_name() -> String {
        "DateTime".to_string()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = String::json_schema(gen).into_object();
        schema.metadata().description = Some("RFC 3339 formatted date-time string".to_string());
        schema.format = Some("date-time".to_string());
        schema.into()
    }
}

// SQLx support
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::{OffsetDateTime, Timestamp};
    use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
    use sqlx::{Decode, Encode, Postgres, Type};

    impl Type<Postgres> for Timestamp {
        fn type_info() -> PgTypeInfo {
            <OffsetDateTime as Type<Postgres>>::type_info()
        }
    }

    impl PgHasArrayType for Timestamp {
        fn array_type_info() -> PgTypeInfo {
            <OffsetDateTime as PgHasArrayType>::array_type_info()
        }
    }

    impl Encode<'_, Postgres> for Timestamp {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>>
        {
            self.0.encode_by_ref(buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for Timestamp {
        fn decode(
            value: PgValueRef<'r>,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
            let dt = <OffsetDateTime as Decode<Postgres>>::decode(value)?;
            Ok(Self(dt))
        }
    }
}
