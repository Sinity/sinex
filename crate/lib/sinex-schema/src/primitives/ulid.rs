//! ULID (Universally Unique Lexicographically Sortable Identifier) implementation.
//!
//! See `docs/ulid.md` for architectural decisions and design rationale.

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Mutex;
use thiserror::Error;
use time::OffsetDateTime;
use ulid::Ulid as InnerUlid;
use uuid::Uuid;

use super::timestamp::Timestamp;

#[derive(Error, Debug)]
pub enum UlidError {
    #[error("Invalid ULID format: {0}")]
    InvalidFormat(String),
    #[error("UUID conversion error: {0}")]
    UuidConversion(String),
}

/// Global monotonic ULID generator state
#[derive(Debug)]
struct MonotonicState {
    last_timestamp: u64,
    last_random: u128,
}

lazy_static! {
    static ref MONOTONIC_STATE: Mutex<MonotonicState> = Mutex::new(MonotonicState {
        last_timestamp: 0,
        last_random: 0,
    });
}

/// A wrapper around ULID that provides `PostgreSQL` compatibility via UUID.
///
/// Time-ordered, globally unique identifiers with `PostgreSQL` integration.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ulid(InnerUlid);

impl fmt::Debug for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ulid({self})")
    }
}

impl Ulid {
    /// Generate a new ULID with monotonic ordering guarantee.
    #[must_use]
    pub fn new() -> Self {
        use rand::Rng;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;

        let mut state = MONOTONIC_STATE.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("ULID monotonic state mutex was poisoned, recovering with fresh state");
            let mut recovered = poisoned.into_inner();
            recovered.last_timestamp = now_ms.saturating_sub(1);
            recovered.last_random = rand::thread_rng().gen::<u128>() & 0x3FFF_FFFF_FFFF_FFFF_FFFF;
            recovered
        });

        let random_part = match now_ms.cmp(&state.last_timestamp) {
            std::cmp::Ordering::Equal => {
                // Same millisecond: increment random component
                match state.last_random.checked_add(1) {
                    Some(next_random) if next_random <= 0x3FFF_FFFF_FFFF_FFFF_FFFF => {
                        state.last_random = next_random;
                        next_random
                    }
                    _ => {
                        // Overflow: advance to next millisecond
                        let next_ts = now_ms.saturating_add(1);
                        state.last_timestamp = next_ts;
                        let new_random =
                            rand::thread_rng().gen::<u128>() & 0x3FFF_FFFF_FFFF_FFFF_FFFF;
                        state.last_random = new_random;
                        new_random
                    }
                }
            }
            std::cmp::Ordering::Greater => {
                // New millisecond: fresh random component
                let mut rng = rand::thread_rng();
                let new_random = rng.gen::<u128>() & 0x3FFF_FFFF_FFFF_FFFF_FFFF;
                state.last_timestamp = now_ms;
                state.last_random = new_random;
                new_random
            }
            std::cmp::Ordering::Less => {
                // Clock regression: maintain monotonicity
                match state.last_random.checked_add(1) {
                    Some(next_random) if next_random <= 0x3FFF_FFFF_FFFF_FFFF_FFFF => {
                        state.last_random = next_random;
                        next_random
                    }
                    _ => {
                        let next_ts = state.last_timestamp.saturating_add(1);
                        state.last_timestamp = next_ts;
                        let new_random =
                            rand::thread_rng().gen::<u128>() & 0x3FFF_FFFF_FFFF_FFFF_FFFF;
                        state.last_random = new_random;
                        new_random
                    }
                }
            }
        };

        drop(state);

        // Build ULID bytes
        let timestamp_ms = std::cmp::min(now_ms, (1u64 << 48) - 1);
        let mut bytes = [0u8; 16];

        // Timestamp (first 6 bytes, big-endian)
        bytes[0] = (timestamp_ms >> 40) as u8;
        bytes[1] = (timestamp_ms >> 32) as u8;
        bytes[2] = (timestamp_ms >> 24) as u8;
        bytes[3] = (timestamp_ms >> 16) as u8;
        bytes[4] = (timestamp_ms >> 8) as u8;
        bytes[5] = timestamp_ms as u8;

        // Random component (last 10 bytes)
        let random_bytes = random_part.to_be_bytes();
        bytes[6..16].copy_from_slice(&random_bytes[6..16]);

        Self(InnerUlid::from_bytes(bytes))
    }

    /// Create from a timestamp
    #[must_use]
    pub fn from_datetime(datetime: Timestamp) -> Self {
        let timestamp_ms = (datetime.unix_timestamp_nanos() / 1_000_000) as u64;
        Self(InnerUlid::from_parts(timestamp_ms, rand::random()))
    }

    /// Get the timestamp component.
    ///
    /// ULIDs with timestamps beyond `OffsetDateTime`'s representable range
    /// (~year 9999) are clamped to the maximum representable value.
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        let timestamp_ms = self.0.timestamp_ms();
        let nanos = i128::from(timestamp_ms) * 1_000_000;
        match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
            Ok(dt) => Timestamp::new(dt),
            Err(_) => {
                // Timestamp exceeds OffsetDateTime range — clamp to max
                Timestamp::new(OffsetDateTime::new_utc(
                    time::Date::MAX,
                    time::Time::from_hms(23, 59, 59).unwrap_or(time::Time::MIDNIGHT),
                ))
            }
        }
    }

    /// Convert to UUID for `PostgreSQL` storage
    #[must_use]
    pub fn to_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.0.to_bytes())
    }

    /// Get as UUID for SQLX parameter binding
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.to_uuid()
    }

    /// Create from UUID
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(InnerUlid::from_bytes(*uuid.as_bytes()))
    }

    /// Get the inner ULID
    #[must_use]
    pub fn inner(&self) -> &InnerUlid {
        &self.0
    }

    /// Convert to bytes
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    /// Create from bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, UlidError> {
        Ok(Self(InnerUlid::from_bytes(bytes)))
    }

    /// Check if this is a nil/zero ULID
    #[must_use]
    pub fn is_nil(&self) -> bool {
        self.0.to_bytes().iter().all(|&b| b == 0)
    }

    /// Create a nil/zero ULID (all zeros)
    #[must_use]
    pub fn nil() -> Self {
        #[allow(clippy::expect_used)]
        Self::from_bytes([0; 16]).expect("nil ULID should always be valid")
    }
}

impl Default for Ulid {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Ulid {
    type Err = UlidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(UlidError::InvalidFormat("Empty string".to_string()));
        }

        if s.len() != 26 {
            return Err(UlidError::InvalidFormat(format!(
                "ULID must be exactly 26 characters, got {}",
                s.len()
            )));
        }

        // Validate Crockford's base32 characters
        for ch in s.chars() {
            match ch {
                '0'..='9'
                | 'A'..='H'
                | 'J'..='K'
                | 'M'..='N'
                | 'P'..='T'
                | 'V'..='Z'
                | 'a'..='h'
                | 'j'..='k'
                | 'm'..='n'
                | 'p'..='t'
                | 'v'..='z' => {}
                _ => {
                    return Err(UlidError::InvalidFormat(format!(
                        "ULID contains invalid base32 character: '{ch}'"
                    )));
                }
            }
        }

        let inner_ulid =
            InnerUlid::from_str(s).map_err(|e| UlidError::InvalidFormat(e.to_string()))?;

        // Validate timestamp range
        let timestamp_ms = inner_ulid.timestamp_ms();
        let max_timestamp = (1u64 << 48) - 1;

        if timestamp_ms > max_timestamp {
            return Err(UlidError::InvalidFormat(format!(
                "ULID timestamp {timestamp_ms} exceeds maximum allowed value {max_timestamp}"
            )));
        }

        Ok(Self(inner_ulid))
    }
}

impl From<InnerUlid> for Ulid {
    fn from(inner: InnerUlid) -> Self {
        Self(inner)
    }
}

impl From<Ulid> for InnerUlid {
    fn from(ulid: Ulid) -> Self {
        ulid.0
    }
}

impl From<Ulid> for Uuid {
    fn from(ulid: Ulid) -> Self {
        ulid.to_uuid()
    }
}

impl From<Uuid> for Ulid {
    fn from(uuid: Uuid) -> Self {
        Self::from_uuid(uuid)
    }
}

// SQLx support
#[cfg(feature = "sqlx")]
mod sqlx_impl {
    use super::{Ulid, Uuid};
    use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
    use sqlx::{Postgres, Type, TypeInfo};

    impl Type<Postgres> for Ulid {
        fn type_info() -> PgTypeInfo {
            <Uuid as Type<Postgres>>::type_info()
        }

        fn compatible(ty: &PgTypeInfo) -> bool {
            ty.name() == "ulid" || <Uuid as Type<Postgres>>::compatible(ty)
        }
    }

    impl PgHasArrayType for Ulid {
        fn array_type_info() -> PgTypeInfo {
            <Uuid as PgHasArrayType>::array_type_info()
        }
    }

    impl sqlx::Encode<'_, Postgres> for Ulid {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync + 'static>>
        {
            self.to_uuid().encode_by_ref(buf)
        }
    }

    impl sqlx::Decode<'_, Postgres> for Ulid {
        fn decode(
            value: PgValueRef<'_>,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
            let uuid = Uuid::decode(value)?;
            Ok(Self::from_uuid(uuid))
        }
    }
}

// Proptest/Arbitrary support
#[cfg(feature = "arbitrary")]
mod arbitrary_impl {
    use super::*;
    use proptest::prelude::*;

    impl Arbitrary for Ulid {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            (
                1577836800000u64..1893456000000u64, // 2020-2030
                any::<u128>(),
            )
                .prop_map(|(timestamp_ms, random_bits)| {
                    let random_component = random_bits & ((1u128 << 80) - 1);
                    let inner = InnerUlid::from_parts(timestamp_ms, random_component);
                    Ulid(inner)
                })
                .boxed()
        }
    }
}

// JSON Schema support
mod schema_impl {
    use super::Ulid;
    use schemars::{
        schema::{InstanceType, Schema, SchemaObject, StringValidation},
        JsonSchema,
    };

    impl JsonSchema for Ulid {
        fn schema_name() -> String {
            "Ulid".to_string()
        }

        fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> Schema {
            let mut schema = SchemaObject {
                instance_type: Some(InstanceType::String.into()),
                string: Some(Box::new(StringValidation {
                    pattern: Some("^[0-9A-HJKMNP-TV-Z]{26}$".to_string()),
                    min_length: Some(26),
                    max_length: Some(26),
                })),
                ..Default::default()
            };
            schema.metadata().description = Some(
                "A Universally Unique Lexicographically Sortable Identifier (ULID)".to_string(),
            );
            Schema::Object(schema)
        }
    }
}
