use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid as InnerUlid;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum UlidError {
    #[error("Invalid ULID format: {0}")]
    InvalidFormat(String),
    #[error("UUID conversion error: {0}")]
    UuidConversion(String),
}

pub type Error = UlidError;

/// A wrapper around ULID that provides PostgreSQL compatibility via UUID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ulid(InnerUlid);

impl Ulid {
    /// Generate a new ULID
    pub fn new() -> Self {
        Self(InnerUlid::new())
    }

    /// Generate a monotonic ULID (guaranteed to be greater than the previous if called in same millisecond)
    pub fn new_monotonic(previous: Option<&Self>) -> Self {
        match previous {
            Some(prev) => {
                let prev_inner = prev.0;
                let now = InnerUlid::new();
                if now <= prev_inner {
                    // If new ULID would be <= previous, increment the random part
                    let mut bytes = prev_inner.to_bytes();
                    // Increment the random part (last 10 bytes)
                    for i in (6..16).rev() {
                        if bytes[i] < 255 {
                            bytes[i] += 1;
                            break;
                        } else {
                            bytes[i] = 0;
                        }
                    }
                    Self(InnerUlid::from_bytes(bytes))
                } else {
                    Self(now)
                }
            }
            None => Self::new(),
        }
    }

    /// Create from a timestamp
    pub fn from_datetime(datetime: DateTime<Utc>) -> Self {
        Self(InnerUlid::from_datetime(datetime.into()))
    }

    /// Get the timestamp component
    pub fn timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(self.0.timestamp_ms() as i64)
            .unwrap_or_else(|| Utc::now())
    }

    /// Convert to UUID for PostgreSQL storage
    pub fn to_uuid(&self) -> Uuid {
        Uuid::from_bytes(self.0.to_bytes())
    }

    /// Create from UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(InnerUlid::from_bytes(*uuid.as_bytes()))
    }

    /// Get the inner ULID
    pub fn inner(&self) -> &InnerUlid {
        &self.0
    }

    /// Convert to bytes
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    /// Create from bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, UlidError> {
        Ok(Self(InnerUlid::from_bytes(bytes)))
    }

    /// Check if this is a nil/zero ULID
    pub fn is_nil(&self) -> bool {
        self.0.to_bytes().iter().all(|&b| b == 0)
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
        InnerUlid::from_str(s)
            .map(Self)
            .map_err(|e| UlidError::InvalidFormat(e.to_string()))
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
    use super::*;
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef, PgHasArrayType};
    use sqlx::{Postgres, Type, TypeInfo};

    impl Type<Postgres> for Ulid {
        fn type_info() -> PgTypeInfo {
            // Register as the ULID type from PostgreSQL
            PgTypeInfo::with_name("ulid")
        }
        
        fn compatible(ty: &PgTypeInfo) -> bool {
            // ULID is compatible with both ulid and uuid types
            ty.name() == "ulid" || ty.name() == "uuid"
        }
    }

    impl PgHasArrayType for Ulid {
        fn array_type_info() -> PgTypeInfo {
            PgTypeInfo::with_name("_ulid")
        }
    }

    impl sqlx::Encode<'_, Postgres> for Ulid {
        fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> sqlx::encode::IsNull {
            self.to_uuid().encode_by_ref(buf)
        }
    }

    impl sqlx::Decode<'_, Postgres> for Ulid {
        fn decode(value: PgValueRef<'_>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let uuid = Uuid::decode(value)?;
            Ok(Self::from_uuid(uuid))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_ulid_creation() {
        let ulid1 = Ulid::new();
        let ulid2 = Ulid::new();
        assert_ne!(ulid1, ulid2);
    }

    #[test]
    fn test_monotonic_ulid() {
        let ulid1 = Ulid::new();
        let ulid2 = Ulid::new_monotonic(Some(&ulid1));
        assert!(ulid2 > ulid1);
    }

    #[test]
    fn test_uuid_conversion() {
        let ulid = Ulid::new();
        let uuid = ulid.to_uuid();
        let ulid2 = Ulid::from_uuid(uuid);
        assert_eq!(ulid, ulid2);
    }

    proptest! {
        #[test]
        fn test_ulid_string_roundtrip(s in "[0-9A-Z]{26}") {
            if let Ok(ulid) = Ulid::from_str(&s) {
                let s2 = ulid.to_string();
                let ulid2 = Ulid::from_str(&s2).unwrap();
                prop_assert_eq!(ulid, ulid2);
            }
        }
    }
}