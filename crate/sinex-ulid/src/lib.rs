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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ulid(InnerUlid);

impl fmt::Debug for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ulid({})", self.to_string())
    }
}

impl Ulid {
    /// Generate a new ULID
    pub fn new() -> Self {
        Self(InnerUlid::new())
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
    
    /// Get as UUID for SQLX parameter binding
    /// This is an alias for to_uuid() but makes the intent clearer in queries
    pub fn as_uuid(&self) -> Uuid {
        self.to_uuid()
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
        // Validate basic requirements before delegating to inner ULID
        if s.is_empty() {
            return Err(UlidError::InvalidFormat("Empty string".to_string()));
        }
        
        if s.len() != 26 {
            return Err(UlidError::InvalidFormat(format!(
                "ULID must be exactly 26 characters, got {}", s.len()
            )));
        }
        
        // Check for valid Crockford's base32 characters only (0-9, A-Z except I, L, O, U)
        // This implementation allows both upper and lower case, but no I, L, O, U
        for ch in s.chars() {
            match ch {
                '0'..='9' | 'A'..='H' | 'J'..='K' | 'M'..='N' | 'P'..='T' | 'V'..='Z' |
                'a'..='h' | 'j'..='k' | 'm'..='n' | 'p'..='t' | 'v'..='z' => {
                    // Valid characters
                }
                _ => {
                    return Err(UlidError::InvalidFormat(format!(
                        "ULID contains invalid base32 character: '{}'", ch
                    )));
                }
            }
        }
        
        // Try to parse first to validate basic format, then check timestamp range
        let inner_ulid = InnerUlid::from_str(s)
            .map_err(|e| UlidError::InvalidFormat(e.to_string()))?;
        
        // Validate timestamp range (ULID timestamp is 48 bits, max value is 2^48 - 1 ms)
        // This corresponds to the year 10895 CE, which is reasonable to restrict
        let timestamp_ms = inner_ulid.timestamp_ms();
        let max_timestamp = (1u64 << 48) - 1; // 2^48 - 1 = 281474976710655
        
        if timestamp_ms > max_timestamp {
            return Err(UlidError::InvalidFormat(format!(
                "ULID timestamp {} exceeds maximum allowed value {}", 
                timestamp_ms, max_timestamp
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