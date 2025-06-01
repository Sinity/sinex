use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgArgumentBuffer, Postgres};
use std::fmt;

/// ULID wrapper type for database compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ulid(ulid::Ulid);

impl Ulid {
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }

    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(ulid::Ulid::from_bytes(bytes))
    }

    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_bytes()
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
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

impl Serialize for Ulid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Ulid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ulid::Ulid::from_string(&s)
            .map(Ulid)
            .map_err(serde::de::Error::custom)
    }
}

// SQLx type implementations
impl sqlx::Type<Postgres> for Ulid {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("BYTEA")
    }
}

impl sqlx::Encode<'_, Postgres> for Ulid {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> sqlx::encode::IsNull {
        let bytes = self.to_bytes();
        <&[u8] as sqlx::Encode<Postgres>>::encode(&bytes[..], buf)
    }
}

impl sqlx::Decode<'_, Postgres> for Ulid {
    fn decode(value: sqlx::postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <Vec<u8> as sqlx::Decode<Postgres>>::decode(value)?;
        if bytes.len() != 16 {
            return Err("Invalid ULID byte length".into());
        }
        let mut array = [0u8; 16];
        array.copy_from_slice(&bytes);
        Ok(Ulid::from_bytes(array))
    }
}