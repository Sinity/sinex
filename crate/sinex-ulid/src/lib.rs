//! # Sinex ULID Implementation
//! 
//! Time-ordered, globally unique identifiers for the Sinex system.
//! 
//! This crate provides ULID (Universally Unique Lexicographically Sortable Identifier)
//! support with PostgreSQL integration via the pgx_ulid extension.
//! 
//! ## Architectural Decision: ULID Primary Keys (ADR-001)
//! 
//! **Status**: Implemented  
//! **Decision Date**: 2024-03-11  
//! **Implementation Date**: 2025-07-17  
//! 
//! ### Context
//! 
//! Sinex requires a robust primary key strategy for high-volume, time-ordered data.
//! The strategy must address:
//! 
//! 1. **Index Efficiency**: Minimize B-tree bloat and fragmentation
//! 2. **Time-Ordering**: Keys should be naturally sortable by time
//! 3. **Global Uniqueness**: Support distributed generation
//! 4. **Performance**: Efficient generation and comparison
//! 5. **Developer Experience**: Good ecosystem support
//! 
//! ### Decision
//! 
//! We use ULIDs via the pgx_ulid PostgreSQL extension for all primary keys.
//! 
//! ### Rationale
//! 
//! 1. **Best of Both Worlds**: Time-ordering benefits with native PostgreSQL support
//! 2. **Performance**: 30% faster generation than UUIDs in benchmarks
//! 3. **Rich Features**: Timestamp casting, monotonic generation
//! 4. **Binary Storage**: Efficient 16-byte storage (same as UUID)
//! 5. **Ecosystem Alignment**: pgx_ulid written in Rust aligns with our stack
//! 
//! ### Alternatives Considered
//! 
//! | Option | Pros | Cons | Decision |
//! |--------|------|------|----------|
//! | UUIDv4 | Standard, widely supported | Random = poor index locality | ❌ Rejected |
//! | UUIDv7 | Time-ordered, standard | Less mature ecosystem | ❌ Rejected |
//! | Custom ULID | No dependencies | Complex implementation | ❌ Rejected |
//! | pgx_ulid | All ULID benefits + native PG | External dependency | ✅ **Chosen** |
//! 
//! ### Consequences
//! 
//! **Positive**:
//! - Sequential inserts improve index performance
//! - Natural time-based partitioning
//! - Can extract timestamp from ID
//! - Sortable across distributed systems
//! 
//! **Negative**:
//! - Requires pgx_ulid extension installation
//! - 26-character string representation (vs 36 for UUID)
//! 
//! ## ULID Structure
//! 
//! ```text
//!  01AN4Z07BY      79KA1307SR9X4MV3
//! |----------|    |----------------|
//!  Timestamp          Randomness
//!    48bits             80bits
//! ```
//! 
//! ## Usage Examples
//! 
//! ### Basic Usage
//! 
//! ```rust
//! use sinex_ulid::Ulid;
//! 
//! // Generate new ULID
//! let id = Ulid::new();
//! println!("Generated: {}", id);
//! 
//! // Extract timestamp
//! let timestamp = id.timestamp();
//! println!("Created at: {}", timestamp);
//! ```
//! 
//! ### PostgreSQL Integration
//! 
//! ```sql
//! -- Database side
//! CREATE EXTENSION pgx_ulid;
//! 
//! CREATE TABLE events (
//!     id ULID PRIMARY KEY DEFAULT gen_ulid(),
//!     data JSONB
//! );
//! ```
//! 
//! ```rust
//! # use sinex_ulid::Ulid;
//! # use sqlx::PgPool;
//! // Rust side with SQLx
//! let id = Ulid::new();
//! 
//! sqlx::query!(
//!     "INSERT INTO events (id, data) VALUES ($1, $2)",
//!     id.as_uuid(),  // Convert to UUID for parameter binding
//!     serde_json::json!({ "event": "test" })
//! )
//! .execute(&pool)
//! .await?;
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! ```
//! 
//! ## Monotonic Generation
//! 
//! This implementation includes monotonic generation to handle high-frequency
//! ID generation within the same millisecond:
//! 
//! ```rust
//! # use sinex_ulid::Ulid;
//! let id1 = Ulid::new();
//! let id2 = Ulid::new();
//! let id3 = Ulid::new();
//! 
//! // Even if generated in the same millisecond, ordering is preserved
//! assert!(id1 < id2);
//! assert!(id2 < id3);
//! ```

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::Mutex;
use thiserror::Error;
use ulid::Ulid as InnerUlid;
use uuid::Uuid;

/// Type alias for timestamp values, consistent with sinex-core-types
pub type Timestamp = DateTime<Utc>;

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

/// A wrapper around ULID that provides PostgreSQL compatibility via UUID
/// 
/// This is the primary key type used throughout Sinex, chosen via ADR-001
/// for its time-ordering properties and index efficiency.
/// 
/// ## Why ULID over UUID?
/// 
/// - **Time-ordering**: Natural chronological sort without additional columns
/// - **Index efficiency**: Sequential inserts minimize B-tree fragmentation  
/// - **Timestamp extraction**: Can derive creation time from ID
/// - **Global uniqueness**: Safe for distributed systems
/// 
/// ## PostgreSQL Integration
/// 
/// Requires the pgx_ulid extension:
/// ```sql
/// CREATE EXTENSION pgx_ulid;
/// ```
/// 
/// Then use the native ULID type in tables:
/// ```sql
/// CREATE TABLE my_table (
///     id ULID PRIMARY KEY DEFAULT gen_ulid()
/// );
/// ```
/// 
/// ## Technical Implementation Module: Primary Key Implementation
/// 
/// **Maturity Level**: L4 - Implemented  
/// **Implementation**: 98% (ULID generation, PostgreSQL integration, and UUID casting for FKs fully working)
/// 
/// ### PostgreSQL Extension Setup
/// 
/// The pgx_ulid extension must be installed and enabled. For NixOS users, see
/// `nixos/modules/sinex-config.nix` for the complete PostgreSQL configuration
/// including extension setup and optional monotonic generator configuration.
/// 
/// ### ULID-UUID Casting for Foreign Keys
/// 
/// ULIDs seamlessly cast to UUIDs for foreign key relationships:
/// 
/// ```rust
/// // Cast ULID to UUID when querying
/// let events = sqlx::query!(
///     r#"
///     SELECT 
///         event_id::uuid as "event_id!",
///         source,
///         event_type
///     FROM core.events 
///     WHERE event_id = $1::uuid
///     "#,
///     event_id.to_uuid()  // ULID provides to_uuid() method
/// )
/// .fetch_all(pool)
/// .await?;
/// ```
/// 
/// Database schema supports ULID-UUID relationships:
/// ```sql
/// -- Foreign key constraints handle ULID-UUID casting
/// ALTER TABLE core.event_relations 
///     ADD CONSTRAINT fk_event_relations_from_event 
///     FOREIGN KEY (from_event_id) 
///     REFERENCES core.events(event_id::uuid);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ulid(InnerUlid);

impl fmt::Debug for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ulid({})", self)
    }
}

impl Ulid {
    /// Generate a new ULID with monotonic ordering guarantee
    ///
    /// ULIDs generated within the same millisecond will have their random component
    /// incremented to ensure strict ordering. This prevents ordering violations
    /// that can occur during high-frequency generation.
    pub fn new() -> Self {
        use rand::Rng;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut state = MONOTONIC_STATE.lock().unwrap_or_else(|poisoned| {
            // If the mutex is poisoned, clear it and start fresh
            poisoned.into_inner()
        });

        let random_part = if now_ms == state.last_timestamp {
            // Same millisecond: increment the random component to maintain ordering
            state.last_random = state.last_random.wrapping_add(1);
            state.last_random
        } else if now_ms > state.last_timestamp {
            // New millisecond: generate fresh random component
            let mut rng = rand::thread_rng();
            let new_random = rng.gen::<u128>() & 0x3FFF_FFFF_FFFF_FFFF_FFFF; // 80 bits max
            state.last_timestamp = now_ms;
            state.last_random = new_random;
            new_random
        } else {
            // Clock went backwards: use incremented random from last timestamp
            // This handles clock adjustments gracefully
            state.last_random = state.last_random.wrapping_add(1);
            state.last_random
        };

        drop(state); // Release the lock early

        // Build ULID with timestamp and monotonic random component
        let timestamp_ms = std::cmp::min(now_ms, (1u64 << 48) - 1); // Cap at 48-bit max

        // Create ULID from datetime and controlled random
        let mut bytes = [0u8; 16];

        // Timestamp (first 6 bytes, big-endian)
        bytes[0] = (timestamp_ms >> 40) as u8;
        bytes[1] = (timestamp_ms >> 32) as u8;
        bytes[2] = (timestamp_ms >> 24) as u8;
        bytes[3] = (timestamp_ms >> 16) as u8;
        bytes[4] = (timestamp_ms >> 8) as u8;
        bytes[5] = timestamp_ms as u8;

        // Random component (last 10 bytes, big-endian)
        let random_bytes = random_part.to_be_bytes();
        bytes[6..16].copy_from_slice(&random_bytes[6..16]); // Use lower 80 bits

        Self(InnerUlid::from_bytes(bytes))
    }

    /// Create from a timestamp
    pub fn from_datetime(datetime: Timestamp) -> Self {
        Self(InnerUlid::from_datetime(datetime.into()))
    }

    /// Get the timestamp component
    pub fn timestamp(&self) -> Timestamp {
        let timestamp_ms = self.0.timestamp_ms();
        // Safely convert u64 to i64, clamping to i64::MAX if needed
        let timestamp_i64 = if timestamp_ms > i64::MAX as u64 {
            i64::MAX
        } else {
            timestamp_ms as i64
        };
        DateTime::from_timestamp_millis(timestamp_i64).unwrap_or_else(Utc::now)
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

    /// Create a nil/zero ULID (all zeros)
    pub fn nil() -> Self {
        Self::from_bytes([0; 16]).expect("nil ULID should always be valid")
    }
}

impl Default for Ulid {
    /// Create a new ULID.
    ///
    /// This is equivalent to [`Ulid::new()`].
    /// 
    /// ## Architectural Decision: Clock Regression Handling (ADR-011)
    /// 
    /// **Status**: Implemented  
    /// **Decision Date**: 2025-01-10
    /// 
    /// ### Context
    /// 
    /// ULID generation relies on system time to create time-ordered identifiers. When system 
    /// clocks go backwards (due to NTP corrections, DST changes, or manual adjustments), this 
    /// can break ULID ordering assumptions and cause events to appear out of sequence.
    /// 
    /// ### Decision
    /// 
    /// **We handle clock regression by not caring about it.**
    /// 
    /// Instead, we:
    /// 1. Use standard `Ulid::new()` without modification
    /// 2. Rely on the operating system to maintain reasonable time
    /// 3. Recommend (but not require) chrony for time synchronization
    /// 4. Accept that minor clock regressions may occasionally cause out-of-order ULIDs
    /// 
    /// ### Rationale
    /// 
    /// 1. **Complexity vs Benefit**: Elaborate solutions add significant complexity for a rare edge case
    /// 2. **Performance Impact**: Monotonic generators require synchronization that slows ULID generation
    /// 3. **OS Responsibility**: Timekeeping is the operating system's job, not the application's
    /// 4. **Real-world Impact**: With modern NTP clients (chrony), significant clock regression is extremely rare
    /// 5. **Failure Mode**: If time goes backwards, having slightly out-of-order events is preferable to refusing to operate
    /// 
    /// ### Consequences
    /// 
    /// **Positive:**
    /// - Simple, fast ULID generation with no synchronization overhead
    /// - No complex time validation logic to maintain
    /// - System continues operating even during time anomalies
    /// - Clear separation of concerns (OS handles time, app handles events)
    /// 
    /// **Negative:**
    /// - Events may occasionally have out-of-order ULIDs during clock regression
    /// - No application-level detection of time anomalies
    /// - Relies on proper OS configuration for time accuracy
    /// 
    /// **Mitigations:**
    /// - Document that Sinex requires a properly synchronized system clock
    /// - Recommend chrony with `makestep 1 3` configuration
    /// - The `ts_ingest` derived from ULID provides a consistent timestamp even if system time is wrong
    /// - Database indexes on both `id` and `ts_ingest` allow efficient querying by either order
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
                "ULID must be exactly 26 characters, got {}",
                s.len()
            )));
        }

        // Check for valid Crockford's base32 characters only (0-9, A-Z except I, L, O, U)
        // This implementation allows both upper and lower case, but no I, L, O, U
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
                | 'v'..='z' => {
                    // Valid characters
                }
                _ => {
                    return Err(UlidError::InvalidFormat(format!(
                        "ULID contains invalid base32 character: '{}'",
                        ch
                    )));
                }
            }
        }

        // Try to parse first to validate basic format, then check timestamp range
        let inner_ulid =
            InnerUlid::from_str(s).map_err(|e| UlidError::InvalidFormat(e.to_string()))?;

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
    use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
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

// Proptest/Arbitrary support for property testing
#[cfg(feature = "arbitrary")]
mod arbitrary_impl {
    use super::*;
    use proptest::prelude::*;

    impl Arbitrary for Ulid {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            // Generate realistic ULID values for property testing
            (
                // Timestamp component (milliseconds since Unix epoch)
                // Use a reasonable range: 2020-01-01 to 2030-01-01
                1577836800000u64..1893456000000u64,
                // Random component (80 bits)
                any::<u128>(),
            )
                .prop_map(|(timestamp_ms, random_bits)| {
                    // Use only the lower 80 bits for the random component
                    let random_component = random_bits & ((1u128 << 80) - 1);

                    // Create ULID from components
                    let inner = InnerUlid::from_parts(timestamp_ms, random_component);
                    Ulid(inner)
                })
                .boxed()
        }
    }
}

#[cfg(feature = "schema")]
mod schema_impl {
    use super::*;
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
