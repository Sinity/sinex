# Making ULID Work Seamlessly with SQLX Compile-Time Macros

## The Problem

SQLX's compile-time macros (`query_as!`) don't understand PostgreSQL extension types like `ulid` from `pgx_ulid`. The current workaround requires:
1. Using `query!` instead of `query_as!`
2. Manually constructing structs
3. Converting between UUID and ULID types

## Solution Approaches

### 1. Type Override Syntax (Current Best Practice)

Use SQLX's type override syntax in queries:

```rust
// For query_as! - use type inference with underscore
let event = sqlx::query_as!(
    RawEvent,
    r#"
    SELECT 
        id::uuid as "id: _",
        source as "source!",
        event_type as "event_type!",
        ts_ingest as "ts_ingest!",
        ts_orig,
        host as "host!",
        ingestor_version,
        payload_schema_id::uuid as "payload_schema_id: _",
        payload as "payload!"
    FROM raw.events
    WHERE id = $1::uuid::ulid
    "#,
    ulid.to_uuid()
)
.fetch_one(pool)
.await?;
```

### 2. Transparent Type Wrapping

Make the Ulid type transparent to SQLX by implementing it as a newtype over UUID:

```rust
#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(transparent)]
pub struct Ulid(uuid::Uuid);

impl Ulid {
    pub fn new() -> Self {
        let ulid = ulid::Ulid::new();
        Self(uuid::Uuid::from_bytes(ulid.to_bytes()))
    }
    
    // ... other methods
}
```

### 3. Custom Type Registration

Register the ULID type with SQLX's type system:

```rust
impl Type<Postgres> for Ulid {
    fn type_info() -> PgTypeInfo {
        // Try to use UUID's type info since we encode/decode as UUID
        <Uuid as Type<Postgres>>::type_info()
    }
    
    fn compatible(ty: &PgTypeInfo) -> bool {
        // Accept both ulid and uuid types
        ty.name() == "ulid" || <Uuid as Type<Postgres>>::compatible(ty)
    }
}
```

### 4. Database Views Approach

Create database views that cast ULID to UUID automatically:

```sql
CREATE VIEW raw.events_uuid AS
SELECT 
    id::uuid as id,
    source,
    event_type,
    ts_ingest,
    ts_orig,
    host,
    ingestor_version,
    payload_schema_id::uuid as payload_schema_id,
    payload
FROM raw.events;
```

Then use `query_as!` with the view.

### 5. SQLX Offline Mode Configuration

Configure SQLX to understand the custom type in offline mode:

1. Create `.sqlx/query-*.json` files with proper type mappings
2. Use `cargo sqlx prepare` to generate offline query data
3. Ensure the type mappings are consistent

## Recommended Implementation

The best approach combines several techniques:

1. **Keep the current Ulid implementation** but enhance it
2. **Use type overrides** in queries for compile-time checking
3. **Create helper functions** to reduce boilerplate

Here's the enhanced implementation:

```rust
// In sinex-ulid/src/lib.rs
impl Ulid {
    /// Helper for SQLX queries - returns UUID for binding
    pub fn as_uuid(&self) -> Uuid {
        self.to_uuid()
    }
}

// In sinex-db/src/queries.rs
/// Helper macro for ULID field selection
macro_rules! select_ulid {
    ($field:ident) => {
        concat!(stringify!($field), "::uuid as \"", stringify!($field), ": _\"")
    };
}

// Usage in queries
pub async fn get_event_by_id(pool: &PgPool, id: Ulid) -> Result<RawEvent> {
    sqlx::query_as!(
        RawEvent,
        r#"
        SELECT 
            id::uuid as "id: _",
            source as "source!",
            event_type as "event_type!",
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!",
            ingestor_version,
            payload_schema_id::uuid as "payload_schema_id: _",
            payload as "payload!"
        FROM raw.events
        WHERE id = $1::uuid::ulid
        "#,
        id.as_uuid()
    )
    .fetch_one(pool)
    .await
}
```

## Type Casting Strategy

Always follow this pattern:
- **Rust → PostgreSQL**: Pass `ulid.to_uuid()` and cast with `$1::uuid::ulid`
- **PostgreSQL → Rust**: Select with `id::uuid as "id: _"` for automatic type inference

## Benefits

1. **Compile-time checking**: `query_as!` validates queries at compile time
2. **Type safety**: Automatic conversions between ULID and UUID
3. **Clean API**: Methods work directly with `Ulid` type
4. **Performance**: No runtime overhead, conversions are cheap

## Future Improvements

1. **Custom SQLX type resolver**: Write a custom type resolver for SQLX that understands ULID
2. **PostgreSQL domain type**: Create a domain type that wraps UUID but is recognized as ULID
3. **SQLX plugin**: Contribute ULID support directly to SQLX

## Testing

Always test ULID operations with compile-time macros:

```rust
#[sqlx::test]
async fn test_ulid_with_query_as_macro(pool: PgPool) {
    let ulid = Ulid::new();
    
    // Insert with query!
    sqlx::query!(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::uuid::ulid, $2, $3, $4, $5)",
        ulid.as_uuid(),
        "test",
        "test",
        "test",
        serde_json::json!({})
    )
    .execute(&pool)
    .await
    .unwrap();
    
    // Select with query_as!
    let event = sqlx::query_as!(
        RawEvent,
        r#"
        SELECT 
            id::uuid as "id: _",
            source as "source!",
            event_type as "event_type!",
            ts_ingest as "ts_ingest!",
            ts_orig,
            host as "host!",
            ingestor_version,
            payload_schema_id::uuid as "payload_schema_id: _",
            payload as "payload!"
        FROM raw.events
        WHERE id = $1::uuid::ulid
        "#,
        ulid.as_uuid()
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(event.id, ulid);
}
```