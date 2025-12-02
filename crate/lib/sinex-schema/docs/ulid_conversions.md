# ULID/UUID Conversion Helpers

Conversion functions between ULID and UUID types for database boundaries. These schema-level
utilities handle the interface between Rust ULID types and PostgreSQL UUID storage.

## Architecture Overview

Sinex uses ULIDs throughout the application for their time-ordering properties, but stores them as
UUIDs in PostgreSQL for compatibility with the `pgx_ulid` extension. This module provides efficient,
zero-copy conversion utilities to bridge that gap.

## Usage Patterns

### Basic Conversions

```rust
use sinex_schema::ulid::Ulid;
use sinex_schema::ulid_conversions::{ulid_to_uuid, uuid_to_ulid};

let ulid = Ulid::new();
let db_uuid = ulid_to_uuid(ulid);
let restored = uuid_to_ulid(db_uuid);
assert_eq!(ulid, restored);
```

### Extension Trait Usage

```rust
use sinex_schema::ulid::Ulid;
use sinex_schema::ulid_conversions::UlidExt;

let ulid = Ulid::new();
let db_uuid = ulid.to_db();

let ulids = vec![Ulid::new(), Ulid::new()];
let db_uuids = ulids.to_uuid_vec();
```

### Query Parameter Binding

```rust,ignore
sqlx::query!(
    "SELECT * FROM core.events WHERE id = $1",
    ulid.to_db()
);
```

## Performance Characteristics

- Zero-copy conversions – ULIDs and UUIDs share the same 16-byte representation.
- Batch operations – collection conversions are optimised for large datasets.
- Optional overhead – minimal overhead for `Option<T>` handling.

## Thread Safety

All conversion functions are pure and thread-safe; no shared state is maintained.
