# Database Conversion Utilities

## Overview

This module provides utilities for converting between ULID types and PostgreSQL-compatible UUID types. These conversions are essential for database operations since PostgreSQL stores ULIDs as UUIDs.

## Core Conversion Functions

### `ulid_to_uuid(ulid: Ulid) -> SqlxUuid`

Convert ULID to PostgreSQL UUID type. This is the core conversion function for preparing ULIDs for database storage.

**Performance**: Zero-copy operation - the same 16 bytes are simply reinterpreted as a different type.

### `uuid_to_ulid(uuid: SqlxUuid) -> Ulid`

Convert PostgreSQL UUID back to ULID for use in application logic.

**Important**: This function assumes the UUID was originally a ULID. Converting arbitrary UUIDs to ULIDs will work technically, but the resulting ULID may not have valid timestamp or monotonic properties.

### `uuid_to_ulid_safe(uuid: SqlxUuid) -> Result<Ulid, String>`

Validates that the UUID follows ULID format constraints before conversion. Use this when you need to ensure the UUID was originally a valid ULID.

**Validation**:

- Checks timestamp is within reasonable range (2010-2100)
- Ensures 48-bit timestamp component is valid

## Convenience Aliases

```rust
pub use ulid_to_uuid as to_db;
pub use uuid_to_ulid as from_db;
pub use uuid_to_ulid_safe as from_db_safe;
```

## Extension Traits

### `UlidExt`

Provides fluent API for ULID conversions:

```rust
let ulid = Ulid::new();
let db_uuid = ulid.to_db();

// For optional values
let maybe_uuid = Ulid::to_db_opt(Some(ulid));
```

### `DbUuidExt`

Provides conversion from database UUIDs:

```rust
let ulid = db_uuid.to_ulid();
```

### `UlidArrayExt`

Handles collections of ULIDs:

```rust
let ulids = vec![Ulid::new(), Ulid::new()];
let uuids = ulids.to_uuid_vec();
```

### `DbUuidCollectionExt`

Handles collections of database UUIDs:

```rust
let uuids: Vec<SqlxUuid> = /* from database */;
let ulids = uuids.to_ulid_vec();
```

## Optional Helpers

```rust
pub fn opt_to_db(ulid: Option<Ulid>) -> Option<SqlxUuid>
pub fn opt_from_db(uuid: Option<SqlxUuid>) -> Option<Ulid>
pub fn opt_vec_to_db(ulids: Option<Vec<Ulid>>) -> Option<Vec<SqlxUuid>>
pub fn opt_vec_from_db(uuids: Option<Vec<SqlxUuid>>) -> Option<Vec<Ulid>>
```

## Usage Examples

### Basic Conversion

```rust
use sinex_schema::primitives::{Ulid, ulid_to_uuid, uuid_to_ulid};

let ulid = Ulid::new();
let db_uuid = ulid_to_uuid(ulid);
let restored = uuid_to_ulid(db_uuid);
assert_eq!(ulid, restored);
```

### With Extension Traits

```rust
use sinex_schema::primitives::{Ulid, UlidExt};

let ulid = Ulid::new();
let db_uuid = ulid.to_db();
```

### Safe Conversion

```rust
use sinex_schema::primitives::{Ulid, uuid_to_ulid_safe};

match uuid_to_ulid_safe(db_uuid) {
    Ok(ulid) => println!("Valid ULID: {}", ulid),
    Err(e) => eprintln!("Invalid ULID format: {}", e),
}
```
