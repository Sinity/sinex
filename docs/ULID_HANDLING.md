# ULID Handling in SQLx

See also: `docs/architecture/Core_Architecture.md` and `docs/architecture/SCHEMA.md` for system‑wide database and schema context.

## Overview
The Sinex project uses ULID (Universally Unique Lexicographically Sortable Identifier) as the primary key type throughout the database. ULID provides time-ordered, lexicographically sortable identifiers that are superior to UUIDs for our use case.

## How SQLx Handles ULID
While we use the `Ulid` type in Rust, SQLx transmits ULID data to/from PostgreSQL as UUID bytes:
- **Encoding**: `Ulid::encode_by_ref()` converts to UUID before sending to PostgreSQL
- **Decoding**: PostgreSQL sends UUID bytes which are converted back to Ulid
- **In PostgreSQL**: The pgx_ulid extension interprets these UUID bytes as ULID values

## The Problem
SQLx's compile-time query checking (`sqlx::query!`) doesn't handle 2D arrays (arrays of arrays) properly, which causes issues when inserting multiple events with array fields like `source_event_ids` and `associated_blob_ids`.

## The Solution
We use PostgreSQL's `ulid_from_uuid()` function to convert UUID values to ULID at the database level:

1. **Simple fields**: Convert using `ulid_from_uuid($1)` for single ULID values
2. **Array fields**: Use `ARRAY(SELECT ulid_from_uuid(elem) FROM unnest($1::uuid[]) AS elem)` to convert UUID arrays to ULID arrays
3. **Batch inserts**: Instead of using UNNEST with 2D arrays (which SQLx doesn't support), we insert events one-by-one within a transaction

## Implementation

```rust
// Convert single UUID to ULID in SQL
"INSERT INTO table (id) VALUES (ulid_from_uuid($1))"

// Convert UUID array to ULID array in SQL
"CASE WHEN $1::uuid[] IS NOT NULL 
     THEN ARRAY(SELECT ulid_from_uuid(elem) FROM unnest($1::uuid[]) AS elem)
     ELSE NULL 
END"
```

## Key Functions
- `gen_ulid()`: Generate a new ULID
- `ulid_from_uuid(uuid)`: Convert UUID to ULID
- `ulid_to_uuid(ulid)`: Convert ULID to UUID (rarely needed)

## Important Notes
1. Always use `sqlx::query!` (compile-time checked) instead of `sqlx::query` (runtime)
2. Pass UUID values from Rust and convert to ULID in SQL
3. For batch operations, use a loop within a transaction rather than complex UNNEST operations
4. The ULID type is provided by the PostgreSQL ULID extension and must be installed
