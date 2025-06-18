# TIM-PrimaryKeyImplementation: ULID Primary Keys with `pgx_ulid`

## Status Dashboard
**Maturity Level**: L4 - Implemented
**Implementation**: 95% (ULID generation and PostgreSQL integration fully working)
**Dependencies**: pgx_ulid PostgreSQL extension, NixOS PostgreSQL configuration
**Blocks**: All database operations, event identification, cross-table relationships

## MVP Specification
- pgx_ulid extension installation and activation
- ULID data type support in PostgreSQL
- gen_ulid() function for primary key defaults
- UUID casting compatibility
- Time-sortable identifier properties

## Enhanced Features
- Monotonic ULID generation for high-concurrency scenarios
- Advanced ULID utilities and operators
- Performance optimization for ULID indexes
- Cross-database ULID compatibility
- ULID-based sharding strategies

## Implementation Checklist
- [x] pgx_ulid extension available in NixOS
- [x] Database extension activation
- [x] ULID type usage in table schemas
- [x] gen_ulid() default generation
- [x] UUID casting compatibility
- [x] Primary key migration patterns
- [ ] Monotonic ULID configuration
- [ ] Performance benchmarking
- [ ] Advanced operator support

*   **Relevant ADR:** `[ADR-001-PrimaryKeyStrategy.md](docs/adr/ADR-001-PrimaryKeyStrategy.md)`
*   **Original UG Context:** Section 1.1

This Technical Implementation Module details the use of ULIDs (Universally Unique Lexicographically Sortable Identifiers) as the standard primary key strategy for the Sinnix Exocortex, leveraging the `pgx_ulid` PostgreSQL extension.

## 1. Rationale Summary

ULIDs, via the `pgx_ulid` extension, are chosen for their excellent balance of time-ordering (for index efficiency), global uniqueness, performance, and rich feature set within PostgreSQL. For a full discussion of alternatives and rationale, refer to `ADR-001-PrimaryKeyStrategy.md`.

## 2. `pgx_ulid` Extension

The `pgx_ulid` extension (by `pksunkara/pgx_ulid`) provides a native `ulid` data type in PostgreSQL, generator functions (`gen_ulid()`, `gen_monotonic_ulid()`), efficient binary storage, and casting capabilities to/from `UUID` and `timestamp`.

### 2.1. Installation and Setup

The `pgx_ulid` extension must be available in the PostgreSQL environment.

*   **NixOS:** Include the appropriate `pgx_ulid` package (e.g., from `pkgs.postgresql_16.pkgs.pgx_ulid` if available, or build from source using `pgrx`) in the NixOS configuration for the PostgreSQL server.
    ```nix
    # Example: in configuration.nix
    # environment.systemPackages = [ pkgs.postgresql_16Packages.pgx_ulid ]; # Or similar, check nixpkgs
    # services.postgresql = {
    #   enable = true;
    #   package = pkgs.postgresql_16;
    //   # Ensure extension is available to the database instance
    // };
    ```
*   **Database Activation:** Once the extension's binaries are installed, activate it within each required database:
    ```sql
    CREATE EXTENSION IF NOT EXISTS pgx_ulid;
    -- Note: The extension name might be 'ulid' or 'pgx_ulid' depending on packaging.
    -- The pgx_ulid README suggests 'CREATE EXTENSION ulid;'.
    ```

### 2.2. Monotonic Generator Setup (Optional)

If the `gen_monotonic_ulid()` function is required for strictly ordered IDs within the same millisecond (useful for extremely high, concurrent insert rates on a single table where precise intra-millisecond ordering is critical), `pgx_ulid` must be added to `shared_preload_libraries`.

*   **`postgresql.conf` (or NixOS `services.postgresql.settings`):**
    ```ini
    shared_preload_libraries = 'pgx_ulid' # Add to existing list if any, e.g., 'pg_stat_statements,pgx_ulid'
    ```
*   **Restart Required:** A PostgreSQL server restart is necessary after changing `shared_preload_libraries`.
*   **Note:** All other `pgx_ulid` functions (`gen_ulid()`, `ulid` type, casting) work without this setting.

## 3. Usage in DDL

Primary key columns will use the `ulid` type and default to `gen_ulid()`.

```sql
CREATE TABLE IF NOT EXISTS example_table (
    id                      ULID PRIMARY KEY DEFAULT gen_ulid(), -- Provided by pgx_ulid
    -- For monotonic generator, if configured:
    -- id                   ULID PRIMARY KEY DEFAULT gen_monotonic_ulid(),
    data                    TEXT,
    created_at_from_ulid    TIMESTAMP GENERATED ALWAYS AS (id::timestamp) STORED, -- Example of casting
    ts_ingest               TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN example_table.id IS 'Primary key using pgx_ulid. Default uses gen_ulid().';
COMMENT ON COLUMN example_table.created_at_from_ulid IS 'Timestamp extracted from the ULID PK, useful for sorting/querying if PK is only index.';
```

## 4. Key `pgx_ulid` Features and Performance

*   **Data Type:** `ulid` (stores 16 bytes, like `uuid`).
*   **Generation:**
    *   `gen_ulid()`: Standard ULID generation. (Approx. 30% faster than `gen_random_uuid()` for generation, ~20% faster for insert+generation [from `pgx_ulid` README]).
    *   `gen_monotonic_ulid()`: For strictly increasing IDs within the same millisecond. Slightly faster than `gen_ulid()` for very high frequency bursts due to incrementing the random part instead of regenerating it.
*   **Casting:**
    *   `some_ulid_column::timestamp`: Extracts the timestamp part of the ULID.
    *   `some_timestamp_column::ulid`: Creates a ULID with the given timestamp and zeroed random part (e.g., `TTTTTTTTTT0000000000000000`). Useful for range queries:
        ```sql
        SELECT * FROM example_table
        WHERE id BETWEEN ('2023-09-15'::timestamp::ulid) AND ('2023-09-16'::timestamp::ulid);
        ```
    *   `some_ulid_column::uuid`: Casts ULID to UUID.
    *   `some_uuid_column::ulid`: Casts UUID to ULID (if UUID bytes are compatible, e.g., from another ULID).

## 5. Client-Side Generation

Client applications (Rust, Python) can generate ULIDs using standard ULID libraries. These are typically generated as 26-character Crockford Base32 strings. `pgx_ulid` handles the conversion of these string representations to its internal binary format upon insertion.

*   **Rust:** Use crates like `ulid` or `lexical_sortable_guid`.
    ```rust
    // use ulid::Ulid;
    // let new_ulid_str = Ulid::new().to_string();
    // // Pass new_ulid_str to sqlx::query! for insertion into a ULID column.
    ```
*   **Python:** Use libraries like `ulid-py`.
    ```python
    # import ulid
    # new_ulid_str = str(ulid.new())
    # # Pass new_ulid_str to psycopg2 for insertion.
    ```

## 6. ULID Generation on Constrained Devices (e.g., ESP32)

This remains a consideration if client-side ULID generation is performed on IoT devices. The `pgx_ulid` extension itself runs on the server and doesn't directly address client-side entropy.

*   **Challenge [CR4]:** Microcontrollers may lack sufficient high-quality entropy for the random component.
*   **ESP32 [CR4]:**
    *   Utilize the hardware True Random Number Generator (TRNG), especially when Wi-Fi/Bluetooth radio is active.
    *   If radios are off, gather entropy from ADC noise (floating ADC pin) and CPU cycle counter jitter to seed a PRNG.
*   The ULIDs generated by these devices, once transmitted to the Exocortex backend, will be inserted as strings into the `ulid` type columns.

