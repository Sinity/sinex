# ADR-001: Primary Key Strategy for Core Tables

*   **Status:** Implemented
*   **Date:** 2024-03-11 (Updated to reflect `pgx_ulid` adoption)
*   **Implementation Date:** 2025-07-17
*   **Context & Problem Statement:**
    The Sinnix Exocortex requires a robust and efficient primary key strategy for its core database tables, especially for high-volume, time-ordered data like `raw.events`. The chosen strategy must address:
    1.  **Index Efficiency:** Minimize B-tree index bloat and fragmentation.
    2.  **Time-Ordering:** Keys should be time-sortable.
    3.  **Global Uniqueness:** Support client-side generation and potential distribution.
    4.  **Performance:** Efficient generation and comparison.
    5.  **Developer Experience & Ecosystem Support:** Easy to work with, good library/tooling.
    6.  **Storage Size:** Reasonably compact.

    Traditional auto-incrementing integers are not suitable for distributed generation. UUIDv4 suffers from poor index locality. Time-ordered UUID variants (like UUIDv7) and ULIDs are strong contenders.

*   **Discussed Options:**

    1.  **UUIDv4:**
        *   **Pros:** Standard, global uniqueness.
        *   **Cons:** Random nature leads to poor database index performance. Not time-ordered.

    2.  **UUIDv7 (RFC 9562):**
        *   **Description:** Time-ordered UUID variant (48-bit Unix ms timestamp, 74 bits randomness).
        *   **Pros:** Standardized, time-ordered, improves index locality, globally unique, native `UUID` type in PostgreSQL.
        *   **Cons:** Newer standard, library support maturing.

    3.  **ULIDs (Universally Unique Lexicographically Sortable Identifiers) - General Concept:**
        *   **Description:** Embeds a 48-bit timestamp, 80 bits randomness.
        *   **Pros:** Time-ordered, globally unique, good randomness, excellent language library support.
        *   **Cons (without a good PG extension):** No native PG type. Textual storage (26 chars) is inefficient. Custom binary storage (`BYTEA` or mapping to `UUID`) requires app-level logic or custom PG functions, adding complexity.

    4.  **`pgx_ulid` PostgreSQL Extension (Rust-based, by pksunkara/pgx_ulid):**
        *   **Description:** Provides a native `ulid` data type in PostgreSQL, `gen_ulid()` and `gen_monotonic_ulid()` generator functions, binary storage, casting to/from `UUID` and `timestamp`, and support for monotonicity.
        *   **Pros:**
            *   Combines all benefits of ULIDs (time-ordering, uniqueness) with the convenience of a native PostgreSQL data type.
            *   **Binary Storage:** Stores ULIDs efficiently (16 bytes, same as UUID).
            *   **Performance:** Benchmarks show `pgx_ulid` `gen_ulid()` is ~30% faster for generation and ~20% faster for insert+generation than `pgcrypto`'s `gen_random_uuid()` or `pg_uuidv7`'s `uuid_generate_v7()`. `gen_monotonic_ulid()` offers further potential benefits for very high-frequency inserts within the same millisecond.
            *   **Type Safety & Ergonomics:** Provides a proper `ulid` type, simplifying DDL and queries.
            *   **Casting:** Supports `ulid::timestamp` and `timestamp::ulid`, useful for time-based range queries directly on ULID primary keys. Also `ulid::uuid` and `uuid::ulid` for interoperability.
            *   **Monotonicity Support:** `gen_monotonic_ulid()` for strictly ordered IDs within the same millisecond (requires `shared_preload_libraries` config).
        *   **Cons:**
            *   Introduces an external PostgreSQL extension dependency. Must be installed and managed (e.g., via NixOS packaging for `pgx_ulid`).
            *   Monotonic generator (`gen_monotonic_ulid()`) requires adding `pgx_ulid` to `shared_preload_libraries` in `postgresql.conf` and a DB restart. Other functions work without this.

    5.  **Custom PL/pgSQL ULID Implementation (Original Consideration):**
        *   **Description:** Using a custom `ULID` domain over `BYTEA` and a PL/pgSQL `generate_ulid_binary()` function.
        *   **Pros:** No external extension dependency beyond what's in PG core (like `gen_random_bytes()`).
        *   **Cons:** PL/pgSQL function likely slower than a C/Rust extension. Lacks the rich type casting and potential for optimized C-level operations of a dedicated extension like `pgx_ulid`. No built-in monotonicity features beyond timestamp ordering.

*   **Decision:**
    The Exocortex will use **ULIDs generated and managed by the `pgx_ulid` PostgreSQL extension (pksunkara/pgx_ulid)** as the primary key strategy for all core tables requiring globally unique, time-ordered identifiers.
    *   The `ulid` data type provided by `pgx_ulid` will be used for primary key columns.
    *   The `gen_ulid()` function will be the default generator (e.g., `DEFAULT gen_ulid()`).
    *   The `gen_monotonic_ulid()` function will be considered for tables with extremely high concurrent insert rates where strict intra-millisecond ordering is beneficial, acknowledging the `shared_preload_libraries` requirement.
    *   Client-side ULID generation (using standard ULID libraries in Rust/Python) remains an option, and these ULIDs can be directly inserted into `ulid` type columns, leveraging `pgx_ulid`'s binary storage and textual input/output.

*   **Rationale for Decision:**
    1.  **Best of Both Worlds:** `pgx_ulid` provides the time-ordering benefits of ULIDs (crucial for index performance) combined with the developer experience and performance characteristics of a native PostgreSQL data type. It directly addresses the main drawback of general ULIDs (lack of native, efficient PG integration).
    2.  **Performance:** The benchmark data provided for `pgx_ulid` indicates superior generation and insertion performance compared to both UUIDv4 and UUIDv7, making it an excellent choice for high-volume tables.
    3.  **Rich Feature Set:** The extension's support for casting to `timestamp` and `uuid`, and its monotonic generator option, provide valuable flexibility and utility beyond simple ID generation. `ulid::timestamp` casting is particularly useful for efficient time-range queries on PKs.
    4.  **Binary Storage Efficiency:** `pgx_ulid` handles the efficient 16-byte binary storage internally, abstracting this complexity from the application.
    5.  **Reduced Custom Code:** Adopting a well-maintained, feature-complete extension like `pgx_ulid` reduces the need for custom PL/pgSQL functions or complex application-level binary encoding/decoding logic for ULIDs.
    6.  **Alignment with Rust Ecosystem:** `pgx_ulid` being written in Rust using `pgrx` aligns well with the Exocortex's primary backend language.

*   **Consequences:**
    *   The `pgx_ulid` extension must be installed in the PostgreSQL environment where the Exocortex database runs. This needs to be handled by the NixOS configuration.
    *   If `gen_monotonic_ulid()` is used, `shared_preload_libraries = 'pgx_ulid'` must be configured in `postgresql.conf`, and the PostgreSQL server restarted.
    *   Application code (Rust, Python) interacting with `ulid` columns will typically send/receive ULIDs as their standard 26-character string representation, which `pgx_ulid` handles for input/output, while storing them efficiently as binary.
    *   The custom `ULID` domain and `generate_ulid_binary()` PL/pgSQL function previously considered are superseded by `pgx_ulid`.

