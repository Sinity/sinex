# Event Type Conversions

The conversion layer (`conversions.rs`) serves as the critical boundary between the PostgreSQL database representation (`EventRecord`) and the application's domain model (`Event<T>`).

## Database to Domain Model

When events are read from the database, the system performs several transformations to reconstruct the rich domain model:

### Provenance Reconstruction
The database stores provenance as a set of flat, nullable columns. The conversion logic applies an XOR invariant to reconstruct the `Provenance` enum:
- **Material**: Requires `source_material_id` and `anchor_byte`.
- **Synthesis**: Requires a non-empty list of `source_event_ids`.
- **Validation**: If a record violates these constraints (e.g., having both or neither), the conversion fails loudly to prevent data corruption.

### Timestamp Precision Recovery
PostgreSQL's `TIMESTAMPTZ` provides microsecond precision. Sinex supports nanosecond precision by storing the remainder in a separate `ts_orig_subnano` column.
- **Nanosecond Assembly**: On read, the sub-nanosecond component is added back to the base timestamp to restore the original high-precision timing.
- **Overflow Handling**: The system checks for potential overflows during assembly and logs warnings if precision cannot be perfectly restored.

### ULID-UUID Bridging
While the application uses ULIDs for their lexicographical properties, the database stores these as native UUIDs for performance. The conversion layer handles this bidirectional mapping losslessly using 128-bit byte arrays.

## Domain Model to Database

When persisting events, the rich domain model is flattened back into a database-compatible format:

### Provenance Flattening
The `extract_provenance` utility maps the `Provenance` enum variants back to the appropriate nullable columns for SQL insertion.

### Metadata Extraction
Metadata such as `ingestor_version` and `host` are captured during the conversion to ensure that every persisted event has a complete audit trail of its origin.

## Search Result Mapping
For full-text search, a specialized `EventSearchRow` provides a lightweight mapping that includes search-specific metadata like ranking scores and highlighted snippets without the overhead of the full provenance chain.
