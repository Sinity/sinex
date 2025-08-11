# Primary Key Naming Conventions

## Decision: Use `id` for all primary keys

After careful consideration, we've standardized on using `id` as the primary key column name across all tables in the Sinex database schema.

### Rationale

1. **Simplicity**: `id` is concise and universally understood
2. **Consistency**: All tables follow the same pattern
3. **SQL ergonomics**: Shorter queries, less verbose joins
4. **Industry standard**: Most modern ORMs and frameworks expect `id`

### Implementation

All tables use ULID type for primary keys:
```sql
CREATE TABLE schema.table_name (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    ...
);
```

### Special Cases

#### Events Table with TimescaleDB
The events table uses `id` as primary key but also leverages it for hypertable partitioning:
```sql
-- Primary key
id ULID PRIMARY KEY

-- Generated timestamp column for time-series operations
ts_ingest TIMESTAMPTZ GENERATED ALWAYS AS (id::timestamp) STORED

-- Hypertable partitioning by ULID's time component
SELECT create_hypertable('core.events', by_range('id', partition_func => 'ulid_to_timestamptz'::regproc));
```

### Foreign Key References
When referencing another table's ID:
```sql
-- Use descriptive names for foreign keys
event_id ULID REFERENCES core.events(id)
operation_id ULID REFERENCES core.operations_log(id)
```

### Migration Notes
- All migrations updated to use `id` from inception (system not yet deployed)
- Schema definitions in `sinex-migrations/src/schema.rs` reflect this convention
- SQLX offline cache regenerated after migration changes