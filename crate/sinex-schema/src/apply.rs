use crate::defs::{
    ArchivedEventAnnotations, ArchivedEventEmbeddings, ArchivedEvents, ArchivedTaggedItems,
    BinarySchemaVersion, Blobs, DocumentChunks, Documents, EmbeddingCache, EmbeddingModels,
    Entities, EntityRelations, EventAnnotations, EventClusterMembers, EventClusters,
    EventEmbeddings, EventPayloadSchemas, EventReplacements, EventTombstones, Events, Manifests,
    ModelEffects, OperationsLog, Runs, SemanticEpochs, SemanticLaneDiffs, SemanticLaneOutputs,
    SemanticLanes, SourceMaterialLinks, SourceMaterialRegistry, TaggedItems, Tags, TemporalLedger,
};
use crate::registry;
use sea_query::{IndexCreateStatement, PostgresQueryBuilder, TableCreateStatement};
use sinex_primitives::validation::validate_pg_identifier;
use sqlx::{Executor, PgPool};

const REQUIRED_EXTENSIONS: &[&str] = &["pg_jsonschema", "vector", "timescaledb", "pg_trgm"];
pub const SHARED_ACCESS_ROLES: &[&str] = &["sinex_event_engine", "sinex_api", "sinex_readonly"];
const EVENTS_REQUIRED_TRIGGERS: &[&str] = &[
    "trg_events_no_update",
    "trg_events_archive_before_delete",
    "trg_events_validate_material_bounds",
    "trg_events_validate_payload",
    "trg_document_projection",
];
const SOURCE_MATERIAL_REQUIRED_TRIGGERS: &[&str] = &["trg_source_material_validate_event_bounds"];
const TEMPORAL_LEDGER_REQUIRED_TRIGGERS: &[&str] = &["trg_tl_no_update_delete"];
const ENTITIES_REQUIRED_TRIGGERS: &[&str] = &["trg_entities_updated_at"];
const ENTITY_RELATIONS_REQUIRED_TRIGGERS: &[&str] = &["trg_entity_relations_updated_at"];
const EVENT_ANNOTATIONS_REQUIRED_TRIGGERS: &[&str] = &["trg_event_annotations_updated_at"];
const EVENT_PAYLOAD_SCHEMAS_REQUIRED_TRIGGERS: &[&str] = &["trg_event_payload_schemas_updated_at"];
const DLQ_EVENTS_REQUIRED_TRIGGERS: &[&str] = &["set_timestamp"];
const EMBEDDING_MODELS_REQUIRED_TRIGGERS: &[&str] = &["trg_embedding_model_create_index"];
const EVENTS_REQUIRED_INDEXES: &[&str] = &[
    "ix_events_material_anchor",
    "ix_events_ts_orig",
    "ix_events_ts_coided",
    "ix_events_ts_persisted",
    "ix_events_source_ts_coided",
    "ix_events_event_type_ts_coided",
    "ix_events_source_type_ts_coided",
    "ix_events_source_ts_orig",
    "ix_events_source_event_ids",
    "ix_events_payload_gin",
    "ix_events_scope_key",
    "ix_events_created_by_operation_id",
    "ix_events_sinex_metric_gauge_latest",
    "ix_events_module_run_synthesis_latest",
];
const ARCHIVED_EVENTS_REQUIRED_INDEXES: &[&str] = &[
    "ix_archived_events_ts_orig",
    "ix_archived_events_source_ts_orig",
    "ix_archived_events_archived_at",
    "ix_archived_events_source_event_ids",
];
const TEMPORAL_LEDGER_REQUIRED_INDEXES: &[&str] = &[
    "uk_temporal_ledger_material_offset_source_type",
    "ix_tl_material_offsets",
    "ix_tl_ts_and_source_type",
];
const DOCUMENT_CHUNKS_REQUIRED_INDEXES: &[&str] = &[
    "ix_document_chunks_chunked_event_id",
    "ix_document_chunks_text_fts",
    "ix_document_chunks_text_trgm",
];
const TELEMETRY_VIEW_RELATIONS: &[&str] = &["current_health", "recent_activity_summary"];
const TELEMETRY_MATERIALIZED_VIEW_RELATIONS: &[&str] = &["current_device_state"];
const TELEMETRY_CONTINUOUS_AGGREGATES: &[&str] = &[
    "gateway_stats_1h",
    "stream_stats_1h",
    "assembly_stats_1h",
    "source_stats_1h",
    "metric_counters_1h",
    "event_engine_batch_stats_1h",
    "current_window_focus",
    "command_frequency_hourly",
    "file_activity_summary",
];

#[derive(Debug)]
pub enum ApplyError {
    Sqlx(sqlx::Error),
    MissingExtensions(Vec<String>),
    Internal(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlx(err) => write!(f, "{err}"),
            Self::MissingExtensions(missing) => write!(
                f,
                "Required PostgreSQL extensions missing: {}",
                missing.join(", ")
            ),
            Self::Internal(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx(err) => Some(err),
            Self::MissingExtensions(_) => None,
            Self::Internal(_) => None,
        }
    }
}

impl From<sqlx::Error> for ApplyError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

pub async fn apply(pool: &PgPool) -> Result<(), ApplyError> {
    let convergible_tables = crate::converge::convergible_tables()?;
    ensure_schemas(pool).await?;
    ensure_required_extensions(pool).await?;
    execute_sql(pool, BOOTSTRAP_SQL).await?;
    create_tables(pool).await?;
    crate::converge::converge_tables(pool, &convergible_tables).await?;
    converge_operations_log_constraints(pool).await?;
    converge_source_material_registry_constraints(pool).await?;
    normalize_manifest_type_values(pool).await?;
    converge_db_check_constraints(pool).await?;
    create_indexes(pool).await?;
    create_triggers_and_functions(pool).await?;
    configure_timescaledb(pool).await?;
    apply_roles_and_grants(pool).await?;
    Ok(())
}

pub async fn ensure_shared_access_roles(pool: &PgPool) -> Result<(), ApplyError> {
    execute_sql(pool, SHARED_ACCESS_ROLES_BOOTSTRAP_SQL).await
}

pub async fn diff(pool: &PgPool) -> Result<Vec<String>, ApplyError> {
    let convergible_tables = crate::converge::convergible_tables()?;
    let mut drifts = Vec::new();

    // Table existence.
    for table in crate::defs::all_tables() {
        if !relation_exists(pool, table.qualified_name).await? {
            drifts.push(format!("missing table {}", table.qualified_name));
        }
    }

    // Column and named constraint gaps — derived from sea-query declarations.
    let column_gaps = crate::converge::report_column_gaps(pool, &convergible_tables).await?;
    drifts.extend(column_gaps);

    drifts.extend(check_table_object_drifts(pool).await?);

    drifts.extend(check_telemetry_drifts(pool).await?);

    drifts.extend(check_constraint_drifts(pool).await?);

    Ok(drifts)
}

/// Trigger and index existence drift for trigger/index-bearing tables. Triggers
/// are installed via CREATE OR REPLACE (not convergence), so a missing one
/// signals an incomplete apply or a manual DROP. Extracted from `diff` to keep
/// it within the cognitive-complexity budget.
async fn check_table_object_drifts(pool: &PgPool) -> Result<Vec<String>, ApplyError> {
    let mut drifts = Vec::new();

    if relation_exists(pool, "core.events").await? {
        for trigger in EVENTS_REQUIRED_TRIGGERS {
            if !trigger_exists(pool, "core.events", trigger).await? {
                drifts.push(format!("missing core.events trigger {trigger}"));
            }
        }
        for index in EVENTS_REQUIRED_INDEXES {
            if !index_exists(pool, "core", "events", index).await? {
                drifts.push(format!("missing core.events index {index}"));
            }
        }
    }

    if relation_exists(pool, "raw.source_material_registry").await? {
        for trigger in SOURCE_MATERIAL_REQUIRED_TRIGGERS {
            if !trigger_exists(pool, "raw.source_material_registry", trigger).await? {
                drifts.push(format!(
                    "missing raw.source_material_registry trigger {trigger}"
                ));
            }
        }
    }

    // Each trigger is installed by create_triggers_and_functions via CREATE OR REPLACE.
    // Missing triggers indicate incomplete apply or manual DROP TRIGGER.
    let extended_trigger_checks: &[(&str, &[&str])] = &[
        ("raw.temporal_ledger", TEMPORAL_LEDGER_REQUIRED_TRIGGERS),
        ("core.entities", ENTITIES_REQUIRED_TRIGGERS),
        ("core.entity_relations", ENTITY_RELATIONS_REQUIRED_TRIGGERS),
        (
            "core.event_annotations",
            EVENT_ANNOTATIONS_REQUIRED_TRIGGERS,
        ),
        (
            "sinex_schemas.event_payload_schemas",
            EVENT_PAYLOAD_SCHEMAS_REQUIRED_TRIGGERS,
        ),
        ("sinex_schemas.dlq_events", DLQ_EVENTS_REQUIRED_TRIGGERS),
        ("core.embedding_models", EMBEDDING_MODELS_REQUIRED_TRIGGERS),
    ];
    for &(qualified_table, required_triggers) in extended_trigger_checks {
        if relation_exists(pool, qualified_table).await? {
            for trigger in required_triggers {
                if !trigger_exists(pool, qualified_table, trigger).await? {
                    drifts.push(format!("missing {qualified_table} trigger {trigger}"));
                }
            }
        }
    }

    if relation_exists(pool, "audit.archived_events").await? {
        for index in ARCHIVED_EVENTS_REQUIRED_INDEXES {
            if !index_exists(pool, "audit", "archived_events", index).await? {
                drifts.push(format!("missing audit.archived_events index {index}"));
            }
        }
    }

    if relation_exists(pool, "raw.temporal_ledger").await? {
        for index in TEMPORAL_LEDGER_REQUIRED_INDEXES {
            if !index_exists(pool, "raw", "temporal_ledger", index).await? {
                drifts.push(format!("missing raw.temporal_ledger index {index}"));
            }
        }
    }

    if relation_exists(pool, "core.document_chunks").await? {
        for index in DOCUMENT_CHUNKS_REQUIRED_INDEXES {
            if !index_exists(pool, "core", "document_chunks", index).await? {
                drifts.push(format!("missing core.document_chunks index {index}"));
            }
        }
    }

    Ok(drifts)
}

/// Telemetry relation-kind drift: ordinary views, materialized views, and
/// continuous-aggregate registrations under `sinex_telemetry`. Extracted from
/// `diff` to keep it within the cognitive-complexity budget.
async fn check_telemetry_drifts(pool: &PgPool) -> Result<Vec<String>, ApplyError> {
    let mut drifts = Vec::new();

    for relation in TELEMETRY_VIEW_RELATIONS {
        match relation_kind(pool, &format!("sinex_telemetry.{relation}")).await? {
            Some('v') => {}
            Some(kind) => {
                drifts.push(format!(
                    "stale sinex_telemetry.{relation} relation kind {kind}; expected a view"
                ));
            }
            None => {
                drifts.push(format!("missing sinex_telemetry.{relation} view"));
            }
        }
    }

    for relation in TELEMETRY_MATERIALIZED_VIEW_RELATIONS {
        match relation_kind(pool, &format!("sinex_telemetry.{relation}")).await? {
            Some('m') => {}
            Some(kind) => {
                drifts.push(format!(
                    "stale sinex_telemetry.{relation} relation kind {kind}; expected a materialized view"
                ));
            }
            None => {
                drifts.push(format!(
                    "missing sinex_telemetry.{relation} materialized view"
                ));
            }
        }
    }

    for relation in TELEMETRY_CONTINUOUS_AGGREGATES {
        match relation_kind(pool, &format!("sinex_telemetry.{relation}")).await? {
            Some(_) if !continuous_aggregate_exists(pool, "sinex_telemetry", relation).await? => {
                drifts.push(format!(
                    "missing sinex_telemetry.{relation} continuous aggregate registration"
                ));
            }
            Some(_) => {}
            None => {
                drifts.push(format!(
                    "missing sinex_telemetry.{relation} continuous aggregate relation"
                ));
            }
        }
    }

    Ok(drifts)
}

/// CHECK-constraint drift: the hand-written operations-log / source-material
/// constraints plus every enum-derived constraint in the registry. Extracted
/// from `diff` to keep it within the cognitive-complexity budget.
async fn check_constraint_drifts(pool: &PgPool) -> Result<Vec<String>, ApplyError> {
    let mut drifts = Vec::new();

    if relation_exists(pool, "core.operations_log").await?
        && !operations_log_operation_type_constraint_is_current(pool).await?
    {
        drifts.push(
            "stale core.operations_log constraint operations_log_operation_type_check".into(),
        );
    }

    if relation_exists(pool, "raw.source_material_registry").await?
        && !source_material_registry_status_constraint_is_current(pool).await?
    {
        drifts.push(
            "stale raw.source_material_registry constraint source_material_registry_status_check"
                .into(),
        );
    }

    for spec in sinex_primitives::schema_constraints::registered_specs() {
        if !relation_exists(pool, &spec.qualified_table()).await? {
            continue;
        }
        if !column_exists(pool, spec.schema, spec.table, spec.column).await? {
            continue;
        }
        if !db_check_constraint_is_current(pool, spec).await? {
            drifts.push(format!(
                "stale {} CHECK on column {} (expected constraint {} from enum {})",
                spec.qualified_table(),
                spec.column,
                spec.constraint_name(),
                spec.enum_name,
            ));
        }
    }

    if relation_exists(pool, "raw.source_material_registry").await?
        && !source_material_registry_timing_constraint_is_current(pool).await?
    {
        drifts.push(
            "stale raw.source_material_registry constraint source_material_registry_timing_info_type_check"
                .into(),
        );
    }

    Ok(drifts)
}

async fn ensure_schemas(pool: &PgPool) -> Result<(), ApplyError> {
    for schema in registry::schema_names() {
        validate_pg_identifier(schema, "schema")
            .map_err(|e| ApplyError::Internal(format!("invalid schema identifier: {e}")))?;
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        execute_sql(pool, &sql).await?;
    }
    execute_sql(pool, "CREATE SCHEMA IF NOT EXISTS sinex_telemetry").await?;
    Ok(())
}

async fn normalize_manifest_type_values(pool: &PgPool) -> Result<(), ApplyError> {
    if !relation_exists(pool, "core.manifests").await? {
        return Ok(());
    }
    if !column_exists(pool, "core", "manifests", "manifest_type").await? {
        return Ok(());
    }

    execute_sql(
        pool,
        r"
        UPDATE core.manifests
        SET manifest_type = 'source'
        WHERE manifest_type = 'ingestor'
        ",
    )
    .await?;

    Ok(())
}

async fn converge_operations_log_constraints(pool: &PgPool) -> Result<(), ApplyError> {
    if !relation_exists(pool, "core.operations_log").await? {
        return Ok(());
    }

    if operations_log_operation_type_constraint_is_current(pool).await? {
        return Ok(());
    }

    execute_sql(
        pool,
        r"
        ALTER TABLE core.operations_log
            DROP CONSTRAINT IF EXISTS operations_log_operation_type_check,
            ADD CONSTRAINT operations_log_operation_type_check
            CHECK (operation_type ~ '^[a-z][a-z0-9_.-]*$')
        ",
    )
    .await?;

    Ok(())
}

async fn operations_log_operation_type_constraint_is_current(
    pool: &PgPool,
) -> Result<bool, ApplyError> {
    let definition = sqlx::query_scalar::<_, String>(
        r"
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = 'core'
          AND r.relname = 'operations_log'
          AND c.conname = 'operations_log_operation_type_check'
        ",
    )
    .fetch_optional(pool)
    .await?;

    Ok(definition
        .is_some_and(|def| operations_log_operation_type_constraint_definition_is_current(&def)))
}

fn operations_log_operation_type_constraint_definition_is_current(definition: &str) -> bool {
    definition.contains("operation_type ~") && definition.contains("^[a-z][a-z0-9_.-]*$")
}

async fn converge_source_material_registry_constraints(pool: &PgPool) -> Result<(), ApplyError> {
    if !relation_exists(pool, "raw.source_material_registry").await? {
        return Ok(());
    }

    if !source_material_registry_status_constraint_is_current(pool).await? {
        execute_sql(
            pool,
            r"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
                ADD CONSTRAINT source_material_registry_status_check
                CHECK (status IN ('sensing', 'completed', 'cancelled', 'recovered_partial', 'failed'))
            ",
        )
        .await?;
    }

    if !source_material_registry_timing_constraint_is_current(pool).await? {
        execute_sql(
            pool,
            r"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_timing_info_type_check,
                ADD CONSTRAINT source_material_registry_timing_info_type_check
                CHECK (timing_info_type IN ('realtime', 'intrinsic', 'inferred', 'declared', 'atemporal', 'staged_at'))
            ",
        )
        .await?;
    }

    Ok(())
}

async fn source_material_registry_status_constraint_is_current(
    pool: &PgPool,
) -> Result<bool, ApplyError> {
    let definition = sqlx::query_scalar::<_, String>(
        r"
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = 'raw'
          AND r.relname = 'source_material_registry'
          AND c.conname = 'source_material_registry_status_check'
        ",
    )
    .fetch_optional(pool)
    .await?;

    Ok(definition
        .is_some_and(|def| source_material_registry_status_constraint_definition_is_current(&def)))
}

fn source_material_registry_status_constraint_definition_is_current(definition: &str) -> bool {
    (definition.contains("status IN") || definition.contains("status = ANY"))
        && definition.contains("'sensing'")
        && definition.contains("'completed'")
        && definition.contains("'cancelled'")
        && definition.contains("'recovered_partial'")
        && definition.contains("'failed'")
}

async fn source_material_registry_timing_constraint_is_current(
    pool: &PgPool,
) -> Result<bool, ApplyError> {
    let definition = sqlx::query_scalar::<_, String>(
        r"
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = 'raw'
          AND r.relname = 'source_material_registry'
          AND c.conname = 'source_material_registry_timing_info_type_check'
        ",
    )
    .fetch_optional(pool)
    .await?;

    Ok(definition
        .is_some_and(|def| source_material_registry_timing_constraint_definition_is_current(&def)))
}

fn source_material_registry_timing_constraint_definition_is_current(definition: &str) -> bool {
    (definition.contains("timing_info_type IN") || definition.contains("timing_info_type = ANY"))
        && definition.contains("'realtime'")
        && definition.contains("'intrinsic'")
        && definition.contains("'inferred'")
        && definition.contains("'declared'")
        && definition.contains("'atemporal'")
        && definition.contains("'staged_at'")
}

// ─────────────────────────────────────────────────────────────────────────────
// DbCheck convergence (issue #1236)
//
// `#[derive(DbCheck)]` enums register their CHECK specs at static init.
// At apply time we iterate them and reconcile each live constraint:
//
//   1. If the table or column does not exist, skip (forward-compatible:
//      newly-added columns pick up the constraint on the next apply).
//   2. If the current versioned constraint (`<column>_check_v<N>`) exists
//      with the expected `IN (...)` body, do nothing.
//   3. Otherwise drop every legacy unversioned constraint
//      (`<table>_<column>_check`) and every older versioned constraint
//      (`<column>_check_v*` whose version != N), then add the current one.
// ─────────────────────────────────────────────────────────────────────────────

async fn converge_db_check_constraints(pool: &PgPool) -> Result<(), ApplyError> {
    for spec in sinex_primitives::schema_constraints::registered_specs() {
        converge_one_db_check(pool, spec).await?;
    }
    Ok(())
}

async fn converge_one_db_check(
    pool: &PgPool,
    spec: &sinex_primitives::schema_constraints::DbCheckSpec,
) -> Result<(), ApplyError> {
    // Defense-in-depth: every identifier comes from a compile-time constant
    // baked into the proc-macro, but validate before format!()-ing into DDL.
    validate_pg_identifier(spec.schema, "schema")
        .map_err(|e| ApplyError::Internal(format!("invalid DbCheck schema: {e}")))?;
    validate_pg_identifier(spec.table, "table")
        .map_err(|e| ApplyError::Internal(format!("invalid DbCheck table: {e}")))?;
    validate_pg_identifier(spec.column, "column")
        .map_err(|e| ApplyError::Internal(format!("invalid DbCheck column: {e}")))?;
    for value in spec.allowed_values {
        if value.contains(['\\', '\0']) {
            return Err(ApplyError::Internal(format!(
                "DbCheck allowed value for {}.{}.{} contains forbidden character: {:?}",
                spec.schema, spec.table, spec.column, value
            )));
        }
    }

    if !relation_exists(pool, &spec.qualified_table()).await? {
        return Ok(()); // Forward-compatible: table doesn't exist yet.
    }
    if !column_exists(pool, spec.schema, spec.table, spec.column).await? {
        return Ok(()); // Forward-compatible: column doesn't exist yet.
    }

    if db_check_constraint_is_current(pool, spec).await? {
        return Ok(());
    }

    // Collect stale constraint names to drop. Always include the current
    // versioned name too — if we got here, either the constraint is missing
    // (drop is a no-op) or its body is wrong and must be replaced. Re-using
    // the same name without dropping first triggers a "constraint already
    // exists" error from PostgreSQL.
    let mut stale = stale_db_check_constraint_names(pool, spec).await?;
    stale.push(spec.constraint_name());

    let new_name = spec.constraint_name();
    let check_clause = spec.check_clause();
    let qualified = spec.qualified_table();

    let mut alter = format!("ALTER TABLE {qualified}");
    for name in &stale {
        alter.push_str(&format!("\n    DROP CONSTRAINT IF EXISTS {name},"));
    }
    alter.push_str(&format!(
        "\n    ADD CONSTRAINT {new_name} CHECK ({check_clause})"
    ));

    execute_sql(pool, &alter).await?;
    Ok(())
}

async fn column_exists(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>(
        r"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
        )",
    )
    .bind(schema)
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

/// Whether the current versioned CHECK constraint exists and its body
/// references each allowed value (and no stranger).
async fn db_check_constraint_is_current(
    pool: &PgPool,
    spec: &sinex_primitives::schema_constraints::DbCheckSpec,
) -> Result<bool, ApplyError> {
    let definition: Option<String> = sqlx::query_scalar(
        r"
        SELECT pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = $1
          AND r.relname = $2
          AND c.conname = $3
        ",
    )
    .bind(spec.schema)
    .bind(spec.table)
    .bind(spec.constraint_name())
    .fetch_optional(pool)
    .await?;
    let Some(def) = definition else {
        return Ok(false);
    };
    // Must mention every allowed value, and must not contain a quoted literal
    // that's not in the allowed set. PostgreSQL renders `col IN ('a','b')` as
    // `CHECK ((col = ANY (ARRAY['a'::text, 'b'::text])))` in many versions; we
    // accept either form.
    for value in spec.allowed_values {
        let needle = format!("'{}'", value.replace('\'', "''"));
        if !def.contains(&needle) {
            return Ok(false);
        }
    }
    // Reject if any extra single-quoted literals exist that aren't in the
    // allowed set. This catches the rename case where an old variant still
    // appears in the live constraint body.
    let allowed_set: std::collections::HashSet<String> = spec
        .allowed_values
        .iter()
        .map(|v| v.replace('\'', "''"))
        .collect();
    let mut chars = def.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c != '\'' {
            continue;
        }
        // Find the matching close quote (escaped quote is '' inside).
        let start = i + 1;
        let mut end = start;
        let bytes = def.as_bytes();
        while end < bytes.len() {
            if bytes[end] == b'\'' {
                if end + 1 < bytes.len() && bytes[end + 1] == b'\'' {
                    end += 2;
                    continue;
                }
                break;
            }
            end += 1;
        }
        if end > def.len() {
            break;
        }
        let literal = &def[start..end];
        // Advance the iterator past the closing quote so we don't re-enter it.
        while let Some(&(j, _)) = chars.peek() {
            if j > end {
                break;
            }
            chars.next();
        }
        if !allowed_set.contains(literal) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Names of all live constraints on this table that should be dropped:
/// the legacy unversioned `<table>_<column>_check` and every versioned
/// `<column>_check_v*` constraint whose version differs from `spec.version`.
async fn stale_db_check_constraint_names(
    pool: &PgPool,
    spec: &sinex_primitives::schema_constraints::DbCheckSpec,
) -> Result<Vec<String>, ApplyError> {
    let legacy = spec.legacy_constraint_name();
    let prefix = spec.constraint_name_prefix();
    let current = spec.constraint_name();
    let pattern = format!("{prefix}%");
    let rows = sqlx::query_scalar::<_, String>(
        r"
        SELECT c.conname
        FROM pg_constraint c
        JOIN pg_class r ON c.conrelid = r.oid
        JOIN pg_namespace n ON r.relnamespace = n.oid
        WHERE n.nspname = $1
          AND r.relname = $2
          AND c.contype = 'c'
          AND (c.conname = $3 OR c.conname LIKE $4)
        ",
    )
    .bind(spec.schema)
    .bind(spec.table)
    .bind(legacy)
    .bind(pattern)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().filter(|name| name != &current).collect())
}

async fn ensure_required_extensions(pool: &PgPool) -> Result<(), ApplyError> {
    let mut missing = Vec::new();

    for extension in REQUIRED_EXTENSIONS {
        let available = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_available_extensions WHERE name = $1)",
        )
        .bind(extension)
        .fetch_one(pool)
        .await?;

        if !available {
            missing.push((*extension).to_string());
            continue;
        }

        let sql = format!(r#"CREATE EXTENSION IF NOT EXISTS "{extension}""#);
        execute_sql(pool, &sql).await?;
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(ApplyError::MissingExtensions(missing))
    }
}

async fn create_tables(pool: &PgPool) -> Result<(), ApplyError> {
    let table_sql = vec![
        render_table(&Blobs::create_table_statement()),
        render_table(&EventPayloadSchemas::create_table_statement()),
        render_table(&EmbeddingModels::create_table_statement()),
        render_table(&EventClusters::create_table_statement()),
        render_table(&OperationsLog::create_table_statement()),
        render_table(&Tags::create_table_statement()),
        render_table(&SourceMaterialRegistry::create_table_statement()),
        render_table(&SourceMaterialLinks::create_table_statement()),
        render_table(&Manifests::create_table_statement()),
        render_table(&Runs::create_table_statement()),
        render_table(&Events::create_table_statement()),
        render_table(&ModelEffects::create_table_statement()),
        render_table(&BinarySchemaVersion::create_table_statement()),
        render_table(&TemporalLedger::create_table_statement()),
        render_table(&Entities::create_table_statement()),
        render_table(&EntityRelations::create_table_statement()),
        render_table(&SemanticEpochs::create_table_statement()),
        render_table(&SemanticLanes::create_table_statement()),
        render_table(&SemanticLaneOutputs::create_table_statement()),
        render_table(&SemanticLaneDiffs::create_table_statement()),
        render_table(&TaggedItems::create_table_statement()),
        render_table(&EventAnnotations::create_table_statement()),
        render_table(&EmbeddingCache::create_table_statement()),
        render_table(&EventEmbeddings::create_table_statement()),
        render_table(&EventClusterMembers::create_table_statement()),
        render_table(&EventTombstones::create_table_statement()),
        render_table(&EventReplacements::create_table_statement()),
        render_table(&Documents::create_table_statement()),
        render_table(&DocumentChunks::create_table_statement()),
    ];

    for sql in table_sql {
        execute_sql(pool, &sql).await?;
    }

    // Apply FK fixups for self-referencing foreign keys affected by a sea-query bug.
    // sea-query emits ON DELETE CASCADE instead of ON DELETE SET NULL for
    // self-referencing FKs. Work around by dropping the wrong constraint and
    // re-adding with the correct action via raw SQL.
    for sql in Tags::create_fk_fixup_sql() {
        execute_sql(pool, &sql).await?;
    }
    for sql in Entities::create_fk_fixup_sql() {
        execute_sql(pool, &sql).await?;
    }

    execute_sql(pool, &ArchivedEvents::create_table_sql()).await?;
    execute_sql(pool, &ArchivedEventAnnotations::create_table_sql()).await?;
    execute_sql(pool, &ArchivedEventEmbeddings::create_table_sql()).await?;
    execute_sql(pool, &ArchivedTaggedItems::create_table_sql()).await?;

    // Privacy policy tables (#1042). Raw DDL with inline named CHECK constraints,
    // mirroring the dlq_events pattern. CREATE TABLE IF NOT EXISTS is idempotent.
    execute_sql(pool, PRIVACY_SCHEMA_SQL).await?;

    Ok(())
}

async fn create_indexes(pool: &PgPool) -> Result<(), ApplyError> {
    let mut index_sql = Vec::new();
    index_sql.extend(render_indexes(SourceMaterialRegistry::create_indexes()));
    index_sql.extend(render_indexes(SourceMaterialLinks::create_indexes()));
    index_sql.extend(render_indexes(Events::create_indexes()));
    index_sql.extend(Events::create_gin_indexes_sql());
    index_sql.extend(ArchivedEvents::create_indexes_sql());
    index_sql.extend(vec![
        format!(
            "CREATE INDEX IF NOT EXISTS ix_archived_annotations_event_id ON audit.archived_annotations(event_id)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS ix_archived_annotations_archived_at ON audit.archived_annotations(archived_at DESC)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS ix_archived_embeddings_event_id ON audit.archived_embeddings(event_id)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS ix_archived_embeddings_archived_at ON audit.archived_embeddings(archived_at DESC)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS ix_archived_tagged_items_item ON audit.archived_tagged_items(item_id, item_type)"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS ix_archived_tagged_items_archived_at ON audit.archived_tagged_items(archived_at DESC)"
        ),
    ]);
    index_sql.extend(EventTombstones::create_indexes_sql());

    index_sql.extend(render_indexes(Blobs::create_indexes()));
    index_sql.extend(render_indexes(TemporalLedger::create_indexes()));
    index_sql.extend(render_indexes(Entities::create_indexes()));
    index_sql.extend(Entities::create_gin_indexes_sql());
    index_sql.extend(Entities::create_trigram_indexes_sql());
    index_sql.extend(render_indexes(EntityRelations::create_indexes()));
    index_sql.extend(render_indexes(SemanticEpochs::create_indexes()));
    index_sql.extend(render_indexes(SemanticLanes::create_indexes()));
    index_sql.extend(render_indexes(SemanticLaneOutputs::create_indexes()));
    index_sql.extend(render_indexes(SemanticLaneDiffs::create_indexes()));
    index_sql.extend(render_indexes(TaggedItems::create_indexes()));
    index_sql.extend(render_indexes(EventAnnotations::create_indexes()));
    index_sql.extend(EventAnnotations::create_gin_indexes_sql());
    index_sql.extend(render_indexes(EmbeddingModels::create_indexes()));
    index_sql.extend(render_indexes(EmbeddingCache::create_indexes()));
    index_sql.extend(EmbeddingCache::create_indexes_sql());
    index_sql.extend(render_indexes(EventEmbeddings::create_indexes()));
    index_sql.extend(EventEmbeddings::create_indexes_sql());
    index_sql.extend(render_indexes(EventPayloadSchemas::create_indexes()));
    index_sql.extend(render_indexes(Manifests::create_indexes()));
    index_sql.extend(render_indexes(Runs::create_indexes()));
    index_sql.extend(render_indexes(EventReplacements::create_indexes()));
    index_sql.extend(render_indexes(Documents::create_indexes()));
    index_sql.extend(render_indexes(DocumentChunks::create_indexes()));
    index_sql.extend(DocumentChunks::create_fts_indexes_sql());
    index_sql.extend(render_indexes(OperationsLog::create_indexes()));
    index_sql.extend(OperationsLog::create_gin_indexes_sql());

    for sql in index_sql {
        execute_sql(pool, &sql).await?;
    }

    Ok(())
}

async fn create_triggers_and_functions(pool: &PgPool) -> Result<(), ApplyError> {
    execute_sql(pool, Events::create_no_update_trigger_sql()).await?;
    execute_sql(pool, Events::create_payload_validation_trigger_sql()).await?;
    execute_sql(pool, Events::create_material_bounds_trigger_sql()).await?;
    execute_sql(
        pool,
        SourceMaterialRegistry::create_event_bounds_trigger_sql(),
    )
    .await?;
    execute_sql(pool, ArchivedEvents::create_archive_trigger_sql()).await?;
    execute_sql(pool, TemporalLedger::create_append_only_trigger_sql()).await?;
    execute_sql(pool, &Entities::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &EntityRelations::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &EventAnnotations::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &EventPayloadSchemas::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, DocumentChunks::create_projection_trigger_sql()).await?;

    execute_sql(pool, OPERATIONS_AND_CASCADE_SQL).await?;
    execute_sql(pool, TOMBSTONE_LIFECYCLE_SQL).await?;
    execute_sql(pool, JSONB_MERGE_SQL).await?;
    execute_sql(pool, EMBEDDING_INDEX_MANAGEMENT_SQL).await?;
    execute_sql(pool, HYBRID_SEARCH_SQL).await?;

    Ok(())
}

async fn configure_timescaledb(pool: &PgPool) -> Result<(), ApplyError> {
    execute_sql(pool, Events::create_hypertable_sql()).await?;
    execute_sql(
        pool,
        "SELECT set_chunk_time_interval('core.events', INTERVAL '7 days')",
    )
    .await?;
    execute_sql(
        pool,
        "SELECT remove_retention_policy('core.events', if_exists => true)",
    )
    .await?;

    // Enable native compression. Remove any existing policy that used compress_after
    // (which fails on UUID-partitioned hypertables because policy_compression's CASE has
    // no uuid branch), then add the corrected compress_created_before policy.
    execute_sql(pool, Events::enable_compression_sql()).await?;
    execute_sql(
        pool,
        "SELECT remove_compression_policy('core.events', if_exists => true)",
    )
    .await?;
    execute_sql(pool, Events::add_compression_policy_sql()).await?;

    execute_sql(
        pool,
        "CREATE INDEX IF NOT EXISTS ix_events_sinex_telemetry ON core.events (source, event_type, id DESC) WHERE source LIKE 'sinex.%'",
    )
    .await?;
    execute_sql(
        pool,
        r"
        CREATE INDEX IF NOT EXISTS ix_events_sinex_metric_gauge_latest
        ON core.events (
            (payload->>'name'),
            ((payload->'labels'->>'module')),
            ((payload->'labels'->>'module_run_id')),
            id DESC
        )
        WHERE source = 'sinex' AND event_type = 'metric.gauge'
        ",
    )
    .await?;
    execute_sql(
        pool,
        r"
        CREATE INDEX IF NOT EXISTS ix_events_module_run_synthesis_latest
        ON core.events (module_run_id, id DESC)
        WHERE module_run_id IS NOT NULL AND source_event_ids IS NOT NULL
        ",
    )
    .await?;

    recreate_telemetry_read_models(pool).await?;
    execute_sql(pool, TELEMETRY_CONTINUOUS_AGGREGATES_SQL).await?;
    execute_sql(pool, TELEMETRY_SQL).await?;
    execute_sql(pool, RECENT_ACTIVITY_SUMMARY_SQL).await?;
    execute_sql(
        pool,
        "DROP VIEW IF EXISTS core.event_temporal_facts, core.derived_scope_summary",
    )
    .await?;
    execute_sql(pool, EVENT_TEMPORAL_FACTS_SQL).await?;
    execute_sql(pool, DERIVED_SCOPE_SUMMARY_SQL).await?;

    Ok(())
}

async fn apply_roles_and_grants(pool: &PgPool) -> Result<(), ApplyError> {
    execute_sql(pool, ROLE_GRANTS_SQL).await?;
    Ok(())
}

async fn execute_sql(pool: &PgPool, sql: &str) -> Result<(), ApplyError> {
    pool.execute(sql).await?;
    Ok(())
}

async fn recreate_telemetry_read_models(pool: &PgPool) -> Result<(), ApplyError> {
    // Schema apply is hash-gated by xtask. When telemetry SQL changes, rebuild the read
    // models decisively so stale materialized view definitions cannot survive.
    execute_sql(
        pool,
        r#"
        DO $$
        DECLARE
            relation_name text;
            relation_kind "char";
            is_continuous_aggregate boolean;
        BEGIN
            FOREACH relation_name IN ARRAY ARRAY[
                'recent_activity_summary',
                'event_engine_batch_stats_1h',
                'file_activity_summary',
                'command_frequency_hourly',
                'current_window_focus',
                'current_system_state',
                'metric_counters_1h',
                'source_stats_1h',
                'assembly_stats_1h',
                'stream_stats_1h',
                'gateway_stats_1h',
                'current_device_state',
                'current_health'
            ]
            LOOP
                SELECT c.relkind
                INTO relation_kind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'sinex_telemetry'
                  AND c.relname = relation_name;

                SELECT EXISTS (
                    SELECT 1
                    FROM timescaledb_information.continuous_aggregates
                    WHERE view_schema = 'sinex_telemetry'
                      AND view_name = relation_name
                )
                INTO is_continuous_aggregate;

                IF is_continuous_aggregate OR relation_kind = 'm' THEN
                    EXECUTE format('DROP MATERIALIZED VIEW sinex_telemetry.%I', relation_name);
                ELSIF relation_kind = 'v' THEN
                    EXECUTE format('DROP VIEW sinex_telemetry.%I', relation_name);
                END IF;
            END LOOP;
        END $$;
        "#,
    )
    .await
}

fn render_table(stmt: &TableCreateStatement) -> String {
    stmt.to_string(PostgresQueryBuilder)
}

fn render_index(mut stmt: IndexCreateStatement) -> String {
    stmt.if_not_exists();
    stmt.to_string(PostgresQueryBuilder)
}

fn render_indexes(stmts: Vec<IndexCreateStatement>) -> Vec<String> {
    stmts.into_iter().map(render_index).collect()
}

pub(crate) async fn relation_exists(
    pool: &PgPool,
    qualified_name: &str,
) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(qualified_name)
        .fetch_one(pool)
        .await?;
    Ok(exists)
}

async fn relation_kind(pool: &PgPool, qualified_name: &str) -> Result<Option<char>, ApplyError> {
    let relation_kind = sqlx::query_scalar::<_, Option<String>>(
        r"
        SELECT c.relkind::text
        FROM pg_class c
        WHERE c.oid = to_regclass($1)
        ",
    )
    .bind(qualified_name)
    .fetch_optional(pool)
    .await?;

    Ok(relation_kind.flatten().and_then(|kind| kind.chars().next()))
}

async fn continuous_aggregate_exists(
    pool: &PgPool,
    schema: &str,
    relation: &str,
) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>(
        r"
        SELECT EXISTS (
            SELECT 1
            FROM timescaledb_information.continuous_aggregates
            WHERE view_schema = $1
              AND view_name = $2
        )
        ",
    )
    .bind(schema)
    .bind(relation)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

async fn trigger_exists(
    pool: &PgPool,
    qualified_table: &str,
    trigger_name: &str,
) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid = to_regclass($1)
              AND tgname = $2
              AND NOT tgisinternal
        )",
    )
    .bind(qualified_table)
    .bind(trigger_name)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

async fn index_exists(
    pool: &PgPool,
    schema: &str,
    table: &str,
    index: &str,
) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = $1
              AND tablename = $2
              AND indexname = $3
        )",
    )
    .bind(schema)
    .bind(table)
    .bind(index)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

const BOOTSTRAP_SQL: &str = r"
CREATE OR REPLACE FUNCTION public.set_current_timestamp_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS sinex_schemas.dlq_events (
    dlq_id UUID PRIMARY KEY DEFAULT uuidv7(),
    failed_event_id UUID NOT NULL,
    automaton_name TEXT NOT NULL,
    agent_name TEXT,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    error_category TEXT NOT NULL CHECK (error_category IN ('retryable','permanent','system','user')),
    failure_reason TEXT NOT NULL,
    original_event_payload JSONB NOT NULL,
    additional_metadata JSONB,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_retry_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    resolved_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_events_automaton ON sinex_schemas.dlq_events (automaton_name);
CREATE INDEX IF NOT EXISTS idx_dlq_events_resolved ON sinex_schemas.dlq_events (resolved_at);
CREATE INDEX IF NOT EXISTS idx_dlq_events_category ON sinex_schemas.dlq_events (error_category);

DROP TRIGGER IF EXISTS set_timestamp ON sinex_schemas.dlq_events;
CREATE TRIGGER set_timestamp
    BEFORE UPDATE ON sinex_schemas.dlq_events
    FOR EACH ROW
    EXECUTE FUNCTION public.set_current_timestamp_updated_at();
";

// Privacy policy schema (#1042). User-controlled, DB-backed redaction policy
// managed via `sinexctl privacy` (CLI deferred to a follow-up) and enforced at
// the event_engine persistence chokepoint. Key MATERIAL never lives in the DB — the
// `encryption_keys` table is a namespace registry only; key bytes resolve from
// env/files via the existing KeyConfig pattern.
const PRIVACY_SCHEMA_SQL: &str = r"
CREATE TABLE IF NOT EXISTS privacy.encryption_keys (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT encryption_keys_name_unique UNIQUE (name),
    CONSTRAINT encryption_keys_name_nonempty CHECK (char_length(name) > 0)
);

CREATE TABLE IF NOT EXISTS privacy.recognizer_backends (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL,
    endpoint_url TEXT,
    config      JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT recognizer_backends_name_unique UNIQUE (name),
    CONSTRAINT recognizer_backends_name_nonempty CHECK (char_length(name) > 0),
    CONSTRAINT recognizer_backends_kind_valid CHECK (
        kind IN (
            'local',
            'presidio',
            'gitleaks',
            'trufflehog',
            'external_http'
        )
    )
);

CREATE TABLE IF NOT EXISTS privacy.dictionaries (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    language    TEXT,
    source_kind TEXT NOT NULL DEFAULT 'user',
    tags        TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT dictionaries_name_unique UNIQUE (name),
    CONSTRAINT dictionaries_name_nonempty CHECK (char_length(name) > 0),
    CONSTRAINT dictionaries_source_kind_valid CHECK (
        source_kind IN ('user', 'seed', 'imported', 'generated')
    )
);

CREATE TABLE IF NOT EXISTS privacy.dictionary_terms (
    id            UUID PRIMARY KEY DEFAULT uuidv7(),
    dictionary_id UUID NOT NULL REFERENCES privacy.dictionaries(id) ON DELETE CASCADE,
    term          TEXT NOT NULL,
    metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT dictionary_terms_nonempty CHECK (char_length(term) > 0),
    CONSTRAINT dictionary_terms_unique UNIQUE (dictionary_id, term)
);

CREATE TABLE IF NOT EXISTS privacy.rules (
    id             UUID PRIMARY KEY DEFAULT uuidv7(),
    name           TEXT NOT NULL,
    description    TEXT NOT NULL DEFAULT '',
    matcher_type   TEXT NOT NULL,
    matcher_value  TEXT NOT NULL,
    matcher_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    recognizer_backend_id UUID REFERENCES privacy.recognizer_backends(id) ON DELETE SET NULL,
    recognizer_kind TEXT NOT NULL DEFAULT 'local_pattern',
    case_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    action         TEXT NOT NULL,
    action_label   TEXT,
    key_namespace  TEXT NOT NULL DEFAULT 'default',
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT rules_name_unique UNIQUE (name),
    CONSTRAINT rules_name_nonempty CHECK (char_length(name) > 0),
    CONSTRAINT rules_matcher_type_valid CHECK (
        matcher_type IN (
            'regex',
            'literal',
            'dictionary',
            'structural',
            'presidio_entity',
            'presidio_analyzer',
            'secret_scanner',
            'external'
        )
    ),
    CONSTRAINT rules_action_valid CHECK (action IN ('redact', 'hash', 'encrypt', 'suppress', 'mask')),
    CONSTRAINT rules_recognizer_kind_valid CHECK (
        recognizer_kind IN (
            'local_pattern',
            'dictionary',
            'presidio_entity',
            'secret_scanner',
            'external'
        )
    )
);

ALTER TABLE privacy.recognizer_backends
    ADD COLUMN IF NOT EXISTS endpoint_url TEXT;

ALTER TABLE privacy.rules
    ADD COLUMN IF NOT EXISTS matcher_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS recognizer_backend_id UUID REFERENCES privacy.recognizer_backends(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS recognizer_kind TEXT NOT NULL DEFAULT 'local_pattern';

ALTER TABLE privacy.rules
    DROP CONSTRAINT IF EXISTS rules_matcher_type_valid,
    DROP CONSTRAINT IF EXISTS rules_action_valid,
    DROP CONSTRAINT IF EXISTS rules_recognizer_kind_valid,
    ADD CONSTRAINT rules_matcher_type_valid CHECK (
        matcher_type IN (
            'regex',
            'literal',
            'dictionary',
            'structural',
            'presidio_entity',
            'presidio_analyzer',
            'secret_scanner',
            'external'
        )
    ),
    ADD CONSTRAINT rules_action_valid CHECK (action IN ('redact', 'hash', 'encrypt', 'suppress', 'mask')),
    ADD CONSTRAINT rules_recognizer_kind_valid CHECK (
        recognizer_kind IN (
            'local_pattern',
            'dictionary',
            'presidio_entity',
            'secret_scanner',
            'external'
        )
    );

CREATE TABLE IF NOT EXISTS privacy.field_rules (
    id           UUID PRIMARY KEY DEFAULT uuidv7(),
    rule_id      UUID NOT NULL REFERENCES privacy.rules(id) ON DELETE CASCADE,
    event_source TEXT,
    event_type   TEXT,
    field_path   TEXT,
    priority     INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT field_rules_scope_unique
        UNIQUE NULLS NOT DISTINCT (rule_id, event_source, event_type, field_path)
);

CREATE INDEX IF NOT EXISTS ix_field_rules_scope
    ON privacy.field_rules (event_source, event_type);

CREATE INDEX IF NOT EXISTS ix_privacy_rules_backend
    ON privacy.rules (recognizer_backend_id);

CREATE INDEX IF NOT EXISTS ix_privacy_dictionary_terms_dictionary
    ON privacy.dictionary_terms (dictionary_id);

CREATE INDEX IF NOT EXISTS ix_privacy_dictionary_terms_term
    ON privacy.dictionary_terms (term);

DROP TRIGGER IF EXISTS trg_privacy_recognizer_backends_updated_at ON privacy.recognizer_backends;
CREATE TRIGGER trg_privacy_recognizer_backends_updated_at
    BEFORE UPDATE ON privacy.recognizer_backends
    FOR EACH ROW
    EXECUTE FUNCTION public.set_current_timestamp_updated_at();

DROP TRIGGER IF EXISTS trg_privacy_dictionaries_updated_at ON privacy.dictionaries;
CREATE TRIGGER trg_privacy_dictionaries_updated_at
    BEFORE UPDATE ON privacy.dictionaries
    FOR EACH ROW
    EXECUTE FUNCTION public.set_current_timestamp_updated_at();

DROP TRIGGER IF EXISTS trg_privacy_rules_updated_at ON privacy.rules;
CREATE TRIGGER trg_privacy_rules_updated_at
    BEFORE UPDATE ON privacy.rules
    FOR EACH ROW
    EXECUTE FUNCTION public.set_current_timestamp_updated_at();
";

const OPERATIONS_AND_CASCADE_SQL: &str = r"
CREATE OR REPLACE FUNCTION core.start_operation(p_operation_type TEXT, p_operator TEXT, p_scope JSONB, p_scope_window tstzrange DEFAULT NULL)
RETURNS UUID AS $$
DECLARE
    v_operation_id UUID;
BEGIN
    IF p_operation_type NOT IN ('replay', 'archive', 'restore', 'purge', 'tombstone') THEN
        RAISE EXCEPTION 'Unsupported managed operation type: %', p_operation_type
            USING ERRCODE = '22023';
    END IF;
    v_operation_id := uuidv7();
    INSERT INTO core.operations_log (id, operation_type, operator, scope, scope_window, result_status)
    VALUES (v_operation_id, p_operation_type, p_operator, p_scope, p_scope_window, 'running');
    RETURN v_operation_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.complete_operation(p_operation_id UUID, p_summary JSONB)
RETURNS VOID AS $$
DECLARE
    v_rows_updated integer;
BEGIN
    UPDATE core.operations_log
    SET result_status = 'success',
        result_message = p_summary->>'message',
        duration_ms = COALESCE(
            duration_ms,
            (EXTRACT(EPOCH FROM (NOW() - uuid_extract_timestamp(p_operation_id))) * 1000)::integer
        ),
        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_summary
    WHERE id = p_operation_id;
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    IF v_rows_updated = 0 THEN
        RAISE EXCEPTION 'operation % not found', p_operation_id USING ERRCODE = 'P0002';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.fail_operation(p_operation_id UUID, p_error JSONB)
RETURNS VOID AS $$
DECLARE
    v_rows_updated integer;
BEGIN
    UPDATE core.operations_log
    SET result_status = 'failure',
        result_message = p_error->>'error',
        duration_ms = COALESCE(
            duration_ms,
            (EXTRACT(EPOCH FROM (NOW() - uuid_extract_timestamp(p_operation_id))) * 1000)::integer
        ),
        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_error
    WHERE id = p_operation_id;
    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    IF v_rows_updated = 0 THEN
        RAISE EXCEPTION 'operation % not found', p_operation_id USING ERRCODE = 'P0002';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.prepare_cascade_session(p_session_id TEXT, p_drop_on_commit BOOLEAN DEFAULT FALSE)
RETURNS TEXT AS $$
DECLARE
    v_table TEXT := format('cascade_analysis_%s', p_session_id);
    v_clause TEXT := CASE WHEN p_drop_on_commit THEN ' ON COMMIT DROP' ELSE '' END;
BEGIN
    IF p_session_id !~ '^[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid session identifier: %', p_session_id;
    END IF;

    EXECUTE format(
        'CREATE TEMP TABLE IF NOT EXISTS %I (
            id UUID PRIMARY KEY,
            depth INT NOT NULL DEFAULT 0,
            parent_ids UUID[] DEFAULT ''{}''::UUID[],
            child_ids UUID[],
            is_archived BOOLEAN DEFAULT FALSE,
            is_live BOOLEAN DEFAULT TRUE,
            processed BOOLEAN DEFAULT FALSE
        )%s',
        v_table,
        v_clause
    );

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (depth)', 'idx_' || v_table || '_depth', v_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (processed)', 'idx_' || v_table || '_processed', v_table);

    RETURN v_table;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.cascade_populate_roots(p_table TEXT, p_event_ids UUID[])
RETURNS BIGINT AS $$
DECLARE
    v_sql TEXT;
    v_rows BIGINT;
BEGIN
    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
    END IF;

    v_sql := format(
        'INSERT INTO %I (id, depth, parent_ids, processed)
         SELECT e.id, 0, COALESCE(e.source_event_ids, ''{}''::UUID[]), FALSE
         FROM core.events e
         WHERE e.id = ANY($1::uuid[])
         ON CONFLICT DO NOTHING',
        p_table
    );
    EXECUTE v_sql USING p_event_ids;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN COALESCE(v_rows, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.cascade_count_nodes(p_table TEXT)
RETURNS BIGINT AS $$
DECLARE
    v_sql TEXT;
    v_count BIGINT;
BEGIN
    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
    END IF;

    v_sql := format('SELECT COUNT(*) FROM %I', p_table);
    EXECUTE v_sql INTO v_count;
    RETURN COALESCE(v_count, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.cascade_depth_histogram(p_table TEXT)
RETURNS TABLE(depth INT, node_count BIGINT) AS $$
DECLARE
    v_sql TEXT;
BEGIN
    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
    END IF;

    v_sql := format('SELECT depth, COUNT(*) AS node_count FROM %I GROUP BY depth ORDER BY depth', p_table);
    RETURN QUERY EXECUTE v_sql;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.cascade_find_integrity_violations(p_table TEXT, p_limit INTEGER DEFAULT 100)
RETURNS TABLE(live_event_id UUID, archived_event_id UUID) AS $$
DECLARE
    v_sql TEXT;
BEGIN
    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
    END IF;

    v_sql := format(
        'WITH archived_set AS (
            SELECT id FROM %I WHERE depth = 0
        ),
        violations AS (
            SELECT e.id AS live_event_id, unnest(e.source_event_ids) AS archived_event_id
            FROM core.events e
            WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived_set)
              AND e.id NOT IN (SELECT id FROM %I)
        )
        SELECT DISTINCT live_event_id, archived_event_id FROM violations LIMIT $1',
        p_table,
        p_table
    );

    RETURN QUERY EXECUTE v_sql USING p_limit;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.cascade_find_integrity_violations_paginated(
    p_table TEXT,
    p_limit INTEGER DEFAULT 1000,
    p_offset INTEGER DEFAULT 0
)
RETURNS TABLE(live_event_id UUID, archived_event_id UUID) AS $$
DECLARE
    v_sql TEXT;
BEGIN
    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
    END IF;

    v_sql := format(
        'WITH archived_set AS (
            SELECT id FROM %I WHERE depth = 0
        ),
        violations AS (
            SELECT e.id AS live_event_id, unnest(e.source_event_ids) AS archived_event_id
            FROM core.events e
            WHERE e.source_event_ids && (SELECT array_agg(id) FROM archived_set)
              AND e.id NOT IN (SELECT id FROM %I)
        )
        SELECT DISTINCT live_event_id, archived_event_id FROM violations LIMIT $1 OFFSET $2',
        p_table,
        p_table
    );

    RETURN QUERY EXECUTE v_sql USING p_limit, p_offset;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.cleanup_cascade_session(p_table TEXT)
RETURNS VOID AS $$
BEGIN
    IF p_table IS NULL OR p_table = '' THEN
        RETURN;
    END IF;
    IF p_table !~ '^cascade_analysis_[A-Za-z0-9_]+$' THEN
        RAISE EXCEPTION 'Invalid cascade table name: %', p_table;
    END IF;
    EXECUTE format('DROP TABLE IF EXISTS %I', p_table);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.expand_cascade(temp_table TEXT, max_depth INTEGER)
RETURNS INTEGER AS $$
DECLARE
    current_depth INTEGER := 0;
    rows_inserted INTEGER;
    pending_at_limit INTEGER;
BEGIN
    LOOP
        IF current_depth >= max_depth THEN
            -- Probe whether we would have inserted anything at the next depth.
            -- If yes, the cascade exceeds the configured limit and we MUST
            -- raise rather than silently truncate; the caller's preview/audit
            -- surfaces depend on this signal to stay honest.
            EXECUTE format(
                'WITH current_level AS (
                    SELECT id FROM %I WHERE depth = $1 AND processed = FALSE
                )
                SELECT COUNT(*)::INTEGER FROM core.events e
                JOIN current_level cl ON e.source_event_ids && ARRAY[cl.id]
                WHERE NOT EXISTS (SELECT 1 FROM %I existing WHERE existing.id = e.id)',
                temp_table,
                temp_table
            ) INTO pending_at_limit USING current_depth;

            IF pending_at_limit > 0 THEN
                RAISE EXCEPTION 'cascade exceeds max depth % (% pending children at limit)',
                    max_depth, pending_at_limit
                    USING ERRCODE = 'P0001';
            END IF;

            EXIT;
        END IF;

        EXECUTE format(
            'WITH current_level AS (
                SELECT id FROM %I WHERE depth = $1 AND processed = FALSE
            ),
            children AS (
                SELECT DISTINCT e.id, COALESCE(e.source_event_ids, ''{}''::uuid[]) AS parent_ids
                FROM core.events e
                JOIN current_level cl ON e.source_event_ids && ARRAY[cl.id]
                WHERE NOT EXISTS (SELECT 1 FROM %I existing WHERE existing.id = e.id)
            )
            INSERT INTO %I (id, depth, parent_ids, processed)
            SELECT c.id, $1 + 1, c.parent_ids, FALSE FROM children c',
            temp_table,
            temp_table,
            temp_table
        ) USING current_depth;

        GET DIAGNOSTICS rows_inserted = ROW_COUNT;
        EXECUTE format('UPDATE %I SET processed = TRUE WHERE depth = $1', temp_table)
            USING current_depth;

        EXIT WHEN rows_inserted = 0;
        current_depth := current_depth + 1;
    END LOOP;

    RETURN current_depth;
END;
$$ LANGUAGE plpgsql;
";

const TOMBSTONE_LIFECYCLE_SQL: &str = r"
CREATE OR REPLACE FUNCTION core.execute_cascade_tombstone(
    p_archived_ids UUID[],
    p_reason TEXT,
    p_operation_id UUID
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    IF p_archived_ids IS NULL OR array_length(p_archived_ids, 1) IS NULL THEN
        RETURN 0;
    END IF;

    INSERT INTO core.event_tombstones (
        id, source, event_type, ts_orig, ts_purged,
        purge_reason, purge_operation_id, archived_at
    )
    SELECT
        ae.id,
        ae.source,
        ae.event_type,
        ae.ts_orig,
        now(),
        p_reason,
        p_operation_id,
        ae.archived_at
    FROM audit.archived_events ae
    WHERE ae.id = ANY(p_archived_ids)
    ON CONFLICT (id) DO NOTHING;

    GET DIAGNOSTICS v_count = ROW_COUNT;

    DELETE FROM audit.archived_events
    WHERE id = ANY(p_archived_ids);

    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION core.execute_cascade_restore(
    p_archived_ids UUID[],
    p_operation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_restored_count BIGINT;
BEGIN
    IF p_archived_ids IS NULL OR array_length(p_archived_ids, 1) IS NULL THEN
        RETURN 0;
    END IF;

    PERFORM pg_catalog.set_config('sinex.operation_id', p_operation_id, true);
    PERFORM pg_catalog.set_config('sinex.archive_reason', 'restored from archive', true);

    -- Defensive: drop temp table from a prior failed call in this session.
    DROP TABLE IF EXISTS _restored_ids;

    -- Step 1: Restore events into a temp table to capture exactly which IDs
    -- were actually inserted. ON CONFLICT DO NOTHING skips IDs already in
    -- core.events; those must NOT be deleted from the archive (#1134).
    CREATE TEMP TABLE _restored_ids (id UUID PRIMARY KEY) ON COMMIT DROP;

    WITH inserted AS (
        INSERT INTO core.events (
            id, source, event_type, host, payload,
            ts_orig, ts_orig_subnano,
            source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
            source_event_ids, associated_blob_ids,
            payload_schema_id, module_run_id,
            temporal_policy, semantics_version, scope_key, equivalence_key,
            created_by_operation_id, automaton_model
        )
        SELECT
            ae.id, ae.source, ae.event_type, ae.host, ae.payload,
            ae.ts_orig, ae.ts_orig_subnano,
            ae.source_material_id, ae.anchor_byte, ae.offset_start, ae.offset_end, ae.offset_kind,
            ae.source_event_ids, ae.associated_blob_ids,
            ae.payload_schema_id, ae.module_run_id,
            ae.temporal_policy, ae.semantics_version, ae.scope_key, ae.equivalence_key,
            ae.created_by_operation_id, ae.automaton_model
        FROM audit.archived_events ae
        WHERE ae.id = ANY(p_archived_ids)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    )
    INSERT INTO _restored_ids SELECT id FROM inserted;

    SELECT count(*)::bigint INTO v_restored_count FROM _restored_ids;

    -- Step 2: Restore side-table state for the events we actually restored.
    -- Annotations.
    INSERT INTO core.event_annotations (
        id, event_id, annotation_type, content, metadata, created_at, updated_at
    )
    SELECT aa.id, aa.event_id, aa.annotation_type, aa.content, aa.metadata,
           aa.created_at, aa.updated_at
    FROM audit.archived_annotations aa
    JOIN _restored_ids r ON aa.event_id = r.id
    ON CONFLICT (id) DO NOTHING;

    -- Embeddings. core.event_embeddings has no created_at/updated_at columns
    -- (only id, event_id, embedding_model_id, embedded_text, embedding). The
    -- archived table is LIKE core.event_embeddings INCLUDING ALL plus audit
    -- columns, so it carries the same shape.
    INSERT INTO core.event_embeddings (
        id, event_id, embedding_model_id, embedded_text, embedding
    )
    SELECT aem.id, aem.event_id, aem.embedding_model_id, aem.embedded_text, aem.embedding
    FROM audit.archived_embeddings aem
    JOIN _restored_ids r ON aem.event_id = r.id
    ON CONFLICT (id) DO NOTHING;

    -- Tagged items. core.tagged_items has columns:
    -- (tag_id, item_id, item_type, tagged_at) — composite primary key, no `id`.
    -- The archived table is LIKE core.tagged_items INCLUDING ALL plus audit
    -- columns, so it carries the same shape.
    INSERT INTO core.tagged_items (
        tag_id, item_id, item_type, tagged_at
    )
    SELECT ati.tag_id, ati.item_id, ati.item_type, ati.tagged_at
    FROM audit.archived_tagged_items ati
    JOIN _restored_ids r ON ati.item_id = r.id
    WHERE ati.item_type = 'event'
    ON CONFLICT (tag_id, item_id, item_type) DO NOTHING;

    -- Step 3: Delete only the archive rows that were actually restored.
    -- Rows where ON CONFLICT DO NOTHING fired stay in the archive.
    DELETE FROM audit.archived_events
    WHERE id IN (SELECT id FROM _restored_ids);

    -- Clean up archive side-tables for restored event IDs.
    DELETE FROM audit.archived_annotations
    WHERE event_id IN (SELECT id FROM _restored_ids);

    DELETE FROM audit.archived_embeddings
    WHERE event_id IN (SELECT id FROM _restored_ids);

    DELETE FROM audit.archived_tagged_items
    WHERE item_id IN (SELECT id FROM _restored_ids) AND item_type = 'event';

    DROP TABLE _restored_ids;
    RETURN v_restored_count;
END;
$$;

CREATE OR REPLACE FUNCTION core.lifecycle_tier_status()
RETURNS TABLE (
    tier TEXT,
    event_count BIGINT,
    oldest_ts TIMESTAMPTZ,
    newest_ts TIMESTAMPTZ,
    distinct_sources BIGINT
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        'live'::TEXT as tier,
        COUNT(*) as event_count,
        MIN(ts_orig) as oldest_ts,
        MAX(ts_orig) as newest_ts,
        COUNT(DISTINCT source) as distinct_sources
    FROM core.events

    UNION ALL

    SELECT
        'archive'::TEXT as tier,
        COUNT(*) as event_count,
        MIN(ts_orig) as oldest_ts,
        MAX(ts_orig) as newest_ts,
        COUNT(DISTINCT source) as distinct_sources
    FROM audit.archived_events

    UNION ALL

    SELECT
        'tombstone'::TEXT as tier,
        COUNT(*) as event_count,
        MIN(ts_orig) as oldest_ts,
        MAX(ts_orig) as newest_ts,
        COUNT(DISTINCT source) as distinct_sources
    FROM core.event_tombstones;
$$;
";

const JSONB_MERGE_SQL: &str = r"
CREATE OR REPLACE FUNCTION core.jsonb_merge_deep(a jsonb, b jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN a IS NULL THEN b
        WHEN b IS NULL THEN a
        WHEN jsonb_typeof(a) = 'object' AND jsonb_typeof(b) = 'object' THEN
            (
                SELECT
                    jsonb_object_agg(
                        k,
                        CASE
                            WHEN e2.value IS NULL THEN e1.value
                            WHEN e1.value IS NULL THEN e2.value
                            ELSE core.jsonb_merge_deep(e1.value, e2.value)
                        END
                    )
                FROM jsonb_each(a) e1(k, value)
                FULL JOIN jsonb_each(b) e2(k, value) USING (k)
            )
        ELSE b
    END
$$;
";

const EMBEDDING_INDEX_MANAGEMENT_SQL: &str = r"
CREATE OR REPLACE FUNCTION core.create_embedding_model_index(
    p_model_id UUID,
    p_dimensions INT
) RETURNS void AS $$
DECLARE
    event_idx_name TEXT;
    cache_idx_name TEXT;
    model_id_str TEXT;
BEGIN
    model_id_str := replace(p_model_id::text, '-', '_');
    event_idx_name := 'ix_event_embeddings_hnsw_' || model_id_str;
    cache_idx_name := 'ix_embedding_cache_hnsw_' || model_id_str;

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON core.event_embeddings
         USING hnsw ((embedding::vector(%s)) vector_cosine_ops)
         WHERE embedding_model_id = %L',
        event_idx_name, p_dimensions, p_model_id
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON core.embedding_cache
         USING hnsw ((embedding::vector(%s)) vector_cosine_ops)
         WHERE embedding_model_id = %L',
        cache_idx_name, p_dimensions, p_model_id
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.drop_embedding_model_index(
    p_model_id UUID
) RETURNS void AS $$
DECLARE
    event_idx_name TEXT;
    cache_idx_name TEXT;
    model_id_str TEXT;
BEGIN
    model_id_str := replace(p_model_id::text, '-', '_');
    event_idx_name := 'ix_event_embeddings_hnsw_' || model_id_str;
    cache_idx_name := 'ix_embedding_cache_hnsw_' || model_id_str;

    EXECUTE format('DROP INDEX IF EXISTS core.%I', event_idx_name);
    EXECUTE format('DROP INDEX IF EXISTS core.%I', cache_idx_name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.embedding_model_index_trigger() RETURNS TRIGGER AS $$
BEGIN
    PERFORM core.create_embedding_model_index(NEW.id, NEW.dimensions);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_embedding_model_create_index ON core.embedding_models;
CREATE TRIGGER trg_embedding_model_create_index
    AFTER INSERT ON core.embedding_models
    FOR EACH ROW
    EXECUTE FUNCTION core.embedding_model_index_trigger();

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT id, dimensions FROM core.embedding_models LOOP
        PERFORM core.create_embedding_model_index(r.id, r.dimensions);
    END LOOP;
END $$;
";

const HYBRID_SEARCH_SQL: &str = r"
CREATE OR REPLACE FUNCTION core.hybrid_search(
    p_query_text    TEXT,
    p_query_vector  vector,
    p_model_id      UUID,
    p_limit         INT DEFAULT 20,
    p_ef_search     INT DEFAULT 100,
    p_rrf_k         FLOAT8 DEFAULT 60.0,
    p_vector_weight FLOAT8 DEFAULT 0.7,
    p_text_weight   FLOAT8 DEFAULT 0.3
) RETURNS TABLE (
    event_id        UUID,
    rrf_score       FLOAT8,
    vector_rank     INT,
    text_rank       INT,
    cosine_distance FLOAT8,
    text_similarity FLOAT8
) LANGUAGE plpgsql AS $$
DECLARE
    v_fetch_limit INT;
BEGIN
    IF p_limit <= 0 THEN
        RAISE EXCEPTION 'p_limit must be positive';
    END IF;
    IF p_ef_search <= 0 THEN
        RAISE EXCEPTION 'p_ef_search must be positive';
    END IF;
    IF p_rrf_k <= 0 THEN
        RAISE EXCEPTION 'p_rrf_k must be positive';
    END IF;

    v_fetch_limit := p_limit * 5;
    PERFORM set_config('hnsw.ef_search', p_ef_search::text, true);

    RETURN QUERY
    WITH vector_results AS (
        SELECT
            ee.event_id,
            (ee.embedding <=> p_query_vector)::FLOAT8 AS cosine_dist,
            ROW_NUMBER() OVER (ORDER BY ee.embedding <=> p_query_vector)::INT AS vrank
        FROM core.event_embeddings ee
        WHERE ee.embedding_model_id = p_model_id
        ORDER BY ee.embedding <=> p_query_vector
        LIMIT v_fetch_limit
    ),
    text_results AS (
        SELECT
            e.id AS event_id,
            GREATEST(
                CASE
                    WHEN p_query_text = '' THEN 0.0::FLOAT8
                    ELSE word_similarity(p_query_text, e.payload::text)::FLOAT8
                END,
                CASE
                    WHEN p_query_text = '' THEN 0.0::FLOAT8
                    ELSE ts_rank_cd(
                        to_tsvector('simple', e.payload::text),
                        websearch_to_tsquery('simple', p_query_text)
                    )::FLOAT8
                END
            ) AS text_sim,
            ROW_NUMBER() OVER (ORDER BY GREATEST(
                CASE
                    WHEN p_query_text = '' THEN 0.0::FLOAT8
                    ELSE word_similarity(p_query_text, e.payload::text)::FLOAT8
                END,
                CASE
                    WHEN p_query_text = '' THEN 0.0::FLOAT8
                    ELSE ts_rank_cd(
                        to_tsvector('simple', e.payload::text),
                        websearch_to_tsquery('simple', p_query_text)
                    )::FLOAT8
                END
            ) DESC, e.id ASC)::INT AS trank
        FROM core.events e
        WHERE e.id IN (SELECT vr.event_id FROM vector_results vr)
           OR (
               p_query_text <> ''
               AND to_tsvector('simple', e.payload::text) @@ websearch_to_tsquery('simple', p_query_text)
           )
        LIMIT v_fetch_limit
    ),
    fused AS (
        SELECT
            COALESCE(vr.event_id, tr.event_id) AS event_id,
            (p_vector_weight / (p_rrf_k + COALESCE(vr.vrank, v_fetch_limit + 1)))
          + (p_text_weight  / (p_rrf_k + COALESCE(tr.trank, v_fetch_limit + 1))) AS rrf_score,
            COALESCE(vr.vrank, v_fetch_limit + 1)::INT AS vector_rank,
            COALESCE(tr.trank, v_fetch_limit + 1)::INT AS text_rank,
            COALESCE(vr.cosine_dist, 1.0)::FLOAT8 AS cosine_distance,
            COALESCE(tr.text_sim, 0.0)::FLOAT8 AS text_similarity
        FROM vector_results vr
        FULL OUTER JOIN text_results tr ON vr.event_id = tr.event_id
    )
    SELECT
        fused.event_id,
        fused.rrf_score,
        fused.vector_rank,
        fused.text_rank,
        fused.cosine_distance,
        fused.text_similarity
    FROM fused
    ORDER BY fused.rrf_score DESC, fused.event_id ASC
    LIMIT p_limit;
END;
$$;
";

const TELEMETRY_SQL: &str = r"
CREATE OR REPLACE VIEW sinex_telemetry.current_health AS
SELECT DISTINCT ON (e.source, e.payload->>'component')
    e.source,
    e.event_type,
    e.payload->>'component' AS component,
    e.payload->>'current_status' AS status,
    e.payload->>'reason' AS reason,
    e.ts_coided AS last_update
FROM core.events e
WHERE e.source = 'sinex'
  AND e.event_type = 'health.status'
  AND e.ts_coided > NOW() - INTERVAL '1 hour'
ORDER BY e.source, e.payload->>'component', e.ts_coided DESC, e.id DESC;

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.current_device_state AS
SELECT DISTINCT ON (payload->>'unit_name')
    payload->>'unit_name' AS unit_name,
    payload->>'unit_type' AS unit_type,
    payload->>'active_state' AS state,
    payload->>'sub_state' AS sub_state,
    ts_coided AS last_update
FROM core.events
WHERE event_type IN ('systemd.unit.started', 'systemd.unit.stopped', 'systemd.unit.failed', 'systemd.unit.reloaded', 'systemd.unit.status', 'systemd.unit.starting', 'systemd.unit.stopping', 'systemd.unit.state_changed')
  AND source = 'systemd'
  AND ts_coided > NOW() - INTERVAL '7 days'
ORDER BY payload->>'unit_name', ts_coided DESC;

CREATE INDEX IF NOT EXISTS ix_current_device_state_unit_name
    ON sinex_telemetry.current_device_state (unit_name);
CREATE INDEX IF NOT EXISTS ix_current_device_state_state
    ON sinex_telemetry.current_device_state (state);
";

const TELEMETRY_CONTINUOUS_AGGREGATES_SQL: &str = r"
CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.gateway_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    source,
    COUNT(*) FILTER (WHERE event_type = 'request.stats') AS stat_events,
    AVG((payload->>'total_requests')::bigint) AS avg_total_requests,
    SUM((payload->>'rate_limited_requests')::bigint) AS total_rate_limited,
    AVG((payload->>'avg_latency_ms')::float) AS avg_latency_ms,
    MAX((payload->>'p99_latency_ms')::float) AS max_p99_latency_ms
FROM core.events
WHERE source LIKE 'sinexd.api%'
  AND event_type IN ('request.stats', 'rate_limit.exceeded', 'replay.stats')
GROUP BY time_bucket('1 hour', id), source
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.gateway_stats_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.stream_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    payload->>'stream' AS stream_name,
    AVG((payload->>'fill_pct')::float) AS avg_fill_pct,
    MAX((payload->>'fill_pct')::float) AS max_fill_pct,
    AVG((payload->>'messages')::bigint) AS avg_messages,
    MAX((payload->>'max_messages')::bigint) AS max_messages,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinexd.event_engine'
  AND event_type = 'stream.stats'
GROUP BY time_bucket('1 hour', id), payload->>'stream'
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.stream_stats_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.assembly_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    MAX((payload->>'active_assemblies')::int) AS max_active_assemblies,
    SUM((payload->>'total_completed')::bigint) AS total_completed,
    SUM((payload->>'total_cancelled')::bigint) AS total_cancelled,
    SUM((payload->>'total_failed')::bigint) AS total_failed,
    SUM((payload->>'total_timed_out')::bigint) AS total_timed_out,
    AVG((payload->>'avg_duration_ms')::float) AS avg_duration_ms,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinexd.event_engine'
  AND event_type = 'assembly.stats'
GROUP BY time_bucket('1 hour', id)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.assembly_stats_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.source_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    payload->>'module_kind' AS module_kind,
    SUM((payload->>'events_processed')::bigint) AS total_events_processed,
    SUM((payload->>'events_dropped')::bigint) AS total_events_dropped,
    AVG((payload->>'avg_latency_ms')::float) AS avg_latency_ms,
    MAX((payload->>'queue_depth')::int) AS max_queue_depth,
    SUM((payload->>'error_count')::bigint) AS total_errors,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinexd.source'
  AND event_type = 'processing.stats'
GROUP BY time_bucket('1 hour', id), payload->>'module_kind'
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.source_stats_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.metric_counters_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    payload->>'component' AS component,
    payload->>'name' AS metric_name,
    SUM((payload->>'value')::bigint) AS total_value,
    MAX((payload->>'value')::bigint) AS max_value,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinex'
  AND event_type = 'metric.counter'
GROUP BY time_bucket('1 hour', id), payload->>'component', payload->>'name'
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.metric_counters_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.event_engine_batch_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    AVG((payload->>'batch_size')::int) AS avg_batch_size,
    MAX((payload->>'batch_size')::int) AS max_batch_size,
    AVG((payload->>'fetch_to_ack_ms')::float) AS avg_latency_ms,
    MAX((payload->>'fetch_to_ack_ms')::float) AS max_latency_ms,
    SUM((payload->>'events_deferred')::int) AS total_deferred,
    SUM((payload->>'events_failed')::int) AS total_failed,
    COUNT(*) FILTER (WHERE (payload->>'had_derived')::boolean) AS derived_batches,
    COUNT(*) AS batch_count,
    MAX((payload->>'validation_valid')::bigint) AS validation_valid,
    MAX((payload->>'validation_skipped')::bigint) AS validation_skipped,
    MAX((payload->>'validation_no_schema')::bigint) AS validation_no_schema,
    MAX((payload->>'validation_schema_not_found')::bigint) AS validation_schema_not_found,
    MAX((payload->>'validation_invalid')::bigint) AS validation_invalid,
    AVG((payload->>'validation_coverage_pct')::float) AS avg_validation_coverage_pct
FROM core.events
WHERE source = 'sinexd.event_engine'
  AND event_type = 'batch.stats'
GROUP BY time_bucket('1 hour', id)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.event_engine_batch_stats_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.current_window_focus
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', id) AS bucket,
    payload->>'workspace_id' AS workspace,
    last(payload->>'window_class', ts_orig) AS window_class,
    last(payload->>'window_title', ts_orig) AS window_title,
    last(payload->>'window_id', ts_orig) AS window_id,
    MAX(ts_orig) AS last_focus_time,
    COUNT(*) AS focus_event_count
FROM core.events
WHERE event_type = 'window.focused'
  AND source LIKE 'wm.%'
GROUP BY time_bucket('5 minutes', id), payload->>'workspace_id'
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.current_window_focus',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.command_frequency_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    LEFT(COALESCE(payload->>'command', payload->>'command_string'), 500) AS command,
    CASE
        WHEN source = 'shell.kitty' THEN COALESCE(payload->>'shell_type', 'kitty')
        WHEN source = 'shell.atuin' THEN 'atuin'
        WHEN source LIKE 'shell.history.%' THEN regexp_replace(source, '^shell\.history\.', '')
        ELSE NULL
    END AS shell,
    COUNT(*) AS total_executions,
    COUNT(*) FILTER (
        WHERE COALESCE((payload->>'exit_code')::int, (payload->>'exit_status')::int) = 0
    ) AS successful_executions,
    COUNT(*) FILTER (
        WHERE COALESCE((payload->>'exit_code')::int, (payload->>'exit_status')::int) IS NOT NULL
          AND COALESCE((payload->>'exit_code')::int, (payload->>'exit_status')::int) != 0
    ) AS failed_executions,
    AVG(
        COALESCE(
            (payload->>'duration_ms')::float,
            (payload->>'execution_time_ms')::float,
            (payload->>'duration_ns')::float / 1000000.0
        )
    ) AS avg_duration_ms
FROM core.events
WHERE event_type = 'command.executed'
  AND (
      source = 'shell.kitty'
      OR source = 'shell.atuin'
      OR source LIKE 'shell.history.%'
  )
  AND COALESCE(payload->>'command', payload->>'command_string') IS NOT NULL
GROUP BY
    time_bucket('1 hour', id),
    LEFT(COALESCE(payload->>'command', payload->>'command_string'), 500),
    CASE
        WHEN source = 'shell.kitty' THEN COALESCE(payload->>'shell_type', 'kitty')
        WHEN source = 'shell.atuin' THEN 'atuin'
        WHEN source LIKE 'shell.history.%' THEN regexp_replace(source, '^shell\.history\.', '')
        ELSE NULL
    END
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.command_frequency_hourly',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.file_activity_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', id) AS bucket,
    regexp_replace(payload->>'path', '/[^/]*$', '') AS directory,
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT payload->>'path') AS unique_files
FROM core.events
WHERE event_type IN ('file.created', 'file.modified', 'file.deleted')
  AND source = 'fs-watcher'
GROUP BY time_bucket('1 hour', id), regexp_replace(payload->>'path', '/[^/]*$', ''), event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.file_activity_summary',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

CREATE MATERIALIZED VIEW IF NOT EXISTS sinex_telemetry.current_system_state
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', id) AS bucket,
    AVG((payload->>'cpu_percent')::float8) AS avg_cpu_percent,
    MAX((payload->>'cpu_percent')::float8) AS max_cpu_percent,
    AVG((payload->>'memory_percent')::float8) AS avg_memory_percent,
    MAX((payload->>'memory_percent')::float8) AS max_memory_percent,
    AVG((payload->>'disk_percent')::float8) AS avg_disk_percent,
    MAX((payload->>'active_units')::bigint) AS current_active_units,
    COUNT(*) AS sample_count
FROM core.events
WHERE (source = 'system.monitor' AND event_type = 'system.resources')
   OR (source = 'system.systemd' AND event_type = 'systemd.units_summary')
GROUP BY time_bucket('5 minutes', id)
WITH NO DATA;

SELECT add_continuous_aggregate_policy('sinex_telemetry.current_system_state',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');
";

const RECENT_ACTIVITY_SUMMARY_SQL: &str = r"
CREATE OR REPLACE VIEW sinex_telemetry.recent_activity_summary AS
(SELECT
    'window_focus' AS activity_type,
    workspace AS context,
    window_class AS detail,
    last_focus_time AS timestamp
 FROM sinex_telemetry.current_window_focus
 WHERE bucket >= NOW() - INTERVAL '30 minutes'
 ORDER BY bucket DESC
 LIMIT 1)

	UNION ALL

	(SELECT
	    'command_execution' AS activity_type,
	    shell AS context,
	    command AS detail,
	    bucket AS timestamp
	 FROM sinex_telemetry.command_frequency_hourly
	 WHERE bucket >= NOW() - INTERVAL '1 hour'
	 ORDER BY total_executions DESC
	 LIMIT 5)

	UNION ALL

	(SELECT
	    'system_load' AS activity_type,
	    'cpu' AS context,
	    avg_cpu_percent::text AS detail,
	    bucket AS timestamp
	 FROM sinex_telemetry.current_system_state
	 WHERE bucket >= NOW() - INTERVAL '30 minutes'
	 ORDER BY bucket DESC
	 LIMIT 1);
	";

/// Unified read surface for event temporal provenance.
///
/// Material events derive timing metadata by joining through their `source_material_id`
/// to `raw.temporal_ledger`. Synthetic events carry inline metadata directly.
/// This view provides a single queryable surface for "why does this event have this time?"
const EVENT_TEMPORAL_FACTS_SQL: &str = r"
CREATE OR REPLACE VIEW core.event_temporal_facts AS

-- Material events: derive temporal facts from the temporal ledger
SELECT
    e.id AS event_id,
    'material' AS provenance_kind,
    e.source,
    e.event_type,
    e.ts_orig,
    tl.ts_capture AS ts_capture,
    tl.source_type AS temporal_source_type,
    tl.precision AS temporal_precision,
    tl.clock AS temporal_clock,
    NULL::text AS temporal_policy,
    NULL::text AS semantics_version,
    NULL::text AS scope_key,
    NULL::text AS equivalence_key,
    NULL::uuid AS created_by_operation_id,
    NULL::text AS automaton_model
FROM core.events e
INNER JOIN LATERAL (
    SELECT
        tl.ts_capture,
        tl.source_type,
        tl.precision,
        tl.clock
    FROM raw.temporal_ledger tl
    WHERE tl.source_material_id = e.source_material_id
      AND tl.offset_start <= e.anchor_byte
      AND tl.offset_end > e.anchor_byte
    ORDER BY
        CASE tl.source_type
            WHEN 'realtime_capture' THEN 0
            WHEN 'intrinsic_content' THEN 1
            WHEN 'inferred_mtime' THEN 2
            WHEN 'inferred_ctime' THEN 3
            WHEN 'inferred_user' THEN 4
            WHEN 'staged_at' THEN 5
            ELSE 99
        END,
        (tl.offset_end - tl.offset_start) ASC,
        tl.ts_capture DESC
    LIMIT 1
) tl ON TRUE
WHERE e.source_material_id IS NOT NULL

UNION ALL

-- Synthetic events: read inline metadata directly
SELECT
    e.id AS event_id,
    'synthetic' AS provenance_kind,
    e.source,
    e.event_type,
    e.ts_orig,
    NULL::timestamptz AS ts_capture,
    NULL::text AS temporal_source_type,
    NULL::text AS temporal_precision,
    NULL::text AS temporal_clock,
    e.temporal_policy,
    e.semantics_version,
    e.scope_key,
    e.equivalence_key,
    e.created_by_operation_id,
    e.automaton_model
FROM core.events e
WHERE e.source_event_ids IS NOT NULL;
";

/// Scope health dashboard for automatons.
///
/// Provides a per-automaton, per-scope summary of derived events: how many exist,
/// when last updated, and what processing metadata (`semantics_version`, `temporal_policy`)
/// they carry. Operators query this to find stale scopes or version mismatches.
const DERIVED_SCOPE_SUMMARY_SQL: &str = r"
CREATE OR REPLACE VIEW core.derived_scope_summary AS
SELECT
    source AS automaton,
    scope_key,
    event_type,
    COUNT(*) AS event_count,
    MAX(ts_coided) AS last_updated,
    semantics_version,
    temporal_policy
FROM core.events
WHERE scope_key IS NOT NULL
GROUP BY source, scope_key, event_type, semantics_version, temporal_policy
ORDER BY last_updated DESC;
";

// Shared grant roles are provisioned during privileged bootstrap (xtask/NixOS or the
// schema-apply bootstrap binary), so declarative schema apply remains safe to run as
// the database owner role.
const ROLE_GRANTS_SQL: &str = r"
GRANT USAGE ON SCHEMA core, raw, sinex_schemas, audit TO sinex_event_engine, sinex_api, sinex_readonly;

-- Privacy policy (#1042): event_engine loads rules at the chokepoint; gateway manages
-- them via sinexctl; readonly may inspect.
GRANT USAGE ON SCHEMA privacy TO sinex_event_engine, sinex_api, sinex_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA privacy TO sinex_event_engine, sinex_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA privacy TO sinex_api;


GRANT EXECUTE ON FUNCTION core.start_operation TO sinex_api;
GRANT EXECUTE ON FUNCTION core.complete_operation TO sinex_api;
GRANT EXECUTE ON FUNCTION core.fail_operation TO sinex_api;
GRANT EXECUTE ON FUNCTION core.execute_cascade_tombstone TO sinex_api;
GRANT EXECUTE ON FUNCTION core.execute_cascade_restore TO sinex_api;
GRANT EXECUTE ON FUNCTION core.lifecycle_tier_status TO sinex_api, sinex_readonly;
GRANT EXECUTE ON FUNCTION core.jsonb_merge_deep TO sinex_event_engine, sinex_api;
";

const SHARED_ACCESS_ROLES_BOOTSTRAP_SQL: &str = r"
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_event_engine') THEN
    CREATE ROLE sinex_event_engine NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_api') THEN
    CREATE ROLE sinex_api NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_readonly') THEN
    CREATE ROLE sinex_readonly NOLOGIN;
  END IF;
END $$;
";
