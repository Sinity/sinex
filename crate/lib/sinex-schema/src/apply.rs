use crate::schema::{
    ArchivedEventAnnotations, ArchivedEventEmbeddings, ArchivedEvents, ArchivedTaggedItems, Blobs,
    EmbeddingCache, EmbeddingModels, Entities, EntityRelations, EventAnnotations,
    EventClusterMembers, EventClusters, EventEmbeddings, EventPayloadSchemas, EventReplacements,
    EventTombstones, Events, GitopsSchemaSources, NodeManifests, NodeRuns, OperationsLog,
    SourceMaterialLinks, SourceMaterialRegistry, TaggedItems, Tags, TemporalLedger, ValidationCache,
};
use crate::schema_registry;
use sea_query::{IndexCreateStatement, PostgresQueryBuilder, TableCreateStatement};
use sqlx::{Executor, PgPool};

const REQUIRED_EXTENSIONS: &[&str] = &["pg_jsonschema", "vector", "timescaledb", "pg_trgm"];
pub const SHARED_ACCESS_ROLES: &[&str] = &["sinex_ingestd", "sinex_gateway", "sinex_readonly"];
const EVENTS_REQUIRED_TRIGGERS: &[&str] =
    &["trg_events_no_update", "trg_events_archive_before_delete"];
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
    "ix_events_node_run_synthesis_latest",
];
const ARCHIVED_EVENTS_REQUIRED_INDEXES: &[&str] = &[
    "ix_archived_events_ts_orig",
    "ix_archived_events_source_ts_orig",
    "ix_archived_events_archived_at",
    "ix_archived_events_superseded_by_event_id",
    "ix_archived_events_source_event_ids",
];
const NODE_MANIFESTS_REQUIRED_INDEXES: &[&str] =
    &["idx_processors_status", "idx_processors_heartbeat"];
const TEMPORAL_LEDGER_REQUIRED_INDEXES: &[&str] = &[
    "uk_temporal_ledger_material_offset_source_type",
    "ix_tl_material_offsets",
    "ix_tl_ts_and_source_type",
];
const TELEMETRY_VIEW_RELATIONS: &[&str] = &[
    "assembly_stats_1h",
    "current_health",
    "gateway_stats_1h",
    "ingestd_batch_stats_1h",
    "metric_counters_1h",
    "current_window_focus",
    "command_frequency_hourly",
    "file_activity_summary",
    "current_system_state",
    "node_stats_1h",
    "recent_activity_summary",
    "stream_stats_1h",
];
const TELEMETRY_MATERIALIZED_VIEW_RELATIONS: &[&str] = &["current_device_state"];
const TELEMETRY_CONTINUOUS_AGGREGATES: &[&str] = &[];

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
    for table in crate::schema::all_tables() {
        if !relation_exists(pool, table.qualified_name).await? {
            drifts.push(format!("missing table {}", table.qualified_name));
        }
    }

    // Column and named constraint gaps — derived from sea-query declarations.
    let column_gaps = crate::converge::report_column_gaps(pool, &convergible_tables).await?;
    drifts.extend(column_gaps);

    // Trigger existence (triggers are managed by CREATE OR REPLACE, not convergence).
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

    if relation_exists(pool, "audit.archived_events").await? {
        for index in ARCHIVED_EVENTS_REQUIRED_INDEXES {
            if !index_exists(pool, "audit", "archived_events", index).await? {
                drifts.push(format!("missing audit.archived_events index {index}"));
            }
        }
    }

    if relation_exists(pool, "core.node_manifests").await? {
        for index in NODE_MANIFESTS_REQUIRED_INDEXES {
            if !index_exists(pool, "core", "node_manifests", index).await? {
                drifts.push(format!("missing core.node_manifests index {index}"));
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

    Ok(drifts)
}

async fn ensure_schemas(pool: &PgPool) -> Result<(), ApplyError> {
    for schema in schema_registry::schema_names() {
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        execute_sql(pool, &sql).await?;
    }
    execute_sql(pool, "CREATE SCHEMA IF NOT EXISTS sinex_telemetry").await?;
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

    if source_material_registry_status_constraint_is_current(pool).await? {
        return Ok(());
    }

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
        render_table(&NodeManifests::create_table_statement()),
        render_table(&NodeRuns::create_table_statement()),
        render_table(&Events::create_table_statement()),
        render_table(&GitopsSchemaSources::create_table_statement()),
        render_table(&ValidationCache::create_table_statement()),
        render_table(&TemporalLedger::create_table_statement()),
        render_table(&Entities::create_table_statement()),
        render_table(&EntityRelations::create_table_statement()),
        render_table(&TaggedItems::create_table_statement()),
        render_table(&EventAnnotations::create_table_statement()),
        render_table(&EmbeddingCache::create_table_statement()),
        render_table(&EventEmbeddings::create_table_statement()),
        render_table(&EventClusterMembers::create_table_statement()),
        render_table(&EventTombstones::create_table_statement()),
        render_table(&EventReplacements::create_table_statement()),
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
    index_sql.extend(render_indexes(TaggedItems::create_indexes()));
    index_sql.extend(render_indexes(EventAnnotations::create_indexes()));
    index_sql.extend(EventAnnotations::create_gin_indexes_sql());
    index_sql.extend(render_indexes(EmbeddingModels::create_indexes()));
    index_sql.extend(render_indexes(EmbeddingCache::create_indexes()));
    index_sql.extend(EmbeddingCache::create_indexes_sql());
    index_sql.extend(render_indexes(EventEmbeddings::create_indexes()));
    index_sql.extend(EventEmbeddings::create_indexes_sql());
    index_sql.extend(render_indexes(EventPayloadSchemas::create_indexes()));
    index_sql.extend(render_indexes(NodeManifests::create_indexes()));
    index_sql.extend(NodeManifests::create_gin_indexes_sql());
    index_sql.extend(render_indexes(NodeRuns::create_indexes()));
    index_sql.extend(render_indexes(GitopsSchemaSources::create_indexes()));
    index_sql.extend(render_indexes(EventReplacements::create_indexes()));
    index_sql.extend(render_indexes(OperationsLog::create_indexes()));
    index_sql.extend(OperationsLog::create_gin_indexes_sql());

    for sql in index_sql {
        execute_sql(pool, &sql).await?;
    }

    Ok(())
}

async fn create_triggers_and_functions(pool: &PgPool) -> Result<(), ApplyError> {
    execute_sql(pool, Events::create_no_update_trigger_sql()).await?;
    execute_sql(pool, ArchivedEvents::create_archive_trigger_sql()).await?;
    execute_sql(pool, TemporalLedger::create_append_only_trigger_sql()).await?;
    execute_sql(pool, &Entities::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &EntityRelations::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &EventAnnotations::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &EventPayloadSchemas::create_updated_at_trigger_sql()).await?;
    execute_sql(pool, &GitopsSchemaSources::create_updated_at_trigger_sql()).await?;

    execute_sql(pool, OPERATIONS_AND_CASCADE_SQL).await?;
    execute_sql(pool, TOMBSTONE_LIFECYCLE_SQL).await?;
    execute_sql(pool, JSONB_MERGE_SQL).await?;
    execute_sql(pool, EMBEDDING_INDEX_MANAGEMENT_SQL).await?;

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

    execute_sql(
        pool,
        "CREATE INDEX IF NOT EXISTS ix_events_sinex_telemetry ON core.events (source, event_type, id DESC) WHERE source LIKE 'sinex.%'",
    )
    .await?;
    execute_sql(
        pool,
        r#"
        CREATE INDEX IF NOT EXISTS ix_events_sinex_metric_gauge_latest
        ON core.events (
            (payload->>'name'),
            ((payload->'labels'->>'node')),
            ((payload->'labels'->>'node_run_id')),
            id DESC
        )
        WHERE source = 'sinex' AND event_type = 'metric.gauge'
        "#,
    )
    .await?;
    execute_sql(
        pool,
        r#"
        CREATE INDEX IF NOT EXISTS ix_events_node_run_synthesis_latest
        ON core.events (node_run_id, id DESC)
        WHERE node_run_id IS NOT NULL AND source_event_ids IS NOT NULL
        "#,
    )
    .await?;

    recreate_telemetry_read_models(pool).await?;
    execute_sql(pool, TELEMETRY_SQL).await?;
    execute_sql(pool, OPERATOR_TELEMETRY_VIEWS_SQL).await?;
    execute_sql(pool, ACTIVITY_READ_MODELS_SQL).await?;
    execute_sql(pool, RECENT_ACTIVITY_SUMMARY_SQL).await?;
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
                'ingestd_batch_stats_1h',
                'current_system_state',
                'file_activity_summary',
                'command_frequency_hourly',
                'current_window_focus',
                'metric_counters_1h',
                'node_stats_1h',
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
            EXTRACT(MILLISECONDS FROM (NOW() - uuid_extract_timestamp(p_operation_id)))::integer
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
            EXTRACT(MILLISECONDS FROM (NOW() - uuid_extract_timestamp(p_operation_id)))::integer
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
    v_count BIGINT;
BEGIN
    IF p_archived_ids IS NULL OR array_length(p_archived_ids, 1) IS NULL THEN
        RETURN 0;
    END IF;

    PERFORM pg_catalog.set_config('sinex.operation_id', p_operation_id, true);
    PERFORM pg_catalog.set_config('sinex.archive_reason', 'restored from archive', true);

    INSERT INTO core.events (
        id, source, event_type, host, payload,
        ts_orig, ts_orig_subnano,
        source_material_id, anchor_byte, offset_start, offset_end, offset_kind,
        source_event_ids, associated_blob_ids,
        payload_schema_id, node_run_id,
        temporal_policy, semantics_version, scope_key, equivalence_key,
        created_by_operation_id, node_model
    )
    SELECT
        ae.id, ae.source, ae.event_type, ae.host, ae.payload,
        ae.ts_orig, ae.ts_orig_subnano,
        ae.source_material_id, ae.anchor_byte, ae.offset_start, ae.offset_end, ae.offset_kind,
        ae.source_event_ids, ae.associated_blob_ids,
        ae.payload_schema_id, ae.node_run_id,
        ae.temporal_policy, ae.semantics_version, ae.scope_key, ae.equivalence_key,
        ae.created_by_operation_id, ae.node_model
    FROM audit.archived_events ae
    WHERE ae.id = ANY(p_archived_ids)
    ON CONFLICT (id) DO NOTHING;

    GET DIAGNOSTICS v_count = ROW_COUNT;

    DELETE FROM audit.archived_events
    WHERE id = ANY(p_archived_ids);

    RETURN v_count;
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
    payload->>'state' AS state,
    payload->>'sub_state' AS sub_state,
    ts_coided AS last_update
FROM core.events
WHERE event_type IN ('systemd.unit_changed', 'udev.device_changed')
  AND source = 'system-ingestor'
  AND ts_coided > NOW() - INTERVAL '7 days'
ORDER BY payload->>'unit_name', ts_coided DESC;

CREATE INDEX IF NOT EXISTS ix_current_device_state_unit_name
    ON sinex_telemetry.current_device_state (unit_name);
CREATE INDEX IF NOT EXISTS ix_current_device_state_state
    ON sinex_telemetry.current_device_state (state);
";

const OPERATOR_TELEMETRY_VIEWS_SQL: &str = r"
CREATE OR REPLACE VIEW sinex_telemetry.gateway_stats_1h AS
SELECT
    time_bucket('1 hour', ts_coided) AS bucket,
    source,
    COUNT(*) FILTER (WHERE event_type = 'request.stats') AS stat_events,
    AVG((payload->>'total_requests')::bigint) AS avg_total_requests,
    SUM((payload->>'rate_limited_requests')::bigint) AS total_rate_limited,
    AVG((payload->>'avg_latency_ms')::float) AS avg_latency_ms,
    MAX((payload->>'p99_latency_ms')::float) AS max_p99_latency_ms
FROM core.events
WHERE source LIKE 'sinex.gateway%'
  AND event_type IN ('request.stats', 'rate_limit.exceeded', 'replay.stats')
GROUP BY bucket, source;

CREATE OR REPLACE VIEW sinex_telemetry.stream_stats_1h AS
SELECT
    time_bucket('1 hour', ts_coided) AS bucket,
    payload->>'stream' AS stream_name,
    AVG((payload->>'fill_pct')::float) AS avg_fill_pct,
    MAX((payload->>'fill_pct')::float) AS max_fill_pct,
    AVG((payload->>'messages')::bigint) AS avg_messages,
    MAX((payload->>'max_messages')::bigint) AS max_messages,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinex.ingestd'
  AND event_type = 'stream.stats'
GROUP BY bucket, payload->>'stream';

CREATE OR REPLACE VIEW sinex_telemetry.assembly_stats_1h AS
SELECT
    time_bucket('1 hour', ts_coided) AS bucket,
    MAX((payload->>'active_assemblies')::int) AS max_active_assemblies,
    SUM((payload->>'total_completed')::bigint) AS total_completed,
    SUM((payload->>'total_cancelled')::bigint) AS total_cancelled,
    SUM((payload->>'total_failed')::bigint) AS total_failed,
    SUM((payload->>'total_timed_out')::bigint) AS total_timed_out,
    AVG((payload->>'avg_duration_ms')::float) AS avg_duration_ms,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinex.ingestd'
  AND event_type = 'assembly.stats'
GROUP BY bucket;

CREATE OR REPLACE VIEW sinex_telemetry.node_stats_1h AS
SELECT
    time_bucket('1 hour', ts_coided) AS bucket,
    payload->>'node_type' AS node_type,
    SUM((payload->>'events_processed')::bigint) AS total_events_processed,
    SUM((payload->>'events_dropped')::bigint) AS total_events_dropped,
    AVG((payload->>'avg_latency_ms')::float) AS avg_latency_ms,
    MAX((payload->>'queue_depth')::int) AS max_queue_depth,
    SUM((payload->>'error_count')::bigint) AS total_errors,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinex.node'
  AND event_type = 'processing.stats'
GROUP BY bucket, payload->>'node_type';

CREATE OR REPLACE VIEW sinex_telemetry.metric_counters_1h AS
SELECT
    time_bucket('1 hour', ts_coided) AS bucket,
    payload->>'component' AS component,
    payload->>'name' AS metric_name,
    SUM((payload->>'value')::bigint) AS total_value,
    MAX((payload->>'value')::bigint) AS max_value,
    COUNT(*) AS sample_count
FROM core.events
WHERE source = 'sinex'
  AND event_type = 'metric.counter'
GROUP BY bucket, payload->>'component', payload->>'name';

CREATE OR REPLACE VIEW sinex_telemetry.ingestd_batch_stats_1h AS
SELECT
    time_bucket('1 hour', ts_coided) AS bucket,
    AVG((payload->>'batch_size')::int) AS avg_batch_size,
    MAX((payload->>'batch_size')::int) AS max_batch_size,
    AVG((payload->>'fetch_to_ack_ms')::float) AS avg_latency_ms,
    MAX((payload->>'fetch_to_ack_ms')::float) AS max_latency_ms,
    SUM((payload->>'events_deferred')::int) AS total_deferred,
    SUM((payload->>'events_failed')::int) AS total_failed,
    COUNT(*) FILTER (WHERE (payload->>'had_synthesis')::boolean) AS synthesis_batches,
    COUNT(*) AS batch_count,
    MAX((payload->>'validation_valid')::bigint) AS validation_valid,
    MAX((payload->>'validation_skipped')::bigint) AS validation_skipped,
    MAX((payload->>'validation_no_schema')::bigint) AS validation_no_schema,
    MAX((payload->>'validation_schema_not_found')::bigint) AS validation_schema_not_found,
    MAX((payload->>'validation_invalid')::bigint) AS validation_invalid,
    AVG((payload->>'validation_coverage_pct')::float) AS avg_validation_coverage_pct
FROM core.events
WHERE source = 'sinex.ingestd'
  AND event_type = 'batch.stats'
GROUP BY bucket;
";

const ACTIVITY_READ_MODELS_SQL: &str = r"
CREATE OR REPLACE VIEW sinex_telemetry.current_window_focus AS
SELECT
    time_bucket('5 minutes', ts_orig) AS bucket,
    payload->>'workspace_id' AS workspace,
    last(payload->>'window_class', ts_orig) AS window_class,
    last(payload->>'window_title', ts_orig) AS window_title,
    last(payload->>'window_id', ts_orig) AS window_id,
    MAX(ts_orig) AS last_focus_time,
    COUNT(*) AS focus_event_count
FROM core.events
WHERE event_type = 'window.focused'
  AND source LIKE 'wm.%'
GROUP BY bucket, payload->>'workspace_id';

CREATE OR REPLACE VIEW sinex_telemetry.command_frequency_hourly AS
SELECT
    time_bucket('1 hour', ts_orig) AS bucket,
    COALESCE(payload->>'command', payload->>'command_string') AS command,
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
    bucket,
    COALESCE(payload->>'command', payload->>'command_string'),
    CASE
        WHEN source = 'shell.kitty' THEN COALESCE(payload->>'shell_type', 'kitty')
        WHEN source = 'shell.atuin' THEN 'atuin'
        WHEN source LIKE 'shell.history.%' THEN regexp_replace(source, '^shell\.history\.', '')
        ELSE NULL
    END;

CREATE OR REPLACE VIEW sinex_telemetry.file_activity_summary AS
SELECT
    time_bucket('1 hour', ts_orig) AS bucket,
    regexp_replace(payload->>'path', '/[^/]*$', '') AS directory,
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT payload->>'path') AS unique_files
FROM core.events
WHERE event_type IN ('file.created', 'file.modified', 'file.deleted')
  AND source = 'fs-watcher'
GROUP BY bucket, regexp_replace(payload->>'path', '/[^/]*$', ''), event_type;

CREATE OR REPLACE VIEW sinex_telemetry.current_system_state AS
SELECT
    time_bucket('5 minutes', ts_orig) AS bucket,
    AVG((payload->>'cpu_percent')::float) AS avg_cpu_percent,
    MAX((payload->>'cpu_percent')::float) AS max_cpu_percent,
    AVG((payload->>'memory_percent')::float) AS avg_memory_percent,
    MAX((payload->>'memory_percent')::float) AS max_memory_percent,
    AVG((payload->>'disk_percent')::float) AS avg_disk_percent,
    last((payload->>'active_units')::int, ts_orig) FILTER (WHERE payload ? 'active_units') AS current_active_units,
    COUNT(*) AS sample_count
FROM core.events
WHERE event_type IN ('system.resources', 'systemd.units_summary')
  AND source = 'system-ingestor'
GROUP BY bucket;
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
    'system_load' AS activity_type,
    'cpu' AS context,
    ROUND(avg_cpu_percent::numeric, 2)::text AS detail,
    bucket AS timestamp
 FROM sinex_telemetry.current_system_state
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
 LIMIT 5);
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
    NULL::text AS node_model
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
    e.node_model
FROM core.events e
WHERE e.source_event_ids IS NOT NULL;
";

/// Scope health dashboard for derived nodes.
///
/// Provides a per-node, per-scope summary of derived events: how many exist,
/// when last updated, and what processing metadata (`semantics_version`, `temporal_policy`)
/// they carry. Operators query this to find stale scopes or version mismatches.
const DERIVED_SCOPE_SUMMARY_SQL: &str = r"
CREATE OR REPLACE VIEW core.derived_scope_summary AS
SELECT
    source AS node,
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
GRANT USAGE ON SCHEMA core, raw, sinex_schemas, audit TO sinex_ingestd, sinex_gateway, sinex_readonly;

REVOKE ALL ON sinex_schemas.gitops_schema_sources FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, UPDATE ON sinex_schemas.gitops_schema_sources TO sinex_ingestd;
GRANT SELECT, INSERT, DELETE ON sinex_schemas.gitops_schema_sources TO sinex_gateway;
GRANT SELECT ON sinex_schemas.gitops_schema_sources TO sinex_readonly;

GRANT EXECUTE ON FUNCTION core.start_operation TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.complete_operation TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.fail_operation TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.execute_cascade_tombstone TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.execute_cascade_restore TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.lifecycle_tier_status TO sinex_gateway, sinex_readonly;
GRANT EXECUTE ON FUNCTION core.jsonb_merge_deep TO sinex_ingestd, sinex_gateway;
";

const SHARED_ACCESS_ROLES_BOOTSTRAP_SQL: &str = r"
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_ingestd') THEN
    CREATE ROLE sinex_ingestd NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_gateway') THEN
    CREATE ROLE sinex_gateway NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_readonly') THEN
    CREATE ROLE sinex_readonly NOLOGIN;
  END IF;
END $$;
";
