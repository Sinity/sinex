use crate::schema::{
    ArchivedEvents, Blobs, EmbeddingCache, EmbeddingModels, Entities, EntityRelations,
    EventAnnotations, EventClusterMembers, EventClusters, EventEmbeddings, EventPayloadSchemas,
    EventTombstones, Events, GitopsSchemaSources, NodeManifests, OperationsLog,
    SourceMaterialRegistry, TaggedItems, Tags, TemporalLedger, ValidationCache,
};
use crate::schema_registry;
use sea_query::{IndexCreateStatement, PostgresQueryBuilder, TableCreateStatement};
use sqlx::PgPool;

const REQUIRED_EXTENSIONS: &[&str] = &["pg_jsonschema", "vector", "timescaledb", "pg_trgm"];

#[derive(Debug)]
pub enum ApplyError {
    Sqlx(sqlx::Error),
    MissingExtensions(Vec<String>),
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
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlx(err) => Some(err),
            Self::MissingExtensions(_) => None,
        }
    }
}

impl From<sqlx::Error> for ApplyError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sqlx(value)
    }
}

pub async fn apply(pool: &PgPool) -> Result<(), ApplyError> {
    ensure_schemas(pool).await?;
    ensure_required_extensions(pool).await?;
    execute_sql(pool, BOOTSTRAP_SQL).await?;
    apply_legacy_renames(pool).await?;
    create_tables(pool).await?;
    converge_tables(pool).await?;
    create_indexes(pool).await?;
    create_triggers_and_functions(pool).await?;
    configure_timescaledb(pool).await?;
    apply_roles_and_grants(pool).await?;
    cleanup_legacy(pool).await?;
    Ok(())
}

pub async fn diff(pool: &PgPool) -> Result<Vec<String>, ApplyError> {
    let mut drifts = Vec::new();

    for table in crate::schema::all_tables() {
        let exists = relation_exists(pool, table.qualified_name).await?;
        if !exists {
            drifts.push(format!("missing table {}", table.qualified_name));
        }
    }

    if relation_exists(pool, "core.events").await? {
        let required_columns = [
            "id",
            "source",
            "event_type",
            "host",
            "payload",
            "ts_orig",
            "ts_orig_subnano",
            "ts_coided",
            "ts_persisted",
            "source_material_id",
            "anchor_byte",
            "offset_start",
            "offset_end",
            "offset_kind",
            "source_event_ids",
            "associated_blob_ids",
            "payload_schema_id",
            "node_version",
        ];
        for column in required_columns {
            if !column_exists(pool, "core", "events", column).await? {
                drifts.push(format!("missing core.events.{column}"));
            }
        }

        for trigger in ["trg_events_no_update", "trg_events_archive_before_delete"] {
            if !trigger_exists(pool, "core.events", trigger).await? {
                drifts.push(format!("missing core.events trigger {trigger}"));
            }
        }
    }

    if relation_exists(pool, "core.node_manifests").await? {
        for column in ["status", "last_heartbeat_at"] {
            if !column_exists(pool, "core", "node_manifests", column).await? {
                drifts.push(format!("missing core.node_manifests.{column}"));
            }
        }
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

        let sql = format!(r#"CREATE EXTENSION IF NOT EXISTS \"{extension}\""#);
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
        render_table(Blobs::create_table_statement()),
        render_table(EventPayloadSchemas::create_table_statement()),
        render_table(EmbeddingModels::create_table_statement()),
        render_table(EventClusters::create_table_statement()),
        render_table(OperationsLog::create_table_statement()),
        render_table(Tags::create_table_statement()),
        render_table(SourceMaterialRegistry::create_table_statement()),
        render_table(Events::create_table_statement()),
        render_table(NodeManifests::create_table_statement()),
        render_table(GitopsSchemaSources::create_table_statement()),
        render_table(ValidationCache::create_table_statement()),
        render_table(TemporalLedger::create_table_statement()),
        render_table(Entities::create_table_statement()),
        render_table(EntityRelations::create_table_statement()),
        render_table(TaggedItems::create_table_statement()),
        render_table(EventAnnotations::create_table_statement()),
        render_table(EmbeddingCache::create_table_statement()),
        render_table(EventEmbeddings::create_table_statement()),
        render_table(EventClusterMembers::create_table_statement()),
        render_table(EventTombstones::create_table_statement()),
    ];

    for sql in table_sql {
        execute_sql(pool, &sql).await?;
    }

    execute_sql(pool, &ArchivedEvents::create_table_sql()).await?;
    Ok(())
}

async fn converge_tables(pool: &PgPool) -> Result<(), ApplyError> {
    execute_sql(pool, EVENTS_CONVERGENCE_SQL).await?;
    execute_sql(pool, NODE_MANIFESTS_CONVERGENCE_SQL).await?;
    Ok(())
}

async fn create_indexes(pool: &PgPool) -> Result<(), ApplyError> {
    let mut index_sql = Vec::new();
    index_sql.extend(render_indexes(SourceMaterialRegistry::create_indexes()));
    index_sql.extend(render_indexes(Events::create_indexes()));
    index_sql.extend(Events::create_gin_indexes_sql());
    index_sql.extend(ArchivedEvents::create_indexes_sql());
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
    index_sql.extend(render_indexes(GitopsSchemaSources::create_indexes()));

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
    execute_optional_sql(
        pool,
        "SELECT set_chunk_time_interval('core.events', INTERVAL '7 days')",
    )
    .await;
    execute_optional_sql(
        pool,
        "SELECT remove_retention_policy('core.events', if_exists => true)",
    )
    .await;

    execute_optional_sql(
        pool,
        "CREATE INDEX IF NOT EXISTS ix_events_sinex_telemetry ON core.events (source, event_type, id DESC) WHERE source LIKE 'sinex.%'",
    )
    .await;

    execute_optional_sql(pool, TELEMETRY_SQL).await;

    Ok(())
}

async fn apply_roles_and_grants(pool: &PgPool) -> Result<(), ApplyError> {
    execute_optional_sql(pool, ROLE_GRANTS_SQL).await;
    Ok(())
}

async fn cleanup_legacy(pool: &PgPool) -> Result<(), ApplyError> {
    execute_optional_sql(pool, "DROP TABLE IF EXISTS public.seaql_migrations").await;
    execute_optional_sql(pool, "DROP INDEX IF EXISTS core.ux_events_material_anchor_id").await;
    Ok(())
}

async fn apply_legacy_renames(pool: &PgPool) -> Result<(), ApplyError> {
    execute_optional_sql(pool, LEGACY_RENAMES_SQL).await;
    Ok(())
}

async fn execute_sql(pool: &PgPool, sql: &str) -> Result<(), ApplyError> {
    sqlx::query(sql).execute(pool).await?;
    Ok(())
}

async fn execute_optional_sql(pool: &PgPool, sql: &str) {
    if let Err(err) = sqlx::query(sql).execute(pool).await {
        tracing::info!(error = %err, "optional schema SQL skipped");
    }
}

fn render_table(stmt: TableCreateStatement) -> String {
    stmt.to_string(PostgresQueryBuilder)
}

fn render_index(stmt: IndexCreateStatement) -> String {
    stmt.to_string(PostgresQueryBuilder)
}

fn render_indexes(stmts: Vec<IndexCreateStatement>) -> Vec<String> {
    stmts.into_iter().map(render_index).collect()
}

async fn relation_exists(pool: &PgPool, qualified_name: &str) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(qualified_name)
        .fetch_one(pool)
        .await?;
    Ok(exists)
}

async fn column_exists(
    pool: &PgPool,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<bool, ApplyError> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
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

const BOOTSTRAP_SQL: &str = r#"
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
"#;

const LEGACY_RENAMES_SQL: &str = r#"
DO $$
BEGIN
    IF to_regclass('core.processor_manifests') IS NOT NULL
       AND to_regclass('core.node_manifests') IS NULL THEN
        EXECUTE 'ALTER TABLE core.processor_manifests RENAME TO node_manifests';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'node_manifests' AND column_name = 'processor_type'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'node_manifests' AND column_name = 'node_type'
    ) THEN
        EXECUTE 'ALTER TABLE core.node_manifests RENAME COLUMN processor_type TO node_type';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'ingestor_version'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'node_version'
    ) THEN
        EXECUTE 'ALTER TABLE core.events RENAME COLUMN ingestor_version TO node_version';
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'ts_ingest'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'events' AND column_name = 'ts_coided'
    ) THEN
        EXECUTE 'ALTER TABLE core.events RENAME COLUMN ts_ingest TO ts_coided';
    END IF;
END;
$$;
"#;

const EVENTS_CONVERGENCE_SQL: &str = r#"
ALTER TABLE core.events
    ADD COLUMN IF NOT EXISTS ts_persisted TIMESTAMPTZ NOT NULL DEFAULT now();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'events_source_event_ids_non_empty'
          AND conrelid = 'core.events'::regclass
    ) THEN
        ALTER TABLE core.events
            ADD CONSTRAINT events_source_event_ids_non_empty
            CHECK (source_event_ids IS NULL OR cardinality(source_event_ids) > 0) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'events_source_material_only_offsets'
          AND conrelid = 'core.events'::regclass
    ) THEN
        ALTER TABLE core.events
            ADD CONSTRAINT events_source_material_only_offsets
            CHECK (
                source_material_id IS NOT NULL
                OR (anchor_byte IS NULL AND offset_start IS NULL AND offset_end IS NULL AND offset_kind IS NULL)
            ) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'events_material_anchor_required'
          AND conrelid = 'core.events'::regclass
    ) THEN
        ALTER TABLE core.events
            ADD CONSTRAINT events_material_anchor_required
            CHECK (source_material_id IS NULL OR anchor_byte IS NOT NULL) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'events_offsets_pairing'
          AND conrelid = 'core.events'::regclass
    ) THEN
        ALTER TABLE core.events
            ADD CONSTRAINT events_offsets_pairing
            CHECK ((offset_start IS NULL) = (offset_end IS NULL)) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'events_offsets_require_kind'
          AND conrelid = 'core.events'::regclass
    ) THEN
        ALTER TABLE core.events
            ADD CONSTRAINT events_offsets_require_kind
            CHECK (offset_kind IS NULL OR (offset_start IS NOT NULL AND offset_end IS NOT NULL)) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'events_offset_order'
          AND conrelid = 'core.events'::regclass
    ) THEN
        ALTER TABLE core.events
            ADD CONSTRAINT events_offset_order
            CHECK (offset_start IS NULL OR offset_end IS NULL OR offset_end >= offset_start) NOT VALID;
    END IF;
END;
$$;

ALTER TABLE core.events VALIDATE CONSTRAINT events_source_event_ids_non_empty;
ALTER TABLE core.events VALIDATE CONSTRAINT events_source_material_only_offsets;
ALTER TABLE core.events VALIDATE CONSTRAINT events_material_anchor_required;
ALTER TABLE core.events VALIDATE CONSTRAINT events_offsets_pairing;
ALTER TABLE core.events VALIDATE CONSTRAINT events_offsets_require_kind;
ALTER TABLE core.events VALIDATE CONSTRAINT events_offset_order;

DROP INDEX IF EXISTS core.ux_events_material_anchor_id;
CREATE INDEX IF NOT EXISTS ix_events_material_anchor
    ON core.events (source_material_id, anchor_byte)
    WHERE source_material_id IS NOT NULL;
"#;

const NODE_MANIFESTS_CONVERGENCE_SQL: &str = r#"
ALTER TABLE core.node_manifests
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE core.node_manifests
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_processors_status ON core.node_manifests(status);
CREATE INDEX IF NOT EXISTS idx_processors_heartbeat ON core.node_manifests(last_heartbeat_at);
"#;

const OPERATIONS_AND_CASCADE_SQL: &str = r#"
CREATE OR REPLACE FUNCTION core.start_operation(p_operation_type TEXT, p_operator TEXT, p_scope JSONB, p_scope_window tstzrange DEFAULT NULL)
RETURNS UUID AS $$
DECLARE
    v_operation_id UUID;
BEGIN
    v_operation_id := uuidv7();
    INSERT INTO core.operations_log (id, operation_type, operator, scope, scope_window, result_status)
    VALUES (v_operation_id, p_operation_type, p_operator, p_scope, p_scope_window, 'running');
    RETURN v_operation_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.complete_operation(p_operation_id UUID, p_summary JSONB)
RETURNS VOID AS $$
BEGIN
    UPDATE core.operations_log
    SET result_status = 'success',
        result_message = p_summary->>'message',
        duration_ms = COALESCE(duration_ms, 0),
        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_summary
    WHERE id = p_operation_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION core.fail_operation(p_operation_id UUID, p_error JSONB)
RETURNS VOID AS $$
BEGIN
    UPDATE core.operations_log
    SET result_status = 'failure',
        result_message = p_error->>'error',
        duration_ms = COALESCE(duration_ms, 0),
        preview_summary = COALESCE(preview_summary, '{}'::jsonb) || p_error
    WHERE id = p_operation_id;
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
BEGIN
    LOOP
        IF current_depth >= max_depth THEN
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
"#;

const TOMBSTONE_LIFECYCLE_SQL: &str = r#"
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
        payload_schema_id, node_version
    )
    SELECT
        ae.id, ae.source, ae.event_type, ae.host, ae.payload,
        ae.ts_orig, ae.ts_orig_subnano,
        ae.source_material_id, ae.anchor_byte, ae.offset_start, ae.offset_end, ae.offset_kind,
        ae.source_event_ids, ae.associated_blob_ids,
        ae.payload_schema_id, ae.node_version
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
"#;

const JSONB_MERGE_SQL: &str = r#"
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
"#;

const EMBEDDING_INDEX_MANAGEMENT_SQL: &str = r#"
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
"#;

const TELEMETRY_SQL: &str = r#"
CREATE OR REPLACE VIEW sinex_telemetry.current_health AS
SELECT
    e.source,
    e.event_type,
    e.payload->>'component' AS component,
    e.payload->>'current_status' AS status,
    e.payload->>'reason' AS reason,
    e.ts_coided AS last_update
FROM core.events e
INNER JOIN (
    SELECT source, MAX(ts_coided) AS max_ts
    FROM core.events
    WHERE source = 'sinex'
      AND event_type = 'health.status'
      AND ts_coided > NOW() - INTERVAL '1 hour'
    GROUP BY source
) latest ON e.source = latest.source AND e.ts_coided = latest.max_ts
WHERE e.event_type = 'health.status';

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
"#;

const ROLE_GRANTS_SQL: &str = r#"
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
"#;
