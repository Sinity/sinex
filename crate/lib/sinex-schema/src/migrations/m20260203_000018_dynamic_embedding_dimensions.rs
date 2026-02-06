//! Migration: Dynamic Embedding Dimensions with Partial HNSW Indexes
//!
//! This migration enables storing vectors of any dimension while maintaining
//! O(log n) similarity search via partial HNSW indexes per embedding model.
//!
//! **Strategy**:
//! - Embedding columns use dimensionless `vector` type (no constraint)
//! - Each embedding model gets its own partial HNSW index with typed dimensions
//! - Queries filtered by `embedding_model_id` use the appropriate partial index
//!
//! **Functions created**:
//! - `core.create_embedding_model_index(model_id, dimensions)` - creates partial HNSW
//! - `core.drop_embedding_model_index(model_id)` - drops the index
//!
//! Call `create_embedding_model_index` when registering a new embedding model.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

const CREATE_INDEX_FUNCTION: &str = r"
CREATE OR REPLACE FUNCTION core.create_embedding_model_index(
    p_model_id ULID,
    p_dimensions INT
) RETURNS void AS $$
DECLARE
    event_idx_name TEXT;
    cache_idx_name TEXT;
    model_id_str TEXT;
BEGIN
    -- Sanitize model ID for use in index name (replace non-alphanumeric)
    model_id_str := replace(p_model_id::text, '-', '_');
    event_idx_name := 'ix_event_embeddings_hnsw_' || model_id_str;
    cache_idx_name := 'ix_embedding_cache_hnsw_' || model_id_str;

    -- Create partial HNSW index on event_embeddings for this model
    -- The cast to vector(N) is required because HNSW needs typed dimensions
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON core.event_embeddings
         USING hnsw ((embedding::vector(%s)) vector_cosine_ops)
         WHERE embedding_model_id = %L',
        event_idx_name, p_dimensions, p_model_id
    );

    -- Create partial HNSW index on embedding_cache for this model
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON core.embedding_cache
         USING hnsw ((embedding::vector(%s)) vector_cosine_ops)
         WHERE embedding_model_id = %L',
        cache_idx_name, p_dimensions, p_model_id
    );

    RAISE NOTICE 'Created HNSW indexes for embedding model % (% dimensions)', p_model_id, p_dimensions;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.create_embedding_model_index(ULID, INT) IS
'Creates partial HNSW indexes for a specific embedding model. Call when registering a new model.';
";

const DROP_INDEX_FUNCTION: &str = r"
CREATE OR REPLACE FUNCTION core.drop_embedding_model_index(
    p_model_id ULID
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

    RAISE NOTICE 'Dropped HNSW indexes for embedding model %', p_model_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION core.drop_embedding_model_index(ULID) IS
'Drops partial HNSW indexes for a specific embedding model. Call when deactivating/removing a model.';
";

// Trigger to auto-create indexes when a model is inserted
const CREATE_MODEL_TRIGGER: &str = r"
CREATE OR REPLACE FUNCTION core.embedding_model_index_trigger() RETURNS TRIGGER AS $$
BEGIN
    -- Auto-create HNSW indexes when a new model is registered
    PERFORM core.create_embedding_model_index(NEW.id, NEW.dimensions);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_embedding_model_create_index ON core.embedding_models;
CREATE TRIGGER trg_embedding_model_create_index
    AFTER INSERT ON core.embedding_models
    FOR EACH ROW
    EXECUTE FUNCTION core.embedding_model_index_trigger();

COMMENT ON TRIGGER trg_embedding_model_create_index ON core.embedding_models IS
'Automatically creates partial HNSW indexes when a new embedding model is registered.';
";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Create the index management functions
        conn.execute_unprepared(CREATE_INDEX_FUNCTION).await?;
        conn.execute_unprepared(DROP_INDEX_FUNCTION).await?;

        // Create trigger for auto-indexing new models
        conn.execute_unprepared(CREATE_MODEL_TRIGGER).await?;

        // Create indexes for any existing models
        conn.execute_unprepared(
            r"
            DO $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN SELECT id, dimensions FROM core.embedding_models LOOP
                    PERFORM core.create_embedding_model_index(r.id, r.dimensions);
                END LOOP;
            END $$;
            ",
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        // Drop trigger
        conn.execute_unprepared(
            "DROP TRIGGER IF EXISTS trg_embedding_model_create_index ON core.embedding_models",
        )
        .await?;

        // Drop indexes for all existing models
        conn.execute_unprepared(
            r"
            DO $$
            DECLARE
                r RECORD;
            BEGIN
                FOR r IN SELECT id FROM core.embedding_models LOOP
                    PERFORM core.drop_embedding_model_index(r.id);
                END LOOP;
            END $$;
            ",
        )
        .await?;

        // Drop functions
        conn.execute_unprepared("DROP FUNCTION IF EXISTS core.embedding_model_index_trigger()")
            .await?;
        conn.execute_unprepared("DROP FUNCTION IF EXISTS core.drop_embedding_model_index(ULID)")
            .await?;
        conn.execute_unprepared(
            "DROP FUNCTION IF EXISTS core.create_embedding_model_index(ULID, INT)",
        )
        .await?;

        Ok(())
    }
}
