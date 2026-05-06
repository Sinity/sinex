use super::common::DbResult;
use sinex_primitives::SinexError;
use sinex_primitives::Uuid;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::HashMap;

pub struct EmbeddingRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> EmbeddingRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    pub async fn register_model(
        &self,
        provider: &str,
        model_name: &str,
        dimensions: i32,
        metadata: &serde_json::Value,
    ) -> DbResult<Uuid> {
        validate_model_input(provider, model_name, dimensions)?;
        self.validate_declared_embedding_dimension(dimensions)
            .await?;

        let row = sqlx::query_scalar!(
            r#"
            INSERT INTO core.embedding_models (provider, model_name, dimensions, metadata)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (provider, model_name) DO UPDATE SET
                dimensions = EXCLUDED.dimensions,
                is_active = true,
                metadata = EXCLUDED.metadata
            RETURNING id as "id!"
            "#,
            provider,
            model_name,
            dimensions,
            metadata,
        )
        .fetch_one(self.pool)
        .await?;
        Ok(row)
    }

    pub async fn ensure_model(
        &self,
        provider: &str,
        model_name: &str,
        dimensions: i32,
    ) -> DbResult<Uuid> {
        self.register_model(provider, model_name, dimensions, &serde_json::json!({}))
            .await
    }

    pub async fn get_active_model(
        &self,
        provider: &str,
        model_name: &str,
    ) -> DbResult<Option<EmbeddingModelRecord>> {
        let row = sqlx::query_as!(
            EmbeddingModelRecord,
            r#"
            SELECT id as "id!", provider as "provider!", model_name as "model_name!",
                   dimensions as "dimensions!", is_active as "is_active!", metadata as "metadata!"
            FROM core.embedding_models
            WHERE provider = $1 AND model_name = $2 AND is_active = true
            "#,
            provider,
            model_name,
        )
        .fetch_optional(self.pool)
        .await?;
        Ok(row)
    }

    pub async fn store_event_embedding(
        &self,
        event_id: Uuid,
        model_id: Uuid,
        embedded_text: &str,
        embedding: &[f32],
    ) -> DbResult<Uuid> {
        validate_non_empty_text("embedded_text", embedded_text)?;
        self.validate_embedding_for_model(model_id, embedding, "store_event_embedding")
            .await?;

        let embedding_str = format_vector(embedding);
        // Use DO UPDATE … RETURNING so we always get back the real persisted ID
        // rather than generating a synthetic UUID when the row already exists.
        let row = sqlx::query!(
            r#"
            INSERT INTO core.event_embeddings (event_id, embedding_model_id, embedded_text, embedding)
            VALUES ($1, $2, $3, $4::text::vector)
            ON CONFLICT (event_id, embedding_model_id) DO UPDATE SET
                embedded_text = EXCLUDED.embedded_text,
                embedding = EXCLUDED.embedding
            RETURNING id as "id!"
            "#,
            event_id,
            model_id,
            embedded_text,
            embedding_str,
        )
        .fetch_one(self.pool)
        .await?;
        Ok(row.id)
    }

    pub async fn insert_event_embeddings(&self, rows: &[EventEmbeddingRow]) -> DbResult<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO core.event_embeddings \
             (event_id, embedding_model_id, embedded_text, embedding) ",
        );
        query.push_values(rows, |mut values, row| {
            values
                .push_bind(row.event_id)
                .push_bind(row.model_id)
                .push_bind(&row.embedded_text)
                .push_bind(format_vector(&row.embedding))
                .push_unseparated("::text::vector");
        });
        query.push(" ON CONFLICT (event_id, embedding_model_id) DO NOTHING");

        for row in rows {
            validate_non_empty_text("embedded_text", &row.embedded_text)?;
            self.validate_embedding_for_model(
                row.model_id,
                &row.embedding,
                "insert_event_embeddings",
            )
            .await?;
        }

        let result = query.build().execute(self.pool).await?;
        Ok(result.rows_affected())
    }

    pub async fn events_without_embeddings(
        &self,
        model_id: Uuid,
        event_types: &[&str],
        limit: i64,
    ) -> DbResult<Vec<EmbeddingTarget>> {
        validate_positive_limit(limit)?;
        if event_types.is_empty() {
            return Err(SinexError::validation(
                "events_without_embeddings requires at least one event type",
            ));
        }
        for event_type in event_types {
            validate_non_empty_text("event_type", event_type)?;
        }
        let event_types: Vec<String> = event_types
            .iter()
            .map(|event_type| (*event_type).to_string())
            .collect();

        let rows = sqlx::query_as::<_, EmbeddingTarget>(
            r#"
            SELECT e.id as event_id,
                   e.event_type as event_type,
                   e.payload::text as text_for_embedding
            FROM core.events e
            WHERE e.event_type = ANY($1)
              AND NOT EXISTS (
                  SELECT 1
                  FROM core.event_embeddings ee
                  WHERE ee.event_id = e.id
                    AND ee.embedding_model_id = $2
              )
            ORDER BY e.id ASC
            LIMIT $3
            "#,
        )
        .bind(event_types.as_slice())
        .bind(model_id)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn search_similar(
        &self,
        model_id: Uuid,
        query_embedding: &[f32],
        limit: i64,
    ) -> DbResult<Vec<SimilarityResult>> {
        validate_positive_limit(limit)?;
        self.validate_embedding_for_model(model_id, query_embedding, "search_similar")
            .await?;

        let embedding_str = format_vector(query_embedding);
        let rows = sqlx::query!(
            r#"
            SELECT ee.event_id as "event_id!", ee.embedded_text as "embedded_text!",
                   (1.0::float8 - (ee.embedding <=> $1::text::vector)) as "similarity!: f64"
            FROM core.event_embeddings ee
            WHERE ee.embedding_model_id = $2
            ORDER BY ee.embedding <=> $1::text::vector
            LIMIT $3
            "#,
            embedding_str,
            model_id,
            limit,
        )
        .fetch_all(self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| SimilarityResult {
                event_id: row.event_id,
                embedded_text: row.embedded_text,
                similarity: row.similarity,
            })
            .collect())
    }

    pub async fn knn_search(
        &self,
        query_embedding: &[f32],
        model_id: Uuid,
        limit: i64,
        ef_search: i32,
    ) -> DbResult<Vec<KnnSearchResult>> {
        validate_positive_limit(limit)?;
        if ef_search <= 0 {
            return Err(SinexError::validation("ef_search must be positive"));
        }
        self.validate_embedding_for_model(model_id, query_embedding, "knn_search")
            .await?;

        let embedding_str = format_vector(query_embedding);
        let rows = sqlx::query_as::<_, KnnSearchResult>(
            r#"
            SELECT hs.event_id,
                   hs.cosine_distance
            FROM core.hybrid_search(
                ''::text,
                $1::text::vector,
                $2::uuid,
                $3::int4,
                $4::int4,
                60.0::float8,
                1.0::float8,
                0.0::float8
            ) hs
            ORDER BY hs.vector_rank ASC, hs.event_id ASC
            "#,
        )
        .bind(embedding_str)
        .bind(model_id)
        .bind(limit as i32)
        .bind(ef_search)
        .fetch_all(self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn hybrid_search(
        &self,
        model_id: Uuid,
        query_text: &str,
        query_embedding: &[f32],
        limit: i64,
        rrf_k: i64,
    ) -> DbResult<Vec<HybridSearchResult>> {
        validate_positive_limit(limit)?;
        if rrf_k <= 0 {
            return Err(SinexError::validation("rrf_k must be positive"));
        }
        validate_non_empty_text("query_text", query_text)?;
        self.validate_embedding_for_model(model_id, query_embedding, "hybrid_search")
            .await?;

        let embedding_str = format_vector(query_embedding);
        let rows = sqlx::query!(
            r#"
            WITH vector_results AS (
                SELECT ee.event_id, ee.embedded_text,
                       ROW_NUMBER() OVER (ORDER BY ee.embedding <=> $1::text::vector) as vector_rank,
                       (1.0::float8 - (ee.embedding <=> $1::text::vector)) as vector_similarity
                FROM core.event_embeddings ee
                WHERE ee.embedding_model_id = $2
                ORDER BY ee.embedding <=> $1::text::vector
                LIMIT $4::int8 * 3
            ),
            fts_results AS (
                SELECT e.id as event_id,
                       ROW_NUMBER() OVER (ORDER BY ts_rank_cd(
                           to_tsvector('simple', e.payload::text),
                           websearch_to_tsquery('simple', $3)
                       ) DESC) as fts_rank,
                       ts_rank_cd(
                           to_tsvector('simple', e.payload::text),
                           websearch_to_tsquery('simple', $3)
                       ) as fts_score
                FROM core.events e
                WHERE to_tsvector('simple', e.payload::text) @@ websearch_to_tsquery('simple', $3)
                ORDER BY fts_score DESC
                LIMIT $4::int8 * 3
            ),
            combined AS (
                SELECT COALESCE(v.event_id, f.event_id) as event_id,
                       v.embedded_text as embedded_text,
                       COALESCE(1.0::float8 / ($5::float8 + v.vector_rank::float8), 0.0::float8) as vector_rrf,
                       COALESCE(1.0::float8 / ($5::float8 + f.fts_rank::float8), 0.0::float8) as fts_rrf,
                       COALESCE(v.vector_similarity, 0.0::float8) as vector_similarity,
                       COALESCE(f.fts_score::float8, 0.0::float8) as fts_score
                FROM vector_results v
                FULL OUTER JOIN fts_results f ON v.event_id = f.event_id
            )
            SELECT event_id as "event_id!", embedded_text as "embedded_text?: String",
                   (vector_rrf + fts_rrf) as "rrf_score!: f64",
                   vector_similarity as "vector_similarity!: f64",
                   fts_score as "fts_score!: f64"
            FROM combined
            ORDER BY (vector_rrf + fts_rrf) DESC
            LIMIT $4::int8
            "#,
            embedding_str,
            model_id,
            query_text,
            limit,
            rrf_k as f64,
        )
        .fetch_all(self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| HybridSearchResult {
                event_id: row.event_id,
                embedded_text: row.embedded_text,
                rrf_score: row.rrf_score,
                vector_similarity: row.vector_similarity,
                fts_score: row.fts_score,
            })
            .collect())
    }

    pub async fn count_embeddings(&self) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT count(*) as "count!"
            FROM core.event_embeddings
            "#
        )
        .fetch_one(self.pool)
        .await?;
        Ok(count)
    }

    pub async fn count_models(&self) -> DbResult<i64> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT count(*) as "count!"
            FROM core.embedding_models
            WHERE is_active = true
            "#
        )
        .fetch_one(self.pool)
        .await?;
        Ok(count)
    }

    pub async fn check_cache(
        &self,
        text_hash: &str,
        model_id: Uuid,
    ) -> DbResult<Option<CachedEmbeddingHit>> {
        validate_non_empty_text("text_hash", text_hash)?;
        let row = sqlx::query!(
            r#"
            UPDATE core.embedding_cache
            SET use_count = use_count + 1, last_used_at = now()
            WHERE text_hash = $1 AND embedding_model_id = $2
            RETURNING id as "id!", embedding::text as "embedding_text!"
            "#,
            text_hash,
            model_id,
        )
        .fetch_optional(self.pool)
        .await?;

        Ok(row.map(|row| CachedEmbeddingHit {
            id: row.id,
            embedding_text: row.embedding_text,
        }))
    }

    pub async fn store_cache(
        &self,
        text_hash: &str,
        model_id: Uuid,
        embedding: &[f32],
        text_sample: Option<&str>,
    ) -> DbResult<Uuid> {
        validate_non_empty_text("text_hash", text_hash)?;
        self.validate_embedding_for_model(model_id, embedding, "store_cache")
            .await?;

        let embedding_str = format_vector(embedding);
        let row = sqlx::query!(
            r#"
            INSERT INTO core.embedding_cache (text_hash, embedding_model_id, embedding, text_sample)
            VALUES ($1, $2, $3::text::vector, $4)
            ON CONFLICT (text_hash, embedding_model_id)
            DO UPDATE SET use_count = core.embedding_cache.use_count + 1,
                          last_used_at = now()
            RETURNING id as "id!"
            "#,
            text_hash,
            model_id,
            embedding_str,
            text_sample,
        )
        .fetch_one(self.pool)
        .await?;
        Ok(row.id)
    }

    pub async fn cache_lookup(
        &self,
        text_hashes: &[String],
        model_id: Uuid,
    ) -> DbResult<HashMap<String, Vec<f32>>> {
        if text_hashes.is_empty() {
            return Ok(HashMap::new());
        }
        for text_hash in text_hashes {
            validate_non_empty_text("text_hash", text_hash)?;
        }

        let rows = sqlx::query!(
            r#"
            UPDATE core.embedding_cache
            SET use_count = use_count + 1, last_used_at = now()
            WHERE text_hash = ANY($1)
              AND embedding_model_id = $2
            RETURNING text_hash as "text_hash!", embedding::text as "embedding_text!"
            "#,
            text_hashes,
            model_id,
        )
        .fetch_all(self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                parse_vector(&row.embedding_text).map(|embedding| (row.text_hash, embedding))
            })
            .collect()
    }

    pub async fn cache_upsert(&self, entries: &[CacheEntry], model_id: Uuid) -> DbResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO core.embedding_cache \
             (text_hash, embedding_model_id, embedding, text_sample) ",
        );
        query.push_values(entries, |mut values, entry| {
            values
                .push_bind(&entry.text_hash)
                .push_bind(model_id)
                .push_bind(format_vector(&entry.embedding))
                .push_unseparated("::text::vector")
                .push_bind(&entry.text_sample);
        });
        query.push(
            " ON CONFLICT (text_hash, embedding_model_id) DO UPDATE SET \
              embedding = EXCLUDED.embedding, \
              text_sample = EXCLUDED.text_sample, \
              use_count = core.embedding_cache.use_count + 1, \
              last_used_at = now()",
        );

        for entry in entries {
            validate_non_empty_text("text_hash", &entry.text_hash)?;
            self.validate_embedding_for_model(model_id, &entry.embedding, "cache_upsert")
                .await?;
        }

        query.build().execute(self.pool).await?;
        Ok(())
    }

    async fn validate_declared_embedding_dimension(&self, dimensions: i32) -> DbResult<()> {
        // Dynamic pgvector columns have no declared typmod. If a future schema
        // narrows them to vector(N), fail model registration before writes fail
        // later in less obvious query paths.
        let declared_dim: Option<i32> = sqlx::query_scalar!(
            r#"
            SELECT ((a.atttypmod - 4) / 4)::int4 as "dim"
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'core'
              AND c.relname = 'event_embeddings'
              AND a.attname = 'embedding'
              AND a.atttypmod > 0
            "#,
        )
        .fetch_optional(self.pool)
        .await?
        .flatten();

        if let Some(declared) = declared_dim
            && declared != dimensions
        {
            return Err(SinexError::validation(format!(
                "dimension mismatch: requested {dimensions} but column declares {declared}"
            )));
        }

        Ok(())
    }

    async fn validate_embedding_for_model(
        &self,
        model_id: Uuid,
        embedding: &[f32],
        context: &str,
    ) -> DbResult<()> {
        validate_embedding_values(embedding)?;

        let dimensions = sqlx::query_scalar!(
            r#"
            SELECT dimensions as "dimensions!"
            FROM core.embedding_models
            WHERE id = $1 AND is_active = true
            "#,
            model_id,
        )
        .fetch_optional(self.pool)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("active embedding model not found")
                .with_context("model_id", model_id.to_string())
                .with_context("context", context)
        })?;

        if dimensions as usize != embedding.len() {
            return Err(SinexError::validation(format!(
                "embedding dimension mismatch: model expects {dimensions}, got {}",
                embedding.len()
            ))
            .with_context("model_id", model_id.to_string())
            .with_context("context", context));
        }

        Ok(())
    }
}

fn validate_model_input(provider: &str, model_name: &str, dimensions: i32) -> DbResult<()> {
    validate_non_empty_text("provider", provider)?;
    validate_non_empty_text("model_name", model_name)?;
    if dimensions <= 0 {
        return Err(SinexError::validation(
            "embedding dimensions must be positive",
        ));
    }
    Ok(())
}

fn validate_non_empty_text(field: &str, value: &str) -> DbResult<()> {
    if value.trim().is_empty() {
        return Err(SinexError::validation(format!("{field} cannot be empty")));
    }
    Ok(())
}

fn validate_positive_limit(limit: i64) -> DbResult<()> {
    if limit <= 0 {
        return Err(SinexError::validation("limit must be positive"));
    }
    Ok(())
}

fn validate_embedding_values(values: &[f32]) -> DbResult<()> {
    if values.is_empty() {
        return Err(SinexError::validation("embedding cannot be empty"));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(SinexError::validation(
            "embedding values must be finite floats",
        ));
    }
    Ok(())
}

fn format_vector(values: &[f32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn parse_vector(value: &str) -> DbResult<Vec<f32>> {
    let trimmed = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| SinexError::database("invalid pgvector text format"))?;

    if trimmed.trim().is_empty() {
        return Ok(Vec::new());
    }

    trimmed
        .split(',')
        .map(|part| {
            part.parse::<f32>()
                .map_err(|error| SinexError::database("invalid pgvector float").with_source(error))
        })
        .collect()
}

#[derive(Debug, sqlx::FromRow)]
pub struct EmbeddingModelRecord {
    pub id: Uuid,
    pub provider: String,
    pub model_name: String,
    pub dimensions: i32,
    pub is_active: bool,
    pub metadata: serde_json::Value,
}

#[derive(Debug)]
pub struct SimilarityResult {
    pub event_id: Uuid,
    pub embedded_text: String,
    pub similarity: f64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct KnnSearchResult {
    pub event_id: Uuid,
    pub cosine_distance: f64,
}

#[derive(Debug)]
pub struct HybridSearchResult {
    pub event_id: Uuid,
    /// The embedded text from the vector side of the hybrid search.
    /// `None` when the result originates exclusively from the FTS path
    /// (i.e. no matching vector embedding exists for this event).
    pub embedded_text: Option<String>,
    pub rrf_score: f64,
    pub vector_similarity: f64,
    pub fts_score: f64,
}

#[derive(Debug)]
pub struct CachedEmbeddingHit {
    pub id: Uuid,
    pub embedding_text: String,
}

#[derive(Debug)]
pub struct CacheEntry {
    pub text_hash: String,
    pub text_sample: String,
    pub embedding: Vec<f32>,
}

#[derive(Debug)]
pub struct EventEmbeddingRow {
    pub event_id: Uuid,
    pub model_id: Uuid,
    pub embedded_text: String,
    pub embedding: Vec<f32>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct EmbeddingTarget {
    pub event_id: Uuid,
    pub event_type: String,
    pub text_for_embedding: String,
}
