use super::common::DbResult;
use sinex_primitives::Uuid;
use sqlx::{PgPool, Row};

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
        let embedding_str = format_vector(embedding);
        let row: (Uuid,) = sqlx::query_as(
            r"
            INSERT INTO core.event_embeddings (event_id, embedding_model_id, embedded_text, embedding)
            VALUES ($1, $2, $3, $4::vector)
            ON CONFLICT DO NOTHING
            RETURNING id
            ",
        )
        .bind(event_id)
        .bind(model_id)
        .bind(embedded_text)
        .bind(&embedding_str)
        .fetch_optional(self.pool)
        .await?
        .unwrap_or((Uuid::now_v7(),));
        Ok(row.0)
    }

    pub async fn search_similar(
        &self,
        model_id: Uuid,
        query_embedding: &[f32],
        limit: i64,
    ) -> DbResult<Vec<SimilarityResult>> {
        let embedding_str = format_vector(query_embedding);
        let rows = sqlx::query(
            r"
            SELECT ee.event_id, ee.embedded_text,
                   1 - (ee.embedding <=> $1::vector) as similarity
            FROM core.event_embeddings ee
            WHERE ee.embedding_model_id = $2
            ORDER BY ee.embedding <=> $1::vector
            LIMIT $3
            ",
        )
        .bind(&embedding_str)
        .bind(model_id)
        .bind(limit)
        .fetch_all(self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| SimilarityResult {
                event_id: r.get("event_id"),
                embedded_text: r.get("embedded_text"),
                similarity: r.get("similarity"),
            })
            .collect())
    }

    pub async fn hybrid_search(
        &self,
        model_id: Uuid,
        query_text: &str,
        query_embedding: &[f32],
        limit: i64,
        rrf_k: i64,
    ) -> DbResult<Vec<HybridSearchResult>> {
        let embedding_str = format_vector(query_embedding);
        let rows = sqlx::query(
            r"
            WITH vector_results AS (
                SELECT ee.event_id, ee.embedded_text,
                       ROW_NUMBER() OVER (ORDER BY ee.embedding <=> $1::vector) as vector_rank,
                       1 - (ee.embedding <=> $1::vector) as vector_similarity
                FROM core.event_embeddings ee
                WHERE ee.embedding_model_id = $2
                ORDER BY ee.embedding <=> $1::vector
                LIMIT $4 * 3
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
                LIMIT $4 * 3
            ),
            combined AS (
                SELECT COALESCE(v.event_id, f.event_id) as event_id,
                       COALESCE(v.embedded_text, '') as embedded_text,
                       COALESCE(1.0 / ($5 + v.vector_rank), 0) as vector_rrf,
                       COALESCE(1.0 / ($5 + f.fts_rank), 0) as fts_rrf,
                       COALESCE(v.vector_similarity, 0) as vector_similarity,
                       COALESCE(f.fts_score, 0) as fts_score
                FROM vector_results v
                FULL OUTER JOIN fts_results f ON v.event_id = f.event_id
            )
            SELECT event_id, embedded_text,
                   vector_rrf + fts_rrf as rrf_score,
                   vector_similarity, fts_score
            FROM combined
            ORDER BY rrf_score DESC
            LIMIT $4
            ",
        )
        .bind(&embedding_str)
        .bind(model_id)
        .bind(query_text)
        .bind(limit)
        .bind(rrf_k)
        .fetch_all(self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| HybridSearchResult {
                event_id: r.get("event_id"),
                embedded_text: r.get("embedded_text"),
                rrf_score: r.get("rrf_score"),
                vector_similarity: r.get("vector_similarity"),
                fts_score: f64::from(r.get::<f32, _>("fts_score")),
            })
            .collect())
    }

    pub async fn count_embeddings(&self) -> DbResult<i64> {
        let count: (i64,) = sqlx::query_as("SELECT count(*) FROM core.event_embeddings")
            .fetch_one(self.pool)
            .await?;
        Ok(count.0)
    }

    pub async fn count_models(&self) -> DbResult<i64> {
        let count: (i64,) =
            sqlx::query_as("SELECT count(*) FROM core.embedding_models WHERE is_active = true")
                .fetch_one(self.pool)
                .await?;
        Ok(count.0)
    }

    pub async fn check_cache(
        &self,
        text_hash: &str,
        model_id: Uuid,
    ) -> DbResult<Option<CachedEmbeddingHit>> {
        let row = sqlx::query(
            r"
            UPDATE core.embedding_cache
            SET use_count = use_count + 1, last_used_at = now()
            WHERE text_hash = $1 AND embedding_model_id = $2
            RETURNING id, embedding::text as embedding_text
            ",
        )
        .bind(text_hash)
        .bind(model_id)
        .fetch_optional(self.pool)
        .await?;

        Ok(row.map(|r| CachedEmbeddingHit {
            id: r.get("id"),
            embedding_text: r.get("embedding_text"),
        }))
    }

    pub async fn store_cache(
        &self,
        text_hash: &str,
        model_id: Uuid,
        embedding: &[f32],
        text_sample: Option<&str>,
    ) -> DbResult<Uuid> {
        let embedding_str = format_vector(embedding);
        let row: (Uuid,) = sqlx::query_as(
            r"
            INSERT INTO core.embedding_cache (text_hash, embedding_model_id, embedding, text_sample)
            VALUES ($1, $2, $3::vector, $4)
            ON CONFLICT (text_hash, embedding_model_id)
            DO UPDATE SET use_count = core.embedding_cache.use_count + 1,
                          last_used_at = now()
            RETURNING id
            ",
        )
        .bind(text_hash)
        .bind(model_id)
        .bind(&embedding_str)
        .bind(text_sample)
        .fetch_one(self.pool)
        .await?;
        Ok(row.0)
    }
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

#[derive(Debug)]
pub struct HybridSearchResult {
    pub event_id: Uuid,
    pub embedded_text: String,
    pub rrf_score: f64,
    pub vector_similarity: f64,
    pub fts_score: f64,
}

#[derive(Debug)]
pub struct CachedEmbeddingHit {
    pub id: Uuid,
    pub embedding_text: String,
}
