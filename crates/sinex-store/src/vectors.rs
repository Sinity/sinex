//! Rebuildable pgvector indexes over complete stored embedding events.
//! Projection limits are coverage, never admission limits or vector truncation.
use crate::{Error, Result};
use sinex_model::event_types::embedding::TextEmbedding;
use sqlx::{Connection, PgConnection};
use uuid::Uuid;

mod projection;
mod rank;
pub use projection::{apply, initialize, rebuild};
pub use rank::cosine;
pub(crate) use rank::rank_sql;

pub const CONSUMER: &str = "read.embeddings";
/// pgvector's vector type and vector HNSW opclass limits, respectively.
const VECTOR_DIMENSIONS: usize = 16_000;
const INDEX_DIMENSIONS: usize = 2_000;

#[derive(Clone, Debug, PartialEq)]
pub struct Projection {
    pub values: Option<Vec<f32>>,
    pub coverage: &'static str,
    pub reason: Option<&'static str>,
}
impl Projection {
    fn fallback(reason: &'static str) -> Self {
        Self {
            values: None,
            coverage: "fallback",
            reason: Some(reason),
        }
    }
}

/// A normalized float32 projection preserves cosine direction where the
/// backend can represent it. The original f64 components remain untouched.
pub fn project(vector: &[f64]) -> Projection {
    if vector.is_empty() {
        return Projection::fallback("empty_vector");
    }
    if vector.len() > VECTOR_DIMENSIONS {
        return Projection::fallback("vector_type_dimension_limit");
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Projection {
            values: None,
            coverage: "unavailable",
            reason: Some("non_finite_vector"),
        };
    }
    let scale = vector.iter().map(|value| value.abs()).fold(0.0, f64::max);
    if scale == 0.0 {
        return Projection::fallback("zero_norm");
    }
    let norm = vector
        .iter()
        .map(|value| (value / scale).powi(2))
        .sum::<f64>()
        .sqrt();
    let values: Vec<f32> = vector
        .iter()
        .map(|value| (value / scale / norm) as f32)
        .collect();
    if vector
        .iter()
        .zip(&values)
        .any(|(original, projected)| *original != 0.0 && *projected == 0.0)
    {
        return Projection::fallback("projection_precision_range");
    }
    Projection {
        values: Some(values),
        coverage: if vector.len() <= INDEX_DIMENSIONS {
            "indexed"
        } else {
            "exact"
        },
        reason: (vector.len() > INDEX_DIMENSIONS).then_some("hnsw_dimension_limit"),
    }
}

/// Resolve a query reference under the caller's current grants and read
/// position. A missing projection is an explicit lag, never an empty answer.
pub async fn validate_reference(
    conn: &mut PgConnection,
    reference: Uuid,
    position: i64,
) -> Result<()> {
    let payload = sqlx::query_scalar!(
        r#"SELECT payload FROM core.visible_events WHERE id=$1 AND event_type='text.embedding'
        AND commit_seq<=$2 AND NOT audit.is_archived_at(id,$2)"#,
        reference,
        position
    )
    .fetch_optional(&mut *conn)
    .await?
    .flatten()
    .filter(|value| value.get("withheld").is_none())
    .ok_or_else(|| Error::Invalid("embedding_reference_unavailable".into()))?;
    let embedding: TextEmbedding = serde_json::from_value(payload)
        .map_err(|error| Error::Invalid(format!("invalid_embedding_reference: {error}")))?;
    embedding.validate().map_err(Error::Invalid)?;
    if cosine(&embedding.vector, &embedding.vector).is_none() {
        return Err(Error::Invalid("embedding_query_direction_unknown".into()));
    }
    let coverage=sqlx::query_scalar!(r#"SELECT coverage AS "coverage!" FROM core.visible_embedding_vectors WHERE embedding_id=$1"#,reference)
        .fetch_optional(conn).await?.ok_or_else(||Error::Invalid("embedding_projection_pending".into()))?;
    if coverage == "unavailable" {
        return Err(Error::Invalid("embedding_projection_unavailable".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Approximate indexed embedding candidates for an explicit requested count.
/// This optimization never supplies query membership; `rank_sql` scores the
/// full filtered relation, including original-vector fallback and missing rows.
pub async fn indexed_candidates(
    conn: &mut PgConnection,
    reference: Uuid,
    position: i64,
    limit: u64,
    targets: Option<&[Uuid]>,
) -> Result<Vec<(Uuid, Uuid, f64)>> {
    validate_reference(conn, reference, position).await?;
    let row=sqlx::query!(r#"SELECT space,dimensions,projected::text AS projected,coverage AS "coverage!" FROM core.visible_embedding_vectors WHERE embedding_id=$1"#,reference).fetch_one(&mut *conn).await?;
    let (Some(space), Some(dimensions), Some(vector)) = (row.space, row.dimensions, row.projected)
    else {
        return Err(Error::Invalid(
            "embedding_index_unavailable_exact_fallback_required".into(),
        ));
    };
    if row.coverage != "indexed" || !(1..=INDEX_DIMENSIONS as i64).contains(&dimensions) {
        return Err(Error::Invalid(
            "embedding_index_unavailable_exact_fallback_required".into(),
        ));
    }
    if space.len() != 64 || !space.bytes().all(|value| value.is_ascii_hexdigit()) {
        return Err(Error::Invalid("invalid_embedding_space".into()));
    }
    let limit = i64::try_from(limit).ok();
    let statement = format!(
        "SELECT p.embedding_id,p.target_id,1-(p.projected::vector({dimensions}) <=> $1::text::vector({dimensions})) AS score \
        FROM core.visible_embedding_vectors p JOIN core.visible_events e ON e.id=p.embedding_id \
        WHERE p.space='{space}' AND p.coverage='indexed' AND core.class_visible(e.class) \
        AND e.commit_seq<=$2 AND NOT audit.is_archived_at(e.id,$2) \
        AND ($4::uuid[] IS NULL OR p.target_id=ANY($4)) \
        ORDER BY p.projected::vector({dimensions}) <=> $1::text::vector({dimensions}) LIMIT $3"
    );
    // Iterative scans improve filtered approximate coverage. The caller owns
    // its transaction; a transaction-local setting cannot leak into a pool.
    let mut tx = conn.begin().await?;
    sqlx::query("SET LOCAL hnsw.iterative_scan='strict_order'")
        .execute(&mut *tx)
        .await?;
    let rows = sqlx::query_as::<_, (Uuid, Uuid, f64)>(&statement)
        .bind(vector)
        .bind(position)
        .bind(limit)
        .bind(targets)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(rows)
}
