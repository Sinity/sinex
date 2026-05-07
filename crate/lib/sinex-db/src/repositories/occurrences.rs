//! Repository for `raw.occurrences` and `raw.material_interpretations`.
//!
//! These repositories support the occurrence/interpretation model that makes
//! replay and dedup explicit:
//!
//! - **`OccurrenceRepository`** — CRUD for stable occurrence slots.
//!   Query: "what occurrences exist for this material?"
//! - **`InterpretationRepository`** — CRUD for interpretation history.
//!   Query: "what's the latest interpretation for this occurrence?"
//!   Query: "compare old vs new interpretation sets for a parser version upgrade."

use super::common::{DbResult, db_error};
use sinex_primitives::events::admission::OccurrenceAnchorKind;
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

// =============================================================================
// OccurrenceRepository
// =============================================================================

/// Manages `raw.occurrences` — stable logical occurrence slots.
#[derive(Clone)]
pub struct OccurrenceRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> OccurrenceRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Record a new occurrence.
    pub async fn record(
        &self,
        source_unit_id: &str,
        source_material_id: Uuid,
        anchor_kind: OccurrenceAnchorKind,
        anchor_data: &JsonValue,
        natural_key: Option<&str>,
    ) -> DbResult<Uuid> {
        let anchor_kind_str = anchor_kind.as_str();
        let id = sqlx::query_scalar!(
            r#"INSERT INTO raw.occurrences (source_unit_id, source_material_id, anchor_kind, anchor_data, natural_key)
               VALUES ($1, $2, $3, $4, $5)
               RETURNING id"#,
            source_unit_id,
            source_material_id,
            anchor_kind_str,
            anchor_data as &JsonValue,
            natural_key,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "record occurrence"))?;

        Ok(id)
    }

    /// Find occurrences for a specific source material, ordered by creation time.
    pub async fn find_by_material(
        &self,
        source_material_id: Uuid,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<OccurrenceRow>> {
        let limit = limit.unwrap_or(1000);
        let offset = offset.unwrap_or(0);

        sqlx::query_as!(
            OccurrenceRow,
            r#"SELECT id, source_unit_id, source_material_id, anchor_kind,
                      anchor_data as "anchor_data: JsonValue", natural_key,
                      created_at as "created_at: OffsetDateTime"
               FROM raw.occurrences
               WHERE source_material_id = $1
               ORDER BY created_at DESC
               LIMIT $2 OFFSET $3"#,
            source_material_id,
            limit,
            offset,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find occurrences by material"))
    }

    /// Find occurrences by source unit, ordered by creation time.
    pub async fn find_by_source_unit(
        &self,
        source_unit_id: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> DbResult<Vec<OccurrenceRow>> {
        let limit = limit.unwrap_or(1000);
        let offset = offset.unwrap_or(0);

        sqlx::query_as!(
            OccurrenceRow,
            r#"SELECT id, source_unit_id, source_material_id, anchor_kind,
                      anchor_data as "anchor_data: JsonValue", natural_key,
                      created_at as "created_at: OffsetDateTime"
               FROM raw.occurrences
               WHERE source_unit_id = $1
               ORDER BY created_at DESC
               LIMIT $2 OFFSET $3"#,
            source_unit_id,
            limit,
            offset,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find occurrences by source unit"))
    }

    /// Find an occurrence by its natural key.
    pub async fn find_by_natural_key(
        &self,
        source_unit_id: &str,
        natural_key: &str,
    ) -> DbResult<Option<OccurrenceRow>> {
        sqlx::query_as!(
            OccurrenceRow,
            r#"SELECT id, source_unit_id, source_material_id, anchor_kind,
                      anchor_data as "anchor_data: JsonValue", natural_key,
                      created_at as "created_at: OffsetDateTime"
               FROM raw.occurrences
               WHERE source_unit_id = $1 AND natural_key = $2
               LIMIT 1"#,
            source_unit_id,
            natural_key,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find occurrence by natural key"))
    }
}

/// A row from `raw.occurrences`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OccurrenceRow {
    pub id: Uuid,
    pub source_unit_id: String,
    pub source_material_id: Uuid,
    pub anchor_kind: String,
    pub anchor_data: JsonValue,
    pub natural_key: Option<String>,
    pub created_at: OffsetDateTime,
}

// =============================================================================
// InterpretationRepository
// =============================================================================

/// Manages `raw.material_interpretations` — interpretation history.
#[derive(Clone)]
pub struct InterpretationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> InterpretationRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// Record an interpretation: a specific parser version interpreted a specific occurrence.
    ///
    /// The previous current interpretation for this occurrence is automatically
    /// marked as `is_current = false`.
    pub async fn record(
        &self,
        occurrence_id: Uuid,
        parser_id: &str,
        parser_version: &str,
        source_unit_id: &str,
        event_id: Uuid,
    ) -> DbResult<Uuid> {
        let id = sqlx::query_scalar!(
            r#"INSERT INTO raw.material_interpretations
               (occurrence_id, parser_id, parser_version, source_unit_id, event_id)
               VALUES ($1, $2, $3, $4, $5)
               RETURNING id"#,
            occurrence_id,
            parser_id,
            parser_version,
            source_unit_id,
            event_id,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "record material interpretation"))?;

        // Mark previous interpretations for this occurrence as not current.
        sqlx::query!(
            r#"UPDATE raw.material_interpretations
               SET is_current = false
               WHERE occurrence_id = $1 AND id != $2 AND is_current = true"#,
            occurrence_id,
            id,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "update interpretation is_current"))?;

        Ok(id)
    }

    /// Find all interpretations for an occurrence, ordered by interpretation time.
    pub async fn find_by_occurrence(
        &self,
        occurrence_id: Uuid,
    ) -> DbResult<Vec<InterpretationRow>> {
        sqlx::query_as!(
            InterpretationRow,
            r#"SELECT id, occurrence_id, parser_id, parser_version, source_unit_id,
                      event_id, interpreted_at as "interpreted_at: OffsetDateTime",
                      is_current
               FROM raw.material_interpretations
               WHERE occurrence_id = $1
               ORDER BY interpreted_at DESC"#,
            occurrence_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find interpretations by occurrence"))
    }

    /// Find the current interpretation for an occurrence.
    pub async fn find_current(
        &self,
        occurrence_id: Uuid,
    ) -> DbResult<Option<InterpretationRow>> {
        sqlx::query_as!(
            InterpretationRow,
            r#"SELECT id, occurrence_id, parser_id, parser_version, source_unit_id,
                      event_id, interpreted_at as "interpreted_at: OffsetDateTime",
                      is_current
               FROM raw.material_interpretations
               WHERE occurrence_id = $1 AND is_current = true
               LIMIT 1"#,
            occurrence_id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find current interpretation"))
    }

    /// Find interpretations by parser and version (for comparing output sets).
    pub async fn find_by_parser_version(
        &self,
        parser_id: &str,
        parser_version: &str,
        limit: Option<i64>,
    ) -> DbResult<Vec<InterpretationRow>> {
        let limit = limit.unwrap_or(1000);

        sqlx::query_as!(
            InterpretationRow,
            r#"SELECT id, occurrence_id, parser_id, parser_version, source_unit_id,
                      event_id, interpreted_at as "interpreted_at: OffsetDateTime",
                      is_current
               FROM raw.material_interpretations
               WHERE parser_id = $1 AND parser_version = $2
               ORDER BY interpreted_at DESC
               LIMIT $3"#,
            parser_id,
            parser_version,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "find interpretations by parser version"))
    }

    /// Find the interpretation record for a specific event (navigate event → interpretation).
    pub async fn find_by_event(
        &self,
        event_id: Uuid,
    ) -> DbResult<Option<InterpretationRow>> {
        sqlx::query_as!(
            InterpretationRow,
            r#"SELECT id, occurrence_id, parser_id, parser_version, source_unit_id,
                      event_id, interpreted_at as "interpreted_at: OffsetDateTime",
                      is_current
               FROM raw.material_interpretations
               WHERE event_id = $1
               LIMIT 1"#,
            event_id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find interpretation by event"))
    }

    /// Mark all interpretations for a parser as not current (e.g., after a parser
    /// version upgrade invalidates all previous outputs).
    pub async fn invalidate_parser_outputs(
        &self,
        parser_id: &str,
        source_unit_id: &str,
    ) -> DbResult<u64> {
        let result = sqlx::query!(
            r#"UPDATE raw.material_interpretations
               SET is_current = false
               WHERE parser_id = $1
                 AND source_unit_id = $2
                 AND is_current = true"#,
            parser_id,
            source_unit_id,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "invalidate parser outputs"))?;

        Ok(result.rows_affected())
    }
}

/// A row from `raw.material_interpretations`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct InterpretationRow {
    pub id: Uuid,
    pub occurrence_id: Uuid,
    pub parser_id: String,
    pub parser_version: String,
    pub source_unit_id: String,
    pub event_id: Uuid,
    pub interpreted_at: OffsetDateTime,
    pub is_current: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::JsonValue;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn record_and_find_occurrence(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let repo = OccurrenceRepository::new(&pool);
        let material_id = Uuid::now_v7();

        let occ_id = repo
            .record(
                "test-unit",
                material_id,
                OccurrenceAnchorKind::ByteOffset,
                &serde_json::json!({"offset": 42}),
                None,
            )
            .await?;

        assert!(!occ_id.is_nil());

        let rows = repo.find_by_material(material_id, None, None).await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, occ_id);
        assert_eq!(rows[0].source_unit_id, "test-unit");
        assert_eq!(rows[0].anchor_kind, "byte_offset");
        Ok(())
    }

    #[sinex_test]
    async fn record_interpretation_and_find_current(ctx: TestContext) -> TestResult<()> {
        let pool = ctx.pool();
        let occ_repo = OccurrenceRepository::new(&pool);
        let int_repo = InterpretationRepository::new(&pool);
        let material_id = Uuid::now_v7();

        let occ_id = occ_repo
            .record(
                "test-unit",
                material_id,
                OccurrenceAnchorKind::ByteOffset,
                &serde_json::json!({"offset": 42}),
                None,
            )
            .await?;

        let event_id = Uuid::now_v7();
        let int_id = int_repo
            .record(occ_id, "test-parser", "1.0.0", "test-unit", event_id)
            .await?;

        assert!(!int_id.is_nil());

        let current = int_repo.find_current(occ_id).await?.expect("should exist");
        assert!(current.is_current);
        assert_eq!(current.event_id, event_id);

        // Record a new interpretation — old one should no longer be current.
        let new_event_id = Uuid::now_v7();
        int_repo
            .record(occ_id, "test-parser", "2.0.0", "test-unit", new_event_id)
            .await?;

        let current2 = int_repo
            .find_current(occ_id)
            .await?
            .expect("should exist");
        assert_eq!(current2.event_id, new_event_id);

        // Old interpretation should no longer be current.
        let all = int_repo.find_by_occurrence(occ_id).await?;
        assert_eq!(all.len(), 2);
        assert_eq!(
            all.iter().filter(|r| r.is_current).count(),
            1,
            "exactly one interpretation should be current"
        );
        Ok(())
    }
}
