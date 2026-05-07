//! Repository for `raw.occurrences` and `raw.material_interpretations`.
//!
//! These tables form the stable replay identity surface. Occurrence records
//! identify logical slots in source material; interpretation records track
//! which parser version produced which event for each occurrence.

use crate::repositories::common::Repository;
use crate::DbResult;
use sinex_primitives::events::AnchorKind;
use sinex_primitives::{Timestamp, Uuid};
use sinex_schema::schema::occurrences::{
    MaterialInterpretationRecord, OccurrenceRecord,
};
use sqlx::PgPool;

/// Repository for `raw.occurrences`.
#[derive(Debug, Clone)]
pub struct OccurrenceRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for OccurrenceRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> OccurrenceRepository<'a> {
    /// Register a new occurrence, or return the existing ID if one exists.
    ///
    /// Occurrence identity is `(source_unit_id, source_material_id, anchor_kind, anchor_data)`.
    /// The `natural_key` provides an additional disambiguation dimension.
    pub async fn ensure_occurrence(
        &self,
        source_unit_id: &str,
        source_material_id: Uuid,
        anchor_kind: AnchorKind,
        anchor_data: serde_json::Value,
        natural_key: Option<&str>,
    ) -> DbResult<OccurrenceRecord> {
        let anchor_kind_str = anchor_kind.as_str();

        sqlx::query_as!(
            OccurrenceRecord,
            r#"
            INSERT INTO raw.occurrences (
                id, source_unit_id, source_material_id,
                anchor_kind, anchor_data, natural_key
            ) VALUES (
                $1, $2, $3, $4, $5, $6
            )
            ON CONFLICT (source_unit_id, source_material_id, anchor_kind,
                         COALESCE(natural_key, anchor_data::text))
            DO UPDATE SET id = occurrences.id
            RETURNING
                id, source_unit_id, source_material_id,
                anchor_kind, anchor_data, natural_key, created_at
            "#,
            Uuid::now_v7(),
            source_unit_id,
            source_material_id,
            anchor_kind_str,
            anchor_data,
            natural_key,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| crate::db_error(e, "ensure occurrence"))
    }

    /// Find occurrences for a source material, ordered by creation time.
    pub async fn find_by_material(
        &self,
        source_material_id: Uuid,
    ) -> DbResult<Vec<OccurrenceRecord>> {
        sqlx::query_as!(
            OccurrenceRecord,
            r#"
            SELECT id, source_unit_id, source_material_id,
                   anchor_kind, anchor_data, natural_key, created_at
            FROM raw.occurrences
            WHERE source_material_id = $1
            ORDER BY created_at
            "#,
            source_material_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| crate::db_error(e, "find occurrences by material"))
    }
}

/// Repository for `raw.material_interpretations`.
#[derive(Debug, Clone)]
pub struct MaterialInterpretationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for MaterialInterpretationRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> MaterialInterpretationRepository<'a> {
    /// Record a new interpretation: parser version X interpreted occurrence Y
    /// and produced event Z. Sets the new record as current and marks any
    /// previous interpretation for the same (occurrence, parser) as not current.
    pub async fn record_interpretation(
        &self,
        occurrence_id: Uuid,
        parser_id: &str,
        parser_version: &str,
        source_unit_id: &str,
        event_id: Uuid,
    ) -> DbResult<MaterialInterpretationRecord> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            crate::db_error(e, "begin transaction for record interpretation")
        })?;

        // Mark previous interpretations for this (occurrence, parser) as not current
        sqlx::query!(
            r#"
            UPDATE raw.material_interpretations
            SET is_current = false
            WHERE occurrence_id = $1 AND parser_id = $2 AND is_current = true
            "#,
            occurrence_id,
            parser_id,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| crate::db_error(e, "mark previous interpretations not current"))?;

        // Insert the new current interpretation
        let record = sqlx::query_as!(
            MaterialInterpretationRecord,
            r#"
            INSERT INTO raw.material_interpretations (
                id, occurrence_id, parser_id, parser_version,
                source_unit_id, event_id, is_current
            ) VALUES (
                $1, $2, $3, $4, $5, $6, true
            )
            RETURNING
                id, occurrence_id, parser_id, parser_version,
                source_unit_id, event_id, interpreted_at, is_current
            "#,
            Uuid::now_v7(),
            occurrence_id,
            parser_id,
            parser_version,
            source_unit_id,
            event_id,
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| crate::db_error(e, "insert material interpretation"))?;

        tx.commit().await.map_err(|e| {
            crate::db_error(e, "commit record interpretation")
        })?;

        Ok(record)
    }

    /// Find all interpretations for a specific occurrence.
    pub async fn find_by_occurrence(
        &self,
        occurrence_id: Uuid,
    ) -> DbResult<Vec<MaterialInterpretationRecord>> {
        sqlx::query_as!(
            MaterialInterpretationRecord,
            r#"
            SELECT id, occurrence_id, parser_id, parser_version,
                   source_unit_id, event_id, interpreted_at, is_current
            FROM raw.material_interpretations
            WHERE occurrence_id = $1
            ORDER BY interpreted_at DESC
            "#,
            occurrence_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| crate::db_error(e, "find interpretations by occurrence"))
    }

    /// Find the current interpretation for an occurrence (any parser).
    pub async fn find_current_by_occurrence(
        &self,
        occurrence_id: Uuid,
    ) -> DbResult<Option<MaterialInterpretationRecord>> {
        sqlx::query_as!(
            MaterialInterpretationRecord,
            r#"
            SELECT id, occurrence_id, parser_id, parser_version,
                   source_unit_id, event_id, interpreted_at, is_current
            FROM raw.material_interpretations
            WHERE occurrence_id = $1 AND is_current = true
            ORDER BY interpreted_at DESC
            LIMIT 1
            "#,
            occurrence_id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| crate::db_error(e, "find current interpretation"))
    }

    /// Find interpretations for a material through occurrence join.
    /// Returns all interpretations for occurrences within the given material.
    pub async fn find_by_material(
        &self,
        source_material_id: Uuid,
    ) -> DbResult<Vec<MaterialInterpretationRecord>> {
        sqlx::query_as!(
            MaterialInterpretationRecord,
            r#"
            SELECT mi.id, mi.occurrence_id, mi.parser_id, mi.parser_version,
                   mi.source_unit_id, mi.event_id, mi.interpreted_at, mi.is_current
            FROM raw.material_interpretations mi
            JOIN raw.occurrences o ON mi.occurrence_id = o.id
            WHERE o.source_material_id = $1
            ORDER BY mi.interpreted_at DESC
            "#,
            source_material_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| crate::db_error(e, "find interpretations by material"))
    }

    /// Find interpretations by parser and version.
    pub async fn find_by_parser_version(
        &self,
        parser_id: &str,
        parser_version: &str,
    ) -> DbResult<Vec<MaterialInterpretationRecord>> {
        sqlx::query_as!(
            MaterialInterpretationRecord,
            r#"
            SELECT id, occurrence_id, parser_id, parser_version,
                   source_unit_id, event_id, interpreted_at, is_current
            FROM raw.material_interpretations
            WHERE parser_id = $1 AND parser_version = $2
            ORDER BY interpreted_at DESC
            "#,
            parser_id,
            parser_version,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| crate::db_error(e, "find interpretations by parser version"))
    }
}
