//! Durable admission outcomes and operation-scoped import reports.

use super::common::{DbResult, Repository, db_error};
use super::state::OperationRecord;
use crate::schema::ImportOutcomeRecord;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

/// Database row for an admitted event produced by an import operation.
#[derive(Debug, Clone, FromRow)]
pub struct ImportEventRow {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub source_material_id: Option<Uuid>,
}

/// Database row for an event replacement visible in an import operation.
#[derive(Debug, Clone, FromRow)]
pub struct ImportReplacementRow {
    pub old_event_id: Uuid,
    pub new_event_id: Uuid,
    pub relation_kind: String,
}

/// All durable evidence needed to render an import idempotence report.
#[derive(Debug, Clone)]
pub struct ImportReportData {
    pub operation: OperationRecord,
    pub admitted: Vec<ImportEventRow>,
    pub replacements: Vec<ImportReplacementRow>,
    pub outcomes: Vec<ImportOutcomeRecord>,
}

/// Durable deduplication outcomes grouped by source namespace for the source
/// status read surface.
#[derive(Debug, Clone, FromRow)]
pub struct SourceDedupCountRow {
    pub source: String,
    pub admitted: i64,
    pub suppressed: i64,
    pub superseded: i64,
    pub failed: i64,
    pub dlq: i64,
}

/// Repository for the audit ledger behind import reports.
pub struct ImportOutcomeRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ImportOutcomeRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl ImportOutcomeRepository<'_> {
    /// Load durable admission outcomes for the source-status view.
    ///
    /// Admitted and superseded events come from the operation lineage and
    /// replacement ledger. Suppressed, failed, and DLQ candidates come from
    /// `audit.import_outcomes`, which is the durable witness for candidates
    /// that never became live rows.
    pub async fn source_status_counts(
        &self,
        sources: &[String],
    ) -> DbResult<Vec<SourceDedupCountRow>> {
        if sources.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_as::<_, SourceDedupCountRow>(
            r#"
            WITH requested AS (
                SELECT DISTINCT source
                FROM unnest($1::text[]) AS requested(source)
            ), operation_events AS (
                SELECT id, source
                FROM core.events
                WHERE created_by_operation_id IS NOT NULL
                  AND source = ANY($1::text[])
                UNION ALL
                SELECT id, source
                FROM audit.archived_events
                WHERE created_by_operation_id IS NOT NULL
                  AND source = ANY($1::text[])
            ), admitted AS (
                SELECT
                    source,
                    COUNT(*) FILTER (
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM audit.event_replacements replacements
                            WHERE replacements.new_event_id = operation_events.id
                              AND replacements.relation_kind = 'superseded'
                        )
                    )::bigint AS admitted,
                    COUNT(*) FILTER (
                        WHERE EXISTS (
                            SELECT 1
                            FROM audit.event_replacements replacements
                            WHERE replacements.new_event_id = operation_events.id
                              AND replacements.relation_kind = 'superseded'
                        )
                    )::bigint AS superseded
                FROM operation_events
                GROUP BY source
            ), outcomes AS (
                SELECT
                    source,
                    COUNT(*) FILTER (WHERE outcome = 'suppressed')::bigint AS suppressed,
                    COUNT(*) FILTER (WHERE outcome = 'failed')::bigint AS failed,
                    COUNT(*) FILTER (WHERE outcome = 'dlq')::bigint AS dlq
                FROM audit.import_outcomes
                WHERE source = ANY($1::text[])
                GROUP BY source
            )
            SELECT
                requested.source,
                COALESCE(admitted.admitted, 0)::bigint AS admitted,
                COALESCE(outcomes.suppressed, 0)::bigint AS suppressed,
                COALESCE(admitted.superseded, 0)::bigint AS superseded,
                COALESCE(outcomes.failed, 0)::bigint AS failed,
                COALESCE(outcomes.dlq, 0)::bigint AS dlq
            FROM requested
            LEFT JOIN admitted USING (source)
            LEFT JOIN outcomes USING (source)
            ORDER BY requested.source
            "#,
        )
        .bind(sources)
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "load source status dedup outcomes"))
    }

    /// Record a suppressed candidate once. Candidates without an operation ID
    /// are intentionally ignored because they cannot belong to a rerun report.
    pub async fn record_suppressed(
        &self,
        operation_id: Option<Uuid>,
        candidate_event_id: Uuid,
        source_material_id: Option<Uuid>,
        source: &str,
        event_type: &str,
        reason: &str,
        existing_event_id: Option<Uuid>,
    ) -> DbResult<()> {
        let Some(operation_id) = operation_id else {
            return Ok(());
        };
        sqlx::query(
            r#"
            INSERT INTO audit.import_outcomes (
                operation_id, candidate_event_id, source_material_id,
                source, event_type, outcome, reason, existing_event_id
            )
            VALUES ($1, $2, $3, $4, $5, 'suppressed', $6, $7)
            ON CONFLICT (operation_id, candidate_event_id, outcome)
            DO UPDATE SET reason = EXCLUDED.reason,
                          existing_event_id = EXCLUDED.existing_event_id
            "#,
        )
        .bind(operation_id)
        .bind(candidate_event_id)
        .bind(source_material_id)
        .bind(source)
        .bind(event_type)
        .bind(reason)
        .bind(existing_event_id)
        .execute(self.pool)
        .await
        .map_err(|error| db_error(error, "record suppressed import outcome"))?;
        Ok(())
    }

    /// Load the operation, admitted outputs, replacement lineage, and durable
    /// rejected-candidate outcomes for one import or replay operation.
    pub async fn report(&self, operation_id: Uuid) -> DbResult<Option<ImportReportData>> {
        let operation = sqlx::query_as::<_, OperationRecord>(
            r#"
            SELECT id, operation_type, operator, scope, result_status,
                   result_message, preview_summary, duration_ms
            FROM core.operations_log
            WHERE id = $1
            "#,
        )
        .bind(operation_id)
        .fetch_optional(self.pool)
        .await
        .map_err(|error| db_error(error, "load import operation"))?;
        let Some(operation) = operation else {
            return Ok(None);
        };

        let admitted = sqlx::query_as::<_, ImportEventRow>(
            r#"
            WITH operation_events AS (
                SELECT id, source, event_type, source_material_id
                FROM core.events
                WHERE created_by_operation_id = $1
                UNION ALL
                SELECT id, source, event_type, source_material_id
                FROM audit.archived_events
                WHERE created_by_operation_id = $1
            )
            SELECT id, source, event_type, source_material_id
            FROM operation_events
            ORDER BY id
            "#,
        )
        .bind(operation_id)
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "load import admitted outputs"))?;

        let replacements = sqlx::query_as::<_, ImportReplacementRow>(
            r#"
            WITH operation_events AS (
                SELECT id
                FROM core.events
                WHERE created_by_operation_id = $1
                UNION ALL
                SELECT id
                FROM audit.archived_events
                WHERE created_by_operation_id = $1
            )
            SELECT er.old_event_id, er.new_event_id, er.relation_kind
            FROM audit.event_replacements er
            JOIN operation_events e ON e.id = er.new_event_id
            WHERE er.relation_kind = 'superseded'
            ORDER BY er.replaced_at, er.id
            "#,
        )
        .bind(operation_id)
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "load import replacement outputs"))?;

        let outcomes = sqlx::query_as::<_, ImportOutcomeRecord>(
            r#"
            SELECT id, operation_id, candidate_event_id, source_material_id,
                   source, event_type, outcome, reason, existing_event_id,
                   recorded_at
            FROM audit.import_outcomes
            WHERE operation_id = $1
            ORDER BY recorded_at, id
            "#,
        )
        .bind(operation_id)
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "load import rejected outcomes"))?;

        Ok(Some(ImportReportData {
            operation,
            admitted,
            replacements,
            outcomes,
        }))
    }
}
