//! Durable admission outcomes and operation-scoped import reports.

use super::common::{DbResult, Repository, db_error};
use super::state::OperationRecord;
use crate::schema::ImportOutcomeRecord;
use sqlx::{FromRow, PgPool};
use std::collections::HashMap;
use uuid::Uuid;

/// Database row for an admitted event produced by an import operation.
#[derive(Debug, Clone, FromRow)]
pub struct ImportEventRow {
    pub id: Uuid,
    pub source: String,
    pub event_type: String,
    pub source_material_id: Option<Uuid>,
}

#[cfg(test)]
#[path = "import_outcomes_test.rs"]
mod tests;

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

#[derive(Debug, Clone, FromRow)]
struct SourceDedupCountRow {
    source: String,
    event_type: String,
    admitted: i64,
    suppressed: i64,
    superseded: i64,
    failed: i64,
    dlq: i64,
}

/// One bounded example reference in a source/event-type deduplication group.
#[derive(Debug, Clone, FromRow)]
pub struct SourceDedupExampleRow {
    pub source: String,
    pub event_type: String,
    pub outcome: String,
    pub candidate_event_id: Uuid,
    pub existing_event_id: Option<Uuid>,
}

/// Durable deduplication outcomes grouped by source namespace and event type
/// for the source-status read surface.
#[derive(Debug, Clone)]
pub struct SourceDedupBreakdownRow {
    pub source: String,
    pub event_type: String,
    pub admitted: i64,
    pub suppressed: i64,
    pub superseded: i64,
    pub failed: i64,
    pub dlq: i64,
    pub examples: Vec<SourceDedupExampleRow>,
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
    /// Load durable admission outcomes for the source-status view, retaining
    /// every requested declared pair even when its counts are zero.
    ///
    /// Admitted and superseded events come from the operation lineage and
    /// replacement ledger. Suppressed, failed, and DLQ candidates come from
    /// `audit.import_outcomes`, which is the durable witness for candidates
    /// that never became live rows.
    pub async fn source_status_breakdown(
        &self,
        pairs: &[(String, String)],
        example_limit: i64,
    ) -> DbResult<Vec<SourceDedupBreakdownRow>> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        if example_limit <= 0 {
            return Err(crate::repositories::common::db_error(
                sqlx::Error::Protocol("source-status example limit must be positive".into()),
                "validate source status example limit",
            ));
        }
        let sources = pairs
            .iter()
            .map(|(source, _)| source.clone())
            .collect::<Vec<_>>();
        let event_types = pairs
            .iter()
            .map(|(_, event_type)| event_type.clone())
            .collect::<Vec<_>>();

        let counts = sqlx::query_as::<_, SourceDedupCountRow>(
            r#"
            WITH requested AS (
                SELECT DISTINCT source, event_type
                FROM unnest($1::text[], $2::text[]) AS requested(source, event_type)
            ), operation_events AS (
                SELECT id, source, event_type
                FROM core.events
                WHERE created_by_operation_id IS NOT NULL
                  AND (source, event_type) IN (SELECT source, event_type FROM requested)
                UNION ALL
                SELECT id, source, event_type
                FROM audit.archived_events
                WHERE created_by_operation_id IS NOT NULL
                  AND (source, event_type) IN (SELECT source, event_type FROM requested)
            ), admitted AS (
                SELECT
                    source,
                    event_type,
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
                GROUP BY source, event_type
            ), outcomes AS (
                SELECT
                    source,
                    event_type,
                    COUNT(*) FILTER (WHERE outcome = 'suppressed')::bigint AS suppressed,
                    COUNT(*) FILTER (WHERE outcome = 'failed')::bigint AS failed,
                    COUNT(*) FILTER (WHERE outcome = 'dlq')::bigint AS dlq
                FROM audit.import_outcomes
                WHERE (source, event_type) IN (SELECT source, event_type FROM requested)
                GROUP BY source, event_type
            )
            SELECT
                requested.source,
                requested.event_type,
                COALESCE(admitted.admitted, 0)::bigint AS admitted,
                COALESCE(outcomes.suppressed, 0)::bigint AS suppressed,
                COALESCE(admitted.superseded, 0)::bigint AS superseded,
                COALESCE(outcomes.failed, 0)::bigint AS failed,
                COALESCE(outcomes.dlq, 0)::bigint AS dlq
            FROM requested
            LEFT JOIN admitted USING (source, event_type)
            LEFT JOIN outcomes USING (source, event_type)
            ORDER BY requested.source, requested.event_type
            "#,
        )
        .bind(&sources)
        .bind(&event_types)
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "load source status dedup breakdown"))?;

        let examples = sqlx::query_as::<_, SourceDedupExampleRow>(
            r#"
            WITH requested AS (
                SELECT DISTINCT source, event_type
                FROM unnest($1::text[], $2::text[]) AS requested(source, event_type)
            ), candidates AS (
                SELECT
                    events.source,
                    events.event_type,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM audit.event_replacements replacements
                            WHERE replacements.new_event_id = events.id
                              AND replacements.relation_kind = 'superseded'
                        ) THEN 'superseded'
                        ELSE 'admitted'
                    END AS outcome,
                    events.id AS candidate_event_id,
                    (
                        SELECT replacements.old_event_id
                        FROM audit.event_replacements replacements
                        WHERE replacements.new_event_id = events.id
                          AND replacements.relation_kind = 'superseded'
                        ORDER BY replacements.replaced_at, replacements.id
                        LIMIT 1
                    ) AS existing_event_id
                FROM core.events events
                JOIN requested USING (source, event_type)
                WHERE events.created_by_operation_id IS NOT NULL
                UNION ALL
                SELECT
                    events.source,
                    events.event_type,
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM audit.event_replacements replacements
                            WHERE replacements.new_event_id = events.id
                              AND replacements.relation_kind = 'superseded'
                        ) THEN 'superseded'
                        ELSE 'admitted'
                    END AS outcome,
                    events.id AS candidate_event_id,
                    (
                        SELECT replacements.old_event_id
                        FROM audit.event_replacements replacements
                        WHERE replacements.new_event_id = events.id
                          AND replacements.relation_kind = 'superseded'
                        ORDER BY replacements.replaced_at, replacements.id
                        LIMIT 1
                    ) AS existing_event_id
                FROM audit.archived_events events
                JOIN requested USING (source, event_type)
                WHERE events.created_by_operation_id IS NOT NULL
                UNION ALL
                SELECT
                    outcomes.source,
                    outcomes.event_type,
                    outcomes.outcome,
                    outcomes.candidate_event_id,
                    outcomes.existing_event_id
                FROM audit.import_outcomes outcomes
                JOIN requested USING (source, event_type)
            ), ranked AS (
                SELECT
                    source,
                    event_type,
                    outcome,
                    candidate_event_id,
                    existing_event_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY source, event_type
                        ORDER BY outcome, candidate_event_id
                    ) AS example_rank
                FROM candidates
            )
            SELECT source, event_type, outcome, candidate_event_id, existing_event_id
            FROM ranked
            WHERE example_rank <= $3
            ORDER BY source, event_type, example_rank
            "#,
        )
        .bind(&sources)
        .bind(&event_types)
        .bind(example_limit)
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "load source status dedup examples"))?;

        let mut rows = counts
            .into_iter()
            .map(|row| SourceDedupBreakdownRow {
                source: row.source,
                event_type: row.event_type,
                admitted: row.admitted,
                suppressed: row.suppressed,
                superseded: row.superseded,
                failed: row.failed,
                dlq: row.dlq,
                examples: Vec::new(),
            })
            .collect::<Vec<_>>();
        let row_indexes = rows
            .iter()
            .enumerate()
            .map(|(index, row)| ((row.source.clone(), row.event_type.clone()), index))
            .collect::<HashMap<_, _>>();
        for example in examples {
            if let Some(index) =
                row_indexes.get(&(example.source.clone(), example.event_type.clone()))
            {
                rows[*index].examples.push(example);
            }
        }
        Ok(rows)
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
