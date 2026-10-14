//! Durable request preparation for the bounded Polylogue publication RPC.
//!
//! This stores request identity and prepared coordinates, not child settlement.
//! The event engine, raw consumer, event rows and DLQ remain those authorities.

use super::*;

pub const POLYLOGUE_PUBLICATION_OPERATION: &str = "polylogue.revision.publish";

impl StateRepository<'_> {
    /// All preparation and retry callers serialize on the same database lock.
    /// The lock is scoped to a transaction, so server death cannot strand it.
    async fn lock_polylogue_request(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        operator: &str,
        request_id: &str,
    ) -> DbResult<()> {
        let key = serde_json::to_string(&(POLYLOGUE_PUBLICATION_OPERATION, operator, request_id))
            .map_err(|error| {
            SinexError::serialization("invalid publication scope").with_source(error)
        })?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key)
            .execute(&mut **tx)
            .await
            .map_err(|error| db_error(error, "lock Polylogue publication request"))?;
        Ok(())
    }

    async fn find_polylogue_request_with_executor<'e, E>(
        executor: E,
        operator: &str,
        request_id: &str,
    ) -> DbResult<Option<OperationRecord>>
    where
        E: Executor<'e, Database = Postgres>,
    {
        sqlx::query_as!(
            OperationRecord,
            r#"SELECT id AS "id!: Id<Operation>", operation_type, operator, scope,
                result_status, result_message, preview_summary, duration_ms
               FROM core.operations_log
               WHERE operation_type = $1 AND operator = $2
                 AND scope @> $3"#,
            POLYLOGUE_PUBLICATION_OPERATION,
            operator,
            serde_json::json!({"request_id": request_id}),
        )
        .fetch_optional(executor)
        .await
        .map_err(|error| db_error(error, "find Polylogue publication request"))
    }

    pub async fn find_polylogue_publication(
        &self,
        operator: &str,
        request_id: &str,
    ) -> DbResult<Option<OperationRecord>> {
        Self::find_polylogue_request_with_executor(self.pool, operator, request_id).await
    }

    /// Persist randomly allocated material/event IDs before any stage or
    /// publish side effect. Concurrent identical requests return this same row;
    /// an altered fingerprint cannot replace the original prepared request.
    pub async fn prepare_polylogue_publication(
        &self,
        operator: &str,
        request_id: &str,
        fingerprint: &str,
        mut prepared_scope: JsonValue,
    ) -> DbResult<OperationRecord> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error(error, "begin Polylogue publication preparation"))?;
        Self::lock_polylogue_request(&mut tx, operator, request_id).await?;
        let existing =
            Self::find_polylogue_request_with_executor(&mut *tx, operator, request_id).await?;
        let record = if let Some(existing) = existing {
            if existing
                .scope
                .as_ref()
                .and_then(|scope| scope.get("fingerprint"))
                .and_then(JsonValue::as_str)
                != Some(fingerprint)
            {
                return Err(SinexError::validation(
                    "Polylogue publication request already binds different exact material",
                ));
            }
            existing
        } else {
            let object = prepared_scope.as_object_mut().ok_or_else(|| {
                SinexError::validation("Polylogue publication preparation is not an object")
            })?;
            object.insert("request_id".into(), serde_json::json!(request_id));
            object.insert("fingerprint".into(), serde_json::json!(fingerprint));
            Self::insert_operation_with_executor(
                &mut *tx,
                Operation {
                    id: None,
                    operation_type: POLYLOGUE_PUBLICATION_OPERATION.into(),
                    operator: operator.into(),
                    scope: Some(prepared_scope),
                    result_status: OperationStatus::Running,
                    result_message: None,
                    preview_summary: None,
                    duration_ms: None,
                },
            )
            .await?
        };
        tx.commit()
            .await
            .map_err(|error| db_error(error, "commit Polylogue publication preparation"))?;
        Ok(record)
    }

    /// Hold the same native request lock while staging or recording publish
    /// evidence. Prepared coordinates already committed in the earlier step.
    pub async fn lock_polylogue_publication(
        &self,
        operator: &str,
        request_id: &str,
    ) -> DbResult<(sqlx::Transaction<'_, Postgres>, OperationRecord)> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error(error, "begin Polylogue publication continuation"))?;
        Self::lock_polylogue_request(&mut tx, operator, request_id).await?;
        let record = Self::find_polylogue_request_with_executor(&mut *tx, operator, request_id)
            .await?
            .ok_or_else(|| SinexError::not_found("Polylogue publication request is unknown"))?;
        Ok((tx, record))
    }

    /// Record observed request-level evidence; this never acknowledges or
    /// finalizes a raw envelope and does not invent a per-child state ledger.
    /// Success records the publication operation's durable PubAck, not child
    /// settlement. Status resolves child outcomes from their native owners.
    pub async fn record_polylogue_publication_evidence(
        &self,
        tx: &mut sqlx::Transaction<'_, Postgres>,
        id: Id<Operation>,
        evidence: JsonValue,
        publication_recorded: bool,
    ) -> DbResult<()> {
        sqlx::query!(
            "UPDATE core.operations_log SET preview_summary = $2, result_status = $3
             WHERE id = $1::uuid AND operation_type = $4",
            id.to_uuid(),
            evidence,
            if publication_recorded {
                "success"
            } else {
                "running"
            },
            POLYLOGUE_PUBLICATION_OPERATION,
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| db_error(error, "record Polylogue publication evidence"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DbPoolExt;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn publication_prepare_serializes_identical_requests_and_survives_reconnect(
        ctx: TestContext,
    ) -> TestResult<()> {
        let first_scope = serde_json::json!({"event_ids": [Uuid::now_v7()], "manifest_material_id": Uuid::now_v7()});
        let second_scope = serde_json::json!({"event_ids": [Uuid::now_v7()], "manifest_material_id": Uuid::now_v7()});
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let left_pool = ctx.pool.clone();
        let right_pool = ctx.pool.clone();
        let left_barrier = barrier.clone();
        let (left, right) = tokio::join!(
            async move {
                left_barrier.wait().await;
                left_pool
                    .state()
                    .prepare_polylogue_publication(
                        "fixture-operator",
                        "fixture-request",
                        "same-body",
                        first_scope,
                    )
                    .await
            },
            async move {
                barrier.wait().await;
                right_pool
                    .state()
                    .prepare_polylogue_publication(
                        "fixture-operator",
                        "fixture-request",
                        "same-body",
                        second_scope,
                    )
                    .await
            }
        );
        let left = left?;
        let right = right?;
        assert_eq!(left.id, right.id);
        assert_eq!(left.scope, right.scope);
        assert!(
            ctx.pool
                .state()
                .prepare_polylogue_publication(
                    "fixture-operator",
                    "fixture-request",
                    "changed-body",
                    serde_json::json!({})
                )
                .await
                .is_err()
        );

        // A fresh connection has no in-process request cache to recover from.
        let reconnected = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(ctx.database_url())
            .await?;
        let after_restart = reconnected
            .state()
            .prepare_polylogue_publication(
                "fixture-operator",
                "fixture-request",
                "same-body",
                serde_json::json!({"event_ids": [Uuid::now_v7()]}),
            )
            .await?;
        assert_eq!(after_restart.id, left.id);
        assert_eq!(after_restart.scope, left.scope);
        assert!(
            reconnected
                .state()
                .prepare_polylogue_publication(
                    "fixture-operator",
                    "fixture-request",
                    "changed-body",
                    serde_json::json!({})
                )
                .await
                .is_err()
        );
        reconnected.close().await;
        Ok(())
    }

    #[sinex_test]
    async fn generic_audit_writer_cannot_bypass_publication_request_claim(
        ctx: TestContext,
    ) -> TestResult<()> {
        let result = ctx
            .pool
            .state()
            .log_operation(Operation {
                id: None,
                operation_type: POLYLOGUE_PUBLICATION_OPERATION.into(),
                operator: "fixture-operator".into(),
                scope: Some(serde_json::json!({"request_id": "fixture-request"})),
                result_status: OperationStatus::Running,
                result_message: None,
                preview_summary: None,
                duration_ms: None,
            })
            .await;
        assert!(result.is_err());
        assert!(
            ctx.pool
                .state()
                .find_polylogue_publication("fixture-operator", "fixture-request")
                .await?
                .is_none()
        );
        Ok(())
    }
}
