//! Repository for semantic epochs, shadow lanes, outputs, and diff reports.

use crate::repositories::{
    Repository,
    common::{DbResult, EnhancedRepository, db_error},
};
use crate::schema::{SemanticEpochs, records};
use crate::{JsonValue, Timestamp};
use serde::Serialize;
use sinex_primitives::{
    EntityRelationDiffReport, EntityRelationLaneOutputs, SemanticEntityOutput, SemanticEpochRecord,
    SemanticLaneRecord as PrimitiveSemanticLaneRecord, SemanticLaneStatus, SemanticRelationOutput,
    SemanticScope, SinexError, Uuid,
    events::{EntityRelatedPayload, EntityResolvedPayload},
};
use sqlx::PgPool;

pub struct SemanticRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for SemanticRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> EnhancedRepository<'a> for SemanticRepository<'a> {
    type Table = SemanticEpochs;
}

#[derive(Debug, Clone)]
pub struct CreateSemanticEpoch {
    pub epoch: SemanticEpochRecord,
    pub created_by: String,
    pub operation_id: Option<Uuid>,
    pub supersedes_epoch_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct CreateSemanticLane {
    pub lane: PrimitiveSemanticLaneRecord,
    pub operation_id: Option<Uuid>,
    pub expires_at: Option<Timestamp>,
}

impl SemanticRepository<'_> {
    pub async fn create_epoch(
        &self,
        input: CreateSemanticEpoch,
    ) -> DbResult<records::SemanticEpochRecord> {
        let scope = serde_json::to_value(&input.epoch.scope).map_err(|error| {
            sinex_primitives::SinexError::serialization("serialize semantic epoch scope")
                .with_std_error(&error)
        })?;
        let components = serde_json::to_value(&input.epoch.components).map_err(|error| {
            sinex_primitives::SinexError::serialization("serialize semantic epoch components")
                .with_std_error(&error)
        })?;

        sqlx::query_as!(
            records::SemanticEpochRecord,
            r#"
            INSERT INTO semantic.epochs (
                id, name, scope, code_ref, config_hash, components,
                prompt_set_hash, model_config_hash, created_by, operation_id,
                supersedes_epoch_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING
                id,
                name,
                scope,
                code_ref,
                config_hash,
                components,
                prompt_set_hash,
                model_config_hash,
                created_by,
                operation_id,
                created_at as "created_at: Timestamp",
                supersedes_epoch_id
            "#,
            input.epoch.epoch_id,
            input.epoch.name,
            scope,
            input.epoch.code_ref,
            input.epoch.config_hash,
            components,
            input.epoch.prompt_set_hash,
            input.epoch.model_config_hash,
            input.created_by,
            input.operation_id,
            input.supersedes_epoch_id,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "create semantic epoch"))
    }

    pub async fn create_lane(
        &self,
        input: CreateSemanticLane,
    ) -> DbResult<records::SemanticLaneRecord> {
        let scope = serde_json::to_value(&input.lane.scope).map_err(|error| {
            sinex_primitives::SinexError::serialization("serialize semantic lane scope")
                .with_std_error(&error)
        })?;
        let kind = serde_json::to_value(input.lane.kind)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| format!("{:?}", input.lane.kind).to_lowercase());
        let status = serde_json::to_value(input.lane.status)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| format!("{:?}", input.lane.status).to_lowercase());

        sqlx::query_as!(
            records::SemanticLaneRecord,
            r#"
            INSERT INTO semantic.lanes (
                id, name, kind, base_epoch_id, candidate_epoch_id, scope,
                status, purpose, operation_id, expires_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING
                id,
                name,
                kind,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            "#,
            input.lane.lane_id,
            input.lane.name,
            kind,
            input.lane.base_epoch_id,
            input.lane.candidate_epoch_id,
            scope,
            status,
            input.lane.purpose,
            input.operation_id,
            input.expires_at.map(|timestamp| timestamp.inner()),
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "create semantic lane"))
    }

    pub async fn list_epochs(&self, limit: i64) -> DbResult<Vec<records::SemanticEpochRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::SemanticEpochRecord,
            r#"
            SELECT
                id,
                name,
                scope,
                code_ref,
                config_hash,
                components,
                prompt_set_hash,
                model_config_hash,
                created_by,
                operation_id,
                created_at as "created_at: Timestamp",
                supersedes_epoch_id
            FROM semantic.epochs
            ORDER BY created_at DESC, id DESC
            LIMIT $1
            "#,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list semantic epochs"))
    }

    pub async fn list_lanes(
        &self,
        status: Option<SemanticLaneStatus>,
        limit: i64,
    ) -> DbResult<Vec<records::SemanticLaneRecord>> {
        let limit = clamp_limit(limit);
        let status = status.map(status_string);
        sqlx::query_as!(
            records::SemanticLaneRecord,
            r#"
            SELECT
                id,
                name,
                kind,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            FROM semantic.lanes
            WHERE ($1::text IS NULL OR status = $1)
            ORDER BY created_at DESC, id DESC
            LIMIT $2
            "#,
            status,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list semantic lanes"))
    }

    pub async fn get_lane(&self, lane_id: Uuid) -> DbResult<records::SemanticLaneRecord> {
        sqlx::query_as!(
            records::SemanticLaneRecord,
            r#"
            SELECT
                id,
                name,
                kind,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            FROM semantic.lanes
            WHERE id = $1
            "#,
            lane_id,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "get semantic lane"))
    }

    pub async fn set_lane_status(
        &self,
        lane_id: Uuid,
        status: SemanticLaneStatus,
        completed_at: Option<Timestamp>,
    ) -> DbResult<records::SemanticLaneRecord> {
        let status = serde_json::to_value(status)
            .ok()
            .and_then(|value| value.as_str().map(str::to_string))
            .unwrap_or_else(|| format!("{status:?}").to_lowercase());

        sqlx::query_as!(
            records::SemanticLaneRecord,
            r#"
            UPDATE semantic.lanes
            SET status = $2, completed_at = COALESCE($3, completed_at)
            WHERE id = $1
            RETURNING
                id,
                name,
                kind,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            "#,
            lane_id,
            status,
            completed_at.map(|timestamp| timestamp.inner()),
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "set semantic lane status"))
    }

    pub async fn discard_lane_outputs(
        &self,
        lane_id: Uuid,
        completed_at: Timestamp,
    ) -> DbResult<(records::SemanticLaneRecord, u64)> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error(error, "begin semantic lane discard transaction"))?;

        let status = status_string(SemanticLaneStatus::Discarded);
        let lane = sqlx::query_as!(
            records::SemanticLaneRecord,
            r#"
            UPDATE semantic.lanes
            SET status = $2, completed_at = $3
            WHERE id = $1
            RETURNING
                id,
                name,
                kind,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            "#,
            lane_id,
            status,
            completed_at.inner(),
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| db_error(error, "mark semantic lane discarded"))?;

        let discarded_outputs = sqlx::query!(
            r#"
            DELETE FROM semantic.lane_outputs
            WHERE lane_id = $1
            "#,
            lane_id,
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| db_error(error, "delete discarded semantic lane outputs"))?
        .rows_affected();

        tx.commit()
            .await
            .map_err(|error| db_error(error, "commit semantic lane discard transaction"))?;

        Ok((lane, discarded_outputs))
    }

    pub async fn write_entity_relation_outputs(
        &self,
        lane_id: Uuid,
        outputs: &EntityRelationLaneOutputs,
    ) -> DbResult<u64> {
        let mut written = 0;
        for entity in &outputs.entities {
            let payload = serde_json::to_value(entity).map_err(|error| {
                sinex_primitives::SinexError::serialization("serialize semantic entity output")
                    .with_std_error(&error)
            })?;
            written += self
                .upsert_lane_output(lane_id, "entity", &entity.entity_key, payload, None, None)
                .await?;
        }
        for relation in &outputs.relations {
            let payload = serde_json::to_value(relation).map_err(|error| {
                sinex_primitives::SinexError::serialization("serialize semantic relation output")
                    .with_std_error(&error)
            })?;
            written += self
                .upsert_lane_output(
                    lane_id,
                    "relation",
                    &relation.relation_key,
                    payload,
                    None,
                    None,
                )
                .await?;
        }
        Ok(written)
    }

    pub async fn seed_entity_relation_outputs_from_canonical_graph(
        &self,
        lane_id: Uuid,
    ) -> DbResult<u64> {
        let entity_rows = sqlx::query!(
            r#"
            SELECT
                id as "id!: Uuid",
                entity_type,
                name,
                canonical_name,
                aliases,
                properties,
                confidence_score
            FROM core.entities
            WHERE is_merged = false
            ORDER BY id
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "read canonical entities for semantic lane"))?;

        let relation_rows = sqlx::query!(
            r#"
            SELECT
                r.id as "id!: Uuid",
                r.from_entity_id as "from_entity_id!: Uuid",
                r.to_entity_id as "to_entity_id!: Uuid",
                r.relation_type,
                r.properties,
                r.confidence_score
            FROM core.entity_relations r
            JOIN core.entities source_entity
              ON source_entity.id = r.from_entity_id
             AND source_entity.is_merged = false
            JOIN core.entities target_entity
              ON target_entity.id = r.to_entity_id
             AND target_entity.is_merged = false
            WHERE r.is_active = true
            ORDER BY r.id
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "read canonical relations for semantic lane"))?;

        let outputs = EntityRelationLaneOutputs {
            entities: entity_rows
                .into_iter()
                .map(|row| SemanticEntityOutput {
                    entity_key: row.id.to_string(),
                    canonical_name: row.canonical_name,
                    entity_type: row.entity_type,
                    category: None,
                    confidence: Some(row.confidence_score),
                    metadata: serde_json::json!({
                        "name": row.name,
                        "aliases": row.aliases,
                        "properties": row.properties,
                        "source": "core.entities",
                    }),
                })
                .collect(),
            relations: relation_rows
                .into_iter()
                .map(|row| SemanticRelationOutput {
                    relation_key: row.id.to_string(),
                    source_entity_key: row.from_entity_id.to_string(),
                    target_entity_key: row.to_entity_id.to_string(),
                    predicate: row.relation_type,
                    weight: Some(row.confidence_score),
                    metadata: serde_json::json!({
                        "properties": row.properties,
                        "source": "core.entity_relations",
                    }),
                })
                .collect(),
        };
        self.write_entity_relation_outputs(lane_id, &outputs).await
    }

    pub async fn seed_entity_relation_outputs_from_event_scope(
        &self,
        lane_id: Uuid,
    ) -> DbResult<u64> {
        let lane = self.get_lane(lane_id).await?;
        let scope = parse_scope_value(&lane.scope)?;
        if scope.kind != "event_set" {
            return Err(SinexError::validation(
                "semantic lane event seeding requires an event_set scope",
            )
            .with_context("lane_id", lane_id.to_string())
            .with_context("scope_kind", scope.kind));
        }

        let event_ids = parse_scope_event_ids(&scope)?;
        let event_types = vec!["entity.resolved".to_string(), "entity.related".to_string()];
        let rows = sqlx::query!(
            r#"
            SELECT
                id as "id!: Uuid",
                event_type,
                payload
            FROM core.events
            WHERE id = ANY($1)
              AND event_type = ANY($2)
            ORDER BY id
            "#,
            &event_ids,
            &event_types,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "read entity events for semantic lane"))?;

        let mut written = 0;
        for row in rows {
            match row.event_type.as_str() {
                "entity.resolved" => {
                    let payload: EntityResolvedPayload = parse_lane_output_payload(
                        "entity.resolved event payload",
                        row.payload.clone(),
                    )?;
                    let output = SemanticEntityOutput {
                        entity_key: payload.entity_id.to_string(),
                        canonical_name: payload.canonical_name,
                        entity_type: payload.entity_type.to_string(),
                        category: None,
                        confidence: None,
                        metadata: serde_json::json!({
                            "original_name": payload.original_name,
                            "source": "core.events",
                            "event_type": row.event_type,
                        }),
                    };
                    let output_key = output.entity_key.clone();
                    let output_payload = serde_json::to_value(output).map_err(|error| {
                        SinexError::serialization("serialize semantic event entity output")
                            .with_std_error(&error)
                    })?;
                    written += self
                        .upsert_lane_output(
                            lane_id,
                            "entity",
                            &output_key,
                            output_payload,
                            Some(row.id),
                            Some(serde_json::json!({"producer": "entity_events"})),
                        )
                        .await?;
                }
                "entity.related" => {
                    let payload: EntityRelatedPayload = parse_lane_output_payload(
                        "entity.related event payload",
                        row.payload.clone(),
                    )?;
                    let output = SemanticRelationOutput {
                        relation_key: row.id.to_string(),
                        source_entity_key: payload.source_entity_id.to_string(),
                        target_entity_key: payload.target_entity_id.to_string(),
                        predicate: payload.relation_type.to_string(),
                        weight: Some(payload.confidence),
                        metadata: serde_json::json!({
                            "source": "core.events",
                            "event_type": row.event_type,
                        }),
                    };
                    let output_key = output.relation_key.clone();
                    let output_payload = serde_json::to_value(output).map_err(|error| {
                        SinexError::serialization("serialize semantic event relation output")
                            .with_std_error(&error)
                    })?;
                    written += self
                        .upsert_lane_output(
                            lane_id,
                            "relation",
                            &output_key,
                            output_payload,
                            Some(row.id),
                            Some(serde_json::json!({"producer": "entity_events"})),
                        )
                        .await?;
                }
                _ => {}
            }
        }
        Ok(written)
    }

    pub async fn record_entity_relation_diff(
        &self,
        diff_id: Uuid,
        baseline_lane_id: Uuid,
        candidate_lane_id: Uuid,
        report: &EntityRelationDiffReport,
    ) -> DbResult<records::SemanticLaneDiffRecord> {
        let counts = serde_json::to_value(&report.counts).map_err(|error| {
            sinex_primitives::SinexError::serialization("serialize semantic lane diff counts")
                .with_std_error(&error)
        })?;
        let examples = serde_json::to_value(&report.examples).map_err(|error| {
            sinex_primitives::SinexError::serialization("serialize semantic lane diff examples")
                .with_std_error(&error)
        })?;
        let report_hash = hash_json(report)?;

        sqlx::query_as!(
            records::SemanticLaneDiffRecord,
            r#"
            INSERT INTO semantic.lane_diffs (
                id, baseline_lane_id, candidate_lane_id, diff_kind,
                counts, examples, report_hash
            )
            VALUES ($1, $2, $3, 'entity_relation', $4, $5, $6)
            RETURNING
                id,
                baseline_lane_id,
                candidate_lane_id,
                diff_kind,
                counts,
                examples,
                report_hash,
                created_at as "created_at: Timestamp"
            "#,
            diff_id,
            baseline_lane_id,
            candidate_lane_id,
            counts,
            examples,
            report_hash,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "record semantic lane diff"))
    }

    pub async fn count_lane_outputs(&self, lane_id: Uuid) -> DbResult<i64> {
        sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!" FROM semantic.lane_outputs WHERE lane_id = $1"#,
            lane_id
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "count semantic lane outputs"))
    }

    pub async fn read_entity_relation_outputs(
        &self,
        lane_id: Uuid,
    ) -> DbResult<EntityRelationLaneOutputs> {
        let rows = self.list_all_lane_outputs(lane_id).await?;
        let mut outputs = EntityRelationLaneOutputs::default();
        for row in rows {
            match row.output_kind.as_str() {
                "entity" => {
                    outputs.entities.push(parse_lane_output_payload(
                        "semantic entity lane output",
                        row.payload,
                    )?);
                }
                "relation" => {
                    outputs.relations.push(parse_lane_output_payload(
                        "semantic relation lane output",
                        row.payload,
                    )?);
                }
                _ => {}
            }
        }
        Ok(outputs)
    }

    async fn list_all_lane_outputs(
        &self,
        lane_id: Uuid,
    ) -> DbResult<Vec<records::SemanticLaneOutputRecord>> {
        sqlx::query_as!(
            records::SemanticLaneOutputRecord,
            r#"
            SELECT
                lane_id,
                output_kind,
                output_key,
                source_event_id,
                source_material_id,
                source_anchor,
                output_hash,
                payload,
                metadata,
                created_at as "created_at: Timestamp"
            FROM semantic.lane_outputs
            WHERE lane_id = $1
            ORDER BY output_kind, output_key
            "#,
            lane_id,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list all semantic lane outputs"))
    }

    pub async fn list_lane_outputs(
        &self,
        lane_id: Uuid,
        limit: i64,
    ) -> DbResult<Vec<records::SemanticLaneOutputRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::SemanticLaneOutputRecord,
            r#"
            SELECT
                lane_id,
                output_kind,
                output_key,
                source_event_id,
                source_material_id,
                source_anchor,
                output_hash,
                payload,
                metadata,
                created_at as "created_at: Timestamp"
            FROM semantic.lane_outputs
            WHERE lane_id = $1
            ORDER BY created_at DESC, output_kind, output_key
            LIMIT $2
            "#,
            lane_id,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list semantic lane outputs"))
    }

    pub async fn list_lane_diffs(
        &self,
        lane_id: Uuid,
        limit: i64,
    ) -> DbResult<Vec<records::SemanticLaneDiffRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::SemanticLaneDiffRecord,
            r#"
            SELECT
                id,
                baseline_lane_id,
                candidate_lane_id,
                diff_kind,
                counts,
                examples,
                report_hash,
                created_at as "created_at: Timestamp"
            FROM semantic.lane_diffs
            WHERE baseline_lane_id = $1 OR candidate_lane_id = $1
            ORDER BY created_at DESC, id DESC
            LIMIT $2
            "#,
            lane_id,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list semantic lane diffs"))
    }

    async fn upsert_lane_output(
        &self,
        lane_id: Uuid,
        output_kind: &str,
        output_key: &str,
        payload: JsonValue,
        source_event_id: Option<Uuid>,
        metadata: Option<JsonValue>,
    ) -> DbResult<u64> {
        let output_hash = hash_json(&payload)?;
        let metadata = metadata.unwrap_or_else(|| serde_json::json!({}));
        let result = sqlx::query!(
            r#"
            INSERT INTO semantic.lane_outputs (
                lane_id, output_kind, output_key, source_event_id, output_hash, payload, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (lane_id, output_kind, output_key)
            DO UPDATE SET output_hash = EXCLUDED.output_hash,
                          source_event_id = EXCLUDED.source_event_id,
                          payload = EXCLUDED.payload,
                          metadata = EXCLUDED.metadata,
                          created_at = CURRENT_TIMESTAMP
            "#,
            lane_id,
            output_kind,
            output_key,
            source_event_id,
            output_hash,
            payload,
            metadata,
        )
        .execute(self.pool)
        .await
        .map_err(|error| db_error(error, "upsert semantic lane output"))?;
        Ok(result.rows_affected())
    }
}

fn hash_json(value: &impl Serialize) -> DbResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        sinex_primitives::SinexError::serialization("serialize semantic lane hash input")
            .with_std_error(&error)
    })?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn parse_lane_output_payload<T: serde::de::DeserializeOwned>(
    label: &str,
    payload: JsonValue,
) -> DbResult<T> {
    serde_json::from_value(payload).map_err(|error| {
        SinexError::serialization(format!("deserialize {label}")).with_std_error(&error)
    })
}

fn parse_scope_value(scope: &JsonValue) -> DbResult<SemanticScope> {
    serde_json::from_value(scope.clone()).map_err(|error| {
        SinexError::serialization("deserialize semantic lane scope").with_std_error(&error)
    })
}

fn parse_scope_event_ids(scope: &SemanticScope) -> DbResult<Vec<Uuid>> {
    let mut event_ids = Vec::with_capacity(scope.input_ids.len());
    for input_id in &scope.input_ids {
        let raw = input_id.strip_prefix("event:").unwrap_or(input_id);
        let event_id = Uuid::parse_str(raw).map_err(|error| {
            SinexError::validation("semantic lane scope contains invalid event id")
                .with_context("input_id", input_id)
                .with_source(error.to_string())
        })?;
        event_ids.push(event_id);
    }
    Ok(event_ids)
}

fn status_string(status: SemanticLaneStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{status:?}").to_lowercase())
}

const fn clamp_limit(limit: i64) -> i64 {
    if limit < 1 {
        1
    } else if limit > 1000 {
        1000
    } else {
        limit
    }
}
