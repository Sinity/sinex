//! Repository for `derivation.product_declarations` (sinex-0vx.4): the
//! write-side registry `derivation.enforce_event_product_declaration()`
//! checks every derived-event write against. Row shape mirrors
//! `sinex_primitives::derivation::DerivationOutputDeclaration`.
//!
//! This repository is intentionally narrow (find-by-id + insert-if-absent):
//! reconciliation policy — deciding whether an existing row that disagrees
//! with a static declaration should be treated as a fail-closed startup
//! error — is a `sinexd` supervisor-startup concern
//! (`crate::automata::product_declarations` there), not a DB-layer one. See
//! sinex-x79t.

use super::common::{DbResult, Repository, db_error};
use crate::schema::records;
use crate::{JsonValue, Timestamp};
use sinex_primitives::Uuid;
use sinex_primitives::derivation::{ClaimSupport, DerivationOutputDeclaration, LaneDiffReport};
use sinex_primitives::error::SinexError;
use sinex_primitives::events::payloads::{
    ActivitySessionBoundaryPayload, ActivityWindowSummaryPayload,
};
use sinex_primitives::events::{EntityRelatedPayload, EntityResolvedPayload};
use sinex_primitives::semantic::{
    EntityRelationLaneOutputs, SemanticEntityOutput, SemanticRelationOutput, SemanticScope,
};
use sinex_primitives::session_lane::{
    SessionBoundaryOutput, SessionLaneOutputs, compute_session_boundaries,
};
use sqlx::PgPool;

/// A `derivation.product_declarations` row as currently stored, for
/// comparison against a static [`DerivationOutputDeclaration`].
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ExistingProductDeclaration {
    pub owner: String,
    pub product_class: String,
    pub write_surface: String,
    pub output_source: Option<String>,
    pub output_event_type: Option<String>,
    pub projection_kind: Option<String>,
    pub artifact_kind: Option<String>,
    pub proposal_kind: Option<String>,
    pub semantics_version: String,
    pub input_eligibility: String,
    pub default_claim_support: serde_json::Value,
    pub verification_command: String,
}

pub struct ProductDeclarationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ProductDeclarationRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl ProductDeclarationRepository<'_> {
    /// Fetch the current row for `declaration_id`, if one exists.
    pub async fn find_by_declaration_id(
        &self,
        declaration_id: &str,
    ) -> DbResult<Option<ExistingProductDeclaration>> {
        sqlx::query_as!(
            ExistingProductDeclaration,
            r#"
            SELECT
                owner,
                product_class,
                write_surface,
                output_source,
                output_event_type,
                projection_kind,
                artifact_kind,
                proposal_kind,
                semantics_version,
                input_eligibility,
                default_claim_support,
                verification_command
            FROM derivation.product_declarations
            WHERE declaration_id = $1
            "#,
            declaration_id,
        )
        .fetch_optional(self.pool)
        .await
        .map_err(|e| db_error(e, "find product declaration by id"))
    }

    /// Insert a new row for `declaration`. A no-op (`ON CONFLICT DO
    /// NOTHING`) if a row for this `declaration_id` already exists — callers
    /// that need fail-closed mismatch detection must call
    /// `find_by_declaration_id` first and compare before inserting.
    pub async fn insert(&self, declaration: &DerivationOutputDeclaration) -> DbResult<()> {
        let default_claim_support =
            serde_json::to_value(declaration.default_support).map_err(|error| {
                SinexError::database(format!(
                    "declaration '{}' default_support could not be serialized: {error}",
                    declaration.declaration_id
                ))
            })?;

        sqlx::query!(
            r#"
            INSERT INTO derivation.product_declarations (
                declaration_id, owner, product_class, write_surface,
                output_source, output_event_type, projection_kind, artifact_kind,
                proposal_kind, semantics_version, input_eligibility,
                default_claim_support, verification_command
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (declaration_id) DO NOTHING
            "#,
            declaration.declaration_id,
            declaration.owner,
            declaration.product_class.as_str(),
            declaration.write_surface.as_str(),
            declaration.output_source,
            declaration.output_event_type,
            declaration.projection_kind,
            declaration.artifact_kind,
            declaration.proposal_kind,
            declaration.semantics_version,
            declaration.input_eligibility.as_str(),
            default_claim_support,
            declaration.verification_command,
        )
        .execute(self.pool)
        .await
        .map_err(|e| db_error(e, "insert product declaration"))?;

        Ok(())
    }
}

// =============================================================================
// DerivationRepository (sinex-0vx.6): `derivation.epochs` / `derivation.lanes`
// / `derivation.lane_outputs` / `derivation.lane_diffs`
// =============================================================================

/// Generic epoch/lane/lane-output/lane-diff control-plane repository
/// (sinex-0vx.6). Generalizes the entity/relation-only `semantic.*` tables
/// (formerly `SemanticRepository`) to every [`sinex_primitives::derivation::
/// DerivedProductClass`] — `output_kind` is a free string ("entity",
/// "relation", "session.boundary", ...) rather than a hardcoded shape, so
/// this repository has no entity/relation-specific knowledge of its own.
///
/// Doctrine guard: `derivation.lane_outputs` rows are evidence, never read
/// as canonical. The only bridge from a lane output to `core.events` is the
/// curation `proposal -> judgment -> finalizer` path (sinex-0vx.5). Nothing
/// in this repository promotes a lane output into an event — callers that
/// need that bridge go through `sinex_db::repositories::authority` /
/// `curation` handlers instead.
pub struct DerivationRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for DerivationRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

#[derive(Debug, Clone)]
pub struct CreateDerivationEpoch {
    pub id: Option<Uuid>,
    pub declaration_id: String,
    pub name: String,
    pub product_class: String,
    pub scope_model: String,
    pub scope: JsonValue,
    pub semantics_version: String,
    pub code_ref: Option<String>,
    pub config_hash: String,
    pub components: JsonValue,
    pub prompt_set_hash: Option<String>,
    pub model_config_hash: Option<String>,
    pub created_by: String,
    pub operation_id: Option<Uuid>,
    pub supersedes_epoch_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct CreateDerivationLane {
    pub id: Option<Uuid>,
    pub declaration_id: String,
    pub name: String,
    /// `canonical` / `shadow` / `experiment`.
    pub kind: String,
    pub product_class: String,
    pub base_epoch_id: Option<Uuid>,
    pub candidate_epoch_id: Uuid,
    pub scope: JsonValue,
    pub purpose: Option<String>,
    pub operation_id: Option<Uuid>,
    pub expires_at: Option<Timestamp>,
}

/// One row to write via [`DerivationRepository::write_lane_outputs`].
#[derive(Debug, Clone)]
pub struct LaneOutputRow {
    pub output_kind: String,
    pub output_key: String,
    pub payload: JsonValue,
    pub claim_support: JsonValue,
    pub source_event_id: Option<Uuid>,
    pub source_material_id: Option<Uuid>,
    pub source_anchor: Option<JsonValue>,
    pub metadata: JsonValue,
}

impl DerivationRepository<'_> {
    pub async fn create_epoch(
        &self,
        input: CreateDerivationEpoch,
    ) -> DbResult<records::DerivationEpochRecord> {
        sqlx::query_as!(
            records::DerivationEpochRecord,
            r#"
            INSERT INTO derivation.epochs (
                id, declaration_id, name, product_class, scope_model, scope,
                semantics_version, code_ref, config_hash, components,
                prompt_set_hash, model_config_hash, created_by, operation_id,
                supersedes_epoch_id
            )
            VALUES (
                COALESCE($1, uuidv7()), $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15
            )
            RETURNING
                id,
                declaration_id,
                name,
                product_class,
                scope_model,
                scope,
                semantics_version,
                code_ref,
                config_hash,
                components,
                prompt_set_hash,
                model_config_hash,
                created_by,
                operation_id,
                supersedes_epoch_id,
                created_at as "created_at: Timestamp"
            "#,
            input.id,
            input.declaration_id,
            input.name,
            input.product_class,
            input.scope_model,
            input.scope,
            input.semantics_version,
            input.code_ref,
            input.config_hash,
            input.components,
            input.prompt_set_hash,
            input.model_config_hash,
            input.created_by,
            input.operation_id,
            input.supersedes_epoch_id,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "create derivation epoch"))
    }

    pub async fn list_epochs(&self, limit: i64) -> DbResult<Vec<records::DerivationEpochRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::DerivationEpochRecord,
            r#"
            SELECT
                id,
                declaration_id,
                name,
                product_class,
                scope_model,
                scope,
                semantics_version,
                code_ref,
                config_hash,
                components,
                prompt_set_hash,
                model_config_hash,
                created_by,
                operation_id,
                supersedes_epoch_id,
                created_at as "created_at: Timestamp"
            FROM derivation.epochs
            ORDER BY created_at DESC, id DESC
            LIMIT $1
            "#,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list derivation epochs"))
    }

    pub async fn create_lane(
        &self,
        input: CreateDerivationLane,
    ) -> DbResult<records::DerivationLaneRecord> {
        sqlx::query_as!(
            records::DerivationLaneRecord,
            r#"
            INSERT INTO derivation.lanes (
                id, declaration_id, name, kind, product_class, base_epoch_id,
                candidate_epoch_id, scope, status, purpose, operation_id,
                expires_at
            )
            VALUES (
                COALESCE($1, uuidv7()), $2, $3, $4, $5, $6, $7, $8, 'planned',
                $9, $10, $11
            )
            RETURNING
                id,
                declaration_id,
                name,
                kind,
                product_class,
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
            input.id,
            input.declaration_id,
            input.name,
            input.kind,
            input.product_class,
            input.base_epoch_id,
            input.candidate_epoch_id,
            input.scope,
            input.purpose,
            input.operation_id,
            input.expires_at.map(|timestamp| timestamp.inner()),
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "create derivation lane"))
    }

    pub async fn get_lane(&self, lane_id: Uuid) -> DbResult<records::DerivationLaneRecord> {
        sqlx::query_as!(
            records::DerivationLaneRecord,
            r#"
            SELECT
                id,
                declaration_id,
                name,
                kind,
                product_class,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            FROM derivation.lanes
            WHERE id = $1
            "#,
            lane_id,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "get derivation lane"))
    }

    pub async fn list_lanes(
        &self,
        status: Option<&str>,
        limit: i64,
    ) -> DbResult<Vec<records::DerivationLaneRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::DerivationLaneRecord,
            r#"
            SELECT
                id,
                declaration_id,
                name,
                kind,
                product_class,
                base_epoch_id,
                candidate_epoch_id,
                scope,
                status,
                purpose,
                operation_id,
                created_at as "created_at: Timestamp",
                completed_at as "completed_at: Timestamp",
                expires_at as "expires_at: Timestamp"
            FROM derivation.lanes
            WHERE ($1::text IS NULL OR status = $1)
            ORDER BY created_at DESC, id DESC
            LIMIT $2
            "#,
            status,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list derivation lanes"))
    }

    pub async fn set_lane_status(
        &self,
        lane_id: Uuid,
        status: &str,
        completed_at: Option<Timestamp>,
    ) -> DbResult<records::DerivationLaneRecord> {
        sqlx::query_as!(
            records::DerivationLaneRecord,
            r#"
            UPDATE derivation.lanes
            SET status = $2, completed_at = COALESCE($3, completed_at)
            WHERE id = $1
            RETURNING
                id,
                declaration_id,
                name,
                kind,
                product_class,
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
        .map_err(|error| db_error(error, "set derivation lane status"))
    }

    /// Mark a lane discarded and delete its raw outputs in one transaction.
    /// Returns the updated lane row and the number of output rows removed.
    pub async fn discard_lane_outputs(
        &self,
        lane_id: Uuid,
        completed_at: Timestamp,
    ) -> DbResult<(records::DerivationLaneRecord, u64)> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| db_error(error, "begin derivation lane discard transaction"))?;

        let lane = sqlx::query_as!(
            records::DerivationLaneRecord,
            r#"
            UPDATE derivation.lanes
            SET status = 'discarded', completed_at = $2
            WHERE id = $1
            RETURNING
                id,
                declaration_id,
                name,
                kind,
                product_class,
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
            completed_at.inner(),
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| db_error(error, "mark derivation lane discarded"))?;

        let discarded_outputs = sqlx::query!(
            r#"DELETE FROM derivation.lane_outputs WHERE lane_id = $1"#,
            lane_id,
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| db_error(error, "delete discarded derivation lane outputs"))?
        .rows_affected();

        tx.commit()
            .await
            .map_err(|error| db_error(error, "commit derivation lane discard transaction"))?;

        Ok((lane, discarded_outputs))
    }

    /// Upsert a batch of generic lane output rows for `lane_id`. Callers
    /// (e.g. the entity/relation semantic-lane handlers) supply
    /// `output_hash` implicitly — this repository hashes `payload` itself so
    /// every caller uses the same hash function.
    pub async fn write_lane_outputs(
        &self,
        lane_id: Uuid,
        product_class: &str,
        outputs: &[LaneOutputRow],
    ) -> DbResult<u64> {
        let mut written = 0u64;
        for output in outputs {
            let output_hash = hash_json(&output.payload)?;
            let result = sqlx::query!(
                r#"
                INSERT INTO derivation.lane_outputs (
                    lane_id, product_class, output_kind, output_key,
                    output_hash, payload, claim_support, source_event_id,
                    source_material_id, source_anchor, metadata
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (lane_id, product_class, output_kind, output_key)
                DO UPDATE SET
                    output_hash = EXCLUDED.output_hash,
                    payload = EXCLUDED.payload,
                    claim_support = EXCLUDED.claim_support,
                    source_event_id = EXCLUDED.source_event_id,
                    source_material_id = EXCLUDED.source_material_id,
                    source_anchor = EXCLUDED.source_anchor,
                    metadata = EXCLUDED.metadata,
                    created_at = CURRENT_TIMESTAMP
                "#,
                lane_id,
                product_class,
                output.output_kind,
                output.output_key,
                output_hash,
                output.payload,
                output.claim_support,
                output.source_event_id,
                output.source_material_id,
                output.source_anchor,
                output.metadata,
            )
            .execute(self.pool)
            .await
            .map_err(|error| db_error(error, "upsert derivation lane output"))?;
            written += result.rows_affected();
        }
        Ok(written)
    }

    /// Read every lane output row for `lane_id`, optionally filtered to one
    /// `output_kind`.
    pub async fn read_lane_outputs(
        &self,
        lane_id: Uuid,
        output_kind: Option<&str>,
    ) -> DbResult<Vec<records::DerivationLaneOutputRecord>> {
        sqlx::query_as!(
            records::DerivationLaneOutputRecord,
            r#"
            SELECT
                lane_id,
                product_class,
                output_kind,
                output_key,
                output_hash,
                payload,
                claim_support,
                source_event_id,
                source_material_id,
                source_anchor,
                metadata,
                created_at as "created_at: Timestamp"
            FROM derivation.lane_outputs
            WHERE lane_id = $1
              AND ($2::text IS NULL OR output_kind = $2)
            ORDER BY output_kind, output_key
            "#,
            lane_id,
            output_kind,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "read derivation lane outputs"))
    }

    pub async fn list_lane_outputs(
        &self,
        lane_id: Uuid,
        limit: i64,
    ) -> DbResult<Vec<records::DerivationLaneOutputRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::DerivationLaneOutputRecord,
            r#"
            SELECT
                lane_id,
                product_class,
                output_kind,
                output_key,
                output_hash,
                payload,
                claim_support,
                source_event_id,
                source_material_id,
                source_anchor,
                metadata,
                created_at as "created_at: Timestamp"
            FROM derivation.lane_outputs
            WHERE lane_id = $1
            ORDER BY created_at DESC, output_kind, output_key
            LIMIT $2
            "#,
            lane_id,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list derivation lane outputs"))
    }

    pub async fn count_lane_outputs(&self, lane_id: Uuid) -> DbResult<i64> {
        sqlx::query_scalar!(
            r#"SELECT COUNT(*) as "count!" FROM derivation.lane_outputs WHERE lane_id = $1"#,
            lane_id,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "count derivation lane outputs"))
    }

    /// Persist a generic [`LaneDiffReport`] as a `derivation.lane_diffs`
    /// row. `diff_kind` mirrors `report.output_kind` — the same
    /// `LaneOutputKind::output_kind()` string, kept as a separate column
    /// name for historical continuity with `semantic.lane_diffs.diff_kind`.
    pub async fn record_lane_diff(
        &self,
        diff_id: Uuid,
        report: &LaneDiffReport,
    ) -> DbResult<records::DerivationLaneDiffRecord> {
        let counts = &report.counts;
        let examples = serde_json::to_value(&report.examples).map_err(|error| {
            SinexError::serialization("serialize derivation lane diff examples")
                .with_std_error(&error)
        })?;
        let report_hash = hash_json(report)?;
        let product_class = report.product_class.as_str();

        sqlx::query_as!(
            records::DerivationLaneDiffRecord,
            r#"
            INSERT INTO derivation.lane_diffs (
                id, baseline_lane_id, candidate_lane_id, product_class,
                diff_kind, counts, examples, report_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING
                id,
                baseline_lane_id,
                candidate_lane_id,
                product_class,
                diff_kind,
                counts,
                examples,
                report_hash,
                created_at as "created_at: Timestamp"
            "#,
            diff_id,
            report.baseline_lane_id,
            report.candidate_lane_id,
            product_class,
            report.output_kind,
            counts,
            examples,
            report_hash,
        )
        .fetch_one(self.pool)
        .await
        .map_err(|error| db_error(error, "record derivation lane diff"))
    }

    pub async fn list_lane_diffs(
        &self,
        lane_id: Uuid,
        limit: i64,
    ) -> DbResult<Vec<records::DerivationLaneDiffRecord>> {
        let limit = clamp_limit(limit);
        sqlx::query_as!(
            records::DerivationLaneDiffRecord,
            r#"
            SELECT
                id,
                baseline_lane_id,
                candidate_lane_id,
                product_class,
                diff_kind,
                counts,
                examples,
                report_hash,
                created_at as "created_at: Timestamp"
            FROM derivation.lane_diffs
            WHERE baseline_lane_id = $1 OR candidate_lane_id = $1
            ORDER BY created_at DESC, id DESC
            LIMIT $2
            "#,
            lane_id,
            limit,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "list derivation lane diffs"))
    }

    // ─── Entity/relation port (sinex-0vx.6) ────────────────────────────────
    //
    // These four methods are the ported `semantic.*`-table-specific entity/
    // relation helpers (formerly `SemanticRepository::write_entity_relation_
    // outputs`/`seed_entity_relation_outputs_from_canonical_graph`/
    // `seed_entity_relation_outputs_from_event_scope`/`read_entity_relation_
    // outputs`), now backed by the generic `derivation.lane_outputs` table
    // (`output_kind` in {"entity", "relation"}) via `write_lane_outputs`
    // above rather than a dedicated `semantic.lane_outputs` table. They stay
    // entity/relation-specific by design — the generic repository above has
    // no such knowledge — mirroring how `crate::semantic::
    // EntityRelationLaneOutputs` is a concrete `LaneOutputKind`, not part of
    // the generic vocabulary.

    pub async fn write_entity_relation_outputs(
        &self,
        lane_id: Uuid,
        product_class: &str,
        outputs: &EntityRelationLaneOutputs,
    ) -> DbResult<u64> {
        let mut rows = Vec::with_capacity(outputs.entities.len() + outputs.relations.len());
        for entity in &outputs.entities {
            rows.push(LaneOutputRow {
                output_kind: "entity".to_string(),
                output_key: entity.entity_key.clone(),
                payload: serde_json::to_value(entity).map_err(|error| {
                    SinexError::serialization("serialize semantic entity output")
                        .with_std_error(&error)
                })?,
                claim_support: claim_support_json(&ClaimSupport::unknown())?,
                source_event_id: None,
                source_material_id: None,
                source_anchor: None,
                metadata: serde_json::json!({}),
            });
        }
        for relation in &outputs.relations {
            rows.push(LaneOutputRow {
                output_kind: "relation".to_string(),
                output_key: relation.relation_key.clone(),
                payload: serde_json::to_value(relation).map_err(|error| {
                    SinexError::serialization("serialize semantic relation output")
                        .with_std_error(&error)
                })?,
                claim_support: claim_support_json(&ClaimSupport::unknown())?,
                source_event_id: None,
                source_material_id: None,
                source_anchor: None,
                metadata: serde_json::json!({}),
            });
        }
        self.write_lane_outputs(lane_id, product_class, &rows).await
    }

    pub async fn seed_entity_relation_outputs_from_canonical_graph(
        &self,
        lane_id: Uuid,
        product_class: &str,
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
        .map_err(|error| db_error(error, "read canonical entities for derivation lane"))?;

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
        .map_err(|error| db_error(error, "read canonical relations for derivation lane"))?;

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
        self.write_entity_relation_outputs(lane_id, product_class, &outputs)
            .await
    }

    pub async fn seed_entity_relation_outputs_from_event_scope(
        &self,
        lane_id: Uuid,
        product_class: &str,
    ) -> DbResult<u64> {
        let lane = self.get_lane(lane_id).await?;
        let scope = parse_scope_value(&lane.scope)?;
        if scope.kind != "event_set" {
            return Err(SinexError::validation(
                "derivation lane entity event seeding requires an event_set scope",
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
        .map_err(|error| db_error(error, "read entity events for derivation lane"))?;

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
                        SinexError::serialization("serialize derivation event entity output")
                            .with_std_error(&error)
                    })?;
                    written += self
                        .write_lane_outputs(
                            lane_id,
                            product_class,
                            &[LaneOutputRow {
                                output_kind: "entity".to_string(),
                                output_key,
                                payload: output_payload,
                                claim_support: claim_support_json(&ClaimSupport::unknown())?,
                                source_event_id: Some(row.id),
                                source_material_id: None,
                                source_anchor: None,
                                metadata: serde_json::json!({"producer": "entity_events"}),
                            }],
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
                        SinexError::serialization("serialize derivation event relation output")
                            .with_std_error(&error)
                    })?;
                    written += self
                        .write_lane_outputs(
                            lane_id,
                            product_class,
                            &[LaneOutputRow {
                                output_kind: "relation".to_string(),
                                output_key,
                                payload: output_payload,
                                claim_support: claim_support_json(&ClaimSupport::unknown())?,
                                source_event_id: Some(row.id),
                                source_material_id: None,
                                source_anchor: None,
                                metadata: serde_json::json!({"producer": "entity_events"}),
                            }],
                        )
                        .await?;
                }
                _ => {}
            }
        }
        Ok(written)
    }

    pub async fn read_entity_relation_outputs(
        &self,
        lane_id: Uuid,
    ) -> DbResult<EntityRelationLaneOutputs> {
        let rows = self.read_lane_outputs(lane_id, None).await?;
        let mut outputs = EntityRelationLaneOutputs::default();
        for row in rows {
            match row.output_kind.as_str() {
                "entity" => {
                    outputs.entities.push(parse_lane_output_payload(
                        "derivation entity lane output",
                        row.payload,
                    )?);
                }
                "relation" => {
                    outputs.relations.push(parse_lane_output_payload(
                        "derivation relation lane output",
                        row.payload,
                    )?);
                }
                _ => {}
            }
        }
        Ok(outputs)
    }

    // ─── Session-boundary port (sinex-0vx.7) ───────────────────────────────
    //
    // Second `LaneOutputKind` instance after the entity/relation port above
    // (sinex-0vx.6) — proves the generic `derivation.lane_outputs` table
    // (`output_kind = "session_boundary"`) generalizes beyond entity/
    // relation with no session-specific schema of its own, same as the
    // entity/relation section's doc says about itself.

    pub async fn write_session_lane_outputs(
        &self,
        lane_id: Uuid,
        product_class: &str,
        outputs: &SessionLaneOutputs,
    ) -> DbResult<u64> {
        let mut rows = Vec::with_capacity(outputs.boundaries.len());
        for boundary in &outputs.boundaries {
            rows.push(LaneOutputRow {
                output_kind: "session_boundary".to_string(),
                output_key: boundary.session_key.clone(),
                payload: serde_json::to_value(boundary).map_err(|error| {
                    SinexError::serialization("serialize session boundary lane output")
                        .with_std_error(&error)
                })?,
                claim_support: claim_support_json(&ClaimSupport::unknown())?,
                source_event_id: None,
                source_material_id: None,
                source_anchor: None,
                metadata: serde_json::json!({}),
            });
        }
        self.write_lane_outputs(lane_id, product_class, &rows).await
    }

    pub async fn read_session_lane_outputs(&self, lane_id: Uuid) -> DbResult<SessionLaneOutputs> {
        let rows = self
            .read_lane_outputs(lane_id, Some("session_boundary"))
            .await?;
        let mut outputs = SessionLaneOutputs::default();
        for row in rows {
            outputs.boundaries.push(parse_lane_output_payload(
                "derivation session boundary lane output",
                row.payload,
            )?);
        }
        Ok(outputs)
    }

    /// Seed a lane from the CANONICAL `activity.session.boundary` events
    /// already persisted in `core.events` — the baseline side of a
    /// session-lane shadow diff, mirroring
    /// `seed_entity_relation_outputs_from_canonical_graph`'s role (read the
    /// existing canonical projection, not recompute it).
    pub async fn seed_session_lane_outputs_from_canonical_events(
        &self,
        lane_id: Uuid,
        product_class: &str,
    ) -> DbResult<u64> {
        let rows = sqlx::query!(
            r#"
            SELECT id as "id!: Uuid", payload
            FROM core.events
            WHERE source = 'derived.session-detector'
              AND event_type = 'activity.session.boundary'
            ORDER BY id
            "#
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| {
            db_error(
                error,
                "read canonical session boundaries for derivation lane",
            )
        })?;

        let mut output_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: ActivitySessionBoundaryPayload =
                parse_lane_output_payload("activity.session.boundary event payload", row.payload)?;
            let boundary = SessionBoundaryOutput {
                session_key: payload.session_id.clone(),
                start_time: payload.start_time,
                end_time: payload.end_time,
                duration_secs: payload.duration_secs,
                event_count: payload.event_count,
                window_count: payload.window_count,
                primary_source: payload.primary_source,
                metadata: serde_json::json!({"source": "core.events"}),
            };
            output_rows.push(LaneOutputRow {
                output_kind: "session_boundary".to_string(),
                output_key: boundary.session_key.clone(),
                payload: serde_json::to_value(&boundary).map_err(|error| {
                    SinexError::serialization("serialize canonical session boundary lane output")
                        .with_std_error(&error)
                })?,
                claim_support: claim_support_json(&ClaimSupport::unknown())?,
                source_event_id: Some(row.id),
                source_material_id: None,
                source_anchor: None,
                metadata: serde_json::json!({"producer": "activity.session.boundary"}),
            });
        }
        self.write_lane_outputs(lane_id, product_class, &output_rows)
            .await
    }

    /// Seed a lane by RECOMPUTING session boundaries from the lane's
    /// `event_set` scope of `activity.window.summary` events — the
    /// candidate/shadow side of a session-lane shadow diff. Uses
    /// [`sinex_primitives::session_lane::compute_session_boundaries`], the
    /// same gap-closure grouping policy `SessionDetector` uses in `sinexd`.
    pub async fn seed_session_lane_outputs_from_window_scope(
        &self,
        lane_id: Uuid,
        product_class: &str,
    ) -> DbResult<u64> {
        let lane = self.get_lane(lane_id).await?;
        let scope = parse_scope_value(&lane.scope)?;
        if scope.kind != "event_set" {
            return Err(SinexError::validation(
                "derivation lane session-window seeding requires an event_set scope",
            )
            .with_context("lane_id", lane_id.to_string())
            .with_context("scope_kind", scope.kind));
        }
        let event_ids = parse_scope_event_ids(&scope)?;

        let rows = sqlx::query!(
            r#"
            SELECT payload
            FROM core.events
            WHERE id = ANY($1)
              AND event_type = 'activity.window.summary'
            ORDER BY id
            "#,
            &event_ids,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|error| db_error(error, "read window summary events for derivation lane"))?;

        let mut windows = Vec::with_capacity(rows.len());
        for row in rows {
            let payload: ActivityWindowSummaryPayload =
                parse_lane_output_payload("activity.window.summary event payload", row.payload)?;
            windows.push(payload);
        }

        let boundaries = compute_session_boundaries(windows);
        let outputs = SessionLaneOutputs { boundaries };
        self.write_session_lane_outputs(lane_id, product_class, &outputs)
            .await
    }
}

fn claim_support_json(support: &ClaimSupport) -> DbResult<JsonValue> {
    serde_json::to_value(support).map_err(|error| {
        SinexError::serialization("serialize derivation lane output claim support")
            .with_std_error(&error)
    })
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
        SinexError::serialization("deserialize derivation lane scope").with_std_error(&error)
    })
}

fn parse_scope_event_ids(scope: &SemanticScope) -> DbResult<Vec<Uuid>> {
    let mut event_ids = Vec::with_capacity(scope.input_ids.len());
    for input_id in &scope.input_ids {
        let raw = input_id.strip_prefix("event:").unwrap_or(input_id);
        let event_id = Uuid::parse_str(raw).map_err(|error| {
            SinexError::validation("derivation lane scope contains invalid event id")
                .with_context("input_id", input_id)
                .with_source(error.to_string())
        })?;
        event_ids.push(event_id);
    }
    Ok(event_ids)
}

fn hash_json(value: &impl serde::Serialize) -> DbResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        SinexError::serialization("serialize derivation lane hash input").with_std_error(&error)
    })?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
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
