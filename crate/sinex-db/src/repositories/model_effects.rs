//! Repository for dormant model-effect records.
//!
//! This repository is intentionally available to typed DB code, but current
//! production paths do not call it. A future live caller must wire model effects
//! through the event/derivation authority boundary instead of treating this as a
//! standalone cache that can silently bypass provenance, replay, or disclosure
//! policy.

use crate::repositories::{
    Repository,
    common::{DbResult, EnhancedRepository, db_error},
};
use crate::schema::ModelEffects;
use sinex_primitives::Uuid;
use sqlx::PgPool;

pub struct ModelEffectRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> Repository<'a> for ModelEffectRepository<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    fn pool(&self) -> &'a PgPool {
        self.pool
    }
}

impl<'a> EnhancedRepository<'a> for ModelEffectRepository<'a> {
    type Table = ModelEffects;
}

impl ModelEffectRepository<'_> {
    /// Look up a recorded effect by composite key for replay.
    pub async fn find_by_composite_key(
        &self,
        key: &str,
    ) -> DbResult<Option<crate::models::model_effect::ModelEffectRow>> {
        sqlx::query_as(
            "SELECT id, provider, model, prompt_hash, schema_hash, input_hash, \
             composite_key, output, output_hash, replay_policy, recorded_at, \
             recorded_by, source_module_name, source_event_id \
             FROM core.model_effects WHERE composite_key = $1 \
             ORDER BY recorded_at DESC LIMIT 1",
        )
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| db_error(e, "find model effect by composite key"))
    }

    /// Insert a new recorded effect.
    pub async fn insert(
        &self,
        provider: &str,
        model: &str,
        prompt_hash: &str,
        schema_hash: Option<&str>,
        input_hash: &str,
        composite_key: &str,
        output: &str,
        output_hash: &str,
        replay_policy: &str,
        recorded_at: &str,
        recorded_by: &str,
        source_module_name: Option<&str>,
        source_event_id: Option<Uuid>,
    ) -> DbResult<Uuid> {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO core.model_effects \
             (id, provider, model, prompt_hash, schema_hash, input_hash, \
              composite_key, output, output_hash, replay_policy, \
              recorded_at, recorded_by, source_module_name, source_event_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
        )
        .bind(id)
        .bind(provider)
        .bind(model)
        .bind(prompt_hash)
        .bind(schema_hash)
        .bind(input_hash)
        .bind(composite_key)
        .bind(output)
        .bind(output_hash)
        .bind(replay_policy)
        .bind(recorded_at)
        .bind(recorded_by)
        .bind(source_module_name)
        .bind(source_event_id)
        .execute(self.pool())
        .await
        .map_err(|e| db_error(e, "insert model effect"))?;
        Ok(id)
    }

    /// Check whether a composite key already has a recorded effect.
    pub async fn has_effect(&self, key: &str) -> DbResult<bool> {
        let row: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM core.model_effects WHERE composite_key = $1)",
        )
        .bind(key)
        .fetch_one(self.pool())
        .await
        .map_err(|e| db_error(e, "check model effect existence"))?;
        Ok(row.0)
    }
}
