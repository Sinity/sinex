//! Model-based stateful property tests for `SchemaManagementRepository`.
//!
//! Generates randomized schema-management operations and runs them against:
//! - A reference model encoding the repository semantics
//! - The real PostgreSQL-backed repository
//!
//! After each step, the real DB state is checked against the model:
//! - Active schema per `(source, event_type)` key
//! - Per-source schema counts including inactive historical versions
//! - Conflict semantics for same-version/different-content registrations

use std::collections::HashMap;

use color_eyre::eyre::{bail, eyre};
use proptest::prelude::*;
use serde_json::{Value, json};
use sinex_db::repositories::schema_management::NewEventSchema;
use sinex_primitives::domain::{EventSource, EventType};
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone)]
enum SchemaOp {
    Register {
        source_idx: u8,
        event_type_idx: u8,
        version_idx: u8,
        content_idx: u8,
    },
    DeprecateActive {
        source_idx: u8,
        event_type_idx: u8,
    },
    CheckActive {
        source_idx: u8,
        event_type_idx: u8,
    },
}

#[derive(Debug, Clone)]
struct ModelSchema {
    source: String,
    event_type: String,
    version: String,
    content_hash: String,
    active: bool,
}

#[derive(Default)]
struct ReferenceModel {
    schemas_by_hash: HashMap<String, ModelSchema>,
    active_by_key: HashMap<(String, String), String>,
}

impl ReferenceModel {
    fn register(&mut self, new_schema: &NewEventSchema) -> std::result::Result<ModelSchema, ()> {
        let key = (
            new_schema.source.as_str().to_string(),
            new_schema.event_type.as_str().to_string(),
        );
        let content_hash = new_schema.calculate_content_hash().map_err(|_| ())?;

        let existing_was_inactive = self
            .schemas_by_hash
            .get(&content_hash)
            .is_some_and(|existing| !existing.active);
        if existing_was_inactive {
            self.deactivate_key(&key);
        }
        if let Some(existing) = self.schemas_by_hash.get_mut(&content_hash) {
            if existing_was_inactive {
                existing.active = true;
                self.active_by_key.insert(key, content_hash.clone());
            }
            return Ok(existing.clone());
        }

        let version_conflict = self.schemas_by_hash.values().any(|schema| {
            schema.source == key.0
                && schema.event_type == key.1
                && schema.version == new_schema.schema_version
                && schema.content_hash != content_hash
        });
        if version_conflict {
            return Err(());
        }

        self.deactivate_key(&key);
        let schema = ModelSchema {
            source: key.0.clone(),
            event_type: key.1.clone(),
            version: new_schema.schema_version.clone(),
            content_hash: content_hash.clone(),
            active: true,
        };
        self.schemas_by_hash.insert(content_hash.clone(), schema.clone());
        self.active_by_key.insert(key, content_hash);
        Ok(schema)
    }

    fn deprecate_active(&mut self, key: &(String, String)) {
        let Some(active_hash) = self.active_by_key.remove(key) else {
            return;
        };
        if let Some(active) = self.schemas_by_hash.get_mut(&active_hash) {
            active.active = false;
        }
    }

    fn expected_active_hash(&self, key: &(String, String)) -> Option<&str> {
        self.active_by_key.get(key).map(String::as_str)
    }

    fn count_for_source(&self, source: &str) -> usize {
        self.schemas_by_hash
            .values()
            .filter(|schema| schema.source == source)
            .count()
    }

    fn deactivate_key(&mut self, key: &(String, String)) {
        if let Some(active_hash) = self.active_by_key.remove(key)
            && let Some(active) = self.schemas_by_hash.get_mut(&active_hash)
        {
            active.active = false;
        }
    }
}

fn source_name(idx: u8) -> &'static str {
    match idx % 2 {
        0 => "schema-source-alpha",
        _ => "schema-source-beta",
    }
}

fn event_type_name(idx: u8) -> &'static str {
    match idx % 2 {
        0 => "schema.event.created",
        _ => "schema.event.updated",
    }
}

fn version_name(idx: u8) -> &'static str {
    match idx % 2 {
        0 => "1.0.0",
        _ => "2.0.0",
    }
}

fn schema_content(idx: u8) -> Value {
    match idx % 3 {
        0 => json!({
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
        }),
        1 => json!({
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "count": { "type": "integer" }
            },
            "required": ["id", "count"],
        }),
        _ => json!({
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string", "minLength": 1 }
            },
            "required": ["id"],
            "additionalProperties": false,
        }),
    }
}

fn operation_strategy() -> impl Strategy<Value = SchemaOp> {
    prop_oneof![
        (0u8..2, 0u8..2, 0u8..2, 0u8..3).prop_map(|(source_idx, event_type_idx, version_idx, content_idx)| {
            SchemaOp::Register {
                source_idx,
                event_type_idx,
                version_idx,
                content_idx,
            }
        }),
        (0u8..2, 0u8..2).prop_map(|(source_idx, event_type_idx)| SchemaOp::DeprecateActive {
            source_idx,
            event_type_idx,
        }),
        (0u8..2, 0u8..2).prop_map(|(source_idx, event_type_idx)| SchemaOp::CheckActive {
            source_idx,
            event_type_idx,
        }),
    ]
}

fn prefixed_source(run_id: &str, idx: u8) -> String {
    format!("{}-{run_id}", source_name(idx))
}

fn prefixed_event_type(run_id: &str, idx: u8) -> String {
    format!("{}-{run_id}", event_type_name(idx))
}

fn make_schema(
    run_id: &str,
    source_idx: u8,
    event_type_idx: u8,
    version_idx: u8,
    content_idx: u8,
) -> NewEventSchema {
    NewEventSchema {
        source: EventSource::from(prefixed_source(run_id, source_idx)),
        event_type: EventType::from(prefixed_event_type(run_id, event_type_idx)),
        schema_version: version_name(version_idx).to_string(),
        schema_content: schema_content(content_idx),
    }
}

async fn verify_repository_matches_model(
    ctx: &TestContext,
    run_id: &str,
    model: &ReferenceModel,
) -> TestResult<()> {
    let repo = ctx.pool.schemas();

    for source_idx in 0..2u8 {
        let source = prefixed_source(run_id, source_idx);
        let listed = repo.list_schemas_for_source(&source, true).await?;
        let expected_count = model.count_for_source(&source);
        if listed.len() != expected_count {
            bail!("list_schemas_for_source({source}) count mismatch: actual={} expected={expected_count}", listed.len());
        }

        for event_type_idx in 0..2u8 {
            let event_type = prefixed_event_type(run_id, event_type_idx);
            let key = (source.clone(), event_type.clone());
            let actual = repo.get_active_schema(&source, &event_type).await;

            match model.expected_active_hash(&key) {
                Some(expected_hash) => {
                    let actual = actual.map_err(|e| {
                        eyre!("expected active schema for {source}/{event_type}, got error: {e}")
                    })?;
                    if actual.content_hash != expected_hash {
                        bail!(
                            "active schema hash mismatch for {source}/{event_type}: actual={} expected={expected_hash}",
                            actual.content_hash
                        );
                    }
                    if !actual.is_active {
                        bail!("active schema must have is_active=true for {source}/{event_type}");
                    }
                }
                None => {
                    if !actual.is_err() {
                        bail!(
                            "expected no active schema for {source}/{event_type}, got {:?}",
                            actual.ok().map(|schema| schema.content_hash)
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[sinex_prop(cases = 18, timeout = "60s")]
async fn prop_schema_repo_model_matches_reference(
    ctx: &TestContext,
    #[strategy(prop::collection::vec(operation_strategy(), 1..20))] ops: Vec<SchemaOp>,
) -> TestResult<()> {
    let repo = ctx.pool.schemas();
    let run_id = uuid::Uuid::now_v7().to_string().replace('-', "");
    let mut model = ReferenceModel::default();

    for op in ops {
        match op {
            SchemaOp::Register {
                source_idx,
                event_type_idx,
                version_idx,
                content_idx,
            } => {
                let schema = make_schema(
                    &run_id,
                    source_idx,
                    event_type_idx,
                    version_idx,
                    content_idx,
                );
                let expected = model.register(&schema);
                let actual = repo.register_schema(schema).await;

                match expected {
                    Ok(expected_schema) => {
                        let actual = actual.map_err(|e| {
                            eyre!(
                                "register_schema unexpectedly failed for {}/{}/{}: {e}",
                                expected_schema.source,
                                expected_schema.event_type,
                                expected_schema.version
                            )
                        })?;
                        if actual.content_hash != expected_schema.content_hash {
                            bail!(
                                "content hash mismatch after register: actual={} expected={}",
                                actual.content_hash,
                                expected_schema.content_hash
                            );
                        }
                        if actual.source.as_str() != expected_schema.source {
                            bail!(
                                "source mismatch after register: actual={} expected={}",
                                actual.source.as_str(),
                                expected_schema.source
                            );
                        }
                        if actual.event_type.as_str() != expected_schema.event_type {
                            bail!(
                                "event_type mismatch after register: actual={} expected={}",
                                actual.event_type.as_str(),
                                expected_schema.event_type
                            );
                        }
                        if actual.schema_version.as_ref() != expected_schema.version {
                            bail!(
                                "schema_version mismatch after register: actual={} expected={}",
                                actual.schema_version.as_ref(),
                                expected_schema.version
                            );
                        }
                        if !actual.is_active {
                            bail!("registered schema must be active");
                        }
                    }
                    Err(()) => {
                        if !actual.is_err() {
                            bail!("same-version different-content registration should fail");
                        }
                    }
                }
            }
            SchemaOp::DeprecateActive {
                source_idx,
                event_type_idx,
            } => {
                let source = prefixed_source(&run_id, source_idx);
                let event_type = prefixed_event_type(&run_id, event_type_idx);
                if let Ok(active) = repo.get_active_schema(&source, &event_type).await {
                    repo.deprecate_schema(active.id.as_uuid()).await?;
                }
                model.deprecate_active(&(source, event_type));
            }
            SchemaOp::CheckActive {
                source_idx,
                event_type_idx,
            } => {
                let source = prefixed_source(&run_id, source_idx);
                let event_type = prefixed_event_type(&run_id, event_type_idx);
                let key = (source.clone(), event_type.clone());
                let actual = repo.get_active_schema(&source, &event_type).await;

                match model.expected_active_hash(&key) {
                    Some(expected_hash) => {
                        let actual = actual.map_err(|e| {
                            eyre!("expected active schema for {source}/{event_type}, got error: {e}")
                        })?;
                        if actual.content_hash != expected_hash {
                            bail!(
                                "active schema hash mismatch for {source}/{event_type}: actual={} expected={expected_hash}",
                                actual.content_hash
                            );
                        }
                    }
                    None => {
                        if !actual.is_err() {
                            bail!("expected no active schema for {source}/{event_type}");
                        }
                    }
                }
            }
        }

        verify_repository_matches_model(ctx, &run_id, &model).await?;
    }

    Ok(())
}
