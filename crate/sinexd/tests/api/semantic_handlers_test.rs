use sinex_db::{DbPoolExt, Event, Provenance};
use sinex_primitives::domain::{EntityTypeName, RelationType};
use sinex_primitives::events::{EntityRelatedPayload, EntityResolvedPayload};
use sinex_primitives::rpc::semantic::{
    SemanticEpochCreateRequest, SemanticLaneCreateRequest,
    SemanticLaneDiffRecordEntityRelationRequest, SemanticLaneDiscardRequest,
    SemanticLaneOutputsListRequest, SemanticLaneOutputsSeedCanonicalGraphRequest,
    SemanticLaneOutputsSeedEntityEventsRequest, SemanticLaneOutputsWriteRequest,
};
use sinex_primitives::{
    EntityRelationLaneOutputs, SemanticComponentVersion, SemanticEntityOutput, SemanticLaneKind,
    SemanticRelationOutput, SemanticScope, Uuid,
};
use sinexd::api::handlers::{
    handle_semantic_epoch_create, handle_semantic_lane_create,
    handle_semantic_lane_diff_record_entity_relation, handle_semantic_lane_discard,
    handle_semantic_lane_outputs_list, handle_semantic_lane_outputs_seed_canonical_graph,
    handle_semantic_lane_outputs_seed_entity_events, handle_semantic_lane_outputs_write,
};
use sinexd::api::rpc_server::RpcAuthContext;
use xtask::sandbox::prelude::*;

fn semantic_scope() -> SemanticScope {
    SemanticScope {
        kind: "event_set".to_string(),
        input_ids: vec!["event:alpha".to_string(), "event:beta".to_string()],
        input_set_hash: "gateway-semantic-input-set".to_string(),
    }
}

#[sinex_test]
async fn semantic_lane_seed_canonical_graph_writes_isolated_outputs(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source = ctx
        .pool()
        .knowledge_graph()
        .create_entity(sinex_db::repositories::CreateEntity::person("Seed Alice"))
        .await?;
    let target = ctx
        .pool()
        .knowledge_graph()
        .create_entity(sinex_db::repositories::CreateEntity::project(
            "Seed Project",
        ))
        .await?;
    ctx.pool()
        .knowledge_graph()
        .create_relation(sinex_db::repositories::CreateEntityRelation::new(
            source.id, target.id, "works_on",
        ))
        .await?;

    let scope = semantic_scope();
    let epoch = handle_semantic_epoch_create(
        ctx.pool(),
        SemanticEpochCreateRequest {
            epoch_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0011)),
            name: "canonical-graph-seed".to_string(),
            scope: scope.clone(),
            code_ref: Some("test@canonical-graph".to_string()),
            config_hash: "canonical-graph-config".to_string(),
            components: Vec::new(),
            prompt_set_hash: None,
            model_config_hash: None,
            created_by: None,
            operation_id: None,
            supersedes_epoch_id: None,
        },
        &auth,
    )
    .await?;
    let epoch_id: Uuid = epoch.epoch["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("epoch response missing id"))?
        .parse()?;
    let lane = handle_semantic_lane_create(
        ctx.pool(),
        SemanticLaneCreateRequest {
            lane_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0012)),
            name: "canonical-graph-lane".to_string(),
            kind: SemanticLaneKind::Canonical,
            base_epoch_id: None,
            candidate_epoch_id: epoch_id,
            scope,
            purpose: "gateway canonical graph seed regression".to_string(),
            operation_id: None,
            expires_at: None,
        },
    )
    .await?;
    let lane_id: Uuid = lane.lane["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("lane response missing id"))?
        .parse()?;

    let seeded = handle_semantic_lane_outputs_seed_canonical_graph(
        ctx.pool(),
        SemanticLaneOutputsSeedCanonicalGraphRequest { lane_id },
    )
    .await?;
    assert_eq!(seeded.written, 3);

    let outputs = handle_semantic_lane_outputs_list(
        ctx.pool(),
        SemanticLaneOutputsListRequest { lane_id, limit: 10 },
    )
    .await?;
    assert_eq!(outputs.outputs.len(), 3);
    assert!(
        outputs
            .outputs
            .iter()
            .any(|output| output["output_kind"] == "entity"
                && output["payload"]["canonical_name"] == "seed_alice")
    );
    assert!(
        outputs
            .outputs
            .iter()
            .any(|output| output["output_kind"] == "relation"
                && output["payload"]["predicate"] == "works_on")
    );

    Ok(())
}

#[sinex_test]
async fn semantic_lane_seed_entity_events_writes_provenanced_outputs(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let source_entity_id = Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0021);
    let target_entity_id = Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0022);
    let material_record = ctx
        .pool()
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("gateway-semantic-entity-events"),
            serde_json::json!({ "test": true }),
        )
        .await?;
    let material_id =
        sinex_primitives::Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let source_event = ctx
        .pool()
        .events()
        .insert(
            Event::builder(EntityResolvedPayload {
                entity_id: source_entity_id,
                canonical_name: "gateway_alice".to_string(),
                entity_type: EntityTypeName::new("person"),
                original_name: "Gateway Alice".to_string(),
            })
            .with_provenance(Provenance::from_material(material_id, 0, None, None))
            .build()
            .expect("valid semantic entity event"),
        )
        .await?;
    let target_event = ctx
        .pool()
        .events()
        .insert(
            Event::builder(EntityResolvedPayload {
                entity_id: target_entity_id,
                canonical_name: "gateway_project".to_string(),
                entity_type: EntityTypeName::new("project"),
                original_name: "Gateway Project".to_string(),
            })
            .with_provenance(Provenance::from_material(material_id, 1, None, None))
            .build()
            .expect("valid semantic entity event"),
        )
        .await?;
    let relation_event = ctx
        .pool()
        .events()
        .insert(
            Event::builder(EntityRelatedPayload {
                source_entity_id,
                target_entity_id,
                relation_type: RelationType::new("works_on"),
                confidence: 0.8,
            })
            .with_provenance(Provenance::from_material(material_id, 2, None, None))
            .build()
            .expect("valid semantic relation event"),
        )
        .await?;

    let event_scope = SemanticScope {
        kind: "event_set".to_string(),
        input_ids: vec![
            format!(
                "event:{}",
                source_event
                    .id
                    .as_ref()
                    .ok_or_else(|| color_eyre::eyre::eyre!("source event missing id"))?
                    .as_uuid()
            ),
            format!(
                "event:{}",
                target_event
                    .id
                    .as_ref()
                    .ok_or_else(|| color_eyre::eyre::eyre!("target event missing id"))?
                    .as_uuid()
            ),
            format!(
                "event:{}",
                relation_event
                    .id
                    .as_ref()
                    .ok_or_else(|| color_eyre::eyre::eyre!("relation event missing id"))?
                    .as_uuid()
            ),
        ],
        input_set_hash: "gateway-entity-event-scope".to_string(),
    };
    let epoch = handle_semantic_epoch_create(
        ctx.pool(),
        SemanticEpochCreateRequest {
            epoch_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0023)),
            name: "entity-event-seed".to_string(),
            scope: event_scope.clone(),
            code_ref: Some("test@entity-events".to_string()),
            config_hash: "entity-events-config".to_string(),
            components: Vec::new(),
            prompt_set_hash: None,
            model_config_hash: None,
            created_by: None,
            operation_id: None,
            supersedes_epoch_id: None,
        },
        &auth,
    )
    .await?;
    let epoch_id: Uuid = epoch.epoch["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("epoch response missing id"))?
        .parse()?;
    let lane = handle_semantic_lane_create(
        ctx.pool(),
        SemanticLaneCreateRequest {
            lane_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0024)),
            name: "entity-event-lane".to_string(),
            kind: SemanticLaneKind::Shadow,
            base_epoch_id: None,
            candidate_epoch_id: epoch_id,
            scope: event_scope,
            purpose: "gateway entity event seed regression".to_string(),
            operation_id: None,
            expires_at: None,
        },
    )
    .await?;
    let lane_id: Uuid = lane.lane["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("lane response missing id"))?
        .parse()?;

    let seeded = handle_semantic_lane_outputs_seed_entity_events(
        ctx.pool(),
        SemanticLaneOutputsSeedEntityEventsRequest { lane_id },
    )
    .await?;
    assert_eq!(seeded.written, 3);

    let outputs = handle_semantic_lane_outputs_list(
        ctx.pool(),
        SemanticLaneOutputsListRequest { lane_id, limit: 10 },
    )
    .await?;
    assert_eq!(outputs.outputs.len(), 3);
    assert!(outputs.outputs.iter().any(|output| {
        output["output_kind"] == "entity"
            && output["source_event_id"]
                == serde_json::json!(source_event.id.as_ref().expect("source id").as_uuid())
            && output["payload"]["canonical_name"] == "gateway_alice"
    }));
    assert!(outputs.outputs.iter().any(|output| {
        output["output_kind"] == "relation"
            && output["source_event_id"]
                == serde_json::json!(relation_event.id.as_ref().expect("relation id").as_uuid())
            && output["payload"]["predicate"] == "works_on"
            && output["metadata"]["producer"] == "entity_events"
    }));

    Ok(())
}

fn lane_outputs(entity_key: &str, relation_key: &str) -> EntityRelationLaneOutputs {
    EntityRelationLaneOutputs {
        entities: vec![SemanticEntityOutput::new(entity_key, "Alpha", "project")],
        relations: vec![SemanticRelationOutput::new(
            relation_key,
            entity_key,
            entity_key,
            "mentions",
        )],
    }
}

#[sinex_test]
async fn semantic_shadow_lane_handlers_do_not_mutate_canonical_graph(
    ctx: TestContext,
) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let canonical_entities_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(ctx.pool())
        .await?;
    let canonical_relations_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.entity_relations")
            .fetch_one(ctx.pool())
            .await?;

    let scope = semantic_scope();
    let baseline_epoch = handle_semantic_epoch_create(
        ctx.pool(),
        SemanticEpochCreateRequest {
            epoch_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0001)),
            name: "gateway-baseline".to_string(),
            scope: scope.clone(),
            code_ref: Some("test@baseline".to_string()),
            config_hash: "baseline-config".to_string(),
            components: vec![SemanticComponentVersion {
                component: "entity-relation-extractor".to_string(),
                version: "1".to_string(),
                config_hash: None,
            }],
            prompt_set_hash: None,
            model_config_hash: None,
            created_by: None,
            operation_id: None,
            supersedes_epoch_id: None,
        },
        &auth,
    )
    .await?;
    let baseline_epoch_id: Uuid = baseline_epoch.epoch["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("baseline epoch response missing id"))?
        .parse()?;

    let candidate_epoch = handle_semantic_epoch_create(
        ctx.pool(),
        SemanticEpochCreateRequest {
            epoch_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0002)),
            name: "gateway-candidate".to_string(),
            scope: scope.clone(),
            code_ref: Some("test@candidate".to_string()),
            config_hash: "candidate-config".to_string(),
            components: Vec::new(),
            prompt_set_hash: None,
            model_config_hash: None,
            created_by: None,
            operation_id: None,
            supersedes_epoch_id: Some(baseline_epoch_id),
        },
        &auth,
    )
    .await?;
    let candidate_epoch_id: Uuid = candidate_epoch.epoch["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("candidate epoch response missing id"))?
        .parse()?;

    let baseline_lane = handle_semantic_lane_create(
        ctx.pool(),
        SemanticLaneCreateRequest {
            lane_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0003)),
            name: "gateway-baseline-lane".to_string(),
            kind: SemanticLaneKind::Canonical,
            base_epoch_id: None,
            candidate_epoch_id: baseline_epoch_id,
            scope: scope.clone(),
            purpose: "gateway semantic handler regression".to_string(),
            operation_id: None,
            expires_at: None,
        },
    )
    .await?;
    let baseline_lane_id: Uuid = baseline_lane.lane["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("baseline lane response missing id"))?
        .parse()?;

    let candidate_lane = handle_semantic_lane_create(
        ctx.pool(),
        SemanticLaneCreateRequest {
            lane_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0004)),
            name: "gateway-shadow-lane".to_string(),
            kind: SemanticLaneKind::Shadow,
            base_epoch_id: Some(baseline_epoch_id),
            candidate_epoch_id,
            scope,
            purpose: "gateway semantic handler regression".to_string(),
            operation_id: None,
            expires_at: None,
        },
    )
    .await?;
    let candidate_lane_id: Uuid = candidate_lane.lane["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("candidate lane response missing id"))?
        .parse()?;

    let baseline_write = handle_semantic_lane_outputs_write(
        ctx.pool(),
        SemanticLaneOutputsWriteRequest {
            lane_id: baseline_lane_id,
            outputs: lane_outputs("entity-a", "relation-a"),
        },
    )
    .await?;
    let candidate_write = handle_semantic_lane_outputs_write(
        ctx.pool(),
        SemanticLaneOutputsWriteRequest {
            lane_id: candidate_lane_id,
            outputs: lane_outputs("entity-b", "relation-b"),
        },
    )
    .await?;
    assert_eq!(baseline_write.written, 2);
    assert_eq!(candidate_write.written, 2);

    let diff = handle_semantic_lane_diff_record_entity_relation(
        ctx.pool(),
        SemanticLaneDiffRecordEntityRelationRequest {
            diff_id: Some(Uuid::from_u128(0x1346_0000_0000_0000_0000_0000_0000_0005)),
            baseline_lane_id,
            candidate_lane_id,
            max_examples: 10,
            mark_candidate_compared: true,
        },
    )
    .await?;
    assert_eq!(diff.diff["diff_kind"], "entity_relation");
    assert_eq!(diff.diff["counts"]["entity_new"], 1);
    assert_eq!(diff.diff["counts"]["entity_missing"], 1);
    assert_eq!(
        diff.candidate_lane
            .as_ref()
            .and_then(|lane| lane["status"].as_str()),
        Some("compared")
    );

    let semantic_output_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM semantic.lane_outputs")
            .fetch_one(ctx.pool())
            .await?;
    let semantic_diff_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM semantic.lane_diffs")
        .fetch_one(ctx.pool())
        .await?;
    assert_eq!(semantic_output_count, 4);
    assert_eq!(semantic_diff_count, 1);

    let discard = handle_semantic_lane_discard(
        ctx.pool(),
        SemanticLaneDiscardRequest {
            lane_id: candidate_lane_id,
        },
    )
    .await?;
    assert_eq!(discard.discarded_outputs, 2);
    assert_eq!(discard.lane["status"], "discarded");

    let candidate_outputs = handle_semantic_lane_outputs_list(
        ctx.pool(),
        SemanticLaneOutputsListRequest {
            lane_id: candidate_lane_id,
            limit: 100,
        },
    )
    .await?;
    assert!(
        candidate_outputs.outputs.is_empty(),
        "discard must remove raw candidate lane outputs"
    );
    let semantic_output_count_after_discard: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM semantic.lane_outputs")
            .fetch_one(ctx.pool())
            .await?;
    assert_eq!(
        semantic_output_count_after_discard, 2,
        "discard must leave unrelated baseline lane outputs intact"
    );

    let canonical_entities_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(ctx.pool())
        .await?;
    let canonical_relations_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.entity_relations")
            .fetch_one(ctx.pool())
            .await?;
    assert_eq!(
        canonical_entities_after, canonical_entities_before,
        "semantic shadow-lane handlers must not write canonical entities"
    );
    assert_eq!(
        canonical_relations_after, canonical_relations_before,
        "semantic shadow-lane handlers must not write canonical relations"
    );

    Ok(())
}
