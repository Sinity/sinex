use sinex_gateway::handlers::{
    handle_semantic_epoch_create, handle_semantic_lane_create,
    handle_semantic_lane_diff_record_entity_relation, handle_semantic_lane_outputs_write,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::rpc::semantic::{
    SemanticEpochCreateRequest, SemanticLaneCreateRequest,
    SemanticLaneDiffRecordEntityRelationRequest, SemanticLaneOutputsWriteRequest,
};
use sinex_primitives::{
    EntityRelationLaneOutputs, SemanticComponentVersion, SemanticEntityOutput, SemanticLaneKind,
    SemanticRelationOutput, SemanticScope, Uuid,
};
use xtask::sandbox::prelude::*;

fn semantic_scope() -> SemanticScope {
    SemanticScope {
        kind: "event_set".to_string(),
        input_ids: vec!["event:alpha".to_string(), "event:beta".to_string()],
        input_set_hash: "gateway-semantic-input-set".to_string(),
    }
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
