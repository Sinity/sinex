use sinex_db::repositories::{
    CreateEntity, CreateEntityRelation, CreateSemanticEpoch, CreateSemanticLane, DbPoolExt,
};
use sinex_primitives::{
    EntityRelationLaneOutputs, SemanticComponentVersion, SemanticEntityOutput, SemanticEpochRecord,
    SemanticLaneKind, SemanticLaneRecord, SemanticLaneStatus, SemanticRelationOutput,
    SemanticScope, Uuid, diff_entity_relation_lanes,
};
use xtask::sandbox::prelude::*;

fn scope() -> SemanticScope {
    SemanticScope {
        kind: "event_set".to_string(),
        input_ids: vec!["event:1".to_string(), "event:2".to_string()],
        input_set_hash: "input-hash".to_string(),
    }
}

fn epoch(id: u128, name: &str, config_hash: &str) -> SemanticEpochRecord {
    SemanticEpochRecord {
        epoch_id: Uuid::from_u128(id),
        name: name.to_string(),
        scope: scope(),
        code_ref: Some("test@sha".to_string()),
        config_hash: config_hash.to_string(),
        components: vec![SemanticComponentVersion {
            component: "entity-extractor".to_string(),
            version: "1".to_string(),
            config_hash: None,
        }],
        prompt_set_hash: None,
        model_config_hash: None,
    }
}

fn lane(
    id: u128,
    name: &str,
    kind: SemanticLaneKind,
    base_epoch_id: Option<Uuid>,
    candidate_epoch_id: Uuid,
) -> SemanticLaneRecord {
    SemanticLaneRecord {
        lane_id: Uuid::from_u128(id),
        name: name.to_string(),
        kind,
        base_epoch_id,
        candidate_epoch_id,
        scope: scope(),
        status: SemanticLaneStatus::Planned,
        purpose: "repository test".to_string(),
    }
}

fn outputs(entity_key: &str, relation_key: &str) -> EntityRelationLaneOutputs {
    EntityRelationLaneOutputs {
        entities: vec![SemanticEntityOutput::new(entity_key, "alpha", "project")],
        relations: vec![SemanticRelationOutput::new(
            relation_key,
            entity_key,
            entity_key,
            "mentions",
        )],
    }
}

#[sinex_test]
async fn semantic_repository_keeps_shadow_outputs_out_of_canonical_entities(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.semantic();
    let canonical_entities_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(&ctx.pool)
        .await?;
    let canonical_relations_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.entity_relations")
            .fetch_one(&ctx.pool)
            .await?;

    let baseline_epoch = repo
        .create_epoch(CreateSemanticEpoch {
            epoch: epoch(1, "baseline", "baseline-hash"),
            created_by: "test".to_string(),
            operation_id: None,
            supersedes_epoch_id: None,
        })
        .await?;
    let candidate_epoch = repo
        .create_epoch(CreateSemanticEpoch {
            epoch: epoch(2, "candidate", "candidate-hash"),
            created_by: "test".to_string(),
            operation_id: None,
            supersedes_epoch_id: Some(baseline_epoch.id),
        })
        .await?;
    let baseline_lane = repo
        .create_lane(CreateSemanticLane {
            lane: lane(
                3,
                "canonical",
                SemanticLaneKind::Canonical,
                None,
                baseline_epoch.id,
            ),
            operation_id: None,
            expires_at: None,
        })
        .await?;
    let candidate_lane = repo
        .create_lane(CreateSemanticLane {
            lane: lane(
                4,
                "shadow",
                SemanticLaneKind::Shadow,
                Some(baseline_epoch.id),
                candidate_epoch.id,
            ),
            operation_id: None,
            expires_at: None,
        })
        .await?;

    let candidate_outputs = outputs("entity-a", "relation-a");
    let written = repo
        .write_entity_relation_outputs(candidate_lane.id, &candidate_outputs)
        .await?;

    assert_eq!(written, 2);
    assert_eq!(repo.count_lane_outputs(candidate_lane.id).await?, 2);
    let read_outputs = repo.read_entity_relation_outputs(candidate_lane.id).await?;
    assert_eq!(read_outputs, candidate_outputs);

    let canonical_entities_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(
        canonical_entities_after, canonical_entities_before,
        "shadow lane writes must not mutate canonical entity projections"
    );
    let canonical_relations_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.entity_relations")
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        canonical_relations_after, canonical_relations_before,
        "shadow lane writes must not mutate canonical relation projections"
    );

    let report = diff_entity_relation_lanes(
        baseline_epoch.id,
        candidate_epoch.id,
        "input-hash",
        &outputs("entity-b", "relation-b"),
        &candidate_outputs,
        10,
    );
    let diff = repo
        .record_entity_relation_diff(
            Uuid::from_u128(5),
            baseline_lane.id,
            candidate_lane.id,
            &report,
        )
        .await?;
    assert_eq!(diff.diff_kind, "entity_relation");
    assert_eq!(diff.baseline_lane_id, baseline_lane.id);
    assert_eq!(diff.candidate_lane_id, candidate_lane.id);
    assert_eq!(diff.counts["entity_new"], 1);
    assert_eq!(diff.counts["entity_missing"], 1);

    let (discarded_lane, discarded_outputs) = repo
        .discard_lane_outputs(candidate_lane.id, sinex_primitives::Timestamp::now())
        .await?;
    assert_eq!(discarded_lane.status, "discarded");
    assert_eq!(discarded_outputs, 2);
    assert_eq!(repo.count_lane_outputs(candidate_lane.id).await?, 0);
    assert_eq!(repo.list_lane_diffs(candidate_lane.id, 10).await?.len(), 1);

    Ok(())
}

#[sinex_test]
async fn semantic_repository_seeds_lane_from_canonical_graph(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool.semantic();
    let source = ctx
        .pool
        .knowledge_graph()
        .create_entity(CreateEntity::person("Canonical Alice"))
        .await?;
    let target = ctx
        .pool
        .knowledge_graph()
        .create_entity(CreateEntity::project("Canonical Project"))
        .await?;
    ctx.pool
        .knowledge_graph()
        .create_relation(CreateEntityRelation::new(source.id, target.id, "works_on"))
        .await?;

    let epoch = repo
        .create_epoch(CreateSemanticEpoch {
            epoch: epoch(11, "canonical", "canonical-hash"),
            created_by: "test".to_string(),
            operation_id: None,
            supersedes_epoch_id: None,
        })
        .await?;
    let lane = repo
        .create_lane(CreateSemanticLane {
            lane: lane(12, "canonical", SemanticLaneKind::Canonical, None, epoch.id),
            operation_id: None,
            expires_at: None,
        })
        .await?;

    let written = repo
        .seed_entity_relation_outputs_from_canonical_graph(lane.id)
        .await?;
    assert_eq!(written, 3);

    let outputs = repo.read_entity_relation_outputs(lane.id).await?;
    assert_eq!(outputs.entities.len(), 2);
    assert_eq!(outputs.relations.len(), 1);
    assert!(
        outputs
            .entities
            .iter()
            .any(|entity| entity.canonical_name == "canonical_alice")
    );
    assert!(
        outputs
            .relations
            .iter()
            .any(|relation| relation.predicate == "works_on")
    );

    Ok(())
}
