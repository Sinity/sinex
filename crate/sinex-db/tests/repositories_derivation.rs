//! Integration tests for `DerivationRepository` (sinex-0vx.6): the generic
//! `derivation.epochs`/`derivation.lanes`/`derivation.lane_outputs`/
//! `derivation.lane_diffs` control-plane repository that replaced the
//! entity/relation-only `SemanticRepository` (`semantic.*` tables, retired
//! in this same change).

use sinex_db::repositories::{
    CreateDerivationEpoch, CreateDerivationLane, CreateEntity, CreateEntityRelation, DbPoolExt,
};
use sinex_db::{Event, Provenance};
use sinex_primitives::activity::ActivitySourceKind;
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, LaneDiffReport, SourceCoverage,
    SupportLevel,
};
use sinex_primitives::domain::{EntityTypeName, RelationType};
use sinex_primitives::events::payloads::{
    ActivitySessionBoundaryPayload, ActivityWindowCloseReason, ActivityWindowSummaryPayload,
};
use sinex_primitives::events::{EntityRelatedPayload, EntityResolvedPayload};
use sinex_primitives::session_lane::SessionLaneOutputs;
use sinex_primitives::{
    EntityRelationLaneOutputs, SemanticEntityOutput, SemanticRelationOutput, Uuid,
};
use std::collections::BTreeMap;
use xtask::sandbox::prelude::*;

/// The same `declaration_id` sinexd's `SEMANTIC_ENTITY_RELATION_DECLARATION`
/// registers (`crate::api::handlers::semantic` there) -- duplicated here
/// rather than depending on the `sinexd` binary crate from `sinex-db`'s test
/// suite (this crate sits below `sinexd` in the dependency graph). Every
/// epoch/lane created below foreign-keys `derivation.epochs.declaration_id`
/// against this row.
const TEST_DECLARATION_ID: &str = "semantic-rpc.entity_relation.semantic_candidate";

async fn seed_declaration(pool: &sqlx::PgPool) -> TestResult<()> {
    let declaration = DerivationOutputDeclaration {
        declaration_id: TEST_DECLARATION_ID,
        owner: "semantic-rpc",
        product_class: DerivedProductClass::SemanticCandidate,
        write_surface: DerivationWriteSurface::CurationWriter,
        output_source: None,
        output_event_type: None,
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::ExplicitOnly,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Heuristic,
            SourceCoverage::Partial,
            ClaimTemporalQuality::Unknown,
        ),
        verification_command: "xtask test -p sinex-db -E 'test(derivation_lane_repository)'",
    };
    pool.product_declarations().insert(&declaration).await?;
    Ok(())
}

fn scope_json() -> serde_json::Value {
    serde_json::json!({
        "kind": "event_set",
        "input_ids": ["event:1", "event:2"],
        "input_set_hash": "input-hash",
    })
}

fn epoch(
    id: u128,
    name: &str,
    config_hash: &str,
    supersedes_epoch_id: Option<Uuid>,
) -> CreateDerivationEpoch {
    CreateDerivationEpoch {
        id: Some(Uuid::from_u128(id)),
        declaration_id: TEST_DECLARATION_ID.to_string(),
        name: name.to_string(),
        product_class: DerivedProductClass::SemanticCandidate.as_str().to_string(),
        scope_model: "event_set".to_string(),
        scope: scope_json(),
        semantics_version: "1.0.0".to_string(),
        code_ref: Some("test@sha".to_string()),
        config_hash: config_hash.to_string(),
        components: serde_json::json!([{"component": "entity-extractor", "version": "1"}]),
        prompt_set_hash: None,
        model_config_hash: None,
        created_by: "test".to_string(),
        operation_id: None,
        supersedes_epoch_id,
    }
}

fn lane(
    id: u128,
    name: &str,
    kind: &str,
    base_epoch_id: Option<Uuid>,
    candidate_epoch_id: Uuid,
) -> CreateDerivationLane {
    CreateDerivationLane {
        id: Some(Uuid::from_u128(id)),
        declaration_id: TEST_DECLARATION_ID.to_string(),
        name: name.to_string(),
        kind: kind.to_string(),
        product_class: DerivedProductClass::SemanticCandidate.as_str().to_string(),
        base_epoch_id,
        candidate_epoch_id,
        scope: scope_json(),
        purpose: Some("repository test".to_string()),
        operation_id: None,
        expires_at: None,
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
async fn derivation_lane_repository_keeps_shadow_outputs_out_of_canonical_entities(
    ctx: TestContext,
) -> TestResult<()> {
    seed_declaration(&ctx.pool).await?;
    let repo = ctx.pool.derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

    let canonical_entities_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(&ctx.pool)
        .await?;
    let canonical_relations_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.entity_relations")
            .fetch_one(&ctx.pool)
            .await?;

    let baseline_epoch = repo
        .create_epoch(epoch(1, "baseline", "baseline-hash", None))
        .await?;
    let candidate_epoch = repo
        .create_epoch(epoch(
            2,
            "candidate",
            "candidate-hash",
            Some(baseline_epoch.id),
        ))
        .await?;
    let baseline_lane = repo
        .create_lane(lane(3, "canonical", "canonical", None, baseline_epoch.id))
        .await?;
    let candidate_lane = repo
        .create_lane(lane(
            4,
            "shadow",
            "shadow",
            Some(baseline_epoch.id),
            candidate_epoch.id,
        ))
        .await?;

    let candidate_outputs = outputs("entity-a", "relation-a");
    let written = repo
        .write_entity_relation_outputs(candidate_lane.id, product_class, &candidate_outputs)
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

    let baseline_only = outputs("entity-b", "relation-b");
    let report = LaneDiffReport::compute::<EntityRelationLaneOutputs>(
        baseline_lane.id,
        candidate_lane.id,
        DerivedProductClass::SemanticCandidate,
        "input-hash",
        &baseline_only,
        &candidate_outputs,
        10,
    )
    .expect("compute lane diff report");
    let diff = repo.record_lane_diff(Uuid::from_u128(5), &report).await?;
    assert_eq!(diff.diff_kind, "entity_relation");
    assert_eq!(diff.product_class, product_class);
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
async fn derivation_lane_repository_seeds_lane_from_canonical_graph(
    ctx: TestContext,
) -> TestResult<()> {
    seed_declaration(&ctx.pool).await?;
    let repo = ctx.pool.derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

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

    let epoch_row = repo
        .create_epoch(epoch(11, "canonical", "canonical-hash", None))
        .await?;
    let lane_row = repo
        .create_lane(lane(12, "canonical", "canonical", None, epoch_row.id))
        .await?;

    let written = repo
        .seed_entity_relation_outputs_from_canonical_graph(lane_row.id, product_class)
        .await?;
    assert_eq!(written, 3);

    let outputs = repo.read_entity_relation_outputs(lane_row.id).await?;
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

#[sinex_test]
async fn derivation_lane_repository_seeds_lane_from_entity_event_scope(
    ctx: TestContext,
) -> TestResult<()> {
    seed_declaration(&ctx.pool).await?;
    let repo = ctx.pool.derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

    let source_entity_id = Uuid::from_u128(101);
    let target_entity_id = Uuid::from_u128(102);
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("derivation-entity-event-scope"),
            serde_json::json!({ "test": true }),
        )
        .await?;
    let material_id =
        sinex_primitives::Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let source_event = ctx
        .pool
        .events()
        .insert(
            Event::builder(EntityResolvedPayload {
                entity_id: source_entity_id,
                canonical_name: "alice".to_string(),
                entity_type: EntityTypeName::new("person"),
                original_name: "Alice".to_string(),
            })
            .with_provenance(Provenance::from_material(material_id, 0, None, None))
            .build()
            .expect("valid derivation entity event"),
        )
        .await?;
    let target_event = ctx
        .pool
        .events()
        .insert(
            Event::builder(EntityResolvedPayload {
                entity_id: target_entity_id,
                canonical_name: "sinex".to_string(),
                entity_type: EntityTypeName::new("project"),
                original_name: "Sinex".to_string(),
            })
            .with_provenance(Provenance::from_material(material_id, 1, None, None))
            .build()
            .expect("valid derivation entity event"),
        )
        .await?;
    let relation_event = ctx
        .pool
        .events()
        .insert(
            Event::builder(EntityRelatedPayload {
                source_entity_id,
                target_entity_id,
                relation_type: RelationType::new("works_on"),
                confidence: 0.75,
            })
            .with_provenance(Provenance::from_material(material_id, 2, None, None))
            .build()
            .expect("valid derivation relation event"),
        )
        .await?;

    let source_event_id = *source_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("source event should have id"))?
        .as_uuid();
    let target_event_id = *target_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("target event should have id"))?
        .as_uuid();
    let relation_event_id = *relation_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("relation event should have id"))?
        .as_uuid();

    let event_scope = serde_json::json!({
        "kind": "event_set",
        "input_ids": [
            format!("event:{source_event_id}"),
            format!("event:{target_event_id}"),
            format!("event:{relation_event_id}"),
        ],
        "input_set_hash": "entity-event-scope",
    });

    let epoch_row = repo
        .create_epoch(CreateDerivationEpoch {
            scope: event_scope.clone(),
            ..epoch(21, "entity-events", "entity-events-hash", None)
        })
        .await?;
    let lane_row = repo
        .create_lane(CreateDerivationLane {
            scope: event_scope,
            ..lane(22, "entity-events", "shadow", None, epoch_row.id)
        })
        .await?;

    let canonical_entities_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(&ctx.pool)
        .await?;
    let written = repo
        .seed_entity_relation_outputs_from_event_scope(lane_row.id, product_class)
        .await?;
    assert_eq!(written, 3);

    let outputs = repo.read_entity_relation_outputs(lane_row.id).await?;
    assert_eq!(outputs.entities.len(), 2);
    assert_eq!(outputs.relations.len(), 1);
    assert!(
        outputs
            .entities
            .iter()
            .any(|entity| entity.entity_key == source_entity_id.to_string()
                && entity.canonical_name == "alice")
    );
    assert!(
        outputs
            .relations
            .iter()
            .any(
                |relation| relation.source_entity_key == source_entity_id.to_string()
                    && relation.target_entity_key == target_entity_id.to_string()
                    && relation.predicate == "works_on"
            )
    );
    let persisted = repo.list_lane_outputs(lane_row.id, 10).await?;
    assert!(
        persisted
            .iter()
            .any(|output| output.source_event_id == Some(source_event_id))
    );
    assert!(
        persisted
            .iter()
            .all(|output| output.metadata["producer"] == "entity_events")
    );
    let canonical_entities_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.entities")
        .fetch_one(&ctx.pool)
        .await?;
    assert_eq!(
        canonical_entities_after, canonical_entities_before,
        "event-scope lane seeding must not mutate canonical entity projections"
    );

    Ok(())
}

fn window_summary(
    window_id: &str,
    start: sinex_primitives::Timestamp,
    duration_secs: i64,
    event_count: u64,
    close_reason: ActivityWindowCloseReason,
) -> ActivityWindowSummaryPayload {
    let end = start + time::Duration::seconds(duration_secs.max(0));
    let mut counts = BTreeMap::new();
    counts.insert(ActivitySourceKind::Window, event_count);
    ActivityWindowSummaryPayload {
        window_id: window_id.to_string(),
        window_start: start,
        window_end: end,
        duration_secs: duration_secs.max(0) as u64,
        event_count,
        source_count: 1,
        sources: vec!["derivation-session-lane-test".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: counts,
        primary_source: ActivitySourceKind::Window,
        close_reason,
    }
}

/// Second `LaneOutputKind` port (sinex-0vx.7): seeding a session lane from a
/// finite `event_set` scope of `activity.window.summary` events recomputes
/// session boundaries via `compute_session_boundaries` -- the exact
/// gap-closure grouping policy `SessionDetector` uses in `sinexd`, replayed
/// as a pure batch function. Mutating that grouping logic (e.g. dropping the
/// `Gap` check) makes this red.
#[sinex_test]
async fn derivation_lane_repository_seeds_session_lane_from_window_scope(
    ctx: TestContext,
) -> TestResult<()> {
    seed_declaration(&ctx.pool).await?;
    let repo = ctx.pool.derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("derivation-session-lane-window-scope"),
            serde_json::json!({ "test": true }),
        )
        .await?;
    let material_id =
        sinex_primitives::Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let start = sinex_primitives::Timestamp::now();
    let first_window = ctx
        .pool
        .events()
        .insert(
            Event::builder(window_summary(
                "session-lane-w1",
                start,
                60,
                5,
                ActivityWindowCloseReason::MaxDuration,
            ))
            .with_provenance(Provenance::from_material(material_id, 0, None, None))
            .build()
            .expect("valid window summary event"),
        )
        .await?;
    let second_window = ctx
        .pool
        .events()
        .insert(
            Event::builder(window_summary(
                "session-lane-w2",
                start + time::Duration::seconds(60),
                60,
                3,
                ActivityWindowCloseReason::Gap,
            ))
            .with_provenance(Provenance::from_material(material_id, 1, None, None))
            .build()
            .expect("valid window summary event"),
        )
        .await?;

    let first_window_id = *first_window
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("first window event should have id"))?
        .as_uuid();
    let second_window_id = *second_window
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("second window event should have id"))?
        .as_uuid();

    let event_scope = serde_json::json!({
        "kind": "event_set",
        "input_ids": [
            format!("event:{first_window_id}"),
            format!("event:{second_window_id}"),
        ],
        "input_set_hash": "session-lane-window-scope",
    });

    let epoch_row = repo
        .create_epoch(CreateDerivationEpoch {
            scope: event_scope.clone(),
            ..epoch(
                31,
                "session-window-scope",
                "session-window-scope-hash",
                None,
            )
        })
        .await?;
    let lane_row = repo
        .create_lane(CreateDerivationLane {
            scope: event_scope,
            ..lane(32, "session-window-scope", "shadow", None, epoch_row.id)
        })
        .await?;

    let written = repo
        .seed_session_lane_outputs_from_window_scope(lane_row.id, product_class)
        .await?;
    assert_eq!(
        written, 1,
        "the two windows must collapse into exactly one session"
    );

    let outputs = repo.read_session_lane_outputs(lane_row.id).await?;
    assert_eq!(outputs.boundaries.len(), 1);
    let session = &outputs.boundaries[0];
    assert_eq!(session.session_key, "activity-session:session-lane-w1");
    assert_eq!(session.event_count, 8);
    assert_eq!(session.window_count, 2);

    Ok(())
}

/// Baseline half of the shadow diff: seeding from CANONICAL
/// `activity.session.boundary` events already in `core.events` reads the
/// existing projection rather than recomputing it (mirrors
/// `seed_entity_relation_outputs_from_canonical_graph`'s role).
#[sinex_test]
async fn derivation_lane_repository_seeds_session_lane_from_canonical_events(
    ctx: TestContext,
) -> TestResult<()> {
    seed_declaration(&ctx.pool).await?;
    let repo = ctx.pool.derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("derivation-session-lane-canonical"),
            serde_json::json!({ "test": true }),
        )
        .await?;
    let material_id =
        sinex_primitives::Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let start = sinex_primitives::Timestamp::now();
    let mut counts = BTreeMap::new();
    counts.insert(ActivitySourceKind::Window, 4u64);
    let canonical_payload = ActivitySessionBoundaryPayload {
        session_id: "activity-session:canonical-w1".to_string(),
        start_time: start,
        end_time: start + time::Duration::seconds(90),
        duration_secs: 90,
        event_count: 4,
        window_count: 1,
        source_count: 1,
        sources: vec!["derivation-session-lane-canonical".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: counts,
        primary_source: ActivitySourceKind::Window,
    };
    ctx.pool
        .events()
        .insert(
            Event::builder(canonical_payload)
                .with_provenance(Provenance::from_material(material_id, 0, None, None))
                .build()
                .expect("valid session boundary event"),
        )
        .await?;

    let epoch_row = repo
        .create_epoch(epoch(
            33,
            "session-canonical",
            "session-canonical-hash",
            None,
        ))
        .await?;
    let lane_row = repo
        .create_lane(lane(
            34,
            "session-canonical",
            "canonical",
            None,
            epoch_row.id,
        ))
        .await?;

    let written = repo
        .seed_session_lane_outputs_from_canonical_events(lane_row.id, product_class)
        .await?;
    assert_eq!(written, 1);

    let outputs = repo.read_session_lane_outputs(lane_row.id).await?;
    assert_eq!(outputs.boundaries.len(), 1);
    assert_eq!(
        outputs.boundaries[0].session_key,
        "activity-session:canonical-w1"
    );
    assert_eq!(outputs.boundaries[0].duration_secs, 90);

    Ok(())
}

/// End-to-end shadow diff over the session-boundary `LaneOutputKind`: a
/// baseline lane seeded from a canonical session and a shadow lane seeded
/// (recomputed) from raw windows covering the same occurrence but a
/// DIFFERENT duration -- `LaneDiffReport::compute::<SessionLaneOutputs>`
/// must classify it as `duration_changed`, not silently as unchanged/new.
#[sinex_test]
async fn derivation_lane_repository_diffs_session_lane_shadow_against_baseline(
    ctx: TestContext,
) -> TestResult<()> {
    seed_declaration(&ctx.pool).await?;
    let repo = ctx.pool.derivation_lanes();
    let product_class = DerivedProductClass::SemanticCandidate.as_str();

    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            sinex_db::repositories::source_materials::material_types::STREAM,
            Some("derivation-session-lane-diff"),
            serde_json::json!({ "test": true }),
        )
        .await?;
    let material_id =
        sinex_primitives::Id::<sinex_db::models::SourceMaterial>::from_uuid(material_record.id);

    let start = sinex_primitives::Timestamp::now();
    let mut counts = BTreeMap::new();
    counts.insert(ActivitySourceKind::Window, 4u64);
    let canonical_payload = ActivitySessionBoundaryPayload {
        session_id: "activity-session:diff-w1".to_string(),
        start_time: start,
        end_time: start + time::Duration::seconds(60),
        duration_secs: 60,
        event_count: 4,
        window_count: 1,
        source_count: 1,
        sources: vec!["derivation-session-lane-diff".to_string()],
        activity_sources: vec![ActivitySourceKind::Window],
        activity_source_counts: counts,
        primary_source: ActivitySourceKind::Window,
    };
    ctx.pool
        .events()
        .insert(
            Event::builder(canonical_payload)
                .with_provenance(Provenance::from_material(material_id, 0, None, None))
                .build()
                .expect("valid session boundary event"),
        )
        .await?;

    let window_event = ctx
        .pool
        .events()
        .insert(
            Event::builder(window_summary(
                "diff-w1",
                start,
                90,
                4,
                ActivityWindowCloseReason::Gap,
            ))
            .with_provenance(Provenance::from_material(material_id, 1, None, None))
            .build()
            .expect("valid window summary event"),
        )
        .await?;
    let window_event_id = *window_event
        .id
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("window event should have id"))?
        .as_uuid();

    let baseline_epoch = repo
        .create_epoch(epoch(
            41,
            "session-diff-baseline",
            "session-diff-baseline-hash",
            None,
        ))
        .await?;
    let baseline_lane = repo
        .create_lane(lane(
            42,
            "session-diff-baseline",
            "canonical",
            None,
            baseline_epoch.id,
        ))
        .await?;
    repo.seed_session_lane_outputs_from_canonical_events(baseline_lane.id, product_class)
        .await?;

    let shadow_scope = serde_json::json!({
        "kind": "event_set",
        "input_ids": [format!("event:{window_event_id}")],
        "input_set_hash": "session-diff-shadow-scope",
    });
    let candidate_epoch = repo
        .create_epoch(CreateDerivationEpoch {
            scope: shadow_scope.clone(),
            supersedes_epoch_id: Some(baseline_epoch.id),
            ..epoch(43, "session-diff-shadow", "session-diff-shadow-hash", None)
        })
        .await?;
    let shadow_lane = repo
        .create_lane(CreateDerivationLane {
            scope: shadow_scope,
            base_epoch_id: Some(baseline_epoch.id),
            ..lane(
                44,
                "session-diff-shadow",
                "shadow",
                None,
                candidate_epoch.id,
            )
        })
        .await?;
    repo.seed_session_lane_outputs_from_window_scope(shadow_lane.id, product_class)
        .await?;

    let baseline_outputs = repo.read_session_lane_outputs(baseline_lane.id).await?;
    let candidate_outputs = repo.read_session_lane_outputs(shadow_lane.id).await?;
    let report = LaneDiffReport::compute::<SessionLaneOutputs>(
        baseline_lane.id,
        shadow_lane.id,
        DerivedProductClass::SemanticCandidate,
        "session-diff-shadow-scope",
        &baseline_outputs,
        &candidate_outputs,
        10,
    )
    .expect("compute session lane diff report");
    assert_eq!(report.output_kind, "session_boundary");
    assert_eq!(report.counts["duration_changed"], 1);
    assert_eq!(report.summary.changed, 1);

    let diff = repo.record_lane_diff(Uuid::from_u128(45), &report).await?;
    assert_eq!(diff.diff_kind, "session_boundary");
    assert_eq!(diff.baseline_lane_id, baseline_lane.id);
    assert_eq!(diff.candidate_lane_id, shadow_lane.id);

    Ok(())
}
