use sinex_primitives::{
    EntityRelationLaneOutputs, SemanticEntityOutput, SemanticRelationOutput, Uuid,
    diff_entity_relation_lanes,
};
use xtask::sandbox::prelude::*;

fn entity(key: &str, name: &str, category: Option<&str>, confidence: f64) -> SemanticEntityOutput {
    let mut entity = SemanticEntityOutput::new(key, name, "project");
    entity.category = category.map(str::to_string);
    entity.confidence = Some(confidence);
    entity
}

fn relation(key: &str, predicate: &str, weight: f64) -> SemanticRelationOutput {
    let mut relation = SemanticRelationOutput::new(key, "a", "b", predicate);
    relation.weight = Some(weight);
    relation
}

#[sinex_test]
async fn entity_relation_lane_diff_counts_churn() -> TestResult<()> {
    let baseline = EntityRelationLaneOutputs {
        entities: vec![
            entity("a", "alpha", Some("tool"), 0.8),
            entity("b", "beta", Some("project"), 0.7),
            entity("old", "old-name", Some("document"), 0.5),
        ],
        relations: vec![
            relation("a->b", "mentions", 0.3),
            relation("old->b", "mentions", 0.4),
        ],
    };
    let candidate = EntityRelationLaneOutputs {
        entities: vec![
            entity("a", "alpha", Some("project"), 0.9),
            entity("b", "beta", Some("project"), 0.7),
            entity("new", "new-name", Some("document"), 0.6),
        ],
        relations: vec![
            relation("a->b", "depends_on", 0.5),
            relation("new->b", "mentions", 0.4),
        ],
    };

    let report = diff_entity_relation_lanes(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        "input-hash",
        &baseline,
        &candidate,
        20,
    );

    assert_eq!(report.input_set_hash, "input-hash");
    assert_eq!(report.counts.entity_new, 1);
    assert_eq!(report.counts.entity_missing, 1);
    assert_eq!(report.counts.entity_category_changed, 1);
    assert_eq!(report.counts.entity_confidence_changed, 1);
    assert_eq!(report.counts.relation_added, 1);
    assert_eq!(report.counts.relation_removed, 1);
    assert_eq!(report.counts.relation_predicate_changed, 1);
    assert_eq!(report.counts.relation_weight_changed, 1);
    assert!(!report.examples.is_empty());
    Ok(())
}

#[sinex_test]
async fn entity_relation_lane_diff_detects_splits_and_merges() -> TestResult<()> {
    let baseline = EntityRelationLaneOutputs {
        entities: vec![
            entity("alpha", "same-name", Some("project"), 0.8),
            entity("m1", "merged-name", Some("project"), 0.8),
            entity("m2", "merged-name", Some("project"), 0.8),
        ],
        relations: vec![],
    };
    let candidate = EntityRelationLaneOutputs {
        entities: vec![
            entity("alpha-1", "same-name", Some("project"), 0.8),
            entity("alpha-2", "same-name", Some("project"), 0.8),
            entity("m", "merged-name", Some("project"), 0.8),
        ],
        relations: vec![],
    };

    let report = diff_entity_relation_lanes(
        Uuid::from_u128(3),
        Uuid::from_u128(4),
        "input-hash",
        &baseline,
        &candidate,
        10,
    );

    assert_eq!(report.counts.entity_split, 1);
    assert_eq!(report.counts.entity_merge, 1);
    Ok(())
}

#[sinex_test]
async fn entity_relation_lane_diff_respects_example_limit() -> TestResult<()> {
    let baseline = EntityRelationLaneOutputs {
        entities: vec![],
        relations: vec![],
    };
    let candidate = EntityRelationLaneOutputs {
        entities: vec![
            entity("a", "alpha", Some("project"), 0.8),
            entity("b", "beta", Some("project"), 0.8),
        ],
        relations: vec![relation("a->b", "mentions", 0.5)],
    };

    let report = diff_entity_relation_lanes(
        Uuid::from_u128(5),
        Uuid::from_u128(6),
        "input-hash",
        &baseline,
        &candidate,
        1,
    );

    assert_eq!(report.counts.entity_new, 2);
    assert_eq!(report.counts.relation_added, 1);
    assert_eq!(report.examples.len(), 1);
    Ok(())
}
