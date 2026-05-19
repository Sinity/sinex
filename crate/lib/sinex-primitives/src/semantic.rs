//! Semantic epoch and shadow-lane comparison primitives.
//!
//! These types are storage-agnostic on purpose. Schema/gateway code can persist
//! them later without inventing a second vocabulary for entity/relation churn.

use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::Uuid;

/// Input scope for a semantic lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SemanticScope {
    /// Scope kind, e.g. `source_material`, `event_set`, `document_chunk_set`.
    pub kind: String,
    /// Stable identifiers in this scope.
    pub input_ids: Vec<String>,
    /// Hash of the resolved ordered input set.
    pub input_set_hash: String,
}

/// Versioned semantic configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SemanticEpochRecord {
    pub epoch_id: Uuid,
    pub name: String,
    pub scope: SemanticScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_ref: Option<String>,
    pub config_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<SemanticComponentVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_set_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_config_hash: Option<String>,
}

/// One semantic component participating in an epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SemanticComponentVersion {
    pub component: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_hash: Option<String>,
}

/// Shadow-lane lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SemanticLaneStatus {
    Planned,
    Running,
    Completed,
    Compared,
    Promoted,
    Discarded,
    Expired,
}

/// Lane class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SemanticLaneKind {
    Canonical,
    Shadow,
    Experiment,
}

/// Registry record for a semantic lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SemanticLaneRecord {
    pub lane_id: Uuid,
    pub name: String,
    pub kind: SemanticLaneKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_epoch_id: Option<Uuid>,
    pub candidate_epoch_id: Uuid,
    pub scope: SemanticScope,
    pub status: SemanticLaneStatus,
    pub purpose: String,
}

/// Candidate entity output inside a lane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SemanticEntityOutput {
    /// Lane-local stable entity key.
    pub entity_key: String,
    pub canonical_name: String,
    pub entity_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub metadata: JsonValue,
}

impl SemanticEntityOutput {
    #[must_use]
    pub fn new(
        entity_key: impl Into<String>,
        canonical_name: impl Into<String>,
        entity_type: impl Into<String>,
    ) -> Self {
        Self {
            entity_key: entity_key.into(),
            canonical_name: canonical_name.into(),
            entity_type: entity_type.into(),
            category: None,
            confidence: None,
            metadata: JsonValue::Null,
        }
    }
}

/// Candidate relation output inside a lane.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SemanticRelationOutput {
    /// Lane-local stable relation key.
    pub relation_key: String,
    pub source_entity_key: String,
    pub target_entity_key: String,
    pub predicate: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
    #[serde(default, skip_serializing_if = "JsonValue::is_null")]
    pub metadata: JsonValue,
}

impl SemanticRelationOutput {
    #[must_use]
    pub fn new(
        relation_key: impl Into<String>,
        source_entity_key: impl Into<String>,
        target_entity_key: impl Into<String>,
        predicate: impl Into<String>,
    ) -> Self {
        Self {
            relation_key: relation_key.into(),
            source_entity_key: source_entity_key.into(),
            target_entity_key: target_entity_key.into(),
            predicate: predicate.into(),
            weight: None,
            metadata: JsonValue::Null,
        }
    }
}

/// Inputs to an entity/relation lane comparison.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EntityRelationLaneOutputs {
    #[serde(default)]
    pub entities: Vec<SemanticEntityOutput>,
    #[serde(default)]
    pub relations: Vec<SemanticRelationOutput>,
}

/// Aggregate churn counts for an entity/relation lane diff.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EntityRelationDiffCounts {
    pub entity_new: usize,
    pub entity_missing: usize,
    pub entity_split: usize,
    pub entity_merge: usize,
    pub entity_category_changed: usize,
    pub entity_confidence_changed: usize,
    pub relation_added: usize,
    pub relation_removed: usize,
    pub relation_predicate_changed: usize,
    pub relation_weight_changed: usize,
}

/// One representative diff example.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EntityRelationDiffExample {
    EntityNew {
        entity_key: String,
        canonical_name: String,
    },
    EntityMissing {
        entity_key: String,
        canonical_name: String,
    },
    EntitySplit {
        canonical_name: String,
        baseline_keys: Vec<String>,
        candidate_keys: Vec<String>,
    },
    EntityMerge {
        canonical_name: String,
        baseline_keys: Vec<String>,
        candidate_keys: Vec<String>,
    },
    EntityCategoryChanged {
        entity_key: String,
        baseline: Option<String>,
        candidate: Option<String>,
    },
    EntityConfidenceChanged {
        entity_key: String,
        baseline: Option<String>,
        candidate: Option<String>,
    },
    RelationAdded {
        relation_key: String,
        predicate: String,
    },
    RelationRemoved {
        relation_key: String,
        predicate: String,
    },
    RelationPredicateChanged {
        relation_key: String,
        baseline: String,
        candidate: String,
    },
    RelationWeightChanged {
        relation_key: String,
        baseline: Option<String>,
        candidate: Option<String>,
    },
}

/// Machine-readable entity/relation lane diff report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EntityRelationDiffReport {
    pub baseline_epoch_id: Uuid,
    pub candidate_epoch_id: Uuid,
    pub input_set_hash: String,
    pub counts: EntityRelationDiffCounts,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<EntityRelationDiffExample>,
}

/// Compare two isolated entity/relation lane output sets.
#[must_use]
pub fn diff_entity_relation_lanes(
    baseline_epoch_id: Uuid,
    candidate_epoch_id: Uuid,
    input_set_hash: impl Into<String>,
    baseline: &EntityRelationLaneOutputs,
    candidate: &EntityRelationLaneOutputs,
    max_examples: usize,
) -> EntityRelationDiffReport {
    let baseline_entities = entity_map(&baseline.entities);
    let candidate_entities = entity_map(&candidate.entities);
    let baseline_relations = relation_map(&baseline.relations);
    let candidate_relations = relation_map(&candidate.relations);

    let mut counts = EntityRelationDiffCounts::default();
    let mut examples = Vec::new();

    for key in candidate_entities
        .keys()
        .filter(|key| !baseline_entities.contains_key(*key))
    {
        counts.entity_new += 1;
        push_example(
            &mut examples,
            max_examples,
            EntityRelationDiffExample::EntityNew {
                entity_key: key.clone(),
                canonical_name: candidate_entities[key].canonical_name.clone(),
            },
        );
    }

    for key in baseline_entities
        .keys()
        .filter(|key| !candidate_entities.contains_key(*key))
    {
        counts.entity_missing += 1;
        push_example(
            &mut examples,
            max_examples,
            EntityRelationDiffExample::EntityMissing {
                entity_key: key.clone(),
                canonical_name: baseline_entities[key].canonical_name.clone(),
            },
        );
    }

    for (key, baseline_entity) in &baseline_entities {
        let Some(candidate_entity) = candidate_entities.get(key) else {
            continue;
        };
        if baseline_entity.category != candidate_entity.category {
            counts.entity_category_changed += 1;
            push_example(
                &mut examples,
                max_examples,
                EntityRelationDiffExample::EntityCategoryChanged {
                    entity_key: key.clone(),
                    baseline: baseline_entity.category.clone(),
                    candidate: candidate_entity.category.clone(),
                },
            );
        }
        if comparable_f64(baseline_entity.confidence) != comparable_f64(candidate_entity.confidence)
        {
            counts.entity_confidence_changed += 1;
            push_example(
                &mut examples,
                max_examples,
                EntityRelationDiffExample::EntityConfidenceChanged {
                    entity_key: key.clone(),
                    baseline: comparable_f64(baseline_entity.confidence),
                    candidate: comparable_f64(candidate_entity.confidence),
                },
            );
        }
    }

    compare_name_multiplicity(
        &baseline.entities,
        &candidate.entities,
        &mut counts,
        &mut examples,
        max_examples,
    );
    compare_relations(
        &baseline_relations,
        &candidate_relations,
        &mut counts,
        &mut examples,
        max_examples,
    );

    EntityRelationDiffReport {
        baseline_epoch_id,
        candidate_epoch_id,
        input_set_hash: input_set_hash.into(),
        counts,
        examples,
    }
}

fn entity_map(entities: &[SemanticEntityOutput]) -> BTreeMap<String, &SemanticEntityOutput> {
    entities
        .iter()
        .map(|entity| (entity.entity_key.clone(), entity))
        .collect()
}

fn relation_map(relations: &[SemanticRelationOutput]) -> BTreeMap<String, &SemanticRelationOutput> {
    relations
        .iter()
        .map(|relation| (relation.relation_key.clone(), relation))
        .collect()
}

fn keys_by_name(entities: &[SemanticEntityOutput]) -> BTreeMap<String, BTreeSet<String>> {
    let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for entity in entities {
        grouped
            .entry(entity.canonical_name.clone())
            .or_default()
            .insert(entity.entity_key.clone());
    }
    grouped
}

fn compare_name_multiplicity(
    baseline: &[SemanticEntityOutput],
    candidate: &[SemanticEntityOutput],
    counts: &mut EntityRelationDiffCounts,
    examples: &mut Vec<EntityRelationDiffExample>,
    max_examples: usize,
) {
    let baseline_by_name = keys_by_name(baseline);
    let candidate_by_name = keys_by_name(candidate);
    let names: BTreeSet<_> = baseline_by_name
        .keys()
        .chain(candidate_by_name.keys())
        .cloned()
        .collect();

    for name in names {
        let baseline_keys = sorted_keys(baseline_by_name.get(&name));
        let candidate_keys = sorted_keys(candidate_by_name.get(&name));
        if baseline_keys.is_empty() || candidate_keys.is_empty() {
            continue;
        }
        if candidate_keys.len() > baseline_keys.len() {
            counts.entity_split += 1;
            push_example(
                examples,
                max_examples,
                EntityRelationDiffExample::EntitySplit {
                    canonical_name: name,
                    baseline_keys,
                    candidate_keys,
                },
            );
        } else if candidate_keys.len() < baseline_keys.len() {
            counts.entity_merge += 1;
            push_example(
                examples,
                max_examples,
                EntityRelationDiffExample::EntityMerge {
                    canonical_name: name,
                    baseline_keys,
                    candidate_keys,
                },
            );
        }
    }
}

fn compare_relations(
    baseline: &BTreeMap<String, &SemanticRelationOutput>,
    candidate: &BTreeMap<String, &SemanticRelationOutput>,
    counts: &mut EntityRelationDiffCounts,
    examples: &mut Vec<EntityRelationDiffExample>,
    max_examples: usize,
) {
    for key in candidate.keys().filter(|key| !baseline.contains_key(*key)) {
        counts.relation_added += 1;
        push_example(
            examples,
            max_examples,
            EntityRelationDiffExample::RelationAdded {
                relation_key: key.clone(),
                predicate: candidate[key].predicate.clone(),
            },
        );
    }

    for key in baseline.keys().filter(|key| !candidate.contains_key(*key)) {
        counts.relation_removed += 1;
        push_example(
            examples,
            max_examples,
            EntityRelationDiffExample::RelationRemoved {
                relation_key: key.clone(),
                predicate: baseline[key].predicate.clone(),
            },
        );
    }

    for (key, baseline_relation) in baseline {
        let Some(candidate_relation) = candidate.get(key) else {
            continue;
        };
        if baseline_relation.predicate != candidate_relation.predicate {
            counts.relation_predicate_changed += 1;
            push_example(
                examples,
                max_examples,
                EntityRelationDiffExample::RelationPredicateChanged {
                    relation_key: key.clone(),
                    baseline: baseline_relation.predicate.clone(),
                    candidate: candidate_relation.predicate.clone(),
                },
            );
        }
        if comparable_f64(baseline_relation.weight) != comparable_f64(candidate_relation.weight) {
            counts.relation_weight_changed += 1;
            push_example(
                examples,
                max_examples,
                EntityRelationDiffExample::RelationWeightChanged {
                    relation_key: key.clone(),
                    baseline: comparable_f64(baseline_relation.weight),
                    candidate: comparable_f64(candidate_relation.weight),
                },
            );
        }
    }
}

fn sorted_keys(keys: Option<&BTreeSet<String>>) -> Vec<String> {
    keys.map_or_else(Vec::new, |keys| keys.iter().cloned().collect())
}

fn comparable_f64(value: Option<f64>) -> Option<String> {
    value.map(|value| format!("{value:.6}"))
}

fn push_example(
    examples: &mut Vec<EntityRelationDiffExample>,
    max_examples: usize,
    example: EntityRelationDiffExample,
) {
    if examples.len() < max_examples {
        examples.push(example);
    }
}
