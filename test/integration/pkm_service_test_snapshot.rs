// PKM Service Integration Tests with Snapshot Testing
//
// Demonstrates snapshot testing for PKM service operations,
// capturing complex entity relationships and metadata.

use crate::common::prelude::*;
use crate::common::generators;
use crate::common::snapshot_testing::{assert_snapshot, snapshot, Redaction};
use serde_json::json;
use sinex_db::{
    annotations, artifacts, knowledge_graph,
    models::{CreateArtifactInput, CreateEntityInput, CreateRelationInput},
};
use sinex_services::pkm::PkmService;
use sinex_ulid::Ulid;
use std::collections::{HashMap, HashSet};

// =============================================================================
// ANNOTATION TESTS WITH SNAPSHOTS
// =============================================================================

/// Test creating a note annotation with snapshot verification
#[sinex_test(timeout = 30)]
async fn test_create_note_annotation_snapshot(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event first
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create a note annotation
    let content = "This is a test note annotation";
    let tags = vec!["test".to_string(), "annotation".to_string()];
    let created_by = "test_user";

    let annotation_id = service
        .create_note(inserted_event.id, content, tags.clone(), created_by)
        .await?;

    // Get the annotation and capture as snapshot
    let annotations = annotations::get_annotations_for_event(ctx.pool(), inserted_event.id).await?;
    
    // Convert to JSON for snapshot testing
    let snapshot_data = json!({
        "annotation": {
            "annotation_type": annotations[0].annotation_type,
            "content": annotations[0].content,
            "created_by": annotations[0].created_by,
            "metadata": annotations[0].metadata,
        },
        "tags": tags,
        "event_association": {
            "event_id": inserted_event.id.to_string(),
            "annotation_count": annotations.len(),
        }
    });

    // Use snapshot assertion with redactions
    assert_snapshot!(
        snapshot_data,
        "note_annotation_creation",
        Redaction::timestamps(),
        Redaction::ulids()
    );

/// Test entity creation workflow with snapshot
#[sinex_test(timeout = 30)]
async fn test_entity_creation_workflow_snapshot(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create multiple entities
    let entities = vec![
        ("John Doe".to_string(), "person".to_string()),
        ("Rust Project".to_string(), "project".to_string()),
        ("Machine Learning".to_string(), "topic".to_string()),
        ("Google Inc".to_string(), "organization".to_string()),
    ];

    let entity_ids = service
        .create_entities_from_list(inserted_event.id, entities.clone())
        .await?;

    // Retrieve all created entities
    let mut created_entities = Vec::new();
    for entity_id in &entity_ids {
        let entity = knowledge_graph::get_entity_by_id(ctx.pool(), *entity_id)
            .await?
            .expect("Entity should exist");
        created_entities.push(json!({
            "name": entity.name,
            "entity_type": entity.entity_type,
            "canonical_name": entity.canonical_name,
            "aliases": entity.aliases,
            "metadata": entity.metadata,
        }));
    }

    // Capture the complete entity creation result
    let snapshot_data = json!({
        "entities_created": created_entities,
        "source_event": inserted_event.id.to_string(),
        "entity_count": entity_ids.len(),
    });

    assert_snapshot!(snapshot_data, "entity_creation_workflow");

    Ok(())
}

// =============================================================================
// COMPLEX RELATIONSHIP TESTS WITH SNAPSHOTS
// =============================================================================

/// Test complex entity relationship graph with snapshots
#[sinex_test(timeout = 30)]
async fn test_complex_relationship_graph_snapshot(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create entities
    let entities = vec![
        ("Alice Johnson".to_string(), "person".to_string()),
        ("Bob Smith".to_string(), "person".to_string()),
        ("Stanford University".to_string(), "organization".to_string()),
        ("AI Research Project".to_string(), "project".to_string()),
        ("Neural Networks".to_string(), "topic".to_string()),
    ];

    let entity_ids = service
        .create_entities_from_list(inserted_event.id, entities.clone())
        .await?;

    let alice_id = entity_ids[0];
    let bob_id = entity_ids[1];
    let stanford_id = entity_ids[2];
    let project_id = entity_ids[3];
    let topic_id = entity_ids[4];

    // Create relationships
    let relationships = vec![
        CreateRelationInput {
            from_entity_id: alice_id,
            to_entity_id: project_id,
            relation_type: "leads".to_string(),
            properties: Some(json!({
                "start_date": "2024-01-01",
                "role": "Principal Investigator"
            })),
        },
        CreateRelationInput {
            from_entity_id: bob_id,
            to_entity_id: project_id,
            relation_type: "collaborates_on".to_string(),
            properties: Some(json!({
                "role": "Research Assistant"
            })),
        },
        CreateRelationInput {
            from_entity_id: project_id,
            to_entity_id: stanford_id,
            relation_type: "hosted_by".to_string(),
            properties: None,
        },
        CreateRelationInput {
            from_entity_id: project_id,
            to_entity_id: topic_id,
            relation_type: "focuses_on".to_string(),
            properties: Some(json!({
                "primary_focus": true
            })),
        },
    ];

    for relation_input in &relationships {
        knowledge_graph::create_relation(ctx.pool(), relation_input.clone()).await?;
    }

    // Build comprehensive graph snapshot
    let mut graph_data = json!({
        "entities": {},
        "relationships": []
    });

    // Add entities to graph
    for (i, entity_id) in entity_ids.iter().enumerate() {
        let entity = knowledge_graph::get_entity_by_id(ctx.pool(), *entity_id)
            .await?
            .unwrap();
        
        graph_data["entities"][&entities[i].0] = json!({
            "type": entity.entity_type,
            "metadata": entity.metadata,
        });

        // Get relationships for this entity
        let relations = knowledge_graph::get_entity_relations(ctx.pool(), *entity_id).await?;
        for rel in relations {
            graph_data["relationships"].as_array_mut().unwrap().push(json!({
                "from": entities.iter().find(|e| entity_ids[entities.iter().position(|x| x.0 == e.0).unwrap()] == rel.from_entity_id).map(|e| &e.0),
                "to": entities.iter().find(|e| entity_ids[entities.iter().position(|x| x.0 == e.0).unwrap()] == rel.to_entity_id).map(|e| &e.0),
                "type": rel.relation_type,
                "properties": rel.properties,
            }));
        }
    }

    // Use fluent snapshot builder with custom redactions
    snapshot(graph_data)
        .name("complex_entity_relationship_graph")
        .redact_timestamps()
        .redact_ulids()
        .redact_field("entities.*.metadata.created_at", json!("[TIMESTAMP]"))
        .assert();

    Ok(())
}

// =============================================================================
// SEARCH RESULTS SNAPSHOT TESTING
// =============================================================================

/// Test entity search results with snapshots
#[sinex_test(timeout = 30)]
async fn test_entity_search_results_snapshot(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create a variety of entities for search testing
    let test_entities = vec![
        ("Machine Learning Fundamentals".to_string(), "topic".to_string()),
        ("Deep Learning with PyTorch".to_string(), "project".to_string()),
        ("Learning Resources Inc".to_string(), "organization".to_string()),
        ("Dr. Sarah Learning".to_string(), "person".to_string()),
        ("Advanced ML Techniques".to_string(), "topic".to_string()),
        ("Quantum Computing".to_string(), "topic".to_string()),
    ];

    service
        .create_entities_from_list(inserted_event.id, test_entities.clone())
        .await?;

    // Search for "learning" entities
    let search_results = knowledge_graph::search_entities(ctx.pool(), "learning", 10).await?;

    // Create search results snapshot
    let snapshot_data = json!({
        "query": "learning",
        "result_count": search_results.len(),
        "results": search_results.iter().map(|e| json!({
            "name": e.name,
            "entity_type": e.entity_type,
            "match_context": {
                "name_contains_query": e.name.to_lowercase().contains("learning"),
                "canonical_name": e.canonical_name,
            }
        })).collect::<Vec<_>>(),
    });

    assert_snapshot!(snapshot_data, "entity_search_results");

    Ok(())
}

// =============================================================================
// ERROR SCENARIO SNAPSHOTS
// =============================================================================

/// Test error responses with snapshots
#[sinex_test(timeout = 30)]
async fn test_validation_error_snapshot(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Attempt to create entities with invalid types
    let invalid_entities = vec![
        ("Valid Person".to_string(), "person".to_string()),
        ("Invalid Entity".to_string(), "invalid_type".to_string()),
        ("Another Invalid".to_string(), "not_a_type".to_string()),
    ];

    let result = service
        .create_entities_from_list(inserted_event.id, invalid_entities.clone())
        .await;

    // Capture error details for snapshot
    let error_snapshot = match result {
        Ok(_) => json!({"error": "Expected error but got success"}),
        Err(e) => json!({
            "error_type": "validation_error",
            "message": e.to_string(),
            "invalid_entities": invalid_entities.iter()
                .filter(|(_, t)| !["person", "project", "topic", "organization"].contains(&t.as_str()))
                .map(|(name, entity_type)| json!({
                    "name": name,
                    "invalid_type": entity_type,
                }))
                .collect::<Vec<_>>(),
        }),
    };

    assert_snapshot!(error_snapshot, "entity_validation_errors");

    Ok(())
}

#[cfg(test)]
mod snapshot_helpers {
    use super::*;

    /// Helper to generate test data for snapshot testing demos
    pub fn generate_complex_pkm_data() -> serde_json::Value {
        json!({
            "entities": [
                {"name": "John Doe", "type": "person"},
                {"name": "AI Project", "type": "project"},
            ],
            "relationships": [
                {"from": "John Doe", "to": "AI Project", "type": "leads"},
            ],
            "annotations": [
                {"type": "note", "content": "Important milestone reached"},
            ],
        })
    }
}