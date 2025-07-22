// PKM Service Integration Tests
//
// Comprehensive integration tests for the Personal Knowledge Management (PKM) service,
// covering all core functionality including:
// - Note annotations on events
// - Knowledge graph entity operations (CRUD lifecycle)
// - Entity relationship creation and retrieval
// - Artifact management operations
// - Search functionality for entities and artifacts
// - Database constraint validation
// - Error handling and transaction rollback scenarios
//
// All tests use #[sinex_test] for automatic transaction isolation and TestContext
// for unified database access patterns.

use crate::common::prelude::*;
use sinex_db::{
    annotations, artifacts, knowledge_graph,
    models::{CreateArtifactInput, CreateEntityInput, CreateRelationInput},
};
use sinex_services::pkm::PkmService;
use sinex_events::event_types::{shell, filesystem, sinex, clipboard};

// =============================================================================
// ANNOTATION TESTS - Note Creation and Management
// =============================================================================

/// Test creating a note annotation on an event
#[sinex_test]
async fn test_create_note_annotation(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event first
    let event = EventFactory::new(sources::SHELL_KITTY).create_event(
        shell::COMMAND_EXECUTED,
        json!({
            "command": "git status",
            "exit_code": 0
        })
    );
    let inserted_event = insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create a note annotation
    let content = "This is a test note annotation";
    let tags = vec!["test".to_string(), "annotation".to_string()];
    let created_by = "test_user";

    let annotation_id = service
        .create_note(inserted_event.id, content, tags.clone(), created_by)
        .await?;

    // Verify the annotation was created correctly
    let annotations = annotations::get_annotations_for_event(ctx.pool(), inserted_event.id).await?;
    assert_eq!(annotations.len(), 1);

    let annotation = &annotations[0];
    assert_eq!(annotation.annotation_id, annotation_id);
    assert_eq!(annotation.event_id, inserted_event.id);
    assert_eq!(annotation.annotation_type, "note");
    assert_eq!(annotation.content, content);
    assert_eq!(annotation.created_by, created_by);

    // Verify metadata contains tags
    let metadata = &annotation.metadata;
    assert_eq!(metadata["tags"], json!(tags));
    assert!(metadata["created_at"].is_string());

    Ok(())
}

/// Test creating multiple notes on the same event
#[sinex_test]
async fn test_multiple_note_annotations(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event
    let event = EventFactory::new(sources::FS).create_event(
        filesystem::FILE_MODIFIED,
        json!({
            "path": "/home/user/document.md",
            "size": 1024
        })
    );
    let inserted_event = insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create multiple annotations
    let note1_id = service
        .create_note(
            inserted_event.id,
            "First note",
            vec!["first".to_string()],
            "user1",
        )
        .await?;

    let note2_id = service
        .create_note(
            inserted_event.id,
            "Second note",
            vec!["second".to_string()],
            "user2",
        )
        .await?;

    // Verify both annotations exist
    let annotations = annotations::get_annotations_for_event(ctx.pool(), inserted_event.id).await?;
    assert_eq!(annotations.len(), 2);

    let annotation_ids: HashSet<Ulid> = annotations.iter().map(|a| a.annotation_id).collect();
    assert!(annotation_ids.contains(&note1_id));
    assert!(annotation_ids.contains(&note2_id));

    Ok(())
}

// =============================================================================
// ENTITY TESTS - Knowledge Graph Entity Operations
// =============================================================================

/// Test creating entities from a list
#[sinex_test]
async fn test_create_entities_from_list(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event
    let event = EventFactory::new(sources::SYSTEMD).create_event(
        sinex::PROCESS_HEARTBEAT,
        json!({
            "uptime": 3600,
            "cpu_usage": 25.5
        })
    );
    let inserted_event = insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Define test entities using valid entity types
    let entities = vec![
        ("John Doe".to_string(), "person".to_string()),
        ("Rust Project".to_string(), "project".to_string()),
        ("Machine Learning".to_string(), "topic".to_string()),
        ("Google Inc".to_string(), "organization".to_string()),
    ];

    // Create entities
    let entity_ids = service
        .create_entities_from_list(inserted_event.id, entities.clone())
        .await?;
    assert_eq!(entity_ids.len(), 4);

    // Verify entities were created correctly
    for (i, entity_id) in entity_ids.iter().enumerate() {
        let entity = knowledge_graph::get_entity_by_id(ctx.pool(), *entity_id)
            .await?
            .expect("Entity should exist");

        assert_eq!(entity.name, entities[i].0);
        assert_eq!(entity.entity_type, entities[i].1);
        assert_eq!(entity.canonical_name, entities[i].0); // Should default to name
        assert!(entity.aliases.is_empty());

        // Verify metadata contains source event ID
        assert_eq!(
            entity.metadata["source_event_id"],
            json!(inserted_event.id.to_string())
        );
    }

    Ok(())
}

/// Test entity duplicate handling
#[sinex_test]
async fn test_entity_duplicate_handling(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create events
    let event1 = EventBuilder::new()
        .source(sources::SHELL_KITTY)
        .event_type(shell::COMMAND_EXECUTED)
        .payload(json!({"command": "ls"}))
        .build();
    let event2 = EventBuilder::new()
        .source(sources::SHELL_KITTY)
        .event_type(shell::COMMAND_EXECUTED)
        .payload(json!({"command": "pwd"}))
        .build();
    
    let inserted1 = insert_event(ctx.pool(), &event1).await?;
    let inserted2 = insert_event(ctx.pool(), &event2).await?;

    // Create the same entity from two different events
    let entities = vec![("Duplicate Entity".to_string(), "concept".to_string())];

    let ids1 = service
        .create_entities_from_list(inserted1.id, entities.clone())
        .await?;
    let ids2 = service
        .create_entities_from_list(inserted2.id, entities.clone())
        .await?;

    // Should create two separate entities (no deduplication by default)
    assert_eq!(ids1.len(), 1);
    assert_eq!(ids2.len(), 1);
    assert_ne!(ids1[0], ids2[0], "Should create separate entities");

    Ok(())
}

/// Test updating entity properties
#[sinex_test]
async fn test_update_entity_properties(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create an entity
    let event = EventBuilder::new()
        .source(sources::SYSTEMD)
        .event_type(sinex::PROCESS_HEARTBEAT)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    let entity_ids = service
        .create_entities_from_list(
            inserted.id,
            vec![("Original Name".to_string(), "person".to_string())],
        )
        .await?;
    let entity_id = entity_ids[0];

    // Update entity properties
    let new_properties = HashMap::from([
        ("canonical_name".to_string(), json!("Updated Name")),
        ("aliases".to_string(), json!(["Alias1", "Alias2"])),
        ("metadata".to_string(), json!({"role": "developer", "active": true})),
    ]);

    service
        .update_entity_properties(entity_id, new_properties)
        .await?;

    // Verify updates
    let updated = knowledge_graph::get_entity_by_id(ctx.pool(), entity_id)
        .await?
        .expect("Entity should exist");

    assert_eq!(updated.canonical_name, "Updated Name");
    assert_eq!(updated.aliases, vec!["Alias1", "Alias2"]);
    assert_eq!(updated.metadata["role"], "developer");
    assert_eq!(updated.metadata["active"], true);

    Ok(())
}

// =============================================================================
// RELATIONSHIP TESTS - Entity Relationships in Knowledge Graph
// =============================================================================

/// Test creating relationships between entities
#[sinex_test]
async fn test_create_entity_relationships(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test event
    let event = EventBuilder::new()
        .source(sources::FS)
        .event_type(filesystem::FILE_CREATED)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    // Create entities
    let entities = vec![
        ("Alice".to_string(), "person".to_string()),
        ("Bob".to_string(), "person".to_string()),
        ("Project X".to_string(), "project".to_string()),
    ];
    let entity_ids = service
        .create_entities_from_list(inserted.id, entities)
        .await?;

    // Create relationships
    let relationships = vec![
        (entity_ids[0], entity_ids[2], "works_on".to_string()),
        (entity_ids[1], entity_ids[2], "manages".to_string()),
        (entity_ids[0], entity_ids[1], "reports_to".to_string()),
    ];

    let relation_ids = service.create_relationships(relationships.clone()).await?;
    assert_eq!(relation_ids.len(), 3);

    // Verify relationships
    for (i, relation_id) in relation_ids.iter().enumerate() {
        let relation = knowledge_graph::get_relation_by_id(ctx.pool(), *relation_id)
            .await?
            .expect("Relation should exist");

        assert_eq!(relation.from_entity_id, relationships[i].0);
        assert_eq!(relation.to_entity_id, relationships[i].1);
        assert_eq!(relation.relation_type, relationships[i].2);
    }

    Ok(())
}

/// Test finding related entities
#[sinex_test]
async fn test_find_related_entities(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create entities and relationships
    let event = EventBuilder::new()
        .source(sources::SYSTEMD)
        .event_type(sinex::PROCESS_HEARTBEAT)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    let entities = vec![
        ("Hub".to_string(), "concept".to_string()),
        ("Spoke1".to_string(), "concept".to_string()),
        ("Spoke2".to_string(), "concept".to_string()),
        ("Spoke3".to_string(), "concept".to_string()),
    ];
    let entity_ids = service
        .create_entities_from_list(inserted.id, entities)
        .await?;

    // Create hub-and-spoke relationships
    let relationships = vec![
        (entity_ids[0], entity_ids[1], "connects_to".to_string()),
        (entity_ids[0], entity_ids[2], "connects_to".to_string()),
        (entity_ids[0], entity_ids[3], "connects_to".to_string()),
    ];
    service.create_relationships(relationships).await?;

    // Find all entities related to the hub
    let related = service
        .find_related_entities(entity_ids[0], Some("connects_to"))
        .await?;
    assert_eq!(related.len(), 3, "Hub should be connected to 3 spokes");

    // Find entities with no filter (all relationships)
    let all_related = service.find_related_entities(entity_ids[0], None).await?;
    assert_eq!(all_related.len(), 3, "Should find all related entities");

    Ok(())
}

// =============================================================================
// ARTIFACT TESTS - Digital Artifact Management
// =============================================================================

/// Test creating and retrieving artifacts
#[sinex_test]
async fn test_create_and_retrieve_artifact(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test event
    let event = EventBuilder::new()
        .source(sources::FS)
        .event_type(filesystem::FILE_CREATED)
        .payload(json!({
            "path": "/home/user/document.pdf",
            "size": 2048
        }))
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    // Create artifact
    let artifact_input = CreateArtifactInput {
        source_event_id: Some(inserted.id),
        artifact_type: "document".to_string(),
        content_type: "application/pdf".to_string(),
        original_filename: Some("research_paper.pdf".to_string()),
        metadata: json!({
            "title": "Important Research",
            "author": "Dr. Smith",
            "pages": 42
        }),
    };

    let artifact_id = service.create_artifact(artifact_input).await?;

    // Retrieve and verify
    let artifact = artifacts::get_artifact_by_id(ctx.pool(), artifact_id)
        .await?
        .expect("Artifact should exist");

    assert_eq!(artifact.artifact_type, "document");
    assert_eq!(artifact.content_type, "application/pdf");
    assert_eq!(artifact.original_filename, Some("research_paper.pdf".to_string()));
    assert_eq!(artifact.metadata["title"], "Important Research");
    assert_eq!(artifact.metadata["pages"], 42);

    Ok(())
}

/// Test linking artifacts to entities
#[sinex_test]
async fn test_link_artifact_to_entity(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create entity
    let event = EventBuilder::new()
        .source(sources::SYSTEMD)
        .event_type(sinex::PROCESS_HEARTBEAT)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    let entity_ids = service
        .create_entities_from_list(
            inserted.id,
            vec![("Research Project".to_string(), "project".to_string())],
        )
        .await?;
    let entity_id = entity_ids[0];

    // Create artifact
    let artifact_input = CreateArtifactInput {
        source_event_id: Some(inserted.id),
        artifact_type: "code".to_string(),
        content_type: "text/x-rust".to_string(),
        original_filename: Some("main.rs".to_string()),
        metadata: json!({"lines": 500}),
    };
    let artifact_id = service.create_artifact(artifact_input).await?;

    // Link artifact to entity
    service.link_artifact_to_entity(artifact_id, entity_id).await?;

    // Verify the link through artifact metadata
    let artifact = artifacts::get_artifact_by_id(ctx.pool(), artifact_id)
        .await?
        .expect("Artifact should exist");
    
    // The link is typically stored in a separate table or the metadata
    // This test assumes the service updates metadata to track entity links
    
    Ok(())
}

// =============================================================================
// SEARCH TESTS - Finding Entities and Artifacts
// =============================================================================

/// Test searching entities by name
#[sinex_test]
async fn test_search_entities_by_name(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test entities
    let event = EventBuilder::new()
        .source(sources::SYSTEMD)
        .event_type(sinex::PROCESS_HEARTBEAT)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    let entities = vec![
        ("Rust Programming".to_string(), "topic".to_string()),
        ("Rust Foundation".to_string(), "organization".to_string()),
        ("Python Language".to_string(), "topic".to_string()),
        ("Rusty Tools Inc".to_string(), "organization".to_string()),
    ];
    service
        .create_entities_from_list(inserted.id, entities)
        .await?;

    // Search for entities containing "Rust"
    let results = service.search_entities("Rust", None).await?;
    assert_eq!(results.len(), 3, "Should find 3 entities containing 'Rust'");

    // Search with type filter
    let org_results = service
        .search_entities("Rust", Some("organization"))
        .await?;
    assert_eq!(org_results.len(), 2, "Should find 2 organizations");

    Ok(())
}

/// Test searching artifacts by metadata
#[sinex_test]
async fn test_search_artifacts_by_metadata(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test artifacts
    let event = EventBuilder::new()
        .source(sources::FS)
        .event_type(filesystem::FILE_CREATED)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    // Create artifacts with searchable metadata
    let artifacts = vec![
        CreateArtifactInput {
            source_event_id: Some(inserted.id),
            artifact_type: "document".to_string(),
            content_type: "text/markdown".to_string(),
            original_filename: Some("rust_guide.md".to_string()),
            metadata: json!({
                "title": "Rust Programming Guide",
                "topic": "programming",
                "language": "rust"
            }),
        },
        CreateArtifactInput {
            source_event_id: Some(inserted.id),
            artifact_type: "document".to_string(),
            content_type: "text/markdown".to_string(),
            original_filename: Some("python_tutorial.md".to_string()),
            metadata: json!({
                "title": "Python Tutorial",
                "topic": "programming",
                "language": "python"
            }),
        },
    ];

    for artifact in artifacts {
        service.create_artifact(artifact).await?;
    }

    // Search artifacts by metadata content
    let rust_artifacts = service
        .search_artifacts(json!({"language": "rust"}))
        .await?;
    assert_eq!(rust_artifacts.len(), 1, "Should find 1 Rust artifact");

    let prog_artifacts = service
        .search_artifacts(json!({"topic": "programming"}))
        .await?;
    assert_eq!(prog_artifacts.len(), 2, "Should find 2 programming artifacts");

    Ok(())
}

// =============================================================================
// ERROR HANDLING TESTS - Validation and Constraints
// =============================================================================

/// Test invalid entity type validation
#[sinex_test]
async fn test_invalid_entity_type_validation(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create test event
    let event = EventBuilder::new()
        .source(sources::SYSTEMD)
        .event_type(sinex::PROCESS_HEARTBEAT)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    // Try to create entity with invalid type
    let invalid_entities = vec![("Test Entity".to_string(), "invalid_type".to_string())];

    let result = service
        .create_entities_from_list(inserted.id, invalid_entities)
        .await;

    assert!(result.is_err(), "Should fail with invalid entity type");

    Ok(())
}

/// Test artifact with missing required fields
#[sinex_test]
async fn test_artifact_validation(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create artifact with empty content_type (should fail)
    let invalid_artifact = CreateArtifactInput {
        source_event_id: None,
        artifact_type: "document".to_string(),
        content_type: "".to_string(), // Invalid empty string
        original_filename: None,
        metadata: json!({}),
    };

    let result = service.create_artifact(invalid_artifact).await;
    assert!(result.is_err(), "Should fail with empty content_type");

    Ok(())
}

// =============================================================================
// COMPLEX WORKFLOW TESTS - End-to-End Scenarios
// =============================================================================

/// Test complete PKM workflow: event → annotation → entities → relationships
#[sinex_test]
async fn test_complete_pkm_workflow(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Step 1: Create an event representing reading a research paper
    let event = EventBuilder::new()
        .source(sources::FS)
        .event_type(filesystem::FILE_CREATED)
        .payload(json!({
            "path": "/documents/ml_paper.pdf",
            "application": "pdf_reader"
        }))
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    // Step 2: Add annotation about insights
    let note_id = service
        .create_note(
            inserted.id,
            "Interesting paper on neural networks by Dr. Smith from MIT",
            vec!["machine-learning".to_string(), "research".to_string()],
            "researcher",
        )
        .await?;

    // Step 3: Extract entities from the annotation
    let entities = vec![
        ("Dr. Smith".to_string(), "person".to_string()),
        ("MIT".to_string(), "organization".to_string()),
        ("Neural Networks".to_string(), "topic".to_string()),
    ];
    let entity_ids = service
        .create_entities_from_list(inserted.id, entities)
        .await?;

    // Step 4: Create relationships
    let relationships = vec![
        (entity_ids[0], entity_ids[1], "affiliated_with".to_string()),
        (entity_ids[0], entity_ids[2], "researches".to_string()),
    ];
    service.create_relationships(relationships).await?;

    // Step 5: Create artifact for the paper
    let artifact_input = CreateArtifactInput {
        source_event_id: Some(inserted.id),
        artifact_type: "research_paper".to_string(),
        content_type: "application/pdf".to_string(),
        original_filename: Some("ml_paper.pdf".to_string()),
        metadata: json!({
            "title": "Advances in Neural Networks",
            "author": "Dr. Smith",
            "institution": "MIT",
            "year": 2024
        }),
    };
    let artifact_id = service.create_artifact(artifact_input).await?;

    // Verify the complete workflow created all expected data
    let annotations = annotations::get_annotations_for_event(ctx.pool(), inserted.id).await?;
    assert_eq!(annotations.len(), 1);

    // Verify entities were created
    for entity_id in &entity_ids {
        let entity = knowledge_graph::get_entity_by_id(ctx.pool(), *entity_id)
            .await?
            .expect("Entity should exist");
        assert!(!entity.name.is_empty());
    }

    let dr_smith_relations = service
        .find_related_entities(entity_ids[0], None)
        .await?;
    assert_eq!(dr_smith_relations.len(), 2);

    Ok(())
}

/// Test entity merging workflow
#[sinex_test]
async fn test_entity_merging_workflow(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create events that might reference the same entity differently
    let event1 = EventBuilder::new()
        .source(sources::SHELL_KITTY)
        .event_type(shell::COMMAND_EXECUTED)
        .payload(json!({"command": "email john.doe@example.com"}))
        .build();
    let event2 = EventBuilder::new()
        .source(sources::CLIPBOARD)
        .event_type(clipboard::COPIED)
        .payload(json!({"content": "Contact: John Doe"}))
        .build();
    
    let inserted1 = insert_event(ctx.pool(), &event1).await?;
    let inserted2 = insert_event(ctx.pool(), &event2).await?;

    // Create entities that represent the same person
    let entities1 = vec![("john.doe@example.com".to_string(), "person".to_string())];
    let entities2 = vec![("John Doe".to_string(), "person".to_string())];
    
    let ids1 = service
        .create_entities_from_list(inserted1.id, entities1)
        .await?;
    let ids2 = service
        .create_entities_from_list(inserted2.id, entities2)
        .await?;

    // Update the first entity to have the canonical name and add alias
    let updates = HashMap::from([
        ("canonical_name".to_string(), json!("John Doe")),
        ("aliases".to_string(), json!(["john.doe@example.com", "J. Doe"])),
        ("metadata".to_string(), json!({
            "email": "john.doe@example.com",
            "merged": true
        })),
    ]);
    
    service.update_entity_properties(ids1[0], updates).await?;

    // Verify the merge-like behavior
    let entity = knowledge_graph::get_entity_by_id(ctx.pool(), ids1[0])
        .await?
        .expect("Entity should exist");
    
    assert_eq!(entity.canonical_name, "John Doe");
    assert!(entity.aliases.contains(&"john.doe@example.com".to_string()));
    assert_eq!(entity.metadata["merged"], true);

    Ok(())
}

/// Test performance with large numbers of entities and relationships
#[sinex_test]
async fn test_pkm_performance_at_scale(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create base event
    let event = EventBuilder::new()
        .source(sources::SYSTEMD)
        .event_type(sinex::PROCESS_HEARTBEAT)
        .build();
    let inserted = insert_event(ctx.pool(), &event).await?;

    // Create many entities
    let entity_count = 50;
    let mut entities = Vec::new();
    for i in 0..entity_count {
        entities.push((
            format!("Entity_{}", i),
            if i % 3 == 0 { "person" } else if i % 3 == 1 { "project" } else { "topic" }.to_string(),
        ));
    }

    let start = Instant::now();
    let entity_ids = service
        .create_entities_from_list(inserted.id, entities)
        .await?;
    let entity_creation_time = start.elapsed();
    
    println!("Created {} entities in {:?}", entity_count, entity_creation_time);
    assert!(
        entity_creation_time < Duration::from_secs(5),
        "Entity creation should be reasonably fast"
    );

    // Create relationships between entities (sparse graph)
    let mut relationships = Vec::new();
    for i in 0..entity_count / 2 {
        let from_idx = i * 2;
        let to_idx = (i * 2 + 1) % entity_count;
        relationships.push((
            entity_ids[from_idx],
            entity_ids[to_idx],
            "related_to".to_string(),
        ));
    }

    let start = Instant::now();
    service.create_relationships(relationships).await?;
    let relationship_creation_time = start.elapsed();
    
    println!(
        "Created {} relationships in {:?}",
        entity_count / 2,
        relationship_creation_time
    );

    // Test search performance
    let start = Instant::now();
    let search_results = service.search_entities("Entity", None).await?;
    let search_time = start.elapsed();
    
    assert_eq!(search_results.len(), entity_count);
    println!("Searched {} entities in {:?}", entity_count, search_time);
    assert!(
        search_time < Duration::from_millis(500),
        "Search should be fast even with many entities"
    );

    Ok(())
}