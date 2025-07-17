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

use crate::common::generators;
use crate::common::prelude::*;
use serde_json::json;
use sinex_db::{
    annotations, artifacts, knowledge_graph,
    models::{CreateArtifactInput, CreateEntityInput, CreateRelationInput},
};
use sinex_services::pkm::PkmService;
use sinex_ulid::Ulid;
use std::collections::{HashMap, HashSet};

// =============================================================================
// ANNOTATION TESTS - Note Creation and Management
// =============================================================================

/// Test creating a note annotation on an event
#[sinex_test(timeout = 30)]
async fn test_create_note_annotation(ctx: TestContext) -> TestResult {
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
#[sinex_test(timeout = 30)]
async fn test_multiple_note_annotations(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

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
#[sinex_test(timeout = 30)]
async fn test_create_entities_from_list(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create a test event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

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

/// Test entity constraint validation - valid entity types
#[sinex_test(timeout = 30)]
async fn test_entity_type_constraints_valid(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Test all valid entity types from the database constraint
    let valid_types = vec![
        "person",
        "project",
        "topic",
        "organization",
        "location",
        "concept",
        "tool",
        "event",
    ];

    for entity_type in valid_types {
        let entities = vec![(format!("Test {}", entity_type), entity_type.to_string())];
        let result = service
            .create_entities_from_list(inserted_event.id, entities)
            .await;
        assert!(
            result.is_ok(),
            "Failed to create entity with type: {}",
            entity_type
        );
    }

    Ok(())
}

/// Test entity constraint validation - invalid entity types
#[sinex_test(timeout = 30)]
async fn test_entity_type_constraints_invalid(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Test invalid entity type
    let entities = vec![("Test Entity".to_string(), "invalid_type".to_string())];
    let result = service
        .create_entities_from_list(inserted_event.id, entities)
        .await;
    assert!(result.is_err(), "Should fail with invalid entity type");

    Ok(())
}

/// Test direct entity creation with full parameters
#[sinex_test(timeout = 30)]
async fn test_direct_entity_creation(ctx: TestContext) -> TestResult {
    // Test direct database entity creation for comprehensive coverage
    let input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Jane Smith".to_string(),
        canonical_name: Some("Dr. Jane Smith".to_string()),
        aliases: Some(vec!["J. Smith".to_string(), "Jane".to_string()]),
        description: Some("A test person entity".to_string()),
        metadata: Some(json!({"department": "engineering", "role": "senior"})),
    };

    let entity = knowledge_graph::create_entity(ctx.pool(), input).await?;

    assert_eq!(entity.name, "Jane Smith");
    assert_eq!(entity.entity_type, "person");
    assert_eq!(entity.canonical_name, "Dr. Jane Smith");
    assert_eq!(entity.aliases, vec!["J. Smith", "Jane"]);
    assert_eq!(entity.description, Some("A test person entity".to_string()));
    assert_eq!(entity.metadata["department"], json!("engineering"));
    assert_eq!(entity.metadata["role"], json!("senior"));
    assert!(entity.merged_into_id.is_none());

    Ok(())
}

/// Test entity search functionality
#[sinex_test(timeout = 30)]
async fn test_entity_search(ctx: TestContext) -> TestResult {
    // Create test entities
    let entities = vec![
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Alice Johnson".to_string(),
            canonical_name: None,
            aliases: None,
            description: Some("Software engineer".to_string()),
            metadata: None,
        },
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Bob Alice".to_string(), // Contains "Alice" for search testing
            canonical_name: None,
            aliases: None,
            description: Some("Product manager".to_string()),
            metadata: None,
        },
        CreateEntityInput {
            entity_type: "project".to_string(),
            name: "Project Alpha".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    ];

    for input in entities {
        knowledge_graph::create_entity(ctx.pool(), input).await?;
    }

    // Test search by partial name
    let results = knowledge_graph::search_entities(ctx.pool(), "Alice", 10).await?;
    assert_eq!(results.len(), 2);

    // Results should be ordered by relevance (exact matches first)
    assert_eq!(results[0].name, "Alice Johnson"); // Exact match first
    assert_eq!(results[1].name, "Bob Alice"); // Partial match second

    // Test search by project name
    let results = knowledge_graph::search_entities(ctx.pool(), "Alpha", 10).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "Project Alpha");

    // Test search with no results
    let results = knowledge_graph::search_entities(ctx.pool(), "NonExistent", 10).await?;
    assert_eq!(results.len(), 0);

    Ok(())
}

/// Test getting entities by type
#[sinex_test(timeout = 30)]
async fn test_get_entities_by_type(ctx: TestContext) -> TestResult {
    // Create entities of different types
    let person_input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Test Person".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    let project_input = CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Test Project".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    knowledge_graph::create_entity(ctx.pool(), person_input).await?;
    knowledge_graph::create_entity(ctx.pool(), project_input).await?;

    // Test filtering by type
    let persons = knowledge_graph::get_entities_by_type(ctx.pool(), "person", 10).await?;
    assert_eq!(persons.len(), 1);
    assert_eq!(persons[0].entity_type, "person");
    assert_eq!(persons[0].name, "Test Person");

    let projects = knowledge_graph::get_entities_by_type(ctx.pool(), "project", 10).await?;
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].entity_type, "project");
    assert_eq!(projects[0].name, "Test Project");

    // Test non-existent type
    let nonexistent = knowledge_graph::get_entities_by_type(ctx.pool(), "nonexistent", 10).await?;
    assert_eq!(nonexistent.len(), 0);

    Ok(())
}

// =============================================================================
// RELATIONSHIP TESTS - Entity Linking and Relations
// =============================================================================

/// Test creating relationships between entities
#[sinex_test(timeout = 30)]
async fn test_link_entities(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create two test entities
    let person_input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "John Developer".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    let project_input = CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Awesome Project".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    let person = knowledge_graph::create_entity(ctx.pool(), person_input).await?;
    let project = knowledge_graph::create_entity(ctx.pool(), project_input).await?;

    // Create relationship between them
    let mut properties = HashMap::new();
    properties.insert("role".to_string(), json!("lead developer"));
    properties.insert("start_date".to_string(), json!("2024-01-01"));

    let relation_id = service
        .link_entities(
            person.entity_id,
            project.entity_id,
            "works_on",
            properties.clone(),
        )
        .await?;

    // Verify relationship was created
    let relations = knowledge_graph::get_entity_relations(ctx.pool(), person.entity_id).await?;
    assert_eq!(relations.len(), 1);

    let relation = &relations[0];
    assert_eq!(relation.relation_id, relation_id);
    assert_eq!(relation.from_entity_id, person.entity_id);
    assert_eq!(relation.to_entity_id, project.entity_id);
    assert_eq!(relation.relation_type, "works_on");
    assert_eq!(relation.metadata["role"], json!("lead developer"));
    assert_eq!(relation.metadata["start_date"], json!("2024-01-01"));
    assert!(relation.strength.is_none());
    assert!(relation.valid_until.is_none());

    Ok(())
}

/// Test bidirectional relationship queries
#[sinex_test(timeout = 30)]
async fn test_bidirectional_relationships(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create entities
    let entity1 = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Alice".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    let entity2 = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Bob".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    // Create relationship from entity1 to entity2
    let _relation_id = service
        .link_entities(
            entity1.entity_id,
            entity2.entity_id,
            "collaborates_with",
            HashMap::new(),
        )
        .await?;

    // Check relationships from entity1's perspective
    let relations1 = knowledge_graph::get_entity_relations(ctx.pool(), entity1.entity_id).await?;
    assert_eq!(relations1.len(), 1);
    assert_eq!(relations1[0].from_entity_id, entity1.entity_id);
    assert_eq!(relations1[0].to_entity_id, entity2.entity_id);

    // Check relationships from entity2's perspective
    let relations2 = knowledge_graph::get_entity_relations(ctx.pool(), entity2.entity_id).await?;
    assert_eq!(relations2.len(), 1);
    assert_eq!(relations2[0].from_entity_id, entity1.entity_id);
    assert_eq!(relations2[0].to_entity_id, entity2.entity_id);

    Ok(())
}

/// Test relationship with strength and validity period
#[sinex_test(timeout = 30)]
async fn test_relationship_with_strength_and_validity(ctx: TestContext) -> TestResult {
    // Create entities
    let entity1 = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Mentor".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    let entity2 = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Student".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    // Create relationship with strength and validity period
    let valid_from = chrono::Utc::now();
    let valid_until = valid_from + chrono::Duration::days(365);

    let relation_input = CreateRelationInput {
        from_entity_id: entity1.entity_id,
        to_entity_id: entity2.entity_id,
        relation_type: "mentors".to_string(),
        strength: Some(0.8),
        metadata: Some(json!({"program": "internship"})),
        valid_from: Some(valid_from),
        valid_until: Some(valid_until),
        created_from_event_id: None,
    };

    let relation = knowledge_graph::create_relation(ctx.pool(), relation_input).await?;

    // Verify relationship properties
    assert_eq!(relation.strength, Some(0.8));
    assert_eq!(relation.valid_from, valid_from);
    assert_eq!(relation.valid_until, Some(valid_until));
    assert_eq!(relation.metadata["program"], json!("internship"));

    // Verify the relationship is found in queries
    let relations = knowledge_graph::get_entity_relations(ctx.pool(), entity1.entity_id).await?;
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].relation_id, relation.relation_id);

    Ok(())
}

/// Test getting relationship by ID
#[sinex_test(timeout = 30)]
async fn test_get_relationship_by_id(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create entities and relationship
    let entity1 = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "topic".to_string(),
            name: "Machine Learning".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    let entity2 = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "topic".to_string(),
            name: "Neural Networks".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    let relation_id = service
        .link_entities(
            entity1.entity_id,
            entity2.entity_id,
            "includes",
            HashMap::new(),
        )
        .await?;

    // Get relationship by ID
    let relation = knowledge_graph::get_relation_by_id(ctx.pool(), relation_id)
        .await?
        .expect("Relationship should exist");

    assert_eq!(relation.relation_id, relation_id);
    assert_eq!(relation.from_entity_id, entity1.entity_id);
    assert_eq!(relation.to_entity_id, entity2.entity_id);
    assert_eq!(relation.relation_type, "includes");

    // Test non-existent relationship ID
    let nonexistent = knowledge_graph::get_relation_by_id(ctx.pool(), Ulid::new()).await?;
    assert!(nonexistent.is_none());

    Ok(())
}

// =============================================================================
// ARTIFACT TESTS - Artifact Management Operations
// =============================================================================

/// Test creating and retrieving artifacts
#[sinex_test(timeout = 30)]
async fn test_create_and_get_artifact(ctx: TestContext) -> TestResult {
    // Create test event for artifact linkage
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create artifact with valid artifact type
    let artifact_input = CreateArtifactInput {
        artifact_type: "document".to_string(),
        title: "Test Document".to_string(),
        source_url: Some("https://example.com/doc.pdf".to_string()),
        original_path: Some("/home/user/documents/doc.pdf".to_string()),
        mime_type: Some("application/pdf".to_string()),
        size_bytes: Some(1024),
        checksum: Some("sha256:abcd1234".to_string()),
        metadata: Some(json!({"author": "Test Author", "version": "1.0"})),
        created_from_event_id: Some(inserted_event.id),
        blob_id: None,
    };

    let artifact = artifacts::create_artifact(ctx.pool(), artifact_input).await?;

    // Verify artifact creation
    assert_eq!(artifact.artifact_type, "document");
    assert_eq!(artifact.title, "Test Document");
    assert_eq!(
        artifact.source_url,
        Some("https://example.com/doc.pdf".to_string())
    );
    assert_eq!(
        artifact.original_path,
        Some("/home/user/documents/doc.pdf".to_string())
    );
    assert_eq!(artifact.mime_type, Some("application/pdf".to_string()));
    assert_eq!(artifact.size_bytes, Some(1024));
    assert_eq!(artifact.checksum, Some("sha256:abcd1234".to_string()));
    assert_eq!(artifact.metadata["author"], json!("Test Author"));
    assert_eq!(artifact.metadata["version"], json!("1.0"));
    assert_eq!(artifact.created_from_event_id, Some(inserted_event.id));
    assert!(artifact.deleted_at.is_none());

    // Retrieve artifact by ID
    let retrieved = artifacts::get_artifact_by_id(ctx.pool(), artifact.artifact_id)
        .await?
        .expect("Artifact should exist");

    assert_eq!(retrieved.artifact_id, artifact.artifact_id);
    assert_eq!(retrieved.title, artifact.title);
    assert_eq!(retrieved.artifact_type, artifact.artifact_type);

    Ok(())
}

/// Test artifact type constraints - valid types
#[sinex_test(timeout = 30)]
async fn test_artifact_type_constraints_valid(ctx: TestContext) -> TestResult {
    // Test all valid artifact types from the database constraint
    let valid_types = vec![
        "note", "webpage", "email", "file", "document", "code", "media",
    ];

    for artifact_type in valid_types {
        let artifact_input = CreateArtifactInput {
            artifact_type: artifact_type.to_string(),
            title: format!("Test {}", artifact_type),
            source_url: None,
            original_path: None,
            mime_type: None,
            size_bytes: None,
            checksum: None,
            metadata: None,
            created_from_event_id: None,
            blob_id: None,
        };

        let result = artifacts::create_artifact(ctx.pool(), artifact_input).await;
        assert!(
            result.is_ok(),
            "Failed to create artifact with type: {}",
            artifact_type
        );
    }

    Ok(())
}

/// Test artifact type constraints - invalid types
#[sinex_test(timeout = 30)]
async fn test_artifact_type_constraints_invalid(ctx: TestContext) -> TestResult {
    let artifact_input = CreateArtifactInput {
        artifact_type: "invalid_type".to_string(),
        title: "Test Invalid".to_string(),
        source_url: None,
        original_path: None,
        mime_type: None,
        size_bytes: None,
        checksum: None,
        metadata: None,
        created_from_event_id: None,
        blob_id: None,
    };

    let result = artifacts::create_artifact(ctx.pool(), artifact_input).await;
    assert!(result.is_err(), "Should fail with invalid artifact type");

    Ok(())
}

/// Test getting recent artifacts
#[sinex_test(timeout = 30)]
async fn test_get_recent_artifacts(ctx: TestContext) -> TestResult {
    // Create multiple artifacts
    for i in 0..3 {
        let artifact_input = CreateArtifactInput {
            artifact_type: "note".to_string(),
            title: format!("Note {}", i),
            source_url: None,
            original_path: None,
            mime_type: None,
            size_bytes: None,
            checksum: None,
            metadata: Some(json!({"index": i})),
            created_from_event_id: None,
            blob_id: None,
        };

        artifacts::create_artifact(ctx.pool(), artifact_input).await?;

        // Small delay to ensure different creation times
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // Get recent artifacts
    let recent = artifacts::get_recent_artifacts(ctx.pool(), 2).await?;
    assert_eq!(recent.len(), 2);

    // Should be ordered by creation time (most recent first)
    assert_eq!(recent[0].title, "Note 2");
    assert_eq!(recent[1].title, "Note 1");

    Ok(())
}

// =============================================================================
// COMPREHENSIVE LIFECYCLE TESTS
// =============================================================================

/// Test complete PKM workflow: event → annotation → entities → relationships → artifacts
#[sinex_test(timeout = 60)]
async fn test_complete_pkm_workflow(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Step 1: Create an event
    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Step 2: Create a note annotation on the event
    let note_id = service
        .create_note(
            inserted_event.id,
            "Meeting notes about new project collaboration",
            vec![
                "meeting".to_string(),
                "project".to_string(),
                "collaboration".to_string(),
            ],
            "alice",
        )
        .await?;

    // Step 3: Extract and create entities from the meeting
    let entities = vec![
        ("Alice Smith".to_string(), "person".to_string()),
        ("Bob Johnson".to_string(), "person".to_string()),
        ("AI Research Project".to_string(), "project".to_string()),
        (
            "Stanford University".to_string(),
            "organization".to_string(),
        ),
    ];

    let entity_ids = service
        .create_entities_from_list(inserted_event.id, entities)
        .await?;
    assert_eq!(entity_ids.len(), 4);

    let alice_id = entity_ids[0];
    let bob_id = entity_ids[1];
    let project_id = entity_ids[2];
    let stanford_id = entity_ids[3];

    // Step 4: Create relationships between entities
    let mut alice_project_props = HashMap::new();
    alice_project_props.insert("role".to_string(), json!("lead researcher"));

    let mut bob_project_props = HashMap::new();
    bob_project_props.insert("role".to_string(), json!("collaborator"));

    let mut project_org_props = HashMap::new();
    project_org_props.insert("relationship".to_string(), json!("hosted_by"));

    let relation1_id = service
        .link_entities(alice_id, project_id, "leads", alice_project_props)
        .await?;
    let relation2_id = service
        .link_entities(bob_id, project_id, "collaborates_on", bob_project_props)
        .await?;
    let relation3_id = service
        .link_entities(project_id, stanford_id, "hosted_by", project_org_props)
        .await?;

    // Step 5: Create related artifacts
    let meeting_minutes = CreateArtifactInput {
        artifact_type: "document".to_string(),
        title: "Meeting Minutes - AI Research Collaboration".to_string(),
        source_url: None,
        original_path: Some("/meetings/2024-07-10-ai-research.md".to_string()),
        mime_type: Some("text/markdown".to_string()),
        size_bytes: Some(2048),
        checksum: None,
        metadata: Some(json!({
            "meeting_date": "2024-07-10",
            "attendees": ["Alice Smith", "Bob Johnson"],
            "project": "AI Research Project"
        })),
        created_from_event_id: Some(inserted_event.id),
        blob_id: None,
    };

    let artifact = artifacts::create_artifact(ctx.pool(), meeting_minutes).await?;

    // Step 6: Verify the complete workflow

    // Verify annotation exists
    let annotations = annotations::get_annotations_for_event(ctx.pool(), inserted_event.id).await?;
    assert_eq!(annotations.len(), 1);
    assert_eq!(annotations[0].annotation_id, note_id);

    // Verify all entities exist
    for entity_id in &entity_ids {
        let entity = knowledge_graph::get_entity_by_id(ctx.pool(), *entity_id).await?;
        assert!(entity.is_some());
    }

    // Verify relationships exist
    let alice_relations = knowledge_graph::get_entity_relations(ctx.pool(), alice_id).await?;
    assert_eq!(alice_relations.len(), 1);
    assert_eq!(alice_relations[0].relation_id, relation1_id);

    let bob_relations = knowledge_graph::get_entity_relations(ctx.pool(), bob_id).await?;
    assert_eq!(bob_relations.len(), 1);
    assert_eq!(bob_relations[0].relation_id, relation2_id);

    let project_relations = knowledge_graph::get_entity_relations(ctx.pool(), project_id).await?;
    assert_eq!(project_relations.len(), 3); // Alice leads it, Bob collaborates, hosted by Stanford

    // Verify artifact exists
    let retrieved_artifact =
        artifacts::get_artifact_by_id(ctx.pool(), artifact.artifact_id).await?;
    assert!(retrieved_artifact.is_some());
    assert_eq!(
        retrieved_artifact.unwrap().created_from_event_id,
        Some(inserted_event.id)
    );

    Ok(())
}

// =============================================================================
// ERROR HANDLING AND EDGE CASES
// =============================================================================

/// Test transaction rollback on entity creation failure
#[sinex_test(timeout = 30)]
async fn test_transaction_rollback_on_entity_failure(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create entities list with one invalid type to force failure
    let entities = vec![
        ("Valid Entity".to_string(), "person".to_string()),
        ("Invalid Entity".to_string(), "invalid_type".to_string()),
    ];

    // This should fail due to invalid entity type constraint
    let result = service
        .create_entities_from_list(inserted_event.id, entities)
        .await;
    assert!(result.is_err());

    // Verify no entities were created (transaction rolled back)
    let all_persons = knowledge_graph::get_entities_by_type(ctx.pool(), "person", 100).await?;
    let valid_entity_exists = all_persons.iter().any(|e| e.name == "Valid Entity");
    assert!(
        !valid_entity_exists,
        "Transaction should have rolled back, no entities should exist"
    );

    Ok(())
}

/// Test handling non-existent entity IDs in relationships
#[sinex_test(timeout = 30)]
async fn test_relationship_with_nonexistent_entities(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    // Create one valid entity
    let entity = knowledge_graph::create_entity(
        ctx.pool(),
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Valid Person".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    )
    .await?;

    // Try to create relationship with non-existent entity
    let fake_id = Ulid::new();
    let result = service
        .link_entities(entity.entity_id, fake_id, "knows", HashMap::new())
        .await;

    assert!(
        result.is_err(),
        "Should fail when linking to non-existent entity"
    );

    Ok(())
}

/// Test duplicate entity names (should be allowed)
#[sinex_test(timeout = 30)]
async fn test_duplicate_entity_names_allowed(ctx: TestContext) -> TestResult {
    // Create two entities with the same name but different types
    let person_input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Apple".to_string(),
        canonical_name: None,
        aliases: None,
        description: Some("Person named Apple".to_string()),
        metadata: None,
    };

    let organization_input = CreateEntityInput {
        entity_type: "organization".to_string(),
        name: "Apple".to_string(),
        canonical_name: Some("Apple Inc.".to_string()),
        aliases: None,
        description: Some("Technology company".to_string()),
        metadata: None,
    };

    let person = knowledge_graph::create_entity(ctx.pool(), person_input).await?;
    let organization = knowledge_graph::create_entity(ctx.pool(), organization_input).await?;

    // Both should be created successfully
    assert_ne!(person.entity_id, organization.entity_id);
    assert_eq!(person.name, "Apple");
    assert_eq!(organization.name, "Apple");
    assert_eq!(person.entity_type, "person");
    assert_eq!(organization.entity_type, "organization");

    // Search should return both
    let search_results = knowledge_graph::search_entities(ctx.pool(), "Apple", 10).await?;
    assert_eq!(search_results.len(), 2);

    Ok(())
}

/// Test empty entity list handling
#[sinex_test(timeout = 30)]
async fn test_empty_entity_list(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create entities from empty list
    let entity_ids = service
        .create_entities_from_list(inserted_event.id, vec![])
        .await?;
    assert_eq!(entity_ids.len(), 0);

    Ok(())
}

/// Test annotation with empty content (should be allowed)
#[sinex_test(timeout = 30)]
async fn test_annotation_with_empty_content(ctx: TestContext) -> TestResult {
    let service = PkmService::new(ctx.pool().clone());

    let event = generators::test_events(1).into_iter().next().unwrap();
    let inserted_event = sinex_db::insert_event_with_validator(ctx.pool(), &event, None).await?;

    // Create annotation with empty content
    let annotation_id = service
        .create_note(
            inserted_event.id,
            "", // Empty content
            vec!["empty".to_string()],
            "test_user",
        )
        .await?;

    // Verify annotation was created
    let annotation = annotations::get_annotation_by_id(ctx.pool(), annotation_id)
        .await?
        .expect("Annotation should exist");

    assert_eq!(annotation.content, "");
    assert_eq!(annotation.metadata["tags"], json!(["empty"]));

    Ok(())
}
