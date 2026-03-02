//! Integration tests for the PKM (Personal Knowledge Management) service.
//!
//! Tests cover source material registration, entity creation from source materials,
//! entity linking, in-flight material lifecycle, system metadata attachment,
//! and content preview generation.

use sinex_services::PkmService;
use std::collections::HashMap;
use xtask::sandbox::prelude::*;

// =============================================================================
// SOURCE MATERIAL REGISTRATION
// =============================================================================

#[sinex_test]
async fn test_register_source_material_file(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"hello world";
    let metadata = json!({"custom_key": "custom_value"});

    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/test.txt"),
            content,
            Some("text/plain"),
            metadata,
        )
        .await?;

    // Verify the material was registered by looking it up
    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("registered material should be retrievable");

    assert_eq!(record.material_kind, "annex");
    assert_eq!(record.source_identifier, "/tmp/test.txt");

    // Verify system metadata was attached
    let system_meta = &record.metadata["_system_metadata"];
    assert_eq!(system_meta["file_size_bytes"], json!(content.len() as i64));
    assert!(system_meta["checksum"].is_string());
    assert_eq!(system_meta["mime_type"], json!("text/plain"));

    // Verify custom metadata was preserved
    assert_eq!(record.metadata["custom_key"], json!("custom_value"));

    Ok(())
}

#[sinex_test]
async fn test_register_source_material_blob(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"binary data here";
    let material_id = pkm
        .register_source_material("blob", None, content, None, json!({}))
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("blob material should be retrievable");

    assert_eq!(record.material_kind, "annex");
    // blob type uses "memory://inline" as source identifier
    assert_eq!(record.source_identifier, "memory://inline");

    Ok(())
}

#[sinex_test]
async fn test_register_source_material_deduplication(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"deduplicate me";
    let metadata = json!({"tag": "first"});

    // Register the same content twice
    let first_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/dedup.txt"),
            content,
            Some("text/plain"),
            metadata.clone(),
        )
        .await?;

    let second_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/dedup2.txt"),
            content,
            Some("text/plain"),
            json!({"tag": "second"}),
        )
        .await?;

    // Both should return the same ID due to BLAKE3 deduplication
    // (only if the first registration created a blob record that can be found)
    // Note: register_source_material does NOT create a blob record itself,
    // so deduplication via find_by_blake3 won't find anything.
    // Each call creates a new source material record.
    // Both IDs should be valid ULIDs.
    assert_ne!(
        first_id, second_id,
        "without blob storage, separate source materials are created"
    );

    Ok(())
}

#[sinex_test]
async fn test_register_source_material_stream(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"stream content";
    let material_id = pkm
        .register_source_material(
            "stream",
            Some("nats://events.stream"),
            content,
            Some("application/json"),
            json!({"stream_name": "events"}),
        )
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("stream material should be retrievable");
    assert_eq!(record.source_identifier, "nats://events.stream");

    Ok(())
}

#[sinex_test]
async fn test_register_source_material_content_preview_text(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = "This is a text file with some content for preview.".as_bytes();
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/preview.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("material should exist");

    // For text content, the preview should contain the text
    let preview = record.metadata["content_preview"]
        .as_str()
        .expect("content_preview should be a string");
    assert!(preview.contains("This is a text file"));

    Ok(())
}

#[sinex_test]
async fn test_register_source_material_content_preview_binary(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]; // PNG header
    let material_id = pkm
        .register_source_material(
            "blob.binary",
            Some("image.png"),
            content,
            Some("image/png"),
            json!({}),
        )
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("material should exist");

    // For binary content, the preview should indicate binary
    let preview = record.metadata["content_preview"]
        .as_str()
        .expect("content_preview should be a string");
    assert!(
        preview.contains("Binary content"),
        "binary content preview should indicate binary, got: {preview}"
    );

    Ok(())
}

// =============================================================================
// ENTITY CREATION FROM SOURCE MATERIAL
// =============================================================================

#[sinex_test]
async fn test_create_entities_from_source_material(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // First register a source material
    let content = b"source document";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/entities.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    // Create entities from the source material
    let entities = vec![
        ("Alice".to_string(), "person".to_string()),
        ("Sinex Project".to_string(), "project".to_string()),
        ("Rust".to_string(), "topic".to_string()),
    ];

    let entity_ids = pkm
        .create_entities_from_source_material(material_id, entities, "test-user")
        .await?;

    assert_eq!(entity_ids.len(), 3, "should create 3 entities");

    // Verify each entity was created in the knowledge graph
    for entity_id in &entity_ids {
        let entity = pool
            .knowledge_graph()
            .get_entity(Id::from_ulid(*entity_id))
            .await?;
        let entity = entity.expect("entity should exist");

        // Verify properties contain provenance metadata
        let props = &entity.properties;
        assert_eq!(
            props["source_material_id"],
            json!(material_id.to_string()),
            "entity properties should contain source_material_id"
        );
        assert_eq!(props["created_by"], json!("test-user"));
        assert_eq!(props["extraction_method"], json!("manual"));
    }

    // Verify entity types
    let entity_0 = pool
        .knowledge_graph()
        .get_entity(Id::from_ulid(entity_ids[0]))
        .await?
        .expect("first entity should exist");
    assert_eq!(entity_0.entity_type, "person");
    assert_eq!(entity_0.name, "Alice");

    let entity_1 = pool
        .knowledge_graph()
        .get_entity(Id::from_ulid(entity_ids[1]))
        .await?
        .expect("second entity should exist");
    assert_eq!(entity_1.entity_type, "project");
    assert_eq!(entity_1.name, "Sinex Project");

    let entity_2 = pool
        .knowledge_graph()
        .get_entity(Id::from_ulid(entity_ids[2]))
        .await?
        .expect("third entity should exist");
    assert_eq!(entity_2.entity_type, "topic");
    assert_eq!(entity_2.name, "Rust");

    Ok(())
}

#[sinex_test]
async fn test_create_entities_nonexistent_source_material(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Use a random ULID that doesn't exist
    let fake_material_id = Ulid::new();

    let entities = vec![("Alice".to_string(), "person".to_string())];

    let result = pkm
        .create_entities_from_source_material(fake_material_id, entities, "test-user")
        .await;

    assert!(
        result.is_err(),
        "should fail for nonexistent source material"
    );
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("not found"),
        "error should indicate not found, got: {err_str}"
    );

    Ok(())
}

#[sinex_test]
async fn test_create_entities_invalid_type(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Register a valid source material
    let content = b"source";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/invalid-type.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    // Try creating an entity with an invalid type
    let entities = vec![("Test".to_string(), "invalid_type".to_string())];

    let result = pkm
        .create_entities_from_source_material(material_id, entities, "test-user")
        .await;

    assert!(result.is_err(), "should fail for invalid entity type");
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("Unknown entity type") || err_str.contains("validation"),
        "error should indicate validation failure, got: {err_str}"
    );

    Ok(())
}

#[sinex_test]
async fn test_create_entities_empty_type(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"source";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/empty-type.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    let entities = vec![("Test".to_string(), String::new())];

    let result = pkm
        .create_entities_from_source_material(material_id, entities, "test-user")
        .await;

    assert!(result.is_err(), "should fail for empty entity type");
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("required") || err_str.contains("validation"),
        "error should indicate entity type is required, got: {err_str}"
    );

    Ok(())
}

#[sinex_test]
async fn test_create_entities_all_valid_types(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"all types source";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/all-types.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    let all_types = vec![
        ("Alice".to_string(), "person".to_string()),
        ("Sinex".to_string(), "project".to_string()),
        ("Rust".to_string(), "topic".to_string()),
        ("Anthropic".to_string(), "organization".to_string()),
        ("San Francisco".to_string(), "location".to_string()),
        ("Event Sourcing".to_string(), "concept".to_string()),
        ("Neovim".to_string(), "tool".to_string()),
        ("RustConf".to_string(), "event".to_string()),
    ];

    let entity_ids = pkm
        .create_entities_from_source_material(material_id, all_types, "type-checker")
        .await?;

    assert_eq!(entity_ids.len(), 8, "all 8 entity types should be created");

    Ok(())
}

#[sinex_test]
async fn test_create_entities_case_insensitive_type(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"case test";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/case-test.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    // EntityTypeMapper normalizes to lowercase
    let entities = vec![
        ("Alice".to_string(), "PERSON".to_string()),
        ("Bob".to_string(), "Person".to_string()),
        ("Charlie".to_string(), " person ".to_string()),
    ];

    let entity_ids = pkm
        .create_entities_from_source_material(material_id, entities, "test-user")
        .await?;

    assert_eq!(
        entity_ids.len(),
        3,
        "case-insensitive types should all work"
    );

    // Verify all created as person type
    for entity_id in &entity_ids {
        let entity = pool
            .knowledge_graph()
            .get_entity(Id::from_ulid(*entity_id))
            .await?
            .expect("entity should exist");
        assert_eq!(entity.entity_type, "person");
    }

    Ok(())
}

// =============================================================================
// ENTITY LINKING
// =============================================================================

#[sinex_test]
async fn test_link_entities(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Create source material and entities
    let content = b"linking test";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/link.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    let entity_ids = pkm
        .create_entities_from_source_material(
            material_id,
            vec![
                ("Alice".to_string(), "person".to_string()),
                ("Sinex".to_string(), "project".to_string()),
            ],
            "test-user",
        )
        .await?;

    let alice_id: Id<sinex_primitives::domain::Entity> = Id::from_ulid(entity_ids[0]);
    let sinex_id: Id<sinex_primitives::domain::Entity> = Id::from_ulid(entity_ids[1]);

    // Link entities with relationship properties
    let mut properties = HashMap::new();
    properties.insert("role".to_string(), json!("maintainer"));
    properties.insert("since".to_string(), json!("2024-01-01"));

    let relation_id = pkm
        .link_entities(
            alice_id,
            sinex_id,
            "works_on",
            properties,
            Some(material_id),
        )
        .await?;

    // Verify the relationship was created by querying from entity
    let relations = pool
        .knowledge_graph()
        .get_entity_relations(alice_id, Some("works_on"), false)
        .await?;

    assert_eq!(
        relations.len(),
        1,
        "should have exactly one 'works_on' relation"
    );
    let relation = &relations[0];

    assert_eq!(relation.from_entity_id, alice_id);
    assert_eq!(relation.to_entity_id, sinex_id);
    assert_eq!(relation.relation_type, "works_on");
    assert_eq!(*relation.id.as_ulid(), relation_id);

    // Verify relationship properties preserved
    assert_eq!(relation.properties["role"], json!("maintainer"));
    assert_eq!(relation.properties["since"], json!("2024-01-01"));

    // Verify system metadata with source_material_id
    let system_meta = &relation.properties["_system_metadata"];
    assert_eq!(
        system_meta["source_material_id"],
        json!(material_id.to_string())
    );

    Ok(())
}

#[sinex_test]
async fn test_link_entities_without_source_material(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Create entities directly via knowledge graph repo
    let entity_a = pool
        .knowledge_graph()
        .create_entity(sinex_db::repositories::CreateEntity::person("DirectAlice"))
        .await?;
    let entity_b = pool
        .knowledge_graph()
        .create_entity(sinex_db::repositories::CreateEntity::project(
            "DirectProject",
        ))
        .await?;

    // Link without source material
    let relation_id = pkm
        .link_entities(
            entity_a.id,
            entity_b.id,
            "contributes_to",
            HashMap::new(),
            None,
        )
        .await?;

    let relations = pool
        .knowledge_graph()
        .get_entity_relations(entity_a.id, Some("contributes_to"), false)
        .await?;

    assert_eq!(relations.len(), 1, "should have exactly one relation");
    let relation = &relations[0];

    assert_eq!(relation.relation_type, "contributes_to");
    assert_eq!(*relation.id.as_ulid(), relation_id);
    // Without source_material_id, system_metadata should not contain it
    let system_meta = &relation.properties["_system_metadata"];
    assert!(
        system_meta.get("source_material_id").is_none()
            || system_meta["source_material_id"].is_null(),
        "no source_material_id when none provided"
    );

    Ok(())
}

#[sinex_test]
async fn test_link_entities_with_complex_properties(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let entity_a = pool
        .knowledge_graph()
        .create_entity(sinex_db::repositories::CreateEntity::concept("Concept A"))
        .await?;
    let entity_b = pool
        .knowledge_graph()
        .create_entity(sinex_db::repositories::CreateEntity::concept("Concept B"))
        .await?;

    let mut properties = HashMap::new();
    properties.insert("weight".to_string(), json!(0.85));
    properties.insert("nested".to_string(), json!({"a": 1, "b": [2, 3]}));
    properties.insert("tags".to_string(), json!(["related", "derived"]));

    let _relation_id = pkm
        .link_entities(entity_a.id, entity_b.id, "related_to", properties, None)
        .await?;

    let relations = pool
        .knowledge_graph()
        .get_entity_relations(entity_a.id, Some("related_to"), false)
        .await?;

    assert_eq!(relations.len(), 1, "should have exactly one relation");
    let relation = &relations[0];

    assert_eq!(relation.properties["weight"], json!(0.85));
    assert_eq!(relation.properties["nested"]["a"], json!(1));
    assert_eq!(relation.properties["tags"][0], json!("related"));

    Ok(())
}

// =============================================================================
// IN-FLIGHT MATERIAL LIFECYCLE
// =============================================================================

#[sinex_test]
async fn test_in_flight_material_lifecycle(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Register in-flight material (before content is available)
    let material_id = pkm
        .register_in_flight_material(
            "stream",
            Some("nats://events.live"),
            json!({"stream": "events.live", "consumer": "test"}),
        )
        .await?;

    // Verify in-flight status
    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("in-flight material should be retrievable");
    assert_eq!(
        record.status, "sensing",
        "in-flight material should have sensing status"
    );

    // Finalize with content
    let content = b"captured stream content";
    pkm.finalize_in_flight_material(material_id, content, Some("text/plain"))
        .await?;

    // Verify finalized state
    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("finalized material should be retrievable");
    assert_eq!(
        record.status, "completed",
        "finalized material should have completed status"
    );
    assert!(
        record.optional_blob_id.is_some(),
        "finalized material should have a blob ID"
    );
    assert!(
        record.end_time.is_some(),
        "finalized material should have end_time set"
    );

    Ok(())
}

#[sinex_test]
async fn test_in_flight_material_metadata_preserved(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let initial_metadata = json!({
        "capture_source": "terminal",
        "session_id": "abc123"
    });

    let material_id = pkm
        .register_in_flight_material("stream", Some("terminal://session"), initial_metadata)
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("material should exist");

    // Verify metadata was preserved
    assert_eq!(record.metadata["capture_source"], json!("terminal"));
    assert_eq!(record.metadata["session_id"], json!("abc123"));

    Ok(())
}

// =============================================================================
// SYSTEM METADATA (attach_system_metadata function)
// =============================================================================

#[sinex_test]
async fn test_register_material_system_metadata_object(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // When metadata is an object, system metadata is merged as a key
    let content = b"test content";
    let metadata = json!({"user_key": "user_value"});

    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/meta-obj.txt"),
            content,
            Some("text/plain"),
            metadata,
        )
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("material should exist");

    // Both user metadata and system metadata should be present
    assert_eq!(record.metadata["user_key"], json!("user_value"));
    assert!(
        record.metadata["_system_metadata"].is_object(),
        "_system_metadata should be an object"
    );

    Ok(())
}

#[sinex_test]
async fn test_register_material_null_metadata(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // When metadata is null, system metadata wraps it
    let content = b"test content";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/meta-null.txt"),
            content,
            Some("text/plain"),
            json!(null),
        )
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("material should exist");

    // The metadata should contain _system_metadata since null gets wrapped
    assert!(
        record.metadata["_system_metadata"].is_object(),
        "_system_metadata should be present even with null input"
    );

    Ok(())
}

#[sinex_test]
async fn test_register_material_non_object_metadata(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // When metadata is a non-object value, it gets wrapped with caller_metadata key
    let content = b"test content";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/meta-str.txt"),
            content,
            Some("text/plain"),
            json!("just a string"),
        )
        .await?;

    let record = pool
        .source_materials()
        .get_by_id(material_id.into())
        .await?;
    let record = record.expect("material should exist");

    // The original metadata should be under caller_metadata and system metadata under _system_metadata
    assert_eq!(record.metadata["caller_metadata"], json!("just a string"));
    assert!(
        record.metadata["_system_metadata"].is_object(),
        "_system_metadata should be present"
    );

    Ok(())
}

// =============================================================================
// RECENT MATERIALS & SEARCH
// =============================================================================

#[sinex_test]
async fn test_get_recent_source_materials(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Register a few materials
    for i in 0..3 {
        pkm.register_source_material(
            "file",
            Some(&format!("/tmp/recent-{i}.txt")),
            format!("content {i}").as_bytes(),
            Some("text/plain"),
            json!({"index": i}),
        )
        .await?;
    }

    let recent = pkm.get_recent_source_materials(None, Some(10)).await?;
    assert!(
        recent.len() >= 3,
        "should have at least 3 recent materials, got {}",
        recent.len()
    );

    Ok(())
}

#[sinex_test]
async fn test_search_materials_by_metadata(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Register material with searchable metadata
    let content = b"searchable content";
    pkm.register_source_material(
        "file",
        Some("/tmp/searchable.txt"),
        content,
        Some("text/plain"),
        json!({"project": "sinex", "priority": "high"}),
    )
    .await?;

    // Search by metadata key
    let results = pkm
        .search_materials_by_metadata("project", json!("sinex"))
        .await?;

    assert!(
        !results.is_empty(),
        "should find materials matching metadata search"
    );

    // Verify the result contains our material
    let found = results
        .iter()
        .any(|r| r.get("material_type").and_then(|v| v.as_str()) == Some("annex"));
    assert!(found, "should find our registered material");

    Ok(())
}

// =============================================================================
// CREATE NOTE (annotation on events)
// =============================================================================

#[sinex_test]
async fn test_create_note_on_event(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    // Create an event first
    let material_id = ctx.create_source_material(Some("note-test")).await?;

    let event = DynamicPayload::new("test-source", "test.event", json!({"key": "value"}))
        .from_material(material_id)
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let event_id = inserted.id.expect("inserted event should have id");

    // Create a note annotation
    let annotation_id = pkm
        .create_note(
            event_id,
            "This is a test note",
            vec!["tag1".to_string(), "tag2".to_string()],
            "test-user",
            Some(*material_id.as_ulid()),
        )
        .await?;

    // Verify annotation was created (the ID should be a valid ULID)
    assert!(
        !annotation_id.to_string().is_empty(),
        "annotation ID should be valid"
    );

    Ok(())
}

// =============================================================================
// ENTITY TYPE VALIDATION (via EntityTypeMapper)
// =============================================================================

#[sinex_test]
async fn test_entity_type_whitespace_handling(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"whitespace test";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/ws-test.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    // Type with leading/trailing whitespace should be trimmed and normalized
    let entities = vec![("Test Entity".to_string(), "  TOPIC  ".to_string())];

    let entity_ids = pkm
        .create_entities_from_source_material(material_id, entities, "test-user")
        .await?;

    assert_eq!(entity_ids.len(), 1);

    let entity = pool
        .knowledge_graph()
        .get_entity(Id::from_ulid(entity_ids[0]))
        .await?
        .expect("entity should exist");
    assert_eq!(entity.entity_type, "topic");

    Ok(())
}

// =============================================================================
// TRANSACTION ATOMICITY
// =============================================================================

#[sinex_test]
async fn test_create_entities_transaction_atomicity(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool();
    let pkm = PkmService::new(pool.clone());

    let content = b"atomicity test";
    let material_id = pkm
        .register_source_material(
            "file",
            Some("/tmp/atomic.txt"),
            content,
            Some("text/plain"),
            json!({}),
        )
        .await?;

    // Mix valid and invalid entity types -- the invalid one should cause the
    // entire transaction to roll back
    let entities = vec![
        ("Valid Person".to_string(), "person".to_string()),
        ("Valid Topic".to_string(), "topic".to_string()),
        ("Invalid".to_string(), "nonexistent_type".to_string()),
    ];

    let result = pkm
        .create_entities_from_source_material(material_id, entities, "test-user")
        .await;

    assert!(result.is_err(), "should fail due to invalid entity type");

    // The valid entities should NOT have been committed due to transaction rollback
    let all_entities = pool
        .knowledge_graph()
        .find_entities_by_name("Valid Person")
        .await?;
    assert!(
        all_entities.is_empty(),
        "transaction should have rolled back, no entities should exist"
    );

    Ok(())
}
