use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use sinex_core::test_macros::sinex_test;
use sinex_core::{TestContext, RawEventBuilder};
use sinex_db::prelude::*;
use sinex_ulid::Ulid;

#[sinex_test]
async fn test_entity_crud_operations(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());

    // Create some test events for source tracking
    let event1 = RawEventBuilder::new("kg.test", "entity.created", json!({})).build();
    let event2 = RawEventBuilder::new("kg.test", "entity.updated", json!({})).build();
    insert_raw_event(ctx.pool(), &event1).await?;
    insert_raw_event(ctx.pool(), &event2).await?;

    // Test entity creation
    let input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "John Doe".to_string(),
        description: Some("Software engineer and knowledge worker".to_string()),
        properties: Some(json!({
            "email": "john.doe@example.com",
            "skills": ["rust", "python", "systems"],
            "location": "San Francisco"
        })),
        confidence_score: Some(0.95),
        source_event_ids: Some(vec![event1.id, event2.id]),
    };

    let entity = service.create_entity(input).await?;
    assert_eq!(entity.entity_type, "person");
    assert_eq!(entity.name, "John Doe");
    assert_eq!(entity.description, Some("Software engineer and knowledge worker".to_string()));
    assert_eq!(entity.confidence_score, Some(0.95));
    assert_eq!(entity.source_event_ids, Some(vec![event1.id, event2.id]));

    // Test get entity by ID
    let retrieved = service.get_entity(entity.entity_id).await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.entity_id, entity.entity_id);
    assert_eq!(retrieved.name, "John Doe");

    // Test update entity
    let updated = service.update_entity(
        entity.entity_id,
        Some("John Smith".to_string()),
        Some("Senior software engineer and knowledge worker".to_string()),
        Some(json!({
            "email": "john.smith@example.com",
            "skills": ["rust", "python", "systems", "architecture"],
            "location": "San Francisco",
            "title": "Senior Engineer"
        })),
        Some(0.98),
    ).await?;
    assert_eq!(updated.name, "John Smith");
    assert_eq!(updated.description, Some("Senior software engineer and knowledge worker".to_string()));
    assert_eq!(updated.confidence_score, Some(0.98));

    // Test delete entity
    let deleted = service.delete_entity(entity.entity_id).await?;
    assert!(deleted);

    // Verify deletion
    let not_found = service.get_entity(entity.entity_id).await?;
    assert!(not_found.is_none());

    Ok(())
}

#[sinex_test]
async fn test_entity_search_and_filtering(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());

    // Create multiple entities of different types
    let entities_data = vec![
        ("person", "Alice Johnson", "Software architect"),
        ("person", "Bob Smith", "Data scientist"),
        ("company", "TechCorp Inc", "Technology company"),
        ("project", "Project Alpha", "Machine learning initiative"),
        ("person", "Charlie Brown", "Product manager"),
    ];

    let mut created_entities = Vec::new();
    for (entity_type, name, description) in entities_data {
        let input = CreateEntityInput {
            entity_type: entity_type.to_string(),
            name: name.to_string(),
            description: Some(description.to_string()),
            properties: Some(json!({"created_in_test": true})),
            confidence_score: Some(0.9),
            source_event_ids: None,
        };
        let entity = service.create_entity(input).await?;
        created_entities.push(entity);
    }

    // Test find entities by name (fuzzy search)
    let alice_results = service.find_entities_by_name("Alice", Some(10)).await?;
    assert_eq!(alice_results.len(), 1);
    assert_eq!(alice_results[0].name, "Alice Johnson");

    let smith_results = service.find_entities_by_name("Smith", Some(10)).await?;
    assert_eq!(smith_results.len(), 1);
    assert_eq!(smith_results[0].name, "Bob Smith");

    // Test partial name search
    let partial_results = service.find_entities_by_name("Joh", Some(10)).await?;
    assert_eq!(partial_results.len(), 1);
    assert_eq!(partial_results[0].name, "Alice Johnson");

    // Test get entities by type
    let person_entities = service.get_entities_by_type("person", Some(10), Some(0)).await?;
    assert_eq!(person_entities.len(), 3);
    for entity in &person_entities {
        assert_eq!(entity.entity_type, "person");
    }

    let company_entities = service.get_entities_by_type("company", Some(10), Some(0)).await?;
    assert_eq!(company_entities.len(), 1);
    assert_eq!(company_entities[0].name, "TechCorp Inc");

    // Test search entities with full-text search
    let software_search = service.search_entities("software", None, Some(10)).await?;
    assert_eq!(software_search.len(), 2); // Alice (architect) and Bob (scientist, if "software" matches description)

    let tech_search = service.search_entities("tech", Some("company"), Some(10)).await?;
    assert_eq!(tech_search.len(), 1);
    assert_eq!(tech_search[0].name, "TechCorp Inc");

    Ok(())
}

#[sinex_test]
async fn test_relation_crud_operations(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());

    // Create test entities
    let person_input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Jane Developer".to_string(),
        description: Some("Software developer".to_string()),
        properties: None,
        confidence_score: Some(0.9),
        source_event_ids: None,
    };
    let person = service.create_entity(person_input).await?;

    let company_input = CreateEntityInput {
        entity_type: "company".to_string(),
        name: "DevCorp".to_string(),
        description: Some("Software development company".to_string()),
        properties: None,
        confidence_score: Some(0.95),
        source_event_ids: None,
    };
    let company = service.create_entity(company_input).await?;

    // Test relation creation
    let relation_input = CreateRelationInput {
        from_entity_id: person.entity_id,
        to_entity_id: company.entity_id,
        relation_type: "works_at".to_string(),
        properties: Some(json!({
            "role": "Senior Developer",
            "start_date": "2023-01-15",
            "department": "Engineering"
        })),
        confidence_score: Some(0.92),
        source_event_ids: None,
    };

    let relation = service.create_relation(relation_input).await?;
    assert_eq!(relation.from_entity_id, person.entity_id);
    assert_eq!(relation.to_entity_id, company.entity_id);
    assert_eq!(relation.relation_type, "works_at");
    assert_eq!(relation.confidence_score, Some(0.92));

    // Test get relation by ID
    let retrieved = service.get_relation(relation.relation_id).await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.relation_id, relation.relation_id);

    // Test get entity relations
    let person_relations = service.get_entity_relations(person.entity_id).await?;
    assert_eq!(person_relations.len(), 1);
    assert_eq!(person_relations[0].relation_id, relation.relation_id);

    let company_relations = service.get_entity_relations(company.entity_id).await?;
    assert_eq!(company_relations.len(), 1);
    assert_eq!(company_relations[0].relation_id, relation.relation_id);

    // Test get outgoing relations
    let outgoing = service.get_outgoing_relations(person.entity_id).await?;
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].relation_type, "works_at");

    // Test get incoming relations
    let incoming = service.get_incoming_relations(company.entity_id).await?;
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].relation_type, "works_at");

    // Test delete relation
    let deleted = service.delete_relation(relation.relation_id).await?;
    assert!(deleted);

    // Verify deletion
    let not_found = service.get_relation(relation.relation_id).await?;
    assert!(not_found.is_none());

    Ok(())
}

#[sinex_test]
async fn test_graph_traversal_operations(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());

    // Create a small knowledge graph
    // Person -> works_at -> Company
    // Person -> knows -> Other Person
    // Company -> located_in -> City
    
    let person = service.create_entity(CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Alex Chen".to_string(),
        description: Some("Software engineer".to_string()),
        properties: None,
        confidence_score: Some(0.9),
        source_event_ids: None,
    }).await?;

    let colleague = service.create_entity(CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Sam Wilson".to_string(),
        description: Some("Data engineer".to_string()),
        properties: None,
        confidence_score: Some(0.9),
        source_event_ids: None,
    }).await?;

    let company = service.create_entity(CreateEntityInput {
        entity_type: "company".to_string(),
        name: "DataFlow Inc".to_string(),
        description: Some("Data processing company".to_string()),
        properties: None,
        confidence_score: Some(0.95),
        source_event_ids: None,
    }).await?;

    let city = service.create_entity(CreateEntityInput {
        entity_type: "location".to_string(),
        name: "Seattle".to_string(),
        description: Some("City in Washington state".to_string()),
        properties: None,
        confidence_score: Some(1.0),
        source_event_ids: None,
    }).await?;

    // Create relations
    let works_at = service.create_relation(CreateRelationInput {
        from_entity_id: person.entity_id,
        to_entity_id: company.entity_id,
        relation_type: "works_at".to_string(),
        properties: None,
        confidence_score: Some(0.9),
        source_event_ids: None,
    }).await?;

    let knows = service.create_relation(CreateRelationInput {
        from_entity_id: person.entity_id,
        to_entity_id: colleague.entity_id,
        relation_type: "knows".to_string(),
        properties: None,
        confidence_score: Some(0.85),
        source_event_ids: None,
    }).await?;

    let located_in = service.create_relation(CreateRelationInput {
        from_entity_id: company.entity_id,
        to_entity_id: city.entity_id,
        relation_type: "located_in".to_string(),
        properties: None,
        confidence_score: Some(0.95),
        source_event_ids: None,
    }).await?;

    // Test get connected entities
    let person_connected = service.get_connected_entities(person.entity_id).await?;
    assert_eq!(person_connected.len(), 2); // company and colleague
    let connected_names: Vec<&str> = person_connected.iter().map(|e| e.name.as_str()).collect();
    assert!(connected_names.contains(&"DataFlow Inc"));
    assert!(connected_names.contains(&"Sam Wilson"));

    let company_connected = service.get_connected_entities(company.entity_id).await?;
    assert_eq!(company_connected.len(), 2); // person and city
    let company_connected_names: Vec<&str> = company_connected.iter().map(|e| e.name.as_str()).collect();
    assert!(company_connected_names.contains(&"Alex Chen"));
    assert!(company_connected_names.contains(&"Seattle"));

    // Test get entity subgraph
    let subgraph = service.get_entity_subgraph(person.entity_id).await?;
    assert_eq!(subgraph.center_entity.entity_id, person.entity_id);
    assert_eq!(subgraph.relations.len(), 2); // works_at and knows
    assert_eq!(subgraph.connected_entities.len(), 2); // company and colleague

    // Test find shortest path
    let path = service.find_shortest_path(person.entity_id, city.entity_id, Some(5)).await?;
    assert!(path.is_some());
    let path = path.unwrap();
    assert_eq!(path.len(), 3); // person -> company -> city
    assert_eq!(path[0], person.entity_id);
    assert_eq!(path[1], company.entity_id);
    assert_eq!(path[2], city.entity_id);

    // Test path that doesn't exist
    let no_path = service.find_shortest_path(colleague.entity_id, city.entity_id, Some(2)).await?;
    assert!(no_path.is_none()); // No direct path within 2 hops

    Ok(())
}

#[sinex_test]
async fn test_graph_statistics(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());

    // Create entities of various types
    let entity_types = vec!["person", "person", "company", "project", "person"];
    let mut entities = Vec::new();

    for (i, entity_type) in entity_types.iter().enumerate() {
        let entity = service.create_entity(CreateEntityInput {
            entity_type: entity_type.to_string(),
            name: format!("Entity {}", i),
            description: Some(format!("Test entity {}", i)),
            properties: None,
            confidence_score: Some(0.9),
            source_event_ids: None,
        }).await?;
        entities.push(entity);
    }

    // Create some relations
    let relation_types = vec!["works_with", "manages", "works_with"];
    for (i, relation_type) in relation_types.iter().enumerate() {
        service.create_relation(CreateRelationInput {
            from_entity_id: entities[i].entity_id,
            to_entity_id: entities[i + 1].entity_id,
            relation_type: relation_type.to_string(),
            properties: None,
            confidence_score: Some(0.8),
            source_event_ids: None,
        }).await?;
    }

    // Test graph statistics
    let stats = service.get_graph_stats().await?;
    assert_eq!(stats.entity_count, 5);
    assert_eq!(stats.relation_count, 3);
    assert_eq!(stats.unique_entity_types, 3); // person, company, project
    assert_eq!(stats.unique_relation_types, 2); // works_with, manages

    // Test entity type distribution
    let type_distribution = service.get_entity_type_distribution().await?;
    
    // Find person type count
    let person_count = type_distribution.iter()
        .find(|tc| tc.entity_type == "person")
        .unwrap();
    assert_eq!(person_count.count, 3);

    // Find company type count
    let company_count = type_distribution.iter()
        .find(|tc| tc.entity_type == "company")
        .unwrap();
    assert_eq!(company_count.count, 1);

    Ok(())
}

#[sinex_test]
async fn test_knowledge_graph_edge_cases(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());
    let nonexistent_id = Ulid::new();

    // Test operations on nonexistent entities
    let result = service.get_entity(nonexistent_id).await?;
    assert!(result.is_none());

    let result = service.update_entity(nonexistent_id, Some("name".to_string()), None, None, None).await;
    assert!(result.is_err());

    let deleted = service.delete_entity(nonexistent_id).await?;
    assert!(!deleted);

    let connected = service.get_connected_entities(nonexistent_id).await?;
    assert!(connected.is_empty());

    let relations = service.get_entity_relations(nonexistent_id).await?;
    assert!(relations.is_empty());

    // Test operations on nonexistent relations
    let relation_result = service.get_relation(nonexistent_id).await?;
    assert!(relation_result.is_none());

    let relation_deleted = service.delete_relation(nonexistent_id).await?;
    assert!(!relation_deleted);

    // Test creating relation with nonexistent entities
    let invalid_relation = service.create_relation(CreateRelationInput {
        from_entity_id: nonexistent_id,
        to_entity_id: Ulid::new(),
        relation_type: "test".to_string(),
        properties: None,
        confidence_score: None,
        source_event_ids: None,
    }).await;
    assert!(invalid_relation.is_err()); // Should fail due to foreign key constraint

    // Test search with no results
    let empty_search = service.search_entities("nonexistent_xyz", None, Some(10)).await?;
    assert!(empty_search.is_empty());

    let empty_name_search = service.find_entities_by_name("nonexistent_name_xyz", Some(10)).await?;
    assert!(empty_name_search.is_empty());

    // Test path finding between nonexistent entities
    let no_path = service.find_shortest_path(nonexistent_id, Ulid::new(), Some(5)).await?;
    assert!(no_path.is_none());

    Ok(())
}

#[sinex_test]
async fn test_entity_cascade_deletion(ctx: TestContext) -> Result<()> {
    let service = KnowledgeGraphService::new(ctx.pool().clone());

    // Create two entities
    let entity1 = service.create_entity(CreateEntityInput {
        entity_type: "test".to_string(),
        name: "Entity 1".to_string(),
        description: None,
        properties: None,
        confidence_score: None,
        source_event_ids: None,
    }).await?;

    let entity2 = service.create_entity(CreateEntityInput {
        entity_type: "test".to_string(),
        name: "Entity 2".to_string(),
        description: None,
        properties: None,
        confidence_score: None,
        source_event_ids: None,
    }).await?;

    // Create relations between them
    let relation1 = service.create_relation(CreateRelationInput {
        from_entity_id: entity1.entity_id,
        to_entity_id: entity2.entity_id,
        relation_type: "connected_to".to_string(),
        properties: None,
        confidence_score: None,
        source_event_ids: None,
    }).await?;

    let relation2 = service.create_relation(CreateRelationInput {
        from_entity_id: entity2.entity_id,
        to_entity_id: entity1.entity_id,
        relation_type: "connected_back".to_string(),
        properties: None,
        confidence_score: None,
        source_event_ids: None,
    }).await?;

    // Verify relations exist
    let entity1_relations = service.get_entity_relations(entity1.entity_id).await?;
    assert_eq!(entity1_relations.len(), 2);

    // Delete entity1 - should cascade delete its relations
    let deleted = service.delete_entity(entity1.entity_id).await?;
    assert!(deleted);

    // Verify entity1 is deleted
    let entity1_result = service.get_entity(entity1.entity_id).await?;
    assert!(entity1_result.is_none());

    // Verify relations involving entity1 are deleted
    let relation1_result = service.get_relation(relation1.relation_id).await?;
    assert!(relation1_result.is_none());

    let relation2_result = service.get_relation(relation2.relation_id).await?;
    assert!(relation2_result.is_none());

    // Verify entity2 still exists
    let entity2_result = service.get_entity(entity2.entity_id).await?;
    assert!(entity2_result.is_some());

    // Verify entity2 has no relations left
    let entity2_relations = service.get_entity_relations(entity2.entity_id).await?;
    assert!(entity2_relations.is_empty());

    Ok(())
}