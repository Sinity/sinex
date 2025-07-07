use crate::common::prelude::*;
use sinex_db::knowledge_graph_correct::*;
use sinex_db::models::{CreateEntityInput, CreateRelationInput};

#[allow(dead_code)]
type TestResult = anyhow::Result<()>;

#[sinex_test]
async fn test_create_entity_basic(ctx: TestContext) -> TestResult {
    let input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "John Doe".to_string(),
        canonical_name: Some("john.doe".to_string()),
        aliases: Some(vec!["Johnny".to_string(), "J. Doe".to_string()]),
        description: Some("A test person entity".to_string()),
        metadata: Some(json!({"department": "engineering", "role": "developer"})),
    };

    let entity = create_entity(ctx.pool(), input).await?;

    assert_eq!(entity.entity_type, "person");
    assert_eq!(entity.name, "John Doe");
    assert_eq!(entity.canonical_name, "john.doe");
    assert_eq!(entity.aliases, vec!["Johnny", "J. Doe"]);
    assert_eq!(entity.description, Some("A test person entity".to_string()));
    assert_eq!(entity.metadata["department"], "engineering");

    Ok(())
}

#[sinex_test]
async fn test_create_entity_minimal(ctx: TestContext) -> TestResult {
    let input = CreateEntityInput {
        entity_type: "file".to_string(),
        name: "document.txt".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    let entity = create_entity(ctx.pool(), input).await?;

    assert_eq!(entity.entity_type, "file");
    assert_eq!(entity.name, "document.txt");
    assert_eq!(entity.canonical_name, "document.txt"); // Should default to name
    assert_eq!(entity.aliases, Vec::<String>::new());
    assert!(entity.description.is_none());
    assert_eq!(entity.metadata, json!({}));

    Ok(())
}

#[sinex_test]
async fn test_get_entity_by_id(ctx: TestContext) -> TestResult {
    let input = CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Sinex System".to_string(),
        canonical_name: None,
        aliases: Some(vec!["sinex".to_string()]),
        description: Some("Event capture system".to_string()),
        metadata: Some(json!({"status": "active", "priority": "high"})),
    };

    let created_entity = create_entity(ctx.pool(), input).await?;

    // Retrieve by ID
    let retrieved = get_entity_by_id(ctx.pool(), created_entity.entity_id).await?;

    assert!(retrieved.is_some());
    let entity = retrieved.unwrap();
    assert_eq!(entity.entity_id, created_entity.entity_id);
    assert_eq!(entity.name, "Sinex System");
    assert_eq!(entity.metadata["status"], "active");

    Ok(())
}

#[sinex_test]
async fn test_get_entity_by_id_not_found(ctx: TestContext) -> TestResult {
    let non_existent_id = Ulid::new();
    let result = get_entity_by_id(ctx.pool(), non_existent_id).await?;
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_get_entities_by_type(ctx: TestContext) -> TestResult {
    // Create entities of different types
    let person_inputs = vec![
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Alice Smith".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Bob Johnson".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    ];

    let file_input = CreateEntityInput {
        entity_type: "file".to_string(),
        name: "config.json".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    for input in person_inputs {
        create_entity(ctx.pool(), input).await?;
    }
    create_entity(ctx.pool(), file_input).await?;

    // Get entities by type
    let people = get_entities_by_type(ctx.pool(), "person", 10).await?;
    assert_eq!(people.len(), 2);
    
    for person in &people {
        assert_eq!(person.entity_type, "person");
    }

    // Should be ordered by creation time DESC
    assert_eq!(people[0].name, "Bob Johnson");
    assert_eq!(people[1].name, "Alice Smith");

    let files = get_entities_by_type(ctx.pool(), "file", 10).await?;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].name, "config.json");

    Ok(())
}

#[sinex_test]
async fn test_search_entities(ctx: TestContext) -> TestResult {
    // Create test entities
    let inputs = vec![
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "John Smith".to_string(),
            canonical_name: Some("john.smith".to_string()),
            aliases: None,
            description: None,
            metadata: None,
        },
        CreateEntityInput {
            entity_type: "person".to_string(),
            name: "Jane Johnson".to_string(),
            canonical_name: Some("jane.johnson".to_string()),
            aliases: None,
            description: None,
            metadata: None,
        },
        CreateEntityInput {
            entity_type: "file".to_string(),
            name: "john_data.csv".to_string(),
            canonical_name: None,
            aliases: None,
            description: None,
            metadata: None,
        },
    ];

    for input in inputs {
        create_entity(ctx.pool(), input).await?;
    }

    // Search for "john"
    let results = search_entities(ctx.pool(), "john", 10).await?;
    assert_eq!(results.len(), 2);
    
    // Exact match should come first (john.smith canonical name)
    assert_eq!(results[0].canonical_name, "john.smith");
    
    // Partial matches follow
    let names: Vec<&str> = results.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"John Smith"));
    assert!(names.contains(&"john_data.csv"));

    // Search for "jane"
    let jane_results = search_entities(ctx.pool(), "jane", 10).await?;
    assert_eq!(jane_results.len(), 1);
    assert_eq!(jane_results[0].name, "Jane Johnson");

    Ok(())
}

#[sinex_test]
async fn test_create_relation(ctx: TestContext) -> TestResult {
    // Create two entities first
    let person_input = CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Developer Alice".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    let project_input = CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Web Application".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    };

    let person = create_entity(ctx.pool(), person_input).await?;
    let project = create_entity(ctx.pool(), project_input).await?;

    // Create relation
    let relation_input = CreateRelationInput {
        from_entity_id: person.entity_id,
        to_entity_id: project.entity_id,
        relation_type: "works_on".to_string(),
        strength: Some(0.95),
        metadata: Some(json!({"role": "lead_developer", "start_date": "2024-01-01"})),
        valid_from: None, // Will use current time
        valid_until: None,
        created_from_event_id: None,
    };

    let relation = create_relation(ctx.pool(), relation_input).await?;

    assert_eq!(relation.from_entity_id, person.entity_id);
    assert_eq!(relation.to_entity_id, project.entity_id);
    assert_eq!(relation.relation_type, "works_on");
    assert_eq!(relation.strength, Some(0.95));
    assert_eq!(relation.metadata["role"], "lead_developer");

    Ok(())
}

#[sinex_test]
async fn test_get_relation_by_id(ctx: TestContext) -> TestResult {
    // Create entities and relation
    let entity1 = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "file".to_string(),
        name: "source.rs".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let entity2 = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "file".to_string(),
        name: "target.rs".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let relation_input = CreateRelationInput {
        from_entity_id: entity1.entity_id,
        to_entity_id: entity2.entity_id,
        relation_type: "imports".to_string(),
        strength: Some(1.0),
        metadata: Some(json!({"import_type": "direct"})),
        valid_from: None,
        valid_until: None,
        created_from_event_id: None,
    };

    let created_relation = create_relation(ctx.pool(), relation_input).await?;

    // Retrieve by ID
    let retrieved = get_relation_by_id(ctx.pool(), created_relation.relation_id).await?;

    assert!(retrieved.is_some());
    let relation = retrieved.unwrap();
    assert_eq!(relation.relation_id, created_relation.relation_id);
    assert_eq!(relation.relation_type, "imports");
    assert_eq!(relation.metadata["import_type"], "direct");

    Ok(())
}

#[sinex_test]
async fn test_get_entity_relations(ctx: TestContext) -> TestResult {
    // Create a central entity and several connected entities
    let central = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Central Person".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let project1 = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Project 1".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let project2 = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Project 2".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let manager = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Manager".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    // Create relations (both outgoing and incoming)
    let relations = vec![
        CreateRelationInput {
            from_entity_id: central.entity_id,
            to_entity_id: project1.entity_id,
            relation_type: "works_on".to_string(),
            strength: Some(0.8),
            metadata: None,
            valid_from: None,
            valid_until: None,
            created_from_event_id: None,
        },
        CreateRelationInput {
            from_entity_id: central.entity_id,
            to_entity_id: project2.entity_id,
            relation_type: "leads".to_string(),
            strength: Some(0.9),
            metadata: None,
            valid_from: None,
            valid_until: None,
            created_from_event_id: None,
        },
        CreateRelationInput {
            from_entity_id: manager.entity_id,
            to_entity_id: central.entity_id,
            relation_type: "manages".to_string(),
            strength: Some(1.0),
            metadata: None,
            valid_from: None,
            valid_until: None,
            created_from_event_id: None,
        },
    ];

    for relation_input in relations {
        create_relation(ctx.pool(), relation_input).await?;
    }

    // Get all relations for central entity
    let entity_relations = get_entity_relations(ctx.pool(), central.entity_id).await?;
    assert_eq!(entity_relations.len(), 3);

    // Check relation types
    let relation_types: Vec<&str> = entity_relations.iter()
        .map(|r| r.relation_type.as_str())
        .collect();
    assert!(relation_types.contains(&"works_on"));
    assert!(relation_types.contains(&"leads"));
    assert!(relation_types.contains(&"manages"));

    Ok(())
}

#[sinex_test]
async fn test_entity_relation_with_event_reference(ctx: TestContext) -> TestResult {
    let entity1 = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "document".to_string(),
        name: "Design Doc".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let entity2 = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "document".to_string(),
        name: "Implementation Doc".to_string(),
        canonical_name: None,
        aliases: None,
        description: None,
        metadata: None,
    }).await?;

    let event_id = Ulid::new();

    let relation_input = CreateRelationInput {
        from_entity_id: entity1.entity_id,
        to_entity_id: entity2.entity_id,
        relation_type: "references".to_string(),
        strength: Some(0.75),
        metadata: Some(json!({"detected_by": "ai_system", "context": "cross_reference"})),
        valid_from: None,
        valid_until: None,
        created_from_event_id: Some(event_id),
    };

    let relation = create_relation(ctx.pool(), relation_input).await?;

    assert_eq!(relation.created_from_event_id, Some(event_id));
    assert_eq!(relation.metadata["detected_by"], "ai_system");

    Ok(())
}

#[sinex_test]
async fn test_complex_knowledge_graph_scenario(ctx: TestContext) -> TestResult {
    // Create a mini knowledge graph representing a software project
    
    // Entities
    let developer = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "person".to_string(),
        name: "Sarah Developer".to_string(),
        canonical_name: Some("sarah.dev".to_string()),
        aliases: Some(vec!["Sarah".to_string(), "sdev".to_string()]),
        description: Some("Senior software developer".to_string()),
        metadata: Some(json!({"skills": ["rust", "python", "sql"], "experience_years": 8})),
    }).await?;

    let project = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "project".to_string(),
        name: "Data Pipeline".to_string(),
        canonical_name: Some("data-pipeline".to_string()),
        aliases: None,
        description: Some("Real-time data processing pipeline".to_string()),
        metadata: Some(json!({"status": "active", "budget": 50000, "tech_stack": ["rust", "postgresql"]})),
    }).await?;

    let module = create_entity(ctx.pool(), CreateEntityInput {
        entity_type: "code_module".to_string(),
        name: "Event Processor".to_string(),
        canonical_name: Some("event_processor".to_string()),
        aliases: None,
        description: Some("Core event processing module".to_string()),
        metadata: Some(json!({"lines_of_code": 2500, "test_coverage": 0.85})),
    }).await?;

    // Relations
    let _dev_project_relation = create_relation(ctx.pool(), CreateRelationInput {
        from_entity_id: developer.entity_id,
        to_entity_id: project.entity_id,
        relation_type: "develops".to_string(),
        strength: Some(1.0),
        metadata: Some(json!({"role": "lead", "allocation": 0.8})),
        valid_from: None,
        valid_until: None,
        created_from_event_id: None,
    }).await?;

    let _project_module_relation = create_relation(ctx.pool(), CreateRelationInput {
        from_entity_id: project.entity_id,
        to_entity_id: module.entity_id,
        relation_type: "contains".to_string(),
        strength: Some(0.9),
        metadata: Some(json!({"importance": "critical", "complexity": "high"})),
        valid_from: None,
        valid_until: None,
        created_from_event_id: None,
    }).await?;

    let _dev_module_relation = create_relation(ctx.pool(), CreateRelationInput {
        from_entity_id: developer.entity_id,
        to_entity_id: module.entity_id,
        relation_type: "authored".to_string(),
        strength: Some(0.95),
        metadata: Some(json!({"authorship_percentage": 0.9, "maintenance_responsibility": true})),
        valid_from: None,
        valid_until: None,
        created_from_event_id: None,
    }).await?;

    // Verify the graph structure
    let developer_relations = get_entity_relations(ctx.pool(), developer.entity_id).await?;
    assert_eq!(developer_relations.len(), 2);

    let project_relations = get_entity_relations(ctx.pool(), project.entity_id).await?;
    assert_eq!(project_relations.len(), 2);

    let module_relations = get_entity_relations(ctx.pool(), module.entity_id).await?;
    assert_eq!(module_relations.len(), 2);

    // Test entity search
    let sarah_results = search_entities(ctx.pool(), "Sarah", 10).await?;
    assert_eq!(sarah_results.len(), 1);
    assert_eq!(sarah_results[0].entity_id, developer.entity_id);

    let pipeline_results = search_entities(ctx.pool(), "Pipeline", 10).await?;
    assert_eq!(pipeline_results.len(), 1);
    assert_eq!(pipeline_results[0].entity_id, project.entity_id);

    // Test entity type filtering
    let people = get_entities_by_type(ctx.pool(), "person", 10).await?;
    assert_eq!(people.len(), 1);
    assert_eq!(people[0].entity_id, developer.entity_id);

    let projects = get_entities_by_type(ctx.pool(), "project", 10).await?;
    assert_eq!(projects.len(), 1);
    assert_eq!(projects[0].entity_id, project.entity_id);

    Ok(())
}