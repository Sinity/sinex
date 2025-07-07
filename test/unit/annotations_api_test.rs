use crate::common::prelude::*;
use sinex_db::annotations_correct::*;
use sinex_db::models::CreateAnnotationInput;

#[allow(dead_code)]
type TestResult = anyhow::Result<()>;

// Helper function to create and insert a test event
async fn create_and_insert_test_event(pool: &DbPool, source: &str, event_type: &str) -> anyhow::Result<RawEvent> {
    let event = create_test_event(source, event_type).await;
    // Insert the event and return the inserted event (which has the actual DB ID)
    let inserted_event = insert_raw_event(
        pool,
        &event.source,
        &event.event_type,
        &event.host,
        event.payload.clone(),
        event.ts_orig,
        event.ingestor_version.as_deref(),
        event.payload_schema_id,
    ).await?;
    Ok(inserted_event)
}

#[sinex_test]
async fn test_create_annotation_basic(ctx: TestContext) -> TestResult {
    // Create a real event first to satisfy foreign key constraint
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    let input = CreateAnnotationInput {
        event_id,
        annotation_type: "classification".to_string(),
        content: "This event represents a file creation operation".to_string(),
        metadata: Some(json!({"confidence": 0.95, "model": "gpt-4"})),
        created_by: "test_user".to_string(),
    };

    let annotation = create_annotation(ctx.pool(), input).await?;

    assert_eq!(annotation.event_id, event_id);
    assert_eq!(annotation.annotation_type, "classification");
    assert_eq!(annotation.content, "This event represents a file creation operation");
    assert_eq!(annotation.created_by, "test_user");
    assert_eq!(annotation.metadata["confidence"], 0.95);

    Ok(())
}

#[sinex_test]
async fn test_create_annotation_minimal(ctx: TestContext) -> TestResult {
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    let input = CreateAnnotationInput {
        event_id,
        annotation_type: "note".to_string(),
        content: "Simple note".to_string(),
        metadata: None,
        created_by: "system".to_string(),
    };

    let annotation = create_annotation(ctx.pool(), input).await?;

    assert_eq!(annotation.event_id, event_id);
    assert_eq!(annotation.annotation_type, "note");
    assert_eq!(annotation.content, "Simple note");
    assert_eq!(annotation.created_by, "system");
    assert_eq!(annotation.metadata, json!({}));

    Ok(())
}

#[sinex_test]
async fn test_get_annotation_by_id(ctx: TestContext) -> TestResult {
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    let input = CreateAnnotationInput {
        event_id,
        annotation_type: "tag".to_string(),
        content: "important".to_string(),
        metadata: Some(json!({"priority": "high"})),
        created_by: "user123".to_string(),
    };

    let created_annotation = create_annotation(ctx.pool(), input).await?;

    // Retrieve it by ID
    let retrieved = get_annotation_by_id(ctx.pool(), created_annotation.annotation_id).await?;

    assert!(retrieved.is_some());
    let annotation = retrieved.unwrap();
    assert_eq!(annotation.annotation_id, created_annotation.annotation_id);
    assert_eq!(annotation.content, "important");
    assert_eq!(annotation.metadata["priority"], "high");

    Ok(())
}

#[sinex_test]
async fn test_get_annotation_by_id_not_found(ctx: TestContext) -> TestResult {
    let non_existent_id = Ulid::new();
    let result = get_annotation_by_id(ctx.pool(), non_existent_id).await?;
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_get_annotations_for_event(ctx: TestContext) -> TestResult {
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    // Create multiple annotations for the same event
    let inputs = vec![
        CreateAnnotationInput {
            event_id,
            annotation_type: "classification".to_string(),
            content: "File operation".to_string(),
            metadata: None,
            created_by: "classifier".to_string(),
        },
        CreateAnnotationInput {
            event_id,
            annotation_type: "sentiment".to_string(),
            content: "neutral".to_string(),
            metadata: Some(json!({"score": 0.5})),
            created_by: "sentiment_analyzer".to_string(),
        },
        CreateAnnotationInput {
            event_id,
            annotation_type: "note".to_string(),
            content: "Manually verified".to_string(),
            metadata: None,
            created_by: "human_reviewer".to_string(),
        },
    ];

    for input in inputs {
        create_annotation(ctx.pool(), input).await?;
    }

    // Create annotation for different event to ensure filtering works
    let other_event = create_and_insert_test_event(ctx.pool(), "other_source", "other_event").await?;
    let other_event_id = other_event.id;
    let other_input = CreateAnnotationInput {
        event_id: other_event_id,
        annotation_type: "other".to_string(),
        content: "Should not appear".to_string(),
        metadata: None,
        created_by: "other_user".to_string(),
    };
    create_annotation(ctx.pool(), other_input).await?;

    // Get annotations for our event
    let annotations = get_annotations_for_event(ctx.pool(), event_id).await?;
    
    assert_eq!(annotations.len(), 3);
    
    // Should be ordered by creation time DESC, so most recent first
    assert_eq!(annotations[0].content, "Manually verified");
    assert_eq!(annotations[1].content, "neutral");
    assert_eq!(annotations[2].content, "File operation");

    // Verify all belong to the correct event
    for annotation in &annotations {
        assert_eq!(annotation.event_id, event_id);
    }

    Ok(())
}

#[sinex_test]
async fn test_update_annotation_content(ctx: TestContext) -> TestResult {
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    let input = CreateAnnotationInput {
        event_id,
        annotation_type: "summary".to_string(),
        content: "Initial summary".to_string(),
        metadata: Some(json!({"version": 1})),
        created_by: "summarizer".to_string(),
    };

    let created_annotation = create_annotation(ctx.pool(), input).await?;
    let original_updated_at = created_annotation.updated_at;

    // Wait a moment to ensure updated_at changes
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Update the content
    let updated_annotation = update_annotation_content(
        ctx.pool(),
        created_annotation.annotation_id,
        "Updated summary with more details"
    ).await?;

    assert_eq!(updated_annotation.annotation_id, created_annotation.annotation_id);
    assert_eq!(updated_annotation.content, "Updated summary with more details");
    assert_eq!(updated_annotation.annotation_type, "summary");
    assert_eq!(updated_annotation.created_by, "summarizer");
    assert!(updated_annotation.updated_at > original_updated_at);

    Ok(())
}

#[sinex_test]
async fn test_delete_annotation(ctx: TestContext) -> TestResult {
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    let input = CreateAnnotationInput {
        event_id,
        annotation_type: "temp".to_string(),
        content: "Temporary annotation".to_string(),
        metadata: None,
        created_by: "temp_user".to_string(),
    };

    let annotation = create_annotation(ctx.pool(), input).await?;

    // Delete the annotation
    let deleted = delete_annotation(ctx.pool(), annotation.annotation_id).await?;
    assert!(deleted);

    // Verify it's gone
    let retrieved = get_annotation_by_id(ctx.pool(), annotation.annotation_id).await?;
    assert!(retrieved.is_none());

    // Try to delete non-existent annotation
    let not_deleted = delete_annotation(ctx.pool(), Ulid::new()).await?;
    assert!(!not_deleted);

    Ok(())
}

#[sinex_test]
async fn test_get_recent_annotations(ctx: TestContext) -> TestResult {
    let event1 = create_and_insert_test_event(ctx.pool(), "test_source1", "test_event1").await?;
    let event2 = create_and_insert_test_event(ctx.pool(), "test_source2", "test_event2").await?;
    let event_id1 = event1.id;
    let event_id2 = event2.id;
    
    // Create multiple annotations across different events
    let inputs = vec![
        CreateAnnotationInput {
            event_id: event_id1,
            annotation_type: "first".to_string(),
            content: "First annotation".to_string(),
            metadata: None,
            created_by: "user1".to_string(),
        },
        CreateAnnotationInput {
            event_id: event_id2,
            annotation_type: "second".to_string(),
            content: "Second annotation".to_string(),
            metadata: None,
            created_by: "user2".to_string(),
        },
        CreateAnnotationInput {
            event_id: event_id1,
            annotation_type: "third".to_string(),
            content: "Third annotation".to_string(),
            metadata: None,
            created_by: "user3".to_string(),
        },
    ];

    for input in inputs {
        create_annotation(ctx.pool(), input).await?;
    }

    // Get recent annotations with limit
    let recent = get_recent_annotations(ctx.pool(), 2).await?;
    assert_eq!(recent.len(), 2);
    
    // Should be ordered by creation time DESC
    assert_eq!(recent[0].content, "Third annotation");
    assert_eq!(recent[1].content, "Second annotation");

    Ok(())
}

#[sinex_test]
async fn test_annotation_complex_metadata(ctx: TestContext) -> TestResult {
    let event = create_and_insert_test_event(ctx.pool(), "test_source", "test_event").await?;
    let event_id = event.id;
    
    let complex_metadata = json!({
        "analysis": {
            "nlp": {
                "entities": ["file", "system", "operation"],
                "keywords": ["create", "important", "sensitive"],
                "language": "en",
                "confidence": 0.87
            },
            "classification": {
                "category": "filesystem",
                "subcategory": "file_creation",
                "risk_level": "low"
            }
        },
        "processing": {
            "timestamp": "2024-01-01T00:00:00Z",
            "model_version": "v2.1.0",
            "processing_time_ms": 342
        }
    });

    let input = CreateAnnotationInput {
        event_id,
        annotation_type: "ai_analysis".to_string(),
        content: "Comprehensive AI analysis of the event".to_string(),
        metadata: Some(complex_metadata.clone()),
        created_by: "ai_system".to_string(),
    };

    let annotation = create_annotation(ctx.pool(), input).await?;

    assert_eq!(annotation.metadata, complex_metadata);
    assert_eq!(annotation.metadata["analysis"]["nlp"]["confidence"], 0.87);
    assert_eq!(annotation.metadata["processing"]["model_version"], "v2.1.0");

    Ok(())
}