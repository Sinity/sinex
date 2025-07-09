use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use sinex_core::test_macros::sinex_test;
use sinex_core::{TestContext, RawEventBuilder};
use sinex_db::prelude::*;
use sinex_ulid::Ulid;

#[sinex_test]
async fn test_annotations_crud_operations(ctx: TestContext) -> Result<()> {
    let service = AnnotationsService::new(ctx.pool().clone());

    // Create a test event first
    let event = RawEventBuilder::new("test.source", "test.event", json!({"data": "test"})).build();
    crate::common::insert_event_with_validator(ctx.pool(), &event).await?;

    // Test annotation creation
    let input = CreateAnnotationInput {
        event_id: event.id,
        annotation_type: "interpretation".to_string(),
        content: "This is a test interpretation of the event".to_string(),
        metadata: Some(json!({"confidence": 0.85, "model": "test-model-v1"})),
        confidence_score: Some(0.85),
        created_by: "test_user".to_string(),
    };

    let annotation = service.create_annotation(input).await?;
    assert_eq!(annotation.event_id, event.id);
    assert_eq!(annotation.annotation_type, "interpretation");
    assert_eq!(annotation.content, "This is a test interpretation of the event");
    assert_eq!(annotation.confidence_score, Some(0.85));
    assert_eq!(annotation.created_by, "test_user");

    // Test get annotation by ID
    let retrieved = service.get_annotation(annotation.annotation_id).await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.annotation_id, annotation.annotation_id);
    assert_eq!(retrieved.event_id, event.id);

    // Test get event annotations
    let event_annotations = service.get_event_annotations(event.id).await?;
    assert_eq!(event_annotations.len(), 1);
    assert_eq!(event_annotations[0].annotation_id, annotation.annotation_id);

    // Test update annotation
    let updated = service.update_annotation(
        annotation.annotation_id,
        Some("Updated interpretation content".to_string()),
        Some(json!({"confidence": 0.92, "model": "test-model-v2", "updated": true})),
        Some(0.92),
    ).await?;
    assert_eq!(updated.content, "Updated interpretation content");
    assert_eq!(updated.confidence_score, Some(0.92));

    // Test delete annotation
    let deleted = service.delete_annotation(annotation.annotation_id).await?;
    assert!(deleted);

    // Verify deletion
    let not_found = service.get_annotation(annotation.annotation_id).await?;
    assert!(not_found.is_none());

    Ok(())
}

#[sinex_test]
async fn test_annotations_by_type_and_search(ctx: TestContext) -> Result<()> {
    let service = AnnotationsService::new(ctx.pool().clone());

    // Create test events
    let event1 = RawEventBuilder::new("test.source", "test.event1", json!({"data": "test1"})).build();
    let event2 = RawEventBuilder::new("test.source", "test.event2", json!({"data": "test2"})).build();
    crate::common::insert_event_with_validator(ctx.pool(), &event1).await?;
    crate::common::insert_event_with_validator(ctx.pool(), &event2).await?;

    // Create annotations of different types
    let annotations_data = vec![
        (event1.id, "interpretation", "This event represents a user action", "ai_agent", 0.9),
        (event1.id, "tag", "user-interaction", "human_reviewer", 1.0),
        (event2.id, "interpretation", "This event indicates system behavior", "ai_agent", 0.85),
        (event2.id, "summary", "Brief system event summary", "summarizer", 0.95),
        (event1.id, "note", "Manual note about this user action", "human_reviewer", 1.0),
    ];

    let mut created_annotations = Vec::new();
    for (event_id, ann_type, content, created_by, confidence) in annotations_data {
        let input = CreateAnnotationInput {
            event_id,
            annotation_type: ann_type.to_string(),
            content: content.to_string(),
            metadata: Some(json!({"source": "test"})),
            confidence_score: Some(confidence),
            created_by: created_by.to_string(),
        };
        let annotation = service.create_annotation(input).await?;
        created_annotations.push(annotation);
    }

    // Test get annotations by type
    let interpretation_annotations = service.get_annotations_by_type("interpretation", Some(10), Some(0)).await?;
    assert_eq!(interpretation_annotations.len(), 2);
    for ann in &interpretation_annotations {
        assert_eq!(ann.annotation_type, "interpretation");
    }

    let tag_annotations = service.get_annotations_by_type("tag", Some(10), Some(0)).await?;
    assert_eq!(tag_annotations.len(), 1);
    assert_eq!(tag_annotations[0].content, "user-interaction");

    // Test search annotations
    let user_search = service.search_annotations(
        "user",
        None,
        None,
        None,
        Some(10),
    ).await?;
    assert_eq!(user_search.len(), 2); // "user action" and "user-interaction"

    // Test search with annotation type filter
    let interpretation_user_search = service.search_annotations(
        "user",
        Some("interpretation"),
        None,
        None,
        Some(10),
    ).await?;
    assert_eq!(interpretation_user_search.len(), 1);
    assert_eq!(interpretation_user_search[0].content, "This event represents a user action");

    // Test search with creator filter
    let human_annotations = service.search_annotations(
        "",
        None,
        Some("human_reviewer"),
        None,
        Some(10),
    ).await?;
    assert_eq!(human_annotations.len(), 2); // tag and note

    // Test search with confidence filter
    let high_confidence = service.search_annotations(
        "",
        None,
        None,
        Some(0.9),
        Some(10),
    ).await?;
    assert_eq!(high_confidence.len(), 3); // confidence >= 0.9

    Ok(())
}

#[sinex_test]
async fn test_bulk_annotations_creation(ctx: TestContext) -> Result<()> {
    let service = AnnotationsService::new(ctx.pool().clone());

    // Create test events
    let events: Vec<_> = (0..5).map(|i| {
        RawEventBuilder::new("bulk.test", "bulk.event", json!({"index": i})).build()
    }).collect();

    for event in &events {
        crate::common::insert_event_with_validator(ctx.pool(), event).await?;
    }

    // Create bulk annotation inputs
    let bulk_inputs: Vec<_> = events.iter().enumerate().map(|(i, event)| {
        CreateAnnotationInput {
            event_id: event.id,
            annotation_type: "bulk_test".to_string(),
            content: format!("Bulk annotation {}", i),
            metadata: Some(json!({"bulk_index": i})),
            confidence_score: Some(0.8),
            created_by: "bulk_processor".to_string(),
        }
    }).collect();

    // Test bulk creation
    let created_annotations = service.bulk_create_annotations(bulk_inputs).await?;
    assert_eq!(created_annotations.len(), 5);

    // Verify all annotations were created
    for (i, annotation) in created_annotations.iter().enumerate() {
        assert_eq!(annotation.content, format!("Bulk annotation {}", i));
        assert_eq!(annotation.annotation_type, "bulk_test");
        assert_eq!(annotation.created_by, "bulk_processor");
    }

    // Test empty bulk creation
    let empty_result = service.bulk_create_annotations(vec![]).await?;
    assert!(empty_result.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_annotation_statistics(ctx: TestContext) -> Result<()> {
    let service = AnnotationsService::new(ctx.pool().clone());

    // Create test events and annotations
    let events: Vec<_> = (0..3).map(|i| {
        RawEventBuilder::new("stats.test", "stats.event", json!({"index": i})).build()
    }).collect();

    for event in &events {
        crate::common::insert_event_with_validator(ctx.pool(), event).await?;
    }

    // Create various annotations
    let annotations_data = vec![
        (events[0].id, "type_a", "content1", "user1", 0.9),
        (events[0].id, "type_b", "content2", "user2", 0.8),
        (events[1].id, "type_a", "content3", "user1", 0.95),
        (events[2].id, "type_c", "content4", "user3", 0.7),
        (events[2].id, "type_a", "content5", "user1", 0.85),
    ];

    for (event_id, ann_type, content, created_by, confidence) in annotations_data {
        let input = CreateAnnotationInput {
            event_id,
            annotation_type: ann_type.to_string(),
            content: content.to_string(),
            metadata: None,
            confidence_score: Some(confidence),
            created_by: created_by.to_string(),
        };
        service.create_annotation(input).await?;
    }

    // Test get annotation statistics
    let stats = service.get_annotation_stats().await?;
    assert_eq!(stats.total_annotations, 5);
    assert_eq!(stats.annotated_events, 3);
    assert_eq!(stats.unique_types, 3); // type_a, type_b, type_c
    assert_eq!(stats.unique_creators, 3); // user1, user2, user3
    assert!(stats.avg_confidence.is_some());
    let avg_confidence = stats.avg_confidence.unwrap();
    assert!(avg_confidence > 0.8 && avg_confidence < 0.9);

    // Test get type distribution
    let type_distribution = service.get_type_distribution().await?;
    assert_eq!(type_distribution.len(), 3);
    
    // Find type_a which should have 3 annotations
    let type_a_count = type_distribution.iter()
        .find(|tc| tc.annotation_type == "type_a")
        .unwrap();
    assert_eq!(type_a_count.count, 3);

    // Test get recent annotations
    let recent = service.get_recent_annotations(Some(3)).await?;
    assert_eq!(recent.len(), 3);
    // Should be ordered by created_at DESC
    assert!(recent[0].created_at >= recent[1].created_at);
    assert!(recent[1].created_at >= recent[2].created_at);

    Ok(())
}

#[sinex_test]
async fn test_annotation_edge_cases(ctx: TestContext) -> Result<()> {
    let service = AnnotationsService::new(ctx.pool().clone());
    let nonexistent_event_id = Ulid::new();
    let nonexistent_annotation_id = Ulid::new();

    // Test create annotation with nonexistent event - should fail
    let invalid_input = CreateAnnotationInput {
        event_id: nonexistent_event_id,
        annotation_type: "test".to_string(),
        content: "test content".to_string(),
        metadata: None,
        confidence_score: None,
        created_by: "test_user".to_string(),
    };

    let result = service.create_annotation(invalid_input).await;
    assert!(result.is_err()); // Should fail due to foreign key constraint

    // Test get nonexistent annotation
    let result = service.get_annotation(nonexistent_annotation_id).await?;
    assert!(result.is_none());

    // Test get annotations for nonexistent event
    let result = service.get_event_annotations(nonexistent_event_id).await?;
    assert!(result.is_empty());

    // Test update nonexistent annotation
    let result = service.update_annotation(
        nonexistent_annotation_id,
        Some("new content".to_string()),
        None,
        None,
    ).await;
    assert!(result.is_err());

    // Test delete nonexistent annotation
    let deleted = service.delete_annotation(nonexistent_annotation_id).await?;
    assert!(!deleted);

    // Test search with no results
    let empty_search = service.search_annotations(
        "nonexistent_content_xyz",
        None,
        None,
        None,
        Some(10),
    ).await?;
    assert!(empty_search.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_annotation_confidence_filtering(ctx: TestContext) -> Result<()> {
    let service = AnnotationsService::new(ctx.pool().clone());

    // Create test event
    let event = RawEventBuilder::new("confidence.test", "confidence.event", json!({})).build();
    crate::common::insert_event_with_validator(ctx.pool(), &event).await?;

    // Create annotations with different confidence scores
    let confidence_levels = vec![0.1, 0.5, 0.7, 0.85, 0.95, 1.0];
    for (i, confidence) in confidence_levels.iter().enumerate() {
        let input = CreateAnnotationInput {
            event_id: event.id,
            annotation_type: "confidence_test".to_string(),
            content: format!("Annotation with confidence {}", confidence),
            metadata: None,
            confidence_score: Some(*confidence),
            created_by: "confidence_tester".to_string(),
        };
        service.create_annotation(input).await?;
    }

    // Test filtering by different confidence thresholds
    let high_confidence = service.search_annotations(
        "",
        Some("confidence_test"),
        None,
        Some(0.8),
        Some(10),
    ).await?;
    assert_eq!(high_confidence.len(), 3); // 0.85, 0.95, 1.0

    let medium_confidence = service.search_annotations(
        "",
        Some("confidence_test"),
        None,
        Some(0.5),
        Some(10),
    ).await?;
    assert_eq!(medium_confidence.len(), 5); // 0.5, 0.7, 0.85, 0.95, 1.0

    let all_confidence = service.search_annotations(
        "",
        Some("confidence_test"),
        None,
        Some(0.0),
        Some(10),
    ).await?;
    assert_eq!(all_confidence.len(), 6); // All annotations

    Ok(())
}