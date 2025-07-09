use crate::common::prelude::*;
use sinex_db::artifacts_correct::*;
use sinex_db::models::{CreateArtifactInput};

#[allow(dead_code)]
type TestResult = anyhow::Result<()>;

#[sinex_test]
async fn test_create_artifact_basic(ctx: TestContext) -> TestResult {
    let input = CreateArtifactInput {
        artifact_type: "document".to_string(),
        title: "Test Document".to_string(),
        source_url: Some("https://example.com/doc.pdf".to_string()),
        original_path: Some("/path/to/doc.pdf".to_string()),
        mime_type: Some("application/pdf".to_string()),
        size_bytes: Some(1024),
        checksum: Some("abc123".to_string()),
        metadata: Some(json!({"author": "Test Author"})),
        created_from_event_id: None,
        blob_id: None,
    };

    let artifact = create_artifact(ctx.pool(), input).await?;

    assert_eq!(artifact.artifact_type, "document");
    assert_eq!(artifact.title, "Test Document");
    assert_eq!(artifact.source_url, Some("https://example.com/doc.pdf".to_string()));
    assert_eq!(artifact.metadata["author"], "Test Author");
    assert_eq!(artifact.size_bytes, Some(1024));

    Ok(())
}

#[sinex_test]
async fn test_create_artifact_minimal(ctx: TestContext) -> TestResult {
    let input = CreateArtifactInput {
        artifact_type: "note".to_string(),
        title: "Simple Note".to_string(),
        source_url: None,
        original_path: None,
        mime_type: None,
        size_bytes: None,
        checksum: None,
        metadata: None,
        created_from_event_id: None,
        blob_id: None,
    };

    let artifact = create_artifact(ctx.pool(), input).await?;

    assert_eq!(artifact.artifact_type, "note");
    assert_eq!(artifact.title, "Simple Note");
    assert!(artifact.source_url.is_none());
    assert_eq!(artifact.metadata, json!({}));

    Ok(())
}

#[sinex_test]
async fn test_get_artifact_by_id(ctx: TestContext) -> TestResult {
    // Create an artifact first
    let input = CreateArtifactInput {
        artifact_type: "media".to_string(),
        title: "Test Image".to_string(),
        source_url: None,
        original_path: None,
        mime_type: Some("image/png".to_string()),
        size_bytes: Some(2048),
        checksum: None,
        metadata: Some(json!({"width": 800, "height": 600})),
        created_from_event_id: None,
        blob_id: None,
    };

    let created_artifact = create_artifact(ctx.pool(), input).await?;

    // Retrieve it by ID
    let retrieved = get_artifact_by_id(ctx.pool(), created_artifact.artifact_id).await?;

    assert!(retrieved.is_some());
    let artifact = retrieved.unwrap();
    assert_eq!(artifact.artifact_id, created_artifact.artifact_id);
    assert_eq!(artifact.title, "Test Image");
    assert_eq!(artifact.metadata["width"], 800);

    Ok(())
}

#[sinex_test]
async fn test_get_artifact_by_id_not_found(ctx: TestContext) -> TestResult {
    let non_existent_id = Ulid::new();
    let result = get_artifact_by_id(ctx.pool(), non_existent_id).await?;
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_get_recent_artifacts(ctx: TestContext) -> TestResult {
    // Create multiple artifacts
    let inputs = vec![
        CreateArtifactInput {
            artifact_type: "document".to_string(),
            title: "Document 1".to_string(),
            source_url: None,
            original_path: None,
            mime_type: None,
            size_bytes: None,
            checksum: None,
            metadata: None,
            created_from_event_id: None,
            blob_id: None,
        },
        CreateArtifactInput {
            artifact_type: "document".to_string(),
            title: "Document 2".to_string(),
            source_url: None,
            original_path: None,
            mime_type: None,
            size_bytes: None,
            checksum: None,
            metadata: None,
            created_from_event_id: None,
            blob_id: None,
        },
        CreateArtifactInput {
            artifact_type: "document".to_string(),
            title: "Document 3".to_string(),
            source_url: None,
            original_path: None,
            mime_type: None,
            size_bytes: None,
            checksum: None,
            metadata: None,
            created_from_event_id: None,
            blob_id: None,
        },
    ];

    for input in inputs {
        create_artifact(ctx.pool(), input).await?;
    }

    // Get recent artifacts
    let recent = get_recent_artifacts(ctx.pool(), 2).await?;
    assert_eq!(recent.len(), 2);
    
    // Should be ordered by creation time DESC, so most recent first
    assert_eq!(recent[0].title, "Document 3");
    assert_eq!(recent[1].title, "Document 2");

    Ok(())
}

#[sinex_test]
async fn test_artifact_with_event_reference(ctx: TestContext) -> TestResult {
    // Create a test event first
    let test_event = EventFactory::new("test")
        .create_event("document.processed", json!({"source": "test"}));
    
    let event_id = assert_event_inserted_with_context(ctx.pool(), &test_event, "test_artifact_with_event_reference").await?;
    
    let input = CreateArtifactInput {
        artifact_type: "document".to_string(),
        title: "Event-generated Document".to_string(),
        source_url: None,
        original_path: None,
        mime_type: None,
        size_bytes: None,
        checksum: None,
        metadata: Some(json!({"processing_status": "completed"})),
        created_from_event_id: Some(event_id),
        blob_id: None,
    };

    let artifact = create_artifact(ctx.pool(), input).await?;

    assert_eq!(artifact.created_from_event_id, Some(event_id));
    assert_eq!(artifact.metadata["processing_status"], "completed");

    Ok(())
}

#[sinex_test]
async fn test_artifact_large_metadata(ctx: TestContext) -> TestResult {
    let large_metadata = json!({
        "tags": ["tag1", "tag2", "tag3", "tag4", "tag5"],
        "properties": {
            "complex": {
                "nested": {
                    "data": "with many levels",
                    "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                }
            }
        },
        "description": "A very long description that contains lots of text to test how the system handles larger metadata objects in the database",
        "processing_history": [
            {"step": 1, "action": "parse", "timestamp": "2024-01-01T00:00:00Z"},
            {"step": 2, "action": "validate", "timestamp": "2024-01-01T00:01:00Z"},
            {"step": 3, "action": "store", "timestamp": "2024-01-01T00:02:00Z"}
        ]
    });

    let input = CreateArtifactInput {
        artifact_type: "document".to_string(),
        title: "Complex Document with Large Metadata".to_string(),
        source_url: None,
        original_path: None,
        mime_type: None,
        size_bytes: None,
        checksum: None,
        metadata: Some(large_metadata.clone()),
        created_from_event_id: None,
        blob_id: None,
    };

    let artifact = create_artifact(ctx.pool(), input).await?;

    assert_eq!(artifact.metadata, large_metadata);
    assert_eq!(artifact.metadata["tags"].as_array().unwrap().len(), 5);
    assert_eq!(artifact.metadata["processing_history"].as_array().unwrap().len(), 3);

    Ok(())
}