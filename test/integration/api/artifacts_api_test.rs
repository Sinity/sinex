use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use sinex_core::test_macros::sinex_test;
use sinex_core::TestContext;
use sinex_db::prelude::*;
use sinex_ulid::Ulid;

#[sinex_test]
async fn test_artifacts_crud_operations(ctx: TestContext) -> Result<()> {
    let service = ArtifactsService::new(ctx.pool().clone());

    // Test artifact creation
    let input = CreateArtifactInput {
        artifact_type: "pkm_note".to_string(),
        canonical_identifier: "test/note/example".to_string(),
        title: Some("Example Note".to_string()),
        tags: Some(vec!["test".to_string(), "example".to_string()]),
        properties: Some(json!({"priority": "high", "status": "draft"})),
        created_at_ts_orig: Some(Utc::now()),
    };

    let artifact = service.create_artifact(input).await?;
    assert_eq!(artifact.artifact_type, "pkm_note");
    assert_eq!(artifact.canonical_identifier, "test/note/example");
    assert_eq!(artifact.current_title, Some("Example Note".to_string()));
    assert_eq!(artifact.tags_denormalized, Some(vec!["test".to_string(), "example".to_string()]));

    // Test get artifact by ID
    let retrieved = service.get_artifact(artifact.artifact_id).await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.artifact_id, artifact.artifact_id);
    assert_eq!(retrieved.artifact_type, "pkm_note");

    // Test get artifact by identifier
    let by_identifier = service.get_artifact_by_identifier("test/note/example").await?;
    assert!(by_identifier.is_some());
    assert_eq!(by_identifier.unwrap().artifact_id, artifact.artifact_id);

    // Test update artifact
    let updated = service.update_artifact(
        artifact.artifact_id,
        Some("Updated Example Note".to_string()),
        Some(vec!["updated".to_string(), "test".to_string()]),
        Some(json!({"priority": "medium", "status": "published"})),
    ).await?;
    assert_eq!(updated.current_title, Some("Updated Example Note".to_string()));
    assert_eq!(updated.tags_denormalized, Some(vec!["updated".to_string(), "test".to_string()]));

    // Test search artifacts
    let search_results = service.search_artifacts(
        Some("pkm_note"),
        Some("Updated"),
        None,
        Some(10),
        Some(0),
    ).await?;
    assert_eq!(search_results.len(), 1);
    assert_eq!(search_results[0].artifact_id, artifact.artifact_id);

    // Test delete artifact
    let deleted = service.delete_artifact(artifact.artifact_id).await?;
    assert!(deleted);

    // Verify deletion
    let not_found = service.get_artifact(artifact.artifact_id).await?;
    assert!(not_found.is_none());

    Ok(())
}

#[sinex_test]
async fn test_artifact_content_operations(ctx: TestContext) -> Result<()> {
    let service = ArtifactsService::new(ctx.pool().clone());

    // Create an artifact first
    let artifact_input = CreateArtifactInput {
        artifact_type: "pkm_note".to_string(),
        canonical_identifier: "test/content/note".to_string(),
        title: Some("Content Test Note".to_string()),
        tags: None,
        properties: None,
        created_at_ts_orig: Some(Utc::now()),
    };
    let artifact = service.create_artifact(artifact_input).await?;

    // Test content creation
    let content_input = CreateArtifactContentInput {
        artifact_id: artifact.artifact_id,
        version_identifier: "v1.0".to_string(),
        content_text: Some("# Test Content\n\nThis is a test note content.".to_string()),
        content_blob_id: None,
        content_format: "text/markdown".to_string(),
        captured_at_ts_orig: Utc::now(),
        capture_method: Some("manual_entry".to_string()),
        metadata: Some(json!({"author": "test_user", "language": "en"})),
    };

    let content = service.create_content(content_input).await?;
    assert_eq!(content.artifact_id, artifact.artifact_id);
    assert_eq!(content.version_identifier, "v1.0");
    assert_eq!(content.content_format, "text/markdown");
    assert!(content.word_count.is_some());
    assert!(content.char_count.is_some());

    // Test get content by ID
    let retrieved_content = service.get_content(content.content_id).await?;
    assert!(retrieved_content.is_some());
    let retrieved_content = retrieved_content.unwrap();
    assert_eq!(retrieved_content.content_id, content.content_id);

    // Test get current content
    let current_content = service.get_current_content(artifact.artifact_id).await?;
    assert!(current_content.is_some());
    assert_eq!(current_content.unwrap().content_id, content.content_id);

    // Create another version
    let content_v2_input = CreateArtifactContentInput {
        artifact_id: artifact.artifact_id,
        version_identifier: "v2.0".to_string(),
        content_text: Some("# Test Content Updated\n\nThis is updated test note content with more details.".to_string()),
        content_blob_id: None,
        content_format: "text/markdown".to_string(),
        captured_at_ts_orig: Utc::now(),
        capture_method: Some("manual_update".to_string()),
        metadata: Some(json!({"author": "test_user", "language": "en", "version": "2.0"})),
    };

    let content_v2 = service.create_content(content_v2_input).await?;

    // Test get all versions
    let versions = service.get_artifact_content_versions(artifact.artifact_id).await?;
    assert_eq!(versions.len(), 2);
    // Should be ordered by captured_at_ts_orig DESC
    assert_eq!(versions[0].version_identifier, "v2.0");
    assert_eq!(versions[1].version_identifier, "v1.0");

    // Test content search
    let search_results = service.search_content("updated", Some("text/markdown"), Some(10)).await?;
    assert!(!search_results.is_empty());
    assert!(search_results.iter().any(|c| c.content_id == content_v2.content_id));

    Ok(())
}

#[sinex_test]
async fn test_artifacts_with_tags_search(ctx: TestContext) -> Result<()> {
    let service = ArtifactsService::new(ctx.pool().clone());

    // Create multiple artifacts with different tags
    let artifacts_data = vec![
        ("note1", vec!["work", "project-a"]),
        ("note2", vec!["work", "project-b"]),
        ("note3", vec!["personal", "ideas"]),
        ("note4", vec!["work", "project-a", "urgent"]),
    ];

    let mut created_artifacts = Vec::new();
    for (name, tags) in artifacts_data {
        let input = CreateArtifactInput {
            artifact_type: "pkm_note".to_string(),
            canonical_identifier: format!("test/{}", name),
            title: Some(format!("Test {}", name)),
            tags: Some(tags),
            properties: None,
            created_at_ts_orig: Some(Utc::now()),
        };
        let artifact = service.create_artifact(input).await?;
        created_artifacts.push(artifact);
    }

    // Test search by tags - unfortunately the current implementation doesn't support tag filtering
    // but we can test the structure is there
    let all_results = service.search_artifacts(
        Some("pkm_note"),
        None,
        None,
        Some(10),
        Some(0),
    ).await?;
    assert_eq!(all_results.len(), 4);

    // Test search by text in title
    let work_results = service.search_artifacts(
        Some("pkm_note"),
        Some("note1"),
        None,
        Some(10),
        Some(0),
    ).await?;
    assert_eq!(work_results.len(), 1);
    assert_eq!(work_results[0].canonical_identifier, "test/note1");

    Ok(())
}

#[sinex_test]
async fn test_artifact_content_hash_deduplication(ctx: TestContext) -> Result<()> {
    let service = ArtifactsService::new(ctx.pool().clone());

    // Create an artifact
    let artifact_input = CreateArtifactInput {
        artifact_type: "document".to_string(),
        canonical_identifier: "test/dedup/doc".to_string(),
        title: Some("Deduplication Test".to_string()),
        tags: None,
        properties: None,
        created_at_ts_orig: Some(Utc::now()),
    };
    let artifact = service.create_artifact(artifact_input).await?;

    let content_text = "This is identical content for deduplication testing.";

    // Create first content version
    let content_input1 = CreateArtifactContentInput {
        artifact_id: artifact.artifact_id,
        version_identifier: "v1".to_string(),
        content_text: Some(content_text.to_string()),
        content_blob_id: None,
        content_format: "text/plain".to_string(),
        captured_at_ts_orig: Utc::now(),
        capture_method: Some("test".to_string()),
        metadata: None,
    };

    let content1 = service.create_content(content_input1).await?;

    // Try to create second content version with identical content
    // This should fail due to unique constraint on (artifact_id, content_hash_blake3, content_format)
    let content_input2 = CreateArtifactContentInput {
        artifact_id: artifact.artifact_id,
        version_identifier: "v2".to_string(),
        content_text: Some(content_text.to_string()),
        content_blob_id: None,
        content_format: "text/plain".to_string(),
        captured_at_ts_orig: Utc::now(),
        capture_method: Some("test".to_string()),
        metadata: None,
    };

    let result = service.create_content(content_input2).await;
    // Should fail due to constraint violation
    assert!(result.is_err());

    // Verify we still have only one content version
    let versions = service.get_artifact_content_versions(artifact.artifact_id).await?;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].content_id, content1.content_id);

    Ok(())
}

#[sinex_test]
async fn test_nonexistent_artifact_operations(ctx: TestContext) -> Result<()> {
    let service = ArtifactsService::new(ctx.pool().clone());
    let nonexistent_id = Ulid::new();

    // Test get nonexistent artifact
    let result = service.get_artifact(nonexistent_id).await?;
    assert!(result.is_none());

    // Test get nonexistent identifier
    let result = service.get_artifact_by_identifier("nonexistent/identifier").await?;
    assert!(result.is_none());

    // Test update nonexistent artifact - should fail
    let result = service.update_artifact(
        nonexistent_id,
        Some("title".to_string()),
        None,
        None,
    ).await;
    assert!(result.is_err());

    // Test delete nonexistent artifact
    let deleted = service.delete_artifact(nonexistent_id).await?;
    assert!(!deleted);

    // Test get content versions for nonexistent artifact
    let versions = service.get_artifact_content_versions(nonexistent_id).await?;
    assert!(versions.is_empty());

    // Test get current content for nonexistent artifact
    let current = service.get_current_content(nonexistent_id).await?;
    assert!(current.is_none());

    Ok(())
}