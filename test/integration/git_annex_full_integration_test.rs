//! Integration tests for git-annex integration with the full Sinex system
//!
//! These tests validate that git-annex works correctly with the complete
//! event capture and storage pipeline, including large file handling,
//! repository management, and fallback scenarios.

use crate::common::prelude::*;
use sinex_db::queries::{
    add_to_work_queue, claim_work_queue_items, complete_work_queue_item, fail_work_queue_item,
    insert_event,
};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::fs;

/// Git-annex integration test helper
struct GitAnnexTestRepo {
    pub path: PathBuf,
    pub _temp_dir: TempDir,
    pub available: bool,
}

impl GitAnnexTestRepo {
    pub async fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();

        // Initialize git repository
        let git_init = Command::new("git")
            .args(["init"])
            .current_dir(&path)
            .output()?;

        if !git_init.status.success() {
            return Ok(Self {
                path,
                _temp_dir: temp_dir,
                available: false,
            });
        }

        // Configure git user for testing
        let _ = Command::new("git")
            .args(["config", "user.name", "Sinex Test"])
            .current_dir(&path)
            .output();

        let _ = Command::new("git")
            .args(["config", "user.email", "test@sinex.dev"])
            .current_dir(&path)
            .output();

        // Initialize git-annex
        let annex_init = Command::new("git")
            .args(["annex", "init", "sinex-test-repo"])
            .current_dir(&path)
            .output()?;

        let available = annex_init.status.success();

        if available {
            // Set up annex configuration for testing
            let _ = Command::new("git")
                .args(["config", "annex.largefiles", "largerthan=1KB"])
                .current_dir(&path)
                .output();
        }

        Ok(Self {
            path,
            _temp_dir: temp_dir,
            available,
        })
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub async fn add_file(&self, relative_path: &str, content: &[u8]) -> Result<String> {
        if !self.available {
            return Err(anyhow::anyhow!("Git-annex not available"));
        }

        let file_path = self.path.join(relative_path);

        // Create parent directories if needed
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        // Write file content
        fs::write(&file_path, content).await?;

        // Add to git-annex
        let add_output = Command::new("git")
            .args(["annex", "add", relative_path])
            .current_dir(&self.path)
            .output()?;

        if !add_output.status.success() {
            return Err(anyhow::anyhow!(
                "Git-annex add failed: {}",
                String::from_utf8_lossy(&add_output.stderr)
            ));
        }

        // Commit the file
        let commit_output = Command::new("git")
            .args(["commit", "-m", &format!("Add {}", relative_path)])
            .current_dir(&self.path)
            .output()?;

        if !commit_output.status.success() {
            return Err(anyhow::anyhow!(
                "Git commit failed: {}",
                String::from_utf8_lossy(&commit_output.stderr)
            ));
        }

        // Get the annex key
        let key_output = Command::new("git")
            .args(["annex", "lookupkey", relative_path])
            .current_dir(&self.path)
            .output()?;

        if key_output.status.success() {
            Ok(String::from_utf8_lossy(&key_output.stdout)
                .trim()
                .to_string())
        } else {
            Err(anyhow::anyhow!("Failed to get annex key"))
        }
    }

    pub async fn get_file_content(&self, relative_path: &str) -> Result<Vec<u8>> {
        if !self.available {
            return Err(anyhow::anyhow!("Git-annex not available"));
        }

        let file_path = self.path.join(relative_path);

        // Ensure file is available
        let get_output = Command::new("git")
            .args(["annex", "get", relative_path])
            .current_dir(&self.path)
            .output()?;

        if !get_output.status.success() {
            return Err(anyhow::anyhow!(
                "Git-annex get failed: {}",
                String::from_utf8_lossy(&get_output.stderr)
            ));
        }

        // Read file content
        let content = fs::read(&file_path).await?;
        Ok(content)
    }
}

#[sinex_test]
async fn test_git_annex_integration_with_event_pipeline(
    ctx: TestContext,
) -> Result<(), anyhow::Error> {
    let pool = ctx.pool();

    let annex_repo = GitAnnexTestRepo::new().await?;

    if !annex_repo.is_available() {
        println!("⚠️  Git-annex not available, skipping git-annex integration tests");
        return Ok(());
    }

    // Test 1: Large file event capture with git-annex storage
    test_large_file_event_capture(&pool, &annex_repo).await?;

    // Test 2: Event processing with git-annex blob references
    test_event_processing_with_annex_blobs(&pool, &annex_repo).await?;

    // Test 3: Worker system with git-annex file retrieval
    test_worker_system_annex_integration(&pool, &annex_repo).await?;

    // Test 4: Query interface with git-annex blob access
    test_query_interface_annex_integration(&pool, &annex_repo).await?;

    Ok(())
}

async fn test_large_file_event_capture(
    pool: &DbPool,
    annex_repo: &GitAnnexTestRepo,
) -> Result<(), anyhow::Error> {
    // Test capturing events with large files that should be stored in git-annex

    // Create large test files (> 1KB to trigger git-annex)
    let large_content = "x".repeat(2048); // 2KB file
    let medium_content = "y".repeat(512); // 512B file (should go to git-annex based on config)

    // Store files in git-annex
    let large_key = annex_repo
        .add_file("large_file.txt", large_content.as_bytes())
        .await?;
    let medium_key = annex_repo
        .add_file("medium_file.txt", medium_content.as_bytes())
        .await?;

    // Create events referencing git-annex stored files
    let large_file_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({
            "path": "/test/large_file.txt",
            "size": large_content.len(),
            "git_annex_key": large_key,
            "storage_type": "git_annex",
            "content_hash": "sha256:placeholder"
        }),
    )
    .build();

    let medium_file_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({
            "path": "/test/medium_file.txt",
            "size": medium_content.len(),
            "git_annex_key": medium_key,
            "storage_type": "git_annex",
            "content_hash": "sha256:placeholder"
        }),
    )
    .build();

    // Insert events into database
    let large_event = insert_event(&pool, &large_file_event).await?;
    let medium_event = insert_event(&pool, &medium_file_event).await?;

    // Verify events are stored correctly
    let retrieved_large = crate::common::get_event_by_id(&pool, large_event.id).await?;
    let retrieved_medium = crate::common::get_event_by_id(&pool, medium_event.id).await?;

    pretty_assertions::assert_eq!(retrieved_large.source, "filesystem");
    pretty_assertions::assert_eq!(retrieved_large.event_type, "file.created");
    assert!(retrieved_large.payload["git_annex_key"].as_str().is_some());
    pretty_assertions::assert_eq!(
        retrieved_large.payload["storage_type"].as_str().unwrap(),
        "git_annex"
    );

    pretty_assertions::assert_eq!(retrieved_medium.source, "filesystem");
    pretty_assertions::assert_eq!(retrieved_medium.event_type, "file.created");
    assert!(retrieved_medium.payload["git_annex_key"].as_str().is_some());

    // Verify we can retrieve the original file content from git-annex
    let retrieved_large_content = annex_repo.get_file_content("large_file.txt").await?;
    let retrieved_medium_content = annex_repo.get_file_content("medium_file.txt").await?;

    pretty_assertions::assert_eq!(retrieved_large_content, large_content.as_bytes());
    pretty_assertions::assert_eq!(retrieved_medium_content, medium_content.as_bytes());

    println!("✅ Large file event capture with git-annex integration successful");
    Ok(())
}

async fn test_event_processing_with_annex_blobs(
    pool: &DbPool,
    annex_repo: &GitAnnexTestRepo,
) -> Result<(), anyhow::Error> {
    // Test event processing that involves retrieving and processing git-annex stored content

    // Create test files with various content types
    let text_content = "This is a test document with important content for processing.";
    let binary_content = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]; // JPEG header bytes
    let json_content = r#"{"type": "document", "content": "structured data", "version": 1}"#;

    let text_key = annex_repo
        .add_file("document.txt", text_content.as_bytes())
        .await?;
    let binary_key = annex_repo.add_file("image.jpg", &binary_content).await?;
    let json_key = annex_repo
        .add_file("data.json", json_content.as_bytes())
        .await?;

    // Create events for processing
    let events = vec![
        RawEventBuilder::new(
            "document_processor",
            "document.analyze",
            json!({
                "document_path": "/docs/document.txt",
                "git_annex_key": text_key,
                "processing_type": "text_analysis",
                "priority": "high"
            }),
        )
        .build(),
        RawEventBuilder::new(
            "image_processor",
            "image.process",
            json!({
                "image_path": "/images/image.jpg",
                "git_annex_key": binary_key,
                "processing_type": "metadata_extraction",
                "priority": "medium"
            }),
        )
        .build(),
        RawEventBuilder::new(
            "data_processor",
            "data.validate",
            json!({
                "data_path": "/data/data.json",
                "git_annex_key": json_key,
                "processing_type": "schema_validation",
                "priority": "low"
            }),
        )
        .build(),
    ];

    let mut event_ids = Vec::new();
    for event in &events {
        let inserted_event = insert_event(&pool, event).await?;
        event_ids.push(inserted_event.id);

        // Add to promotion queue for processing
        add_to_work_queue(&pool, inserted_event.id, "annex-test-agent", 3).await?;
    }

    // Simulate worker processing events with git-annex content retrieval
    let mut processed_events = Vec::new();

    for (i, event_id) in event_ids.iter().enumerate() {
        // Claim work item
        let claimed_items =
            claim_work_queue_items(&pool, "annex-test-agent", &format!("annex-worker-{}", i), 1)
                .await?;

        assert!(
            !claimed_items.is_empty(),
            "Should claim work item for event {}",
            i
        );

        let queue_item = &claimed_items[0];

        // Retrieve event details
        let event = crate::common::get_event_by_id(&pool, *event_id).await?;

        // Extract git-annex key and retrieve content
        if let Some(_annex_key) = event.payload["git_annex_key"].as_str() {
            let file_name = match event.source.as_str() {
                "document_processor" => "document.txt",
                "image_processor" => "image.jpg",
                "data_processor" => "data.json",
                _ => continue,
            };

            // Simulate processing by retrieving content
            let content = annex_repo.get_file_content(file_name).await?;

            // Verify content matches expectations
            match event.source.as_str() {
                "document_processor" => {
                    pretty_assertions::assert_eq!(content, text_content.as_bytes());
                }
                "image_processor" => {
                    pretty_assertions::assert_eq!(content, binary_content);
                }
                "data_processor" => {
                    pretty_assertions::assert_eq!(content, json_content.as_bytes());
                    // Verify JSON is valid
                    let _: serde_json::Value = serde_json::from_slice(&content)?;
                }
                _ => {}
            }

            processed_events.push((event_id, event.source.clone(), content.len()));
        }

        // Complete processing
        complete_work_queue_item(&pool, queue_item.queue_id).await?;
    }

    // Verify all events were processed successfully
    pretty_assertions::assert_eq!(processed_events.len(), 3, "All events should be processed");

    // Check that promotion queue is empty
    let remaining_work =
        claim_work_queue_items(&pool, "annex-test-agent", "cleanup-worker", 10).await?;
    assert!(remaining_work.is_empty(), "No work should remain in queue");

    println!("✅ Event processing with git-annex blob integration successful");
    Ok(())
}

async fn test_worker_system_annex_integration(
    pool: &DbPool,
    annex_repo: &GitAnnexTestRepo,
) -> Result<(), anyhow::Error> {
    // Test that the worker system can handle concurrent access to git-annex files

    // Create multiple files for concurrent processing
    let file_count = 10;
    let mut file_keys = HashMap::new();

    for i in 0..file_count {
        let content = format!(
            "Worker test file {} content with unique data: {}",
            i,
            "x".repeat(i * 100)
        );
        let file_name = format!("worker_test_{}.txt", i);
        let key = annex_repo.add_file(&file_name, content.as_bytes()).await?;
        file_keys.insert(i, (file_name, key, content));
    }

    // Create events for concurrent processing
    let mut event_ids = Vec::new();
    for i in 0..file_count {
        let (file_name, key, _) = &file_keys[&i];

        let event = RawEventBuilder::new(
            "concurrent_processor",
            "file.process",
            json!({
                "file_id": i,
                "file_name": file_name,
                "git_annex_key": key,
                "worker_test": true
            }),
        )
        .build();

        let inserted_event = insert_event(&pool, &event).await?;
        add_to_work_queue(&pool, inserted_event.id, "concurrent-test-agent", 3).await?;
        event_ids.push(inserted_event.id);
    }

    // Start multiple workers concurrently
    let successful_workers = Arc::new(AtomicU32::new(0));
    let failed_workers = Arc::new(AtomicU32::new(0));
    let processed_files = Arc::new(AtomicU32::new(0));

    let mut worker_handles = Vec::new();

    for worker_id in 0..5 {
        let pool = pool.clone();
        let annex_repo_path = annex_repo.path.clone();
        let success_count = successful_workers.clone();
        let failure_count = failed_workers.clone();
        let file_count_processed = processed_files.clone();

        let handle = tokio::spawn(async move {
            let worker_name = format!("concurrent-worker-{}", worker_id);

            loop {
                // Try to claim work
                let claimed =
                    claim_work_queue_items(&pool, "concurrent-test-agent", &worker_name, 1).await;

                match claimed {
                    Ok(items) => {
                        if items.is_empty() {
                            break; // No more work
                        }

                        for item in items {
                            // Get event details
                            if let Ok(event) =
                                crate::common::get_event_by_id(&pool, item.raw_event_id).await
                            {
                                if let Some(file_name) = event.payload["file_name"].as_str() {
                                    // Simulate accessing git-annex file
                                    let file_path = annex_repo_path.join(file_name);

                                    // Get file content using git-annex
                                    let get_result = Command::new("git")
                                        .args(["annex", "get", file_name])
                                        .current_dir(&annex_repo_path)
                                        .output();

                                    match get_result {
                                        Ok(output) if output.status.success() => {
                                            // Try to read file
                                            if let Ok(content) = std::fs::read(&file_path) {
                                                // Simulate processing
                                                let _content_len = content.len();
                                                file_count_processed.fetch_add(1, Ordering::SeqCst);

                                                // Complete successfully
                                                let _ =
                                                    complete_work_queue_item(&pool, item.queue_id)
                                                        .await;
                                                success_count.fetch_add(1, Ordering::SeqCst);
                                            } else {
                                                failure_count.fetch_add(1, Ordering::SeqCst);
                                                let next_retry = chrono::Utc::now()
                                                    + chrono::Duration::minutes(5);
                                                let _ = fail_work_queue_item(
                                                    &pool,
                                                    item.queue_id,
                                                    "File read failed",
                                                    next_retry,
                                                )
                                                .await;
                                            }
                                        }
                                        _ => {
                                            failure_count.fetch_add(1, Ordering::SeqCst);
                                            let next_retry =
                                                chrono::Utc::now() + chrono::Duration::minutes(5);
                                            let _ = fail_work_queue_item(
                                                &pool,
                                                item.queue_id,
                                                "Git-annex get failed",
                                                next_retry,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        failure_count.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                }

                // Brief pause to allow other workers
                tokio::task::yield_now().await;
            }
        });

        worker_handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in worker_handles {
        handle.await?;
    }

    let successful = successful_workers.load(Ordering::SeqCst);
    let failed = failed_workers.load(Ordering::SeqCst);
    let processed = processed_files.load(Ordering::SeqCst);

    // Verify results
    assert!(
        successful > 0,
        "Should have some successful worker operations"
    );
    pretty_assertions::assert_eq!(processed, file_count as u32, "Should process all files");

    // System should remain healthy
    let health_check =
        sqlx::query("SELECT COUNT(*) FROM raw.events WHERE source = 'concurrent_processor'")
            .fetch_one(&pool)
            .await;
    assert!(
        health_check.is_ok(),
        "System should remain healthy after concurrent git-annex access"
    );

    println!(
        "✅ Worker system git-annex integration successful: {} successful, {} failed, {} processed",
        successful, failed, processed
    );
    Ok(())
}

async fn test_query_interface_annex_integration(
    pool: &DbPool,
    annex_repo: &GitAnnexTestRepo,
) -> Result<(), anyhow::Error> {
    // Test that query interface can access git-annex stored content

    // Create test files with searchable content
    let documents = vec![
        ("important_document.txt", "This document contains important information about the Sinex project."),
        ("meeting_notes.md", "# Meeting Notes\n\n- Discussed git-annex integration\n- Reviewed test coverage\n- Next steps defined"),
        ("data_export.json", r#"{"events": 1000, "sources": ["filesystem", "terminal"], "status": "complete"}"#),
    ];

    let mut document_events = Vec::new();

    for (filename, content) in &documents {
        // Store in git-annex
        let key = annex_repo.add_file(filename, content.as_bytes()).await?;

        // Create event
        let event = RawEventBuilder::new(
            "document_system",
            "document.indexed",
            json!({
                "filename": filename,
                "git_annex_key": key,
                "content_type": if filename.ends_with(".json") { "application/json" } else { "text/plain" },
                "size": content.len(),
                "searchable": true
            })
        ).build();

        let inserted_event = insert_event(&pool, &event).await?;
        document_events.push((inserted_event.id, filename, key));
    }

    // Test 1: Query events with git-annex references
    let all_document_events =
        crate::common::get_events_by_source(&pool, "document_system", 10).await?;
    pretty_assertions::assert_eq!(
        all_document_events.len(),
        3,
        "Should find all document events"
    );

    for event in &all_document_events {
        assert!(
            event.payload["git_annex_key"].as_str().is_some(),
            "Event should have git-annex key"
        );
        assert!(
            event.payload["filename"].as_str().is_some(),
            "Event should have filename"
        );
    }

    // Test 2: Query specific file types
    let json_events: Vec<_> = all_document_events
        .iter()
        .filter(|e| e.payload["content_type"].as_str() == Some("application/json"))
        .collect();
    pretty_assertions::assert_eq!(json_events.len(), 1, "Should find one JSON file");

    // Test 3: Simulate content retrieval for query results
    let mut retrieved_contents = HashMap::new();

    for event in &all_document_events {
        if let Some(filename) = event.payload["filename"].as_str() {
            if let Some(_key) = event.payload["git_annex_key"].as_str() {
                // Retrieve content from git-annex
                match annex_repo.get_file_content(filename).await {
                    Ok(content) => {
                        let content_str = String::from_utf8_lossy(&content);
                        retrieved_contents.insert(filename.to_string(), content_str.to_string());
                    }
                    Err(e) => {
                        println!("⚠️  Failed to retrieve {}: {}", filename, e);
                    }
                }
            }
        }
    }

    // Verify retrieved content matches original
    pretty_assertions::assert_eq!(
        retrieved_contents.len(),
        3,
        "Should retrieve all file contents"
    );

    for (filename, original_content) in &documents {
        assert!(
            retrieved_contents.contains_key(*filename),
            "Should have retrieved {}",
            filename
        );
        pretty_assertions::assert_eq!(
            retrieved_contents[*filename],
            *original_content,
            "Content should match for {}",
            filename
        );
    }

    // Test 4: Search functionality with git-annex content
    let searchable_events: Vec<_> = all_document_events
        .iter()
        .filter(|e| e.payload["searchable"].as_bool() == Some(true))
        .collect();
    pretty_assertions::assert_eq!(
        searchable_events.len(),
        3,
        "All events should be searchable"
    );

    // Test 5: Performance of git-annex content access
    let start_time = Instant::now();

    for event in &all_document_events {
        if let Some(filename) = event.payload["filename"].as_str() {
            let _ = annex_repo.get_file_content(filename).await;
        }
    }

    let access_duration = start_time.elapsed();
    assert!(
        access_duration < Duration::from_secs(5),
        "Git-annex content access should be reasonably fast: {:?}",
        access_duration
    );

    println!("✅ Query interface git-annex integration successful");
    Ok(())
}

#[sinex_test]
async fn test_git_annex_fallback_scenarios(ctx: TestContext) -> Result<(), anyhow::Error> {
    let pool = ctx.pool();

    // Test system behavior when git-annex is not available
    test_annex_unavailable_fallback(&pool).await?;

    // Test system behavior when git-annex operations fail
    test_annex_operation_failure_handling(&pool).await?;

    // Test system recovery when git-annex becomes available again
    test_annex_recovery_scenarios(&pool).await?;

    Ok(())
}

async fn test_annex_unavailable_fallback(pool: &DbPool) -> Result<(), anyhow::Error> {
    // Test system behavior when git-annex is not available

    // Create events that would normally use git-annex
    let large_file_content = "x".repeat(2048); // 2KB content

    let fallback_event = RawEventBuilder::new(
        "filesystem",
        "file.created",
        json!({
            "path": "/test/large_file_no_annex.txt",
            "size": large_file_content.len(),
            "content": large_file_content, // Store directly in event since no git-annex
            "storage_type": "inline", // Fallback storage
            "fallback_reason": "git_annex_unavailable"
        }),
    )
    .build();

    // Insert event (should succeed even without git-annex)
    let inserted_event = insert_event(&pool, &fallback_event).await?;

    // Verify event was stored
    let retrieved_event = crate::common::get_event_by_id(&pool, inserted_event.id).await?;
    pretty_assertions::assert_eq!(
        retrieved_event.payload["storage_type"].as_str().unwrap(),
        "inline"
    );
    pretty_assertions::assert_eq!(
        retrieved_event.payload["content"].as_str().unwrap(),
        large_file_content
    );
    pretty_assertions::assert_eq!(
        retrieved_event.payload["fallback_reason"].as_str().unwrap(),
        "git_annex_unavailable"
    );

    // System should continue working normally
    let health_check = sqlx::query("SELECT 1").fetch_one(&pool).await;
    assert!(
        health_check.is_ok(),
        "System should remain healthy without git-annex"
    );

    println!("✅ Git-annex unavailable fallback handling successful");
    Ok(())
}

async fn test_annex_operation_failure_handling(pool: &DbPool) -> Result<(), anyhow::Error> {
    // Test handling of git-annex operation failures

    // Simulate scenarios where git-annex operations might fail
    let failure_scenarios = vec![
        ("corrupted_repo", "Repository corruption detected"),
        ("disk_full", "No space left on device"),
        ("permission_denied", "Permission denied accessing annex"),
        ("network_timeout", "Remote operation timed out"),
    ];

    for (scenario, error_message) in failure_scenarios {
        let failure_event = RawEventBuilder::new(
            "filesystem",
            "file.created",
            json!({
                "path": format!("/test/{}_test.txt", scenario),
                "size": 1024,
                "git_annex_operation": "add",
                "git_annex_error": error_message,
                "storage_type": "failed_annex",
                "fallback_applied": true
            }),
        )
        .build();

        // Event should be stored despite git-annex failure
        let inserted_event = insert_event(&pool, &failure_event).await?;

        // Verify error is recorded
        let retrieved = crate::common::get_event_by_id(&pool, inserted_event.id).await?;
        pretty_assertions::assert_eq!(
            retrieved.payload["git_annex_error"].as_str().unwrap(),
            error_message
        );
        pretty_assertions::assert_eq!(
            retrieved.payload["fallback_applied"].as_bool().unwrap(),
            true
        );
    }

    // System should handle multiple failures gracefully
    let error_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM raw.events WHERE payload->>'storage_type' = 'failed_annex'",
    )
    .fetch_one(&pool)
    .await?;

    pretty_assertions::assert_eq!(error_count, 4, "All failure scenarios should be recorded");

    println!("✅ Git-annex operation failure handling successful");
    Ok(())
}

async fn test_annex_recovery_scenarios(pool: &DbPool) -> Result<(), anyhow::Error> {
    // Test system recovery when git-annex becomes available again

    let temp_dir = TempDir::new()?;
    let repo_path = temp_dir.path().join("recovery_test");
    fs::create_dir_all(&repo_path).await?;

    // Phase 1: Simulate git-annex unavailable period
    let unavailable_events = vec![
        ("file1.txt", "Content during outage 1"),
        ("file2.txt", "Content during outage 2"),
        ("file3.txt", "Content during outage 3"),
    ];

    let mut outage_event_ids = Vec::new();

    for (filename, content) in &unavailable_events {
        let outage_event = RawEventBuilder::new(
            "filesystem",
            "file.created",
            json!({
                "path": format!("/recovery_test/{}", filename),
                "size": content.len(),
                "content": content,
                "storage_type": "pending_annex",
                "annex_status": "unavailable"
            }),
        )
        .build();

        let inserted_event = insert_event(&pool, &outage_event).await?;
        outage_event_ids.push((inserted_event.id, filename, content));
    }

    // Phase 2: Simulate git-annex becoming available
    // Initialize git repository
    let git_init = Command::new("git")
        .args(["init"])
        .current_dir(&repo_path)
        .output()?;

    if git_init.status.success() {
        // Configure git
        let _ = Command::new("git")
            .args(["config", "user.name", "Recovery Test"])
            .current_dir(&repo_path)
            .output();

        let _ = Command::new("git")
            .args(["config", "user.email", "recovery@test.local"])
            .current_dir(&repo_path)
            .output();

        // Try to initialize git-annex
        let annex_init = Command::new("git")
            .args(["annex", "init", "recovery-test"])
            .current_dir(&repo_path)
            .output()?;

        if annex_init.status.success() {
            // Phase 3: Migrate pending events to git-annex
            for (event_id, filename, content) in &outage_event_ids {
                // Write file to repository
                let file_path = repo_path.join(filename);
                fs::write(&file_path, content.as_bytes()).await?;

                // Add to git-annex
                let add_result = Command::new("git")
                    .args(["annex", "add", filename])
                    .current_dir(&repo_path)
                    .output()?;

                if add_result.status.success() {
                    // Update event to reflect successful migration
                    let migration_event = RawEventBuilder::new(
                        "filesystem",
                        "file.migrated",
                        json!({
                            "original_event_id": event_id,
                            "path": format!("/recovery_test/{}", filename),
                            "migration_status": "successful",
                            "storage_type": "git_annex",
                            "migration_timestamp": chrono::Utc::now().to_rfc3339()
                        }),
                    )
                    .build();

                    let _ = insert_event(&pool, &migration_event).await?;
                }
            }

            // Verify migration events were created
            let migration_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM raw.events WHERE event_type = 'file.migrated'",
            )
            .fetch_one(&pool)
            .await?;

            pretty_assertions::assert_eq!(
                migration_count,
                3,
                "All files should be migrated to git-annex"
            );

            println!("✅ Git-annex recovery scenario successful");
        } else {
            println!("⚠️  Git-annex not available for recovery test");
        }
    }

    Ok(())
}
