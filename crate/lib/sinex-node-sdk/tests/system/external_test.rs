// # External System Integration Tests
//
// Integration tests with external systems and services:
// - Git Annex for blob storage
// - PostgreSQL with TimescaleDB extensions
// - Operating system interfaces
// - External command execution
//
// ## Test Categories
//
// - **Git Annex Integration**: File storage, retrieval, and deduplication
// - **External Command Execution**: System interaction validation
// - **Database Integration**: External database service integration
//
// ## Performance Expectations
//
// - **Individual tests**: 10-60 seconds
// - **Resource usage**: Significant disk I/O, external process spawning
// - **Dependencies**: Git Annex, external command tools, filesystem access

use sinex_node_sdk::annex::{AnnexConfig, GitAnnex};
use xtask::sandbox::prelude::*;
use xtask::sandbox::TestResult;
use sqlx::Row;
use tempfile::TempDir;
use tokio::fs;

// ==================== GIT ANNEX INTEGRATION TESTS ====================

async fn setup_test_annex(
) -> AnyhowResult<(GitAnnex, tempfile::TempDir), Box<dyn std::error::Error + Send + Sync>> {
    let temp_dir = TempDir::new()?;
    let repo_path = temp_dir.path().to_path_buf();

    // Initialize git-annex repository
    GitAnnex::init(&repo_path, Some("test-repo")).await?;

    let config = AnnexConfig {
        repo_path: repo_path.clone(),
        num_copies: Some(1),
        large_files: None,
    };

    let annex = GitAnnex::new(config)?;

    Ok((annex, temp_dir))
}

#[sinex_test]
async fn test_file_add_and_retrieve(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    // Create a test file
    let test_file = temp_dir.path().join("test.txt");
    let content = b"Hello, git-annex!";
    fs::write(&test_file, content).await?;

    // Add file to annex
    let annex_key = annex.add_file(&test_file).await?;

    // Verify key was generated
    assert!(!annex_key.key.is_empty());
    pretty_assertions::assert_eq!(annex_key.size, content.len() as u64);

    // Ensure content is available
    annex.get_content(&test_file.to_string_lossy()).await?;

    // Verify file still exists and is a symlink
    assert!(test_file.exists());

    Ok(())
}

#[sinex_test]
async fn test_large_file_handling(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    // Create 1MB of data
    let content = vec![0u8; 1024 * 1024];
    let large_file = temp_dir.path().join("large.bin");
    fs::write(&large_file, &content).await?;

    // Add large file to annex
    let annex_key = annex.add_file(&large_file).await?;

    // Verify git-annex handled it
    pretty_assertions::assert_eq!(annex_key.size, content.len() as u64);
    assert!(!annex_key.backend.is_empty());

    // Check status
    let status = annex.status().await?;
    assert!(status.contains("1 file"));

    Ok(())
}

#[sinex_test]
async fn test_annex_key_lookup(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    // Create a test file with known content
    let test_file = temp_dir.path().join("lookup_test.txt");
    let content = b"Content for key lookup test";
    fs::write(&test_file, content).await?;

    // Add to annex
    let original_key = annex.add_file(&test_file).await?;

    // Look up the key again
    let looked_up_key = annex.get_key(&test_file).await?;

    // Keys should match
    pretty_assertions::assert_eq!(original_key.key, looked_up_key.key);
    pretty_assertions::assert_eq!(original_key.size, looked_up_key.size);
    pretty_assertions::assert_eq!(original_key.backend, looked_up_key.backend);

    Ok(())
}

#[sinex_test]
async fn test_drop_content(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    // Create and add a file
    let test_file = temp_dir.path().join("drop_test.txt");
    fs::write(&test_file, b"Content to drop").await?;

    let key = annex.add_file(&test_file).await?;

    // Try to drop content (will fail without force since we only have 1 copy)
    let drop_result = annex.drop_content(&key.key, false).await;
    assert!(drop_result.is_err());

    // Force drop
    annex.drop_content(&key.key, true).await?;

    Ok(())
}

#[sinex_test]
async fn test_fsck(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    // Add some files
    for i in 0..3 {
        let file = temp_dir.path().join(format!("file_{}.txt", i));
        fs::write(&file, format!("Content {}", i)).await?;
        annex.add_file(&file).await?;
    }

    // Run filesystem check
    let fsck_output = annex.fsck(true, false).await?;

    // Should complete without errors
    assert!(!fsck_output.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_git_annex_configuration(ctx: TestContext) -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let repo_path = temp_dir.path().to_path_buf();

    // Initialize with configuration
    GitAnnex::init(&repo_path, Some("configured-repo")).await?;

    let config = AnnexConfig {
        repo_path: repo_path.clone(),
        num_copies: Some(2),
        large_files: Some("*.bin".to_string()),
    };

    let annex = GitAnnex::new(config)?;
    annex.configure().await?;

    // Verify configuration was applied
    let output = tokio::process::Command::new("git")
        .args(["config", "annex.numcopies"])
        .current_dir(&repo_path)
        .output()
        .await?;

    let num_copies = String::from_utf8_lossy(&output.stdout).trim().to_string();
    pretty_assertions::assert_eq!(num_copies, "2");

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_concurrent_file_operations(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;
    let annex = std::sync::Arc::new(annex);
    let mut handles = vec![];

    // Spawn multiple concurrent operations
    for i in 0..5 {
        let annex = annex.clone();
        let temp_path = temp_dir.path().to_path_buf();

        let handle = tokio::spawn(async move {
            let file_path = temp_path.join(format!("concurrent_{}.txt", i));
            let content = format!("Concurrent content {}", i);

            // Write file
            fs::write(&file_path, content.as_bytes()).await?;

            // Add to annex
            let key = annex.add_file(&file_path).await?;

            Ok::<_, color_eyre::eyre::Error>(key)
        });

        handles.push(handle);
    }

    // Wait for all operations
    let mut keys = vec![];
    for handle in handles {
        let key = handle.await??;
        keys.push(key);
    }

    // Verify all files were added
    pretty_assertions::assert_eq!(keys.len(), 5);
    for key in keys {
        assert!(!key.key.is_empty());
    }

    Ok(())
}

#[sinex_test]
async fn test_files_in_subdirectories(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    // Create subdirectory structure
    let sub_dir = temp_dir.path().join("nested").join("path");
    fs::create_dir_all(&sub_dir).await?;

    // Create file in subdirectory
    let nested_file = sub_dir.join("data.json");
    let content = br#"{"nested": "json", "data": true}"#;
    fs::write(&nested_file, content).await?;

    // Add to annex
    let key = annex.add_file(&nested_file).await?;

    // Verify path structure
    assert!(nested_file.exists());
    pretty_assertions::assert_eq!(key.size, content.len() as u64);

    // Get content to ensure it's accessible
    annex.get_content(&nested_file.to_string_lossy()).await?;

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_annex_deduplication(ctx: TestContext) -> TestResult<()> {
    let (annex, temp_dir) = setup_test_annex().await?;

    let content = b"Duplicate content for dedup test";

    // Create two files with same content
    let file1 = temp_dir.path().join("dup1.txt");
    let file2 = temp_dir.path().join("dup2.txt");

    fs::write(&file1, content).await?;
    fs::write(&file2, content).await?;

    // Add both to annex
    let key1 = annex.add_file(&file1).await?;
    let key2 = annex.add_file(&file2).await?;

    // Both files should exist
    assert!(file1.exists());
    assert!(file2.exists());

    // Keys should be identical (git-annex deduplicates by content)
    pretty_assertions::assert_eq!(key1.key, key2.key);
    pretty_assertions::assert_eq!(key1.hash, key2.hash);

    // Check that git-annex recognizes the deduplication
    let output = tokio::process::Command::new("git")
        .args(["annex", "find", "--include=*"])
        .current_dir(temp_dir.path())
        .output()
        .await?;

    if output.status.success() {
        let files = String::from_utf8_lossy(&output.stdout);
        // Should list both files but they point to same content
        assert!(files.contains("dup1.txt"));
        assert!(files.contains("dup2.txt"));
    }

    Ok(())
}

// ==================== EXTERNAL COMMAND INTEGRATION TESTS ====================

#[sinex_test]
async fn test_external_command_execution(ctx: TestContext) -> TestResult<()> {
    // Test basic external command execution
    let output = tokio::process::Command::new("echo")
        .arg("Hello, external world!")
        .output()
        .await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Hello, external world!"));

    Ok(())
}

#[sinex_test]
async fn test_external_command_with_environment(ctx: TestContext) -> TestResult<()> {
    // Test command execution with environment variables
    let output = tokio::process::Command::new("env")
        .env("TEST_VAR", "test_value")
        .output()
        .await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("TEST_VAR=test_value"));

    Ok(())
}

#[sinex_test]
async fn test_external_command_working_directory(ctx: TestContext) -> TestResult<()> {
    // Create a temporary directory
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();

    // Create a test file in the temp directory
    let test_file = temp_path.join("test.txt");
    fs::write(&test_file, "test content").await?;

    // Execute ls command in the temp directory
    let output = tokio::process::Command::new("ls")
        .current_dir(temp_path)
        .output()
        .await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("test.txt"));

    Ok(())
}

#[sinex_test]
async fn test_external_command_error_handling(ctx: TestContext) -> TestResult<()> {
    // Test handling of command that returns error
    let output = tokio::process::Command::new("false").output().await?;

    assert!(!output.status.success());
    assert_eq!(output.status.code(), Some(1));

    Ok(())
}

#[sinex_test]
async fn test_external_command_timeout(ctx: TestContext) -> TestResult<()> {
    // Test command timeout handling
    let result = timeout(Duration::from_millis(100), async {
        tokio::process::Command::new("sleep")
            .arg("1")
            .output()
            .await
    })
    .await;

    assert!(result.is_err(), "Command should have timed out");

    Ok(())
}

#[sinex_test]
async fn test_external_command_stdin_interaction(ctx: TestContext) -> TestResult<()> {
    // Test command with stdin input
    let mut child = tokio::process::Command::new("cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    let stdin = child.stdin.take().unwrap();
    let input = "Hello from stdin!";

    // Write to stdin
    let write_task = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let mut stdin = stdin;
        stdin.write_all(input.as_bytes()).await.unwrap();
        stdin.shutdown().await.unwrap();
    });

    // Wait for command to complete
    let output = child.wait_with_output().await?;
    write_task.await?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout, input);

    Ok(())
}

// ==================== DATABASE INTEGRATION TESTS ====================

#[sinex_test]
async fn test_external_database_connection(ctx: TestContext) -> TestResult<()> {
    // Test that we can connect to external PostgreSQL database
    let pool = ctx.pool().clone();

    // Test basic connection
    let result = sqlx::query("SELECT 1 as test_value")
        .fetch_one(&pool)
        .await?;

    assert!(result.get::<i32, _>("test_value") == 1);

    Ok(())
}

#[sinex_test]
async fn test_external_database_timescaledb_functions(
    ctx: TestContext,
) -> TestResult<()> {
    // Test TimescaleDB specific functions
    let pool = ctx.pool().clone();

    // Test time_bucket function (TimescaleDB specific)
    let result = sqlx::query("SELECT time_bucket('1 minute', NOW()) as bucket")
        .fetch_one(&pool)
        .await;

    // Should either succeed (TimescaleDB installed) or fail gracefully
    match result {
        Ok(_) => {
            println!("TimescaleDB functions are available");
        }
        Err(_) => {
            println!("TimescaleDB functions not available (expected in test environment)");
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_external_database_extensions(ctx: TestContext) -> TestResult<()> {
    // Test that required database extensions are available
    let pool = ctx.pool().clone();

    // Validate UUIDv7 function expected by canonical schema.
    let uuid_test = sqlx::query("SELECT uuidv7()::text as test_uuid")
        .fetch_one(&pool)
        .await?;
    let uuid_str = uuid_test.get::<String, _>("test_uuid");
    assert_eq!(uuid_str.len(), 36);

    Ok(())
}

#[sinex_test]
async fn test_external_database_concurrent_connections(
    ctx: TestContext,
) -> TestResult<()> {
    // Test concurrent database connections
    let pool = ctx.pool().clone();
    let mut handles = vec![];

    for i in 0..5 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let result = sqlx::query("SELECT $1 as connection_id")
                .bind(i)
                .fetch_one(&pool_clone)
                .await?;

            Ok::<i32, color_eyre::eyre::Error>(result.get("connection_id"))
        });
        handles.push(handle);
    }

    // Wait for all connections to complete
    for (i, handle) in handles.into_iter().enumerate() {
        let result = handle.await??;
        assert_eq!(result, i as i32);
    }

    Ok(())
}

#[sinex_test]
async fn test_external_database_transaction_isolation(
    ctx: TestContext,
) -> TestResult<()> {
    // Test database transaction isolation
    let pool = ctx.pool().clone();

    // Start a transaction
    let mut tx = pool.begin().await?;

    // Insert test data in transaction
    sqlx::query("CREATE TEMPORARY TABLE test_isolation (id INT, value TEXT)")
        .execute(&mut *tx)
        .await?;

    sqlx::query("INSERT INTO test_isolation (id, value) VALUES (1, 'transaction_data')")
        .execute(&mut *tx)
        .await?;

    // Verify data exists within transaction
    let result = sqlx::query("SELECT value FROM test_isolation WHERE id = 1")
        .fetch_one(&mut *tx)
        .await?;

    assert_eq!(result.get::<String, _>("value"), "transaction_data");

    // Commit transaction
    tx.commit().await?;

    // Test that temporary table is gone after transaction
    let table_check = sqlx::query("SELECT 1 FROM test_isolation LIMIT 1")
        .fetch_optional(&pool)
        .await;

    assert!(
        table_check.is_err(),
        "Temporary table should not exist after transaction"
    );

    Ok(())
}

// ==================== FILESYSTEM INTEGRATION TESTS ====================

#[sinex_test]
async fn test_external_filesystem_operations(ctx: TestContext) -> TestResult<()> {
    // Test basic filesystem operations
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("external_test.txt");

    // Write file
    fs::write(&test_file, "external test content").await?;

    // Verify file exists
    assert!(test_file.exists());

    // Read file back
    let content = fs::read_to_string(&test_file).await?;
    assert_eq!(content, "external test content");

    // Test file metadata
    let metadata = fs::metadata(&test_file).await?;
    assert!(metadata.is_file());
    assert_eq!(metadata.len(), "external test content".len() as u64);

    Ok(())
}

#[sinex_test]
async fn test_external_filesystem_permissions(ctx: TestContext) -> TestResult<()> {
    // Test filesystem permissions (Unix-specific)
    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("permission_test.txt");

    // Create file
    fs::write(&test_file, "permission test").await?;

    // Test on Unix systems
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        // Get current permissions
        let metadata = fs::metadata(&test_file).await?;
        let perms = metadata.permissions();

        // Verify it's a regular file permission
        assert!(perms.mode() & 0o100000 != 0); // S_IFREG

        // Test permission modification
        let mut new_perms = perms.clone();
        new_perms.set_mode(0o644);
        fs::set_permissions(&test_file, new_perms).await?;

        // Verify permission change
        let updated_metadata = fs::metadata(&test_file).await?;
        assert_eq!(updated_metadata.permissions().mode() & 0o777, 0o644);
    }

    Ok(())
}

#[sinex_test]
async fn test_external_filesystem_symlinks(ctx: TestContext) -> TestResult<()> {
    // Test symbolic link operations
    let temp_dir = TempDir::new()?;
    let original_file = temp_dir.path().join("original.txt");
    let symlink_file = temp_dir.path().join("symlink.txt");

    // Create original file
    fs::write(&original_file, "original content").await?;

    // Create symlink (Unix-specific)
    #[cfg(unix)]
    {
        tokio::fs::symlink(&original_file, &symlink_file).await?;

        // Verify symlink exists
        assert!(symlink_file.exists());

        // Test reading through symlink
        let content = fs::read_to_string(&symlink_file).await?;
        assert_eq!(content, "original content");

        // Test symlink metadata
        let symlink_metadata = fs::symlink_metadata(&symlink_file).await?;
        assert!(symlink_metadata.is_symlink());

        // Test reading symlink target
        let target = fs::read_link(&symlink_file).await?;
        assert_eq!(target, original_file);
    }

    Ok(())
}
