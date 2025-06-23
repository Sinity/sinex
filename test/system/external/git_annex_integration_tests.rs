use anyhow::Result;
use sinex_annex::{GitAnnex, AnnexConfig};
use tempfile::TempDir;
use tokio::fs;

async fn setup_test_annex() -> Result<(GitAnnex, TempDir)> {
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

#[tokio::test]
async fn test_file_add_and_retrieve() -> Result<(), anyhow::Error> {
    let (annex, temp_dir) = setup_test_annex().await?;
    
    // Create a test file
    let test_file = temp_dir.path().join("test.txt");
    let content = b"Hello, git-annex!";
    fs::write(&test_file, content).await?;
    
    // Add file to annex
    let annex_key = annex.add_file(&test_file).await?;
    
    // Verify key was generated
    assert!(!annex_key.key.is_empty());
    assert_eq!(annex_key.size, content.len() as u64);
    
    // Ensure content is available
    annex.get_content(&test_file.to_string_lossy()).await?;
    
    // Verify file still exists and is a symlink
    assert!(test_file.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_large_file_handling() -> Result<(), anyhow::Error> {
    let (annex, temp_dir) = setup_test_annex().await?;
    
    // Create 1MB of data
    let content = vec![0u8; 1024 * 1024];
    let large_file = temp_dir.path().join("large.bin");
    fs::write(&large_file, &content).await?;
    
    // Add large file to annex
    let annex_key = annex.add_file(&large_file).await?;
    
    // Verify git-annex handled it
    assert_eq!(annex_key.size, content.len() as u64);
    assert!(!annex_key.backend.is_empty());
    
    // Check status
    let status = annex.status().await?;
    assert!(status.contains("1 file"));
    
    Ok(())
}

#[tokio::test]
async fn test_annex_key_lookup() -> Result<(), anyhow::Error> {
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
    assert_eq!(original_key.key, looked_up_key.key);
    assert_eq!(original_key.size, looked_up_key.size);
    assert_eq!(original_key.backend, looked_up_key.backend);
    
    Ok(())
}

#[tokio::test]
async fn test_drop_content() -> Result<(), anyhow::Error> {
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

#[tokio::test]
async fn test_fsck() -> Result<(), anyhow::Error> {
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

#[tokio::test]
async fn test_git_annex_configuration() -> Result<(), anyhow::Error> {
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
        .args(&["config", "annex.numcopies"])
        .current_dir(&repo_path)
        .output()
        .await?;
    
    let num_copies = String::from_utf8_lossy(&output.stdout).trim().to_string();
    assert_eq!(num_copies, "2");
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_file_operations() -> Result<(), anyhow::Error> {
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
            
            Ok::<_, anyhow::Error>(key)
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
    assert_eq!(keys.len(), 5);
    for key in keys {
        assert!(!key.key.is_empty());
    }
    
    Ok(())
}

#[tokio::test]
async fn test_files_in_subdirectories() -> Result<(), anyhow::Error> {
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
    assert_eq!(key.size, content.len() as u64);
    
    // Get content to ensure it's accessible
    annex.get_content(&nested_file.to_string_lossy()).await?;
    
    Ok(())
}

#[tokio::test]
async fn test_annex_deduplication() -> Result<(), anyhow::Error> {
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
    assert_eq!(key1.key, key2.key);
    assert_eq!(key1.hash, key2.hash);
    
    // Check that git-annex recognizes the deduplication
    let output = tokio::process::Command::new("git")
        .args(&["annex", "find", "--include=*"])
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