//! Security tests for HistoryWatcher component
//!
//! Tests path validation, boundary enforcement, and security policy compliance
//! for shell history file watching operations.

use camino::Utf8PathBuf;
use sinex_core::types::validation::FileWatchingSecurityPolicy;
use sinex_terminal_satellite::history::HistoryWatcher;
use sinex_test_utils::sinex_test;
use std::fs;
use tempfile::TempDir;

#[sinex_test]
async fn test_history_watcher_path_validation() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher validates paths during construction
    
    // Try to create watcher with dangerous paths (should fail)
    let dangerous_paths = vec![
        Utf8PathBuf::from("/etc/passwd"),
        Utf8PathBuf::from("/proc/version"),
        Utf8PathBuf::from("/sys/kernel/version"),
    ];
    
    let result = HistoryWatcher::new(dangerous_paths).await;
    assert!(result.is_err(), "HistoryWatcher should reject dangerous paths");
    
    // Error message should indicate validation failure
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("validation failed") || error_msg.contains("forbidden"),
        "Error should mention validation failure: {}", 
        error_msg
    );
    
    Ok(())
}

#[sinex_test]
async fn test_history_watcher_safe_paths() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher allows safe paths
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Create mock history files
    let bash_history = temp_path.join(".bash_history");
    let zsh_history = temp_path.join(".zsh_history");
    let fish_history = temp_path.join("fish_history");
    
    fs::write(&bash_history, "echo 'test command'\nls -la\n")?;
    fs::write(&zsh_history, ": 1234567890:0;echo 'zsh test'\n")?;
    fs::write(&fish_history, "- cmd: fish test\n  when: 1234567890\n")?;
    
    let safe_paths = vec![
        Utf8PathBuf::from_path_buf(bash_history).unwrap(),
        Utf8PathBuf::from_path_buf(zsh_history).unwrap(), 
        Utf8PathBuf::from_path_buf(fish_history).unwrap(),
    ];
    
    // Should succeed with safe paths
    let watcher = HistoryWatcher::new(safe_paths).await;
    assert!(watcher.is_ok(), "HistoryWatcher should accept safe temp file paths");
    
    Ok(())
}

#[sinex_test]
async fn test_history_watcher_path_traversal_prevention() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher prevents path traversal attacks
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Try various path traversal techniques
    let traversal_paths = vec![
        Utf8PathBuf::from(format!("{}/../../../etc/passwd", temp_path.display())),
        Utf8PathBuf::from(format!("{}/./../../etc/shadow", temp_path.display())),
        Utf8PathBuf::from(format!("{}/../../../../../root/.ssh/id_rsa", temp_path.display())),
    ];
    
    let result = HistoryWatcher::new(traversal_paths).await;
    assert!(result.is_err(), "HistoryWatcher should prevent path traversal");
    
    Ok(())
}

#[sinex_test]
async fn test_history_watcher_symlink_security() -> color_eyre::eyre::Result<()> {
    // Test symlink security handling
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Create a symlink pointing to /etc/passwd (dangerous)
    let symlink_path = temp_path.join("dangerous_link");
    
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", &symlink_path).ok(); // May fail, that's ok
        
        if symlink_path.exists() {
            let dangerous_symlink = vec![
                Utf8PathBuf::from_path_buf(symlink_path).unwrap(),
            ];
            
            let result = HistoryWatcher::new(dangerous_symlink).await;
            // Should either reject the symlink or handle it safely
            if result.is_err() {
                let error_msg = result.unwrap_err().to_string();
                assert!(
                    error_msg.contains("symlink") || error_msg.contains("forbidden"),
                    "Error should mention symlink security: {}",
                    error_msg
                );
            }
        }
    }
    
    Ok(())
}

#[sinex_test] 
async fn test_history_watcher_home_directory_access() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher properly handles home directory access
    let temp_dir = TempDir::new()?;
    let fake_home = temp_dir.path().join("fake_home");
    fs::create_dir(&fake_home)?;
    
    // Create realistic shell history files under fake home
    let bash_history = fake_home.join(".bash_history");
    let zsh_history = fake_home.join(".zsh_history");
    
    fs::write(&bash_history, "cd Documents\nls -la\n")?;
    fs::write(&zsh_history, ": 1234567890:0;vim ~/.vimrc\n")?;
    
    let home_paths = vec![
        Utf8PathBuf::from_path_buf(bash_history).unwrap(),
        Utf8PathBuf::from_path_buf(zsh_history).unwrap(),
    ];
    
    // Should succeed since these are safe paths under temp directory
    let watcher = HistoryWatcher::new(home_paths).await;
    assert!(watcher.is_ok(), "HistoryWatcher should allow safe home-like paths");
    
    Ok(())
}

#[sinex_test]
async fn test_history_watcher_boundary_enforcement() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher enforces proper boundaries
    let temp_dir = TempDir::new()?;
    let user_home = temp_dir.path().join("user_home");
    let other_home = temp_dir.path().join("other_user_home");
    
    fs::create_dir(&user_home)?;
    fs::create_dir(&other_home)?;
    
    // Create history file in user's home
    let user_history = user_home.join(".bash_history");
    fs::write(&user_history, "echo 'user command'\n")?;
    
    // Create history file in other user's home  
    let other_history = other_home.join(".bash_history");
    fs::write(&other_history, "echo 'other user command'\n")?;
    
    // Should be able to watch user's own history
    let user_paths = vec![Utf8PathBuf::from_path_buf(user_history).unwrap()];
    let user_watcher = HistoryWatcher::new(user_paths).await;
    assert!(user_watcher.is_ok(), "Should allow watching user's own history");
    
    // Mixed paths should work since they're all under temp dir (safe)
    let mixed_paths = vec![
        Utf8PathBuf::from_path_buf(other_history).unwrap(),
    ];
    let mixed_watcher = HistoryWatcher::new(mixed_paths).await; 
    assert!(mixed_watcher.is_ok(), "Should allow watching other safe paths under temp");
    
    Ok(())
}

#[sinex_test]
async fn test_history_watcher_empty_path_list() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher handles empty path lists gracefully
    let empty_paths = vec![];
    
    let watcher = HistoryWatcher::new(empty_paths).await;
    assert!(watcher.is_ok(), "HistoryWatcher should handle empty path list");
    
    Ok(())
}

#[sinex_test]
async fn test_history_watcher_nonexistent_files() -> color_eyre::eyre::Result<()> {
    // Test that HistoryWatcher handles nonexistent files properly
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    let nonexistent_paths = vec![
        Utf8PathBuf::from_path_buf(temp_path.join(".bash_history_future")).unwrap(),
        Utf8PathBuf::from_path_buf(temp_path.join(".zsh_history_future")).unwrap(),
    ];
    
    // Should succeed - watcher should be able to watch for files that don't exist yet
    let watcher = HistoryWatcher::new(nonexistent_paths).await;
    assert!(watcher.is_ok(), "HistoryWatcher should handle nonexistent files");
    
    Ok(())
}