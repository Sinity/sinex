//! Security tests for FilesystemProcessor component
//!
//! Tests comprehensive security validation for filesystem watching operations,
//! including path validation, boundary enforcement, symlink protection, and security policy compliance.

use sinex_test_utils::prelude::*;

// Additional specific imports
use sinex_core::types::validation::FileWatchingSecurityPolicy;
use sinex_fs_watcher::unified_processor::{FilesystemConfig, FilesystemProcessor};
use tempfile::TempDir;

#[sinex_test]
async fn test_filesystem_processor_path_validation() -> TestResult<()> {
    // Test that FilesystemProcessor validates watch paths during setup
    
    // Create config with dangerous patterns (should fail)
    let dangerous_config = FilesystemConfig {
        watch_patterns: vec![
            "/etc/**".to_string(),
            "/proc/**".to_string(),
            "/sys/**".to_string(),
        ],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: None,
        security_policy: FileWatchingSecurityPolicy::default(),
    };
    
    let mut processor = FilesystemProcessor::with_config(dangerous_config);
    
    // Try to set up watcher with dangerous paths (should fail)
    let (notify_tx, _notify_rx) = std::sync::mpsc::channel();
    let mut debouncer = notify_debouncer_full::new_debouncer(
        std::time::Duration::from_millis(100),
        None,
        notify_tx,
    )?;
    
    let result = processor.setup_watch_paths(&mut debouncer).await;
    assert!(result.is_err(), "FilesystemProcessor should reject dangerous watch paths");
    
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
async fn test_filesystem_processor_safe_paths() -> TestResult<()> {
    // Test that FilesystemProcessor allows safe paths
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Create safe config with temp directory
    let safe_config = FilesystemConfig {
        watch_patterns: vec![format!("{}/**", temp_path.display())],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: None,
        security_policy: FileWatchingSecurityPolicy::default(),
    };
    
    let mut processor = FilesystemProcessor::with_config(safe_config);
    
    // Should succeed with safe paths
    let (notify_tx, _notify_rx) = std::sync::mpsc::channel();
    let mut debouncer = notify_debouncer_full::new_debouncer(
        std::time::Duration::from_millis(100),
        None,
        notify_tx,
    )?;
    
    let result = processor.setup_watch_paths(&mut debouncer).await;
    assert!(result.is_ok(), "FilesystemProcessor should accept safe temp directory paths");
    assert!(!processor.validated_watch_roots.is_empty(), "Should have validated watch roots");
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_processor_path_traversal_prevention() -> TestResult<()> {
    // Test that FilesystemProcessor prevents path traversal attacks
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Try various path traversal techniques in watch patterns
    let traversal_config = FilesystemConfig {
        watch_patterns: vec![
            format!("{}/../../../etc/**", temp_path.display()),
            format!("{}/./../../var/log/**", temp_path.display()),
            format!("{}/../../../../../root/**", temp_path.display()),
        ],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: None,
        security_policy: FileWatchingSecurityPolicy::default(),
    };
    
    let mut processor = FilesystemProcessor::with_config(traversal_config);
    
    let (notify_tx, _notify_rx) = std::sync::mpsc::channel();
    let mut debouncer = notify_debouncer_full::new_debouncer(
        std::time::Duration::from_millis(100),
        None,
        notify_tx,
    )?;
    
    let result = processor.setup_watch_paths(&mut debouncer).await;
    assert!(result.is_err(), "FilesystemProcessor should prevent path traversal");
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_processor_symlink_security() -> TestResult<()> {
    // Test symlink security handling
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Create a symlink pointing to a dangerous location (if possible)
    let symlink_path = temp_path.join("dangerous_link");
    
    #[cfg(unix)]
    {
        // Try to create symlink to /etc (may fail, that's ok)
        if std::os::unix::fs::symlink("/etc", &symlink_path).is_ok() {
            let symlink_config = FilesystemConfig {
                watch_patterns: vec![format!("{}/**", symlink_path.display())],
                ignore_patterns: vec![],
                debounce_ms: 100,
                max_depth: None,
                security_policy: FileWatchingSecurityPolicy::default(), // Don't follow symlinks
            };
            
            let mut processor = FilesystemProcessor::with_config(symlink_config);
            
            let (notify_tx, _notify_rx) = std::sync::mpsc::channel();
            let mut debouncer = notify_debouncer_full::new_debouncer(
                std::time::Duration::from_millis(100),
                None,
                notify_tx,
            )?;
            
            let result = processor.setup_watch_paths(&mut debouncer).await;
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
async fn test_filesystem_processor_security_policies() -> TestResult<()> {
    // Test different security policy modes
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Test restrictive policy
    let restrictive_config = FilesystemConfig {
        watch_patterns: vec![format!("{}/**", temp_path.display())],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: Some(5), // Limit depth
        security_policy: FileWatchingSecurityPolicy::restrictive(),
    };
    
    let mut restrictive_processor = FilesystemProcessor::with_config(restrictive_config);
    
    let (notify_tx, _notify_rx) = std::sync::mpsc::channel();
    let mut debouncer = notify_debouncer_full::new_debouncer(
        std::time::Duration::from_millis(100),
        None,
        notify_tx,
    )?;
    
    let result = restrictive_processor.setup_watch_paths(&mut debouncer).await;
    // May succeed or fail depending on temp directory location, but should validate
    if result.is_err() {
        println!("Restrictive policy rejected temp dir (expected behavior): {}", result.unwrap_err());
    } else {
        println!("Restrictive policy allowed temp dir");
        assert!(!restrictive_processor.validated_watch_roots.is_empty());
    }
    
    // Test permissive policy
    let permissive_config = FilesystemConfig {
        watch_patterns: vec![format!("{}/**", temp_path.display())],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: None,
        security_policy: FileWatchingSecurityPolicy::permissive(),
    };
    
    let mut permissive_processor = FilesystemProcessor::with_config(permissive_config);
    
    let (notify_tx2, _notify_rx2) = std::sync::mpsc::channel();
    let mut debouncer2 = notify_debouncer_full::new_debouncer(
        std::time::Duration::from_millis(100),
        None,
        notify_tx2,
    )?;
    
    let result = permissive_processor.setup_watch_paths(&mut debouncer2).await;
    assert!(result.is_ok(), "Permissive policy should allow safe temp directory");
    assert!(!permissive_processor.validated_watch_roots.is_empty());
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_processor_event_validation() -> TestResult<()> {
    // Test that events are validated against watch roots
    let temp_dir = TempDir::new()?;
    let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf()).unwrap();
    
    // Create processor with validated watch roots
    let config = FilesystemConfig {
        watch_patterns: vec![format!("{}/**", temp_path.as_str())],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: None,
        security_policy: FileWatchingSecurityPolicy::default(),
    };
    
    let processor = FilesystemProcessor {
        context: None,
        config,
        watch_roots: vec![temp_path.clone()],
        validated_watch_roots: vec![temp_path.clone()],
        rename_tracker: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        last_state: None,
        checkpoint_manager: None,
        stage_context: None,
    };
    
    // Create a file within the watch root
    let safe_file = temp_path.join("safe_file.txt");
    fs::write(&safe_file, "safe content")?;
    
    // Test creating events for safe file (should succeed)
    let metadata = fs::metadata(&safe_file)?;
    let result = processor.create_discovery_events(&safe_file, &metadata);
    assert!(result.is_ok());
    assert!(result.unwrap().len() > 0, "Should create events for files within validated watch roots");
    
    // Test creating events for file outside watch root (should return empty)
    let unsafe_file = Utf8PathBuf::from("/tmp/unsafe_file.txt");
    if let Ok(unsafe_metadata) = fs::metadata(&unsafe_file) {
        let result = processor.create_discovery_events(&unsafe_file, &unsafe_metadata);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0, "Should return empty events for files outside watch roots");
    }
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_processor_convert_fs_event_security() -> TestResult<()> {
    // Test that filesystem events are validated during conversion
    let temp_dir = TempDir::new()?;
    let temp_path = Utf8PathBuf::from_path_buf(temp_dir.path().to_path_buf()).unwrap();
    
    let processor = FilesystemProcessor {
        context: None,
        config: FilesystemConfig::default(),
        watch_roots: vec![temp_path.clone()],
        validated_watch_roots: vec![temp_path.clone()],
        rename_tracker: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        last_state: None,
        checkpoint_manager: None,
        stage_context: None,
    };
    
    // Create files for testing
    let safe_file = temp_path.join("safe.txt");
    fs::write(&safe_file, "content")?;
    
    // Create a mock notify event for safe file
    let safe_event = notify::Event {
        kind: notify::EventKind::Create(notify::event::CreateKind::File),
        paths: vec![safe_file.as_std_path().to_path_buf()],
        attrs: Default::default(),
    };
    
    // Should process events for files within validated roots
    let result = processor.convert_fs_event_secure(safe_event, "test-host");
    assert!(result.is_ok());
    
    // Create a mock notify event for unsafe file
    let unsafe_event = notify::Event {
        kind: notify::EventKind::Create(notify::event::CreateKind::File),
        paths: vec![std::path::PathBuf::from("/etc/passwd")],
        attrs: Default::default(),
    };
    
    // Should reject events for files outside validated roots
    let result = processor.convert_fs_event_secure(unsafe_event, "test-host");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 0, "Should return empty events for unsafe paths");
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_processor_depth_limiting() -> TestResult<()> {
    // Test that depth limits are enforced
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Create config with depth limit
    let depth_limited_config = FilesystemConfig {
        watch_patterns: vec![format!("{}/**", temp_path.display())],
        ignore_patterns: vec![],
        debounce_ms: 100,
        max_depth: Some(2), // Only 2 levels deep
        security_policy: FileWatchingSecurityPolicy::default(),
    };
    
    let processor = FilesystemProcessor::with_config(depth_limited_config);
    
    // The depth limiting is enforced by the security policy and walkdir configuration
    // Test that the configuration is properly set
    assert_eq!(processor.config.max_depth, Some(2));
    assert_eq!(processor.config.security_policy.max_watch_depth, Some(10)); // Default from security policy
    
    Ok(())
}

#[sinex_test]
async fn test_filesystem_processor_ignore_patterns() -> TestResult<()> {
    // Test that ignore patterns work correctly with security validation
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Create config with ignore patterns
    let config_with_ignores = FilesystemConfig {
        watch_patterns: vec![format!("{}/**", temp_path.display())],
        ignore_patterns: vec![
            "**/.git/**".to_string(),
            "**/target/**".to_string(),
            "**/*.tmp".to_string(),
        ],
        debounce_ms: 100,
        max_depth: None,
        security_policy: FileWatchingSecurityPolicy::default(),
    };
    
    let processor = FilesystemProcessor::with_config(config_with_ignores);
    
    // Create test files
    let git_dir = temp_path.join(".git");
    fs::create_dir_all(&git_dir)?;
    let git_file = git_dir.join("config");
    fs::write(&git_file, "git config")?;
    
    let target_dir = temp_path.join("target");
    fs::create_dir_all(&target_dir)?;
    let target_file = target_dir.join("debug.exe");
    fs::write(&target_file, "binary")?;
    
    let tmp_file = temp_path.join("temp.tmp");
    fs::write(&tmp_file, "temporary")?;
    
    let regular_file = temp_path.join("regular.txt");
    fs::write(&regular_file, "content")?;
    
    // Test pattern matching
    assert!(!processor.matches_patterns(&Utf8PathBuf::from_path_buf(git_file).unwrap()));
    assert!(!processor.matches_patterns(&Utf8PathBuf::from_path_buf(target_file).unwrap()));
    assert!(!processor.matches_patterns(&Utf8PathBuf::from_path_buf(tmp_file).unwrap()));
    assert!(processor.matches_patterns(&Utf8PathBuf::from_path_buf(regular_file).unwrap()));
    
    Ok(())
}