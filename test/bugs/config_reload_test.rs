use sinex_collector::config::{CollectorConfig, ConfigManager};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_config_reload_race_condition() {
    // Create a config manager with a test config file
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    
    let initial_config = r#"
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    
    std::fs::write(&config_path, initial_config).unwrap();
    
    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));
    
    // Start watching for changes
    let mut update_rx = manager.start_watching().await.unwrap();
    
    // Rapidly update config multiple times
    for i in 0..10 {
        let new_config = format!(r#"
enabled_events = ["file.created", "file.modified"]

[event.files]
watch_paths = ["/tmp", "/home/user{}"]
"#, i);
        
        std::fs::write(&config_path, new_config).unwrap();
        
        // Small delay to trigger filesystem events
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    // Try to receive all updates
    let mut update_count = 0;
    while let Ok(Some(_)) = timeout(Duration::from_secs(2), update_rx.recv()).await {
        update_count += 1;
    }
    
    // We wrote 10 times but may receive fewer due to debouncing
    // This might expose race conditions in the watcher
    println!("Config updates received: {}/10", update_count);
    
    // Final config should have the last value
    let final_config = manager.get_config().await;
    assert!(final_config.enabled_events.contains(&"file.modified".to_string()));
}

#[tokio::test]
async fn test_config_malformed_handling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("config.toml");
    
    // Start with valid config
    let valid_config = r#"enabled_events = ["file.created"]"#;
    std::fs::write(&config_path, valid_config).unwrap();
    
    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));
    let mut update_rx = manager.start_watching().await.unwrap();
    
    // Write invalid TOML
    std::fs::write(&config_path, "this is not valid TOML!").unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // The watcher should handle this gracefully
    // But the current implementation might panic!
    let _update_result = timeout(Duration::from_secs(1), update_rx.recv()).await;
    
    // Should still have the old valid config
    let current_config = manager.get_config().await;
    assert_eq!(current_config.enabled_events.len(), 1);
}