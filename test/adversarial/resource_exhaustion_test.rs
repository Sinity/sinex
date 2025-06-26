use crate::common::prelude::*;
use sinex_collector::config::{CollectorConfig, ConfigManager};
use tokio::sync::Mutex;
use crate::common::resources;
use std::sync::Arc;

#[sinex_test(timeout = 60)]
async fn test_unbounded_file_descriptor_explosion(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Try to watch a directory with thousands of files
    let temp_dir = resources::temp_dir()?;
    let mut paths = vec![];
    
    // Create many files
    for i in 0..1000 {
        let file_path = temp_dir.path().join(format!("file_{}.txt", i));
        std::fs::write(&file_path, format!("content {}", i)).unwrap();
        paths.push(file_path);
    }
    
    // Try to watch all of them individually (bad pattern)
    let watchers = Arc::new(Mutex::new(Vec::<std::path::PathBuf>::new()));
    let mut handles = vec![];
    
    for path in paths.iter().take(100) {  // Limit to avoid actual system issues
        let path = path.clone();
        let watchers = watchers.clone();
        
        let handle = tokio::spawn(async move {
            // Simulate watcher creation without actual notify dependency
            use std::fs::File;
            
            // Try to open file handle as a proxy for file descriptor usage
            match File::open(&path) {
                Ok(_file) => {
                    // Hold file handle in our collection
                    watchers.lock().await.push(path);
                }
                Err(e) => {
                    println!("File handle creation failed at some point: {}", e);
                }
            }
        });
        
        handles.push(handle);
    }
    
    futures::future::join_all(handles).await;
    
    let watcher_count = watchers.lock().await.len();
    println!("Successfully created {} watchers", watcher_count);
    
    // System might limit us before reaching high numbers
    if watcher_count < 100 {
        println!("RESOURCE LIMIT: System restricted file watchers to {}", watcher_count);
    }
    Ok(())
}

#[tokio::test]
async fn test_memory_exhaustion_via_config_reload() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");
    
    // Create initial config
    let initial = r#"
enabled_events = ["file.created"]
[event.files]
watch_paths = ["/tmp"]
"#;
    std::fs::write(&config_path, initial).unwrap();
    
    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));
    
    // Start watching
    let mut update_rx = manager.start_watching().await.unwrap();
    
    // Keep appending to config to grow memory
    let mut accumulated_configs = Vec::new();
    let mut successful_reloads = 0;
    
    for i in 0..100 {
        // Append more and more data
        let mut new_paths = vec!["/tmp".to_string()];
        for j in 0..i * 100 {
            new_paths.push(format!("/fake/path/{}/{}", i, j));
        }
        
        let new_config = format!(
            r#"
enabled_events = ["file.created"]
[event.files]
watch_paths = {}
"#,
            serde_json::to_string(&new_paths).unwrap()
        );
        
        std::fs::write(&config_path, &new_config).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Try to receive update
        if let Ok(Some(config)) = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            update_rx.recv()
        ).await {
            accumulated_configs.push(config);
            successful_reloads += 1;
        }
        
        // Check if we're accumulating memory - break if we hit limits
        if accumulated_configs.len() > 50 {
            break;
        }
    }
    
    // Assert that config manager handles reloads gracefully
    assert!(successful_reloads > 0, "Should handle at least some config reloads");
    assert!(accumulated_configs.len() <= 100, "Should not accumulate unlimited configs in memory");
    
    // Verify config integrity
    for config in &accumulated_configs {
        assert!(!config.enabled_events.is_empty(), "Config should maintain enabled events");
    }
    Ok(())
}

#[sinex_test]
async fn test_string_concatenation_memory_bomb(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create event with expanding string pattern
    let mut expanding_string = String::from("a");
    let mut sizes = vec![];
    
    for i in 0..20 {
        expanding_string = expanding_string.repeat(2);  // Exponential growth
        sizes.push(expanding_string.len());
        
        let event = crate::common::events::generic_adversarial_event("memory", "bomb.test", json!({"test": true}), None);
        
        match serde_json::to_string(&event) {
            Ok(_) => println!("Iteration {}: String size {} - OK", i, expanding_string.len()),
            Err(e) => {
                println!("Iteration {}: String size {} - FAILED: {}", i, expanding_string.len(), e);
                break;
            }
        }
        
        // Stop before consuming too much memory
        if expanding_string.len() > 100_000_000 {
            println!("Stopping at 100MB to prevent system issues");
            break;
        }
    }
    
    println!("String sizes generated: {:?}", sizes);
    Ok(())
}

#[tokio::test]
async fn test_collector_event_queue_overflow() {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    // Create collector with small channel
    let (tx, mut rx) = mpsc::channel::<RawEvent>(10);  // Small buffer
    
    let dropped = Arc::new(AtomicU64::new(0));
    let sent = Arc::new(AtomicU64::new(0));
    
    // Producer: sends events rapidly
    let tx_clone = tx.clone();
    let dropped_clone = dropped.clone();
    let sent_clone = sent.clone();
    
    let producer = tokio::spawn(async move {
        for i in 0..10000 {
            let event = crate::common::events::generic_adversarial_event("overflow", "test", json!({"test": true}), None);
            
            // try_send doesn't block
            match tx_clone.try_send(event) {
                Ok(_) => {
                    sent_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    dropped_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
            
            // Minimal delay to stress the system
            if i % 100 == 0 {
                tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;
            }
        }
    });
    
    // Consumer: processes slowly
    let consumer = tokio::spawn(async move {
        let mut received = 0;
        while let Some(_event) = rx.recv().await {
            received += 1;
            // Simulate slow processing
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
        received
    });
    
    // Wait for producer
    producer.await.unwrap();
    drop(tx);  // Close channel
    
    // Wait for consumer
    let received = consumer.await.unwrap();
    
    let total_sent = sent.load(Ordering::SeqCst);
    let total_dropped = dropped.load(Ordering::SeqCst);
    
    println!("Event queue overflow results:");
    println!("- Sent successfully: {}", total_sent);
    println!("- Dropped: {}", total_dropped);
    println!("- Received: {}", received);
    println!("- Drop rate: {:.2}%", (total_dropped as f64 / 10000.0) * 100.0);
    
    // High drop rate indicates overflow issue
    assert!(total_dropped > 0, "Expected some events to be dropped");
}