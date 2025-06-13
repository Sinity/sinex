use sinex_collector::config::{CollectorConfig, ConfigManager};
use sinex_db::models::RawEvent;
use sinex_ulid::Ulid;
use std::sync::Arc;
use tokio::sync::Mutex;
use tempfile::TempDir;

#[tokio::test]
async fn test_unbounded_file_descriptor_explosion() {
    // Try to watch a directory with thousands of files
    let temp_dir = TempDir::new().unwrap();
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
}

#[tokio::test]
async fn test_memory_exhaustion_via_config_reload() {
    let temp_dir = TempDir::new().unwrap();
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
            println!("Config {} loaded, total in memory: {}", i, accumulated_configs.len());
        }
        
        // Check if we're accumulating memory
        if accumulated_configs.len() > 50 {
            println!("WARNING: Config updates accumulating in memory!");
            break;
        }
    }
}

#[test]
fn test_json_depth_stack_overflow() {
    // Create extremely deeply nested JSON
    fn create_nested_json(depth: usize) -> serde_json::Value {
        if depth == 0 {
            serde_json::json!({"value": "bottom"})
        } else {
            serde_json::json!({
                "level": depth,
                "nested": create_nested_json(depth - 1)
            })
        }
    }
    
    // Test increasing depths
    for depth in [100, 500, 1000, 5000, 10000] {
        println!("Testing JSON depth: {}", depth);
        
        let result = std::panic::catch_unwind(|| {
            let nested = create_nested_json(depth);
            let serialized = serde_json::to_string(&nested);
            
            match serialized {
                Ok(json_str) => {
                    println!("  Serialized to {} bytes", json_str.len());
                    
                    // Try to parse it back
                    match serde_json::from_str::<serde_json::Value>(&json_str) {
                        Ok(_) => println!("  Parsed successfully"),
                        Err(e) => println!("  Parse failed: {}", e),
                    }
                }
                Err(e) => println!("  Serialization failed: {}", e),
            }
        });
        
        if result.is_err() {
            println!("  STACK OVERFLOW at depth {}", depth);
            break;
        }
    }
}

#[test]
fn test_string_concatenation_memory_bomb() {
    use sinex_db::models::RawEvent;
    use sinex_ulid::Ulid;
    
    // Create event with expanding string pattern
    let mut expanding_string = String::from("a");
    let mut sizes = vec![];
    
    for i in 0..20 {
        expanding_string = expanding_string.repeat(2);  // Exponential growth
        sizes.push(expanding_string.len());
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "memory".to_string(),
            event_type: "bomb.test".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: serde_json::json!({
                "iteration": i,
                "data": &expanding_string[..expanding_string.len().min(1000)], // Cap for test
                "actual_size": expanding_string.len()
            }),
        };
        
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
}

#[tokio::test]
async fn test_collector_event_queue_overflow() {
    use tokio::sync::mpsc;
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
            let event = RawEvent {
                id: Ulid::new(),
                source: "overflow".to_string(),
                event_type: "test".to_string(),
                ts_ingest: chrono::Utc::now(),
                ts_orig: None,
                host: "test".to_string(),
                ingestor_version: None,
                payload_schema_id: None,
                payload: serde_json::json!({"seq": i}),
            };
            
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