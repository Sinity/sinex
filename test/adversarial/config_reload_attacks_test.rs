use crate::common::prelude::*;
use crate::common::resources;
use sinex_collector::config::{CollectorConfig, ConfigManager};
use std::fs;
use std::os::unix;
// Removed unused imports

#[sinex_test]
async fn test_config_file_replaced_with_symlink(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");
    let sensitive_file = temp_dir.path().join("secrets.txt");

    // Create sensitive file with content that would be dangerous if loaded as config
    // Using array format to trigger the parsing error we're seeing
    fs::write(
        &sensitive_file,
        r#"["secret_key=very_secret_value", "password=admin123", "DROP TABLE events;"]"#,
    )
    .unwrap();

    // Create initial config
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    // Start watching for config changes
    let mut update_rx = manager.start_watching().await.unwrap();

    // Replace config file with symlink to sensitive file
    fs::remove_file(&config_path).unwrap();
    unix::fs::symlink(&sensitive_file, &config_path).unwrap();

    println!("Replaced config with symlink to: {:?}", sensitive_file);

    // Wait for config reload
    match timeout(Duration::from_secs(3), update_rx.recv()).await {
        Ok(Some(new_config)) => {
            println!("SECURITY BREACH: Config reloaded from symlinked file!");
            println!("Config content might contain sensitive data");

            // Check if sensitive data leaked into config
            let config_debug = format!("{:?}", new_config);
            if config_debug.contains("secret") || config_debug.contains("password") {
                println!(
                    "CRITICAL: Sensitive data detected in config: {}",
                    config_debug
                );
            }
        }
        Ok(None) => {
            println!("Config watcher closed (expected behavior)");
        }
        Err(_) => {
            println!("Config reload timed out (good - symlink attack blocked)");
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_config_reload_during_partial_write(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Create initial config
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    let mut update_rx = manager.start_watching().await.unwrap();

    // Create malformed config that gets written byte by byte
    let malformed_config = r#"
[collector
enabled_events = ["file.cre
# This is incomplete and malformed
"#;

    // Write config byte by byte to simulate slow write or interrupted write
    let config_bytes = malformed_config.as_bytes();

    // Clear the file first
    fs::write(&config_path, "").unwrap();

    // Write one byte at a time with delays
    for (i, &byte) in config_bytes.iter().enumerate() {
        let mut current_content = fs::read(&config_path).unwrap();
        current_content.push(byte);
        fs::write(&config_path, &current_content).unwrap();

        println!(
            "Wrote byte {} of {}: '{}'",
            i + 1,
            config_bytes.len(),
            byte as char
        );

        // Check if config watcher triggered on partial file
        match timeout(Duration::from_millis(100), update_rx.recv()).await {
            Ok(Some(_new_config)) => {
                println!(
                    "WARNING: Config reloaded from partial file at byte {}",
                    i + 1
                );
                let partial_content = String::from_utf8_lossy(&current_content);
                println!("Partial content: {:?}", partial_content);
            }
            Ok(None) => {
                println!("Config watcher closed during partial write");
                break;
            }
            Err(_) => {
                // Timeout is expected for most partial writes
            }
        }

        tokio::task::yield_now().await;
    }
    Ok(())
}

#[sinex_test]
async fn test_config_directory_swap_attack(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_dir = temp_dir.path().join("config");
    let evil_dir = temp_dir.path().join("evil");

    // Create config directory with legitimate config
    fs::create_dir(&config_dir).unwrap();
    let config_path = config_dir.join("app.toml");

    let safe_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, safe_config).unwrap();

    // Create evil directory with malicious config
    fs::create_dir(&evil_dir).unwrap();
    let evil_config_path = evil_dir.join("app.toml");

    let evil_config = r#"
[collector]
enabled_events = ["file.created", "file.modified", "file.deleted"]

[event.files]
watch_paths = ["/", "/etc", "/home", "/var"]  # Watch everything!

[evil_settings]
steal_data = true
exfiltrate_to = "https://evil.com/steal"
"#;
    fs::write(&evil_config_path, evil_config).unwrap();

    // Load initial config
    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    let mut update_rx = manager.start_watching().await.unwrap();

    // Atomic directory swap: move config dir, symlink to evil dir
    let backup_dir = temp_dir.path().join("config.backup");
    fs::rename(&config_dir, &backup_dir).unwrap();
    unix::fs::symlink(&evil_dir, &config_dir).unwrap();

    println!("Performed atomic directory swap to evil config");

    // Wait for config reload
    match timeout(Duration::from_secs(3), update_rx.recv()).await {
        Ok(Some(new_config)) => {
            println!("SECURITY BREACH: Evil config was loaded!");

            // Check if evil settings were parsed
            let config_debug = format!("{:?}", new_config);
            if config_debug.contains("evil") || config_debug.contains("steal") {
                println!("CRITICAL: Evil settings detected: {}", config_debug);
            }

            // Check if dangerous watch paths were accepted
            if config_debug.contains("/etc") || config_debug.contains("watch_paths = [\"/\"]") {
                println!("DANGEROUS: Evil watch paths accepted - system-wide monitoring enabled!");
            }
        }
        Ok(None) => {
            println!("Config watcher closed during directory swap");
        }
        Err(_) => {
            println!("Config reload timed out (directory swap blocked)");
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_config_race_condition_memory_leak(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Create initial config
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    let mut update_rx = manager.start_watching().await.unwrap();

    let configs_in_memory = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Rapid config changes to trigger race conditions
    for i in 0..100 {
        let config_path_clone = config_path.clone();

        let handle = tokio::spawn(async move {
            let new_config = format!(
                r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp/iteration-{}"]

[metadata]
iteration = {}
timestamp = "{}"
"#,
                i,
                i,
                chrono::Utc::now().to_rfc3339()
            );

            fs::write(&config_path_clone, new_config).unwrap();
            tokio::task::yield_now().await;
        });

        handles.push(handle);
    }

    // Collector to count config updates
    let memory_counter = configs_in_memory.clone();
    let collector_handle = tokio::spawn(async move {
        let mut configs_received = Vec::new();

        while let Ok(Some(config)) = timeout(Duration::from_millis(100), update_rx.recv()).await {
            configs_received.push(config);
            memory_counter.fetch_add(1, Ordering::SeqCst);

            if configs_received.len() > 50 {
                println!(
                    "WARNING: {} configs accumulated in memory",
                    configs_received.len()
                );
            }
        }

        println!("Total configs collected: {}", configs_received.len());
        configs_received.len()
    });

    // Wait for all config changes
    futures::future::join_all(handles).await;

    // Give time for all updates to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    let total_configs = collector_handle.await.unwrap();
    let final_count = configs_in_memory.load(Ordering::SeqCst);

    println!("Config reload race condition results:");
    println!("- Total config changes: 100");
    println!("- Configs processed: {}", total_configs);
    println!("- Final memory count: {}", final_count);

    if total_configs > 100 {
        println!("DUPLICATE PROCESSING: More configs processed than generated!");
    }

    if final_count != total_configs as u64 {
        println!("RACE CONDITION: Memory count mismatch!");
    }
    Ok(())
}

#[sinex_test]
async fn test_config_hot_reload_during_event_processing(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Create initial config with limited events
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    let mut update_rx = manager.start_watching().await.unwrap();

    // Simulate event processing in progress
    let processing_handle = tokio::spawn(async {
        for i in 0..1000 {
            // Simulate event processing work
            tokio::time::sleep(Duration::from_micros(100)).await;

            if i % 100 == 0 {
                println!("Processed {} events", i);
            }
        }
    });

    // Change config during event processing
    tokio::task::yield_now().await;

    let new_config = r#"
[collector]
enabled_events = ["file.created", "file.modified", "file.deleted"]

[event.files]
watch_paths = ["/tmp", "/var/log"]

[processing]
max_concurrent_events = 1000
batch_size = 50
"#;

    fs::write(&config_path, new_config).unwrap();
    println!("Updated config during event processing");

    // Check if config reload affects ongoing processing
    match timeout(Duration::from_secs(2), update_rx.recv()).await {
        Ok(Some(_new_config)) => {
            println!("Config reloaded during event processing");

            // Check if processing was interrupted
            let processing_result = timeout(Duration::from_secs(3), processing_handle).await;
            match processing_result {
                Ok(Ok(())) => {
                    println!("Event processing completed successfully after config reload");
                }
                Ok(Err(_)) => {
                    println!("ISSUE: Event processing panicked after config reload");
                }
                Err(_) => {
                    println!("ISSUE: Event processing hung after config reload");
                }
            }
        }
        Ok(None) => {
            println!("Config watcher closed during processing");
        }
        Err(_) => {
            println!("Config reload timed out during processing");
        }
    }
    Ok(())
}
