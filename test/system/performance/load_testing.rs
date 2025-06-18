//! Load testing suite for Sinex
//! Tests system behavior under various load conditions

#![cfg(feature = "test_common")] // Disable entire file - missing sinex_test_common dependency

// use sinex_test_common::{setup_test_env, TestEnv};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_sustained_10k_events_per_second() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    let collector = env.start_collector_with_config(|cfg| {
        cfg.event_batch_size = 1000;
        cfg.batch_timeout_ms = 100;
        cfg.channel_buffer_size = 100_000;
    }).await?;
    
    let worker = env.start_worker_with_config(|cfg| {
        cfg.concurrency = 16;
        cfg.batch_size = 500;
    }).await?;
    
    // Event counter
    let events_sent = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    
    // Generate events at 10k/sec for 10 seconds
    let target_rate = 10_000u64;
    let duration_secs = 10;
    let mut tasks = JoinSet::new();
    
    // Use multiple tasks to achieve high throughput
    let num_generators = 10;
    let events_per_generator = target_rate / num_generators;
    
    for generator_id in 0..num_generators {
        let env_clone = env.clone();
        let counter = events_sent.clone();
        
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(
                Duration::from_micros(1_000_000 / events_per_generator)
            );
            
            for i in 0..(events_per_generator * duration_secs) {
                interval.tick().await;
                
                let event = env_clone.create_event(
                    "load_test",
                    serde_json::json!({
                        "generator": generator_id,
                        "sequence": i,
                        "timestamp": Instant::now()
                    })
                );
                
                if env_clone.send_event(event).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
    
    // Wait for all generators to complete
    while let Some(result) = tasks.join_next().await {
        result?;
    }
    
    let elapsed = start_time.elapsed();
    let total_sent = events_sent.load(Ordering::Relaxed);
    let actual_rate = (total_sent as f64) / elapsed.as_secs_f64();
    
    println!("Sent {} events in {:?} ({:.2} events/sec)", 
             total_sent, elapsed, actual_rate);
    
    // Allow time for processing
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Verify events were processed
    let processed_count = env.count_events().await?;
    let processing_rate = (processed_count as f64) / (elapsed.as_secs_f64() + 5.0);
    
    println!("Processed {} events ({:.2}% success rate, {:.2} events/sec)",
             processed_count,
             (processed_count as f64 / total_sent as f64) * 100.0,
             processing_rate);
    
    // Success criteria
    assert!(actual_rate >= 9_000.0, "Failed to achieve target send rate");
    assert!(processed_count >= (total_sent * 95 / 100), "Too many events lost");
    assert!(collector.is_healthy().await?, "Collector unhealthy after load");
    assert!(worker.is_healthy().await?, "Worker unhealthy after load");
    
    Ok(())
}

#[tokio::test]
async fn test_burst_100k_events() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    
    // Configure for burst handling
    let collector = env.start_collector_with_config(|cfg| {
        cfg.event_batch_size = 5000;
        cfg.batch_timeout_ms = 50;
        cfg.channel_buffer_size = 200_000;
    }).await?;
    
    let worker = env.start_worker_with_config(|cfg| {
        cfg.concurrency = 32;
        cfg.batch_size = 1000;
    }).await?;
    
    // Prepare burst
    let burst_size = 100_000;
    let mut events = Vec::with_capacity(burst_size);
    
    for i in 0..burst_size {
        events.push(env.create_event(
            "burst_test",
            serde_json::json!({
                "sequence": i,
                "burst_id": "test_100k"
            })
        ));
    }
    
    // Send burst
    let start = Instant::now();
    let semaphore = Arc::new(Semaphore::new(1000)); // Limit concurrent sends
    let mut tasks = JoinSet::new();
    
    for event in events {
        let permit = semaphore.clone().acquire_owned().await?;
        let env_clone = env.clone();
        
        tasks.spawn(async move {
            let _permit = permit;
            env_clone.send_event(event).await
        });
    }
    
    // Wait for all sends to complete
    let mut send_failures = 0;
    while let Some(result) = tasks.join_next().await {
        if result?.is_err() {
            send_failures += 1;
        }
    }
    
    let send_duration = start.elapsed();
    println!("Sent {} events in {:?} ({} failures)",
             burst_size, send_duration, send_failures);
    
    // Allow processing time
    let process_start = Instant::now();
    let mut last_count = 0;
    let mut stable_iterations = 0;
    
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let current_count = env.count_events().await?;
        
        if current_count == last_count {
            stable_iterations += 1;
            if stable_iterations >= 3 {
                break;
            }
        } else {
            stable_iterations = 0;
        }
        
        last_count = current_count;
        
        if process_start.elapsed() > Duration::from_secs(60) {
            break;
        }
    }
    
    let process_duration = process_start.elapsed();
    let processed = env.count_events().await?;
    
    println!("Processed {} events in {:?} ({:.2}% success)",
             processed, process_duration,
             (processed as f64 / burst_size as f64) * 100.0);
    
    // Success criteria
    assert!(send_failures < (burst_size / 100), "Too many send failures");
    assert!(processed >= (burst_size * 90 / 100), "Too many events lost");
    assert!(collector.is_healthy().await?, "Collector unhealthy after burst");
    assert!(worker.is_healthy().await?, "Worker unhealthy after burst");
    
    Ok(())
}

#[tokio::test]
async fn test_worker_scaling_behavior() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    let collector = env.start_collector().await?;
    
    // Start with minimal workers
    let worker_manager = env.start_worker_manager_with_config(|cfg| {
        cfg.min_workers = 2;
        cfg.max_workers = 32;
        cfg.scale_up_threshold = 1000; // Queue size
        cfg.scale_down_threshold = 100;
    }).await?;
    
    // Verify initial state
    let initial_workers = worker_manager.get_worker_count().await?;
    assert_eq!(initial_workers, 2);
    
    // Generate moderate load
    for _ in 0..5000 {
        env.send_event(env.create_event("scale_test", json!({}))).await?;
    }
    
    // Allow time for scaling
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Should have scaled up
    let scaled_workers = worker_manager.get_worker_count().await?;
    assert!(scaled_workers > initial_workers, "Workers did not scale up");
    
    // Wait for queue to drain
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // Should scale back down
    let final_workers = worker_manager.get_worker_count().await?;
    assert!(final_workers < scaled_workers, "Workers did not scale down");
    
    // Verify all events processed
    let processed = env.count_events().await?;
    assert!(processed >= 4500, "Events lost during scaling");
    
    Ok(())
}

#[tokio::test]
async fn test_database_query_performance() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    
    // Populate database with test data
    let num_events = 1_000_000;
    env.populate_test_events(num_events).await?;
    
    // Test various query patterns
    let queries = vec![
        ("Recent events", "SELECT * FROM raw.events ORDER BY created_at DESC LIMIT 100"),
        ("Count by source", "SELECT source, COUNT(*) FROM raw.events GROUP BY source"),
        ("Time range", "SELECT * FROM raw.events WHERE created_at > NOW() - INTERVAL '1 hour'"),
        ("JSON search", "SELECT * FROM raw.events WHERE payload->>'type' = 'test'"),
        ("ULID range", "SELECT * FROM raw.events WHERE id > '01234567890123456789012345' LIMIT 1000"),
    ];
    
    for (name, query) in queries {
        let start = Instant::now();
        let result = env.execute_query(query).await?;
        let duration = start.elapsed();
        
        println!("{}: {} rows in {:?}", name, result.len(), duration);
        
        // All queries should complete within 100ms
        assert!(duration < Duration::from_millis(100),
                "{} query too slow: {:?}", name, duration);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_memory_usage_under_load() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    let collector = env.start_collector().await?;
    let worker = env.start_worker().await?;
    
    // Capture initial memory usage
    let initial_memory = env.get_memory_usage().await?;
    println!("Initial memory: {} MB", initial_memory / 1_048_576);
    
    // Generate sustained load
    let duration = Duration::from_secs(30);
    let start = Instant::now();
    let mut events_sent = 0u64;
    
    while start.elapsed() < duration {
        for _ in 0..100 {
            env.send_event(env.create_event("memory_test", json!({
                "data": "x".repeat(1000) // 1KB payload
            }))).await?;
            events_sent += 1;
        }
        
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    
    // Check memory usage
    let peak_memory = env.get_memory_usage().await?;
    let memory_increase = peak_memory - initial_memory;
    
    println!("Sent {} events", events_sent);
    println!("Peak memory: {} MB (increase: {} MB)",
             peak_memory / 1_048_576,
             memory_increase / 1_048_576);
    
    // Memory should not grow unbounded
    let max_expected_memory = 512 * 1_048_576; // 512 MB
    assert!(peak_memory < initial_memory + max_expected_memory,
            "Memory usage too high");
    
    // Allow cleanup
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // Memory should return close to baseline
    let final_memory = env.get_memory_usage().await?;
    assert!(final_memory < initial_memory + (100 * 1_048_576),
            "Memory not released after load");
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_source_performance() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    
    // Start collector with multiple sources
    let collector = env.start_collector_with_sources(vec![
        "filesystem",
        "terminal",
        "clipboard",
        "window_manager",
        "process_monitor",
    ]).await?;
    
    let worker = env.start_worker().await?;
    
    // Generate events from all sources concurrently
    let mut tasks = JoinSet::new();
    let events_per_source = 10_000;
    let sources = vec![
        "filesystem", "terminal", "clipboard", 
        "window_manager", "process_monitor"
    ];
    
    for source in sources {
        let env_clone = env.clone();
        tasks.spawn(async move {
            let start = Instant::now();
            
            for i in 0..events_per_source {
                let event = env_clone.create_event(source, json!({
                    "sequence": i,
                    "source": source,
                }));
                env_clone.send_event(event).await?;
            }
            
            Ok::<_, anyhow::Error>((source, start.elapsed()))
        });
    }
    
    // Collect results
    let mut source_times = Vec::new();
    while let Some(result) = tasks.join_next().await {
        source_times.push(result??);
    }
    
    // Print performance by source
    for (source, duration) in &source_times {
        let rate = events_per_source as f64 / duration.as_secs_f64();
        println!("{}: {:.2} events/sec", source, rate);
    }
    
    // Wait for processing
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // Verify all events processed
    let total_expected = events_per_source * source_times.len();
    let processed = env.count_events().await?;
    
    println!("Total processed: {} / {} ({:.2}%)",
             processed, total_expected,
             (processed as f64 / total_expected as f64) * 100.0);
    
    assert!(processed >= (total_expected * 95 / 100),
            "Too many events lost from concurrent sources");
    
    Ok(())
}

#[tokio::test]
async fn test_backpressure_handling() -> anyhow::Result<()> {
    let env = setup_test_env().await?;
    
    // Start collector with small buffer
    let collector = env.start_collector_with_config(|cfg| {
        cfg.channel_buffer_size = 1000;
        cfg.event_batch_size = 100;
    }).await?;
    
    // Start slow worker
    let worker = env.start_worker_with_config(|cfg| {
        cfg.concurrency = 1;
        cfg.batch_size = 10;
        cfg.processing_delay_ms = Some(100); // Simulate slow processing
    }).await?;
    
    // Send events faster than can be processed
    let mut send_results = Vec::new();
    let start = Instant::now();
    
    for i in 0..5000 {
        let event = env.create_event("backpressure", json!({"seq": i}));
        let result = env.send_event_with_timeout(event, Duration::from_millis(50)).await;
        send_results.push((i, result.is_ok(), start.elapsed()));
    }
    
    // Analyze backpressure behavior
    let successful_sends = send_results.iter().filter(|(_, ok, _)| *ok).count();
    let first_rejection = send_results.iter()
        .position(|(_, ok, _)| !ok)
        .unwrap_or(send_results.len());
    
    println!("Successful sends: {} / {}", successful_sends, send_results.len());
    println!("First rejection at event: {}", first_rejection);
    
    // Should apply backpressure before running out of memory
    assert!(first_rejection < 2000, "Backpressure applied too late");
    assert!(successful_sends > 1000, "Too aggressive backpressure");
    
    // System should remain healthy
    assert!(collector.is_healthy().await?);
    assert!(worker.is_healthy().await?);
    
    Ok(())
}