// # Stream Processing Performance Tests
//
// Tests Redis Streams performance including message throughput,
// consumer group behavior, and stream processing latency.
// Focuses on the event streaming backbone of the Sinex system.

use redis::cmd;
use serde_json::json;
use sinex_satellite_sdk::RedisStreamClient;
use sinex_test_utils::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

/// Stream performance metrics
struct StreamMetrics {
    operation_times: HashMap<String, Vec<StdDuration>>,
    throughput_measurements: Vec<(Instant, usize, String)>,
    error_counts: HashMap<String, usize>,
    success_counts: HashMap<String, usize>,
    message_sizes: Vec<usize>,
    start_time: Instant,
}

impl StreamMetrics {
    fn new() -> Self {
        Self {
            operation_times: HashMap::new(),
            throughput_measurements: Vec::new(),
            error_counts: HashMap::new(),
            success_counts: HashMap::new(),
            message_sizes: Vec::new(),
            start_time: Instant::now(),
        }
    }

    fn record_operation(&mut self, operation: &str, duration: StdDuration, success: bool) {
        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }

        self.operation_times
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
    }

    fn record_throughput(&mut self, operation: &str, count: usize) {
        self.throughput_measurements
            .push((Instant::now(), count, operation.to_string()));
    }

    fn record_message_size(&mut self, size: usize) {
        self.message_sizes.push(size);
    }

    fn average_latency(&self, operation: &str) -> StdDuration {
        if let Some(times) = self.operation_times.get(operation) {
            if !times.is_empty() {
                return times.iter().sum::<StdDuration>() / times.len() as u32;
            }
        }
        StdDuration::from_millis(0)
    }

    fn percentile_latency(&self, operation: &str, percentile: f64) -> StdDuration {
        if let Some(times) = self.operation_times.get(operation) {
            if !times.is_empty() {
                let mut sorted = times.clone();
                sorted.sort();
                let index =
                    ((sorted.len() as f64 * percentile / 100.0) as usize).min(sorted.len() - 1);
                return sorted[index];
            }
        }
        StdDuration::from_millis(0)
    }

    fn calculate_throughput(&self, operation: &str) -> f64 {
        let success_count = self.success_counts.get(operation).unwrap_or(&0);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            *success_count as f64 / elapsed
        } else {
            0.0
        }
    }

    fn success_rate(&self, operation: &str) -> f64 {
        let success = self.success_counts.get(operation).unwrap_or(&0);
        let errors = self.error_counts.get(operation).unwrap_or(&0);
        let total = success + errors;
        if total > 0 {
            *success as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn average_message_size(&self) -> f64 {
        if self.message_sizes.is_empty() {
            0.0
        } else {
            self.message_sizes.iter().sum::<usize>() as f64 / self.message_sizes.len() as f64
        }
    }

    fn print_summary(&self) {
        println!("\n📊 Stream Performance Summary:");
        println!("Total test duration: {:?}", self.start_time.elapsed());
        println!(
            "Average message size: {:.1} bytes",
            self.average_message_size()
        );

        for operation in self.operation_times.keys() {
            println!("\n🔍 Operation: {}", operation);
            println!(
                "  - Success count: {}",
                self.success_counts.get(operation).unwrap_or(&0)
            );
            println!(
                "  - Error count: {}",
                self.error_counts.get(operation).unwrap_or(&0)
            );
            println!("  - Success rate: {:.2}%", self.success_rate(operation));
            println!(
                "  - Throughput: {:.2} ops/sec",
                self.calculate_throughput(operation)
            );
            println!("  - Average latency: {:?}", self.average_latency(operation));
            println!(
                "  - P50 latency: {:?}",
                self.percentile_latency(operation, 50.0)
            );
            println!(
                "  - P95 latency: {:?}",
                self.percentile_latency(operation, 95.0)
            );
            println!(
                "  - P99 latency: {:?}",
                self.percentile_latency(operation, 99.0)
            );
        }
    }
}

// =============================================================================
// Basic Stream Performance Tests
// =============================================================================

/// Test basic stream write performance
#[sinex_bench]
async fn test_stream_write_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let mut metrics = StreamMetrics::new();

    let stream_key = "sinex:performance:write-test";
    let message_count = 2000;

    println!(
        "✍️  Testing stream write performance with {} messages",
        message_count
    );

    // Clean up any existing stream
    let _ = redis_client.del(stream_key).await;

    for i in 0..message_count {
        let operation_start = Instant::now();

        let message_data = json!({
            "message_id": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "event_type": "stream.write.performance.test",
            "payload": format!("performance-message-{}", i),
            "metadata": {
                "test_run": "write_performance",
                "batch_id": i / 100,
                "worker_id": "main"
            }
        });

        let message_json = serde_json::to_string(&message_data).unwrap_or_default();
        metrics.record_message_size(message_json.len());

        match redis_client.xadd(stream_key, "*", &message_data).await {
            Ok(_) => {
                metrics.record_operation("stream_write", operation_start.elapsed(), true);
            }
            Err(e) => {
                metrics.record_operation("stream_write", operation_start.elapsed(), false);
                println!("Write {} failed: {}", i, e);
            }
        }

        if i % 200 == 0 {
            println!("  Written {} messages", i + 1);
            metrics.record_throughput("stream_write", i + 1);
        }
    }

    metrics.print_summary();

    // Verify stream length
    let stream_info = redis_client.xlen::<_, usize>(stream_key).await?;
    println!("🔍 Stream verification: {} messages in stream", stream_info);

    // Performance assertions
    assert!(
        metrics.calculate_throughput("stream_write") > 500.0,
        "Stream write throughput should be > 500 messages/sec"
    );
    assert!(
        metrics.average_latency("stream_write") < StdDuration::from_millis(5),
        "Average write latency should be < 5ms"
    );
    assert!(
        metrics.percentile_latency("stream_write", 95.0) < StdDuration::from_millis(20),
        "P95 write latency should be < 20ms"
    );
    assert!(
        metrics.success_rate("stream_write") > 99.0,
        "Write success rate should be > 99%"
    );

    println!("✅ Stream write performance test passed");
    Ok(())
}

/// Test stream read performance with consumer groups
#[sinex_bench]
async fn test_stream_read_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let mut metrics = StreamMetrics::new();

    let stream_key = "sinex:performance:read-test";
    let consumer_group = "read-performance-group";
    let consumer_name = "read-performance-consumer";
    let message_count = 1500;

    println!("📖 Testing stream read performance");

    // Clean up and populate stream
    let _ = redis_client.del(stream_key).await;

    println!("  Populating stream with {} messages", message_count);
    for i in 0..message_count {
        let message_data = json!({
            "message_id": i,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "event_type": "stream.read.performance.test",
            "payload": format!("read-test-message-{}", i),
            "size": format!("message-size-{}", i % 10)
        });

        redis_client.xadd(stream_key, "*", &message_data).await?;
    }

    // Create consumer group
    match redis_client
        .xgroup_create(stream_key, consumer_group, "0", true)
        .await
    {
        Ok(_) => println!("  Created consumer group: {}", consumer_group),
        Err(e) => println!("  Consumer group creation: {}", e),
    }

    // Test reading performance
    println!("  Testing read performance");

    let mut messages_read = 0;
    let batch_size = 50;

    while messages_read < message_count {
        let operation_start = Instant::now();

        match cmd("XREADGROUP")
            .arg("GROUP")
            .arg(consumer_group)
            .arg(consumer_name)
            .arg("COUNT")
            .arg(batch_size)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async::<_, redis::streams::StreamReadReply>(&mut redis_client)
            .await
        {
            Ok(messages) => {
                let read_duration = operation_start.elapsed();

                if messages.keys.is_empty() {
                    println!("  No more messages available");
                    break;
                }

                // Record performance for batch read
                metrics.record_operation("stream_read_batch", read_duration, true);

                // Acknowledge messages and measure ACK performance
                let ack_start = Instant::now();
                for message in &messages {
                    if let Err(e) = redis_client
                        .xack(stream_key, consumer_group, &message.id)
                        .await
                    {
                        println!("  ACK failed for {}: {}", message.id, e);
                    }
                }
                let ack_duration = ack_start.elapsed();

                metrics.record_operation("stream_ack_batch", ack_duration, true);

                messages_read += messages.keys.len();

                if messages_read % 200 == 0 {
                    println!("  Read {} messages", messages_read);
                    metrics.record_throughput("stream_read", messages_read);
                }
            }
            Err(e) => {
                metrics.record_operation("stream_read_batch", operation_start.elapsed(), false);
                println!("  Read failed: {}", e);
                break;
            }
        }
    }

    println!("  Total messages read: {}", messages_read);

    metrics.print_summary();

    // Performance assertions
    assert!(
        metrics.calculate_throughput("stream_read_batch") > 100.0,
        "Stream read throughput should be > 100 batches/sec"
    );
    assert!(
        metrics.average_latency("stream_read_batch") < StdDuration::from_millis(50),
        "Average read batch latency should be < 50ms"
    );
    assert!(
        metrics.average_latency("stream_ack_batch") < StdDuration::from_millis(20),
        "Average ACK batch latency should be < 20ms"
    );
    assert!(
        metrics.success_rate("stream_read_batch") > 95.0,
        "Read batch success rate should be > 95%"
    );
    assert!(
        messages_read >= message_count * 95 / 100,
        "Should read at least 95% of messages"
    );

    println!("✅ Stream read performance test passed");
    Ok(())
}

/// Test concurrent stream processing performance
#[sinex_bench]
async fn test_concurrent_stream_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let shared_metrics = Arc::new(Mutex::new(StreamMetrics::new()));

    let stream_key = "sinex:performance:concurrent-test";
    let consumer_group = "concurrent-performance-group";
    let producer_count = 5;
    let consumer_count = 3;
    let messages_per_producer = 300;

    println!("🔄 Testing concurrent stream performance:");
    println!("  - Producers: {}", producer_count);
    println!("  - Consumers: {}", consumer_count);
    println!("  - Messages per producer: {}", messages_per_producer);

    // Clean up existing stream
    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let _ = redis_client.del(stream_key).await;

    // Create consumer group
    match redis_client
        .xgroup_create(stream_key, consumer_group, "$", true)
        .await
    {
        Ok(_) => println!("  Created consumer group"),
        Err(e) => println!("  Consumer group setup: {}", e),
    }

    // Start concurrent producers
    let producer_handles = (0..producer_count)
        .map(|producer_id| {
            let metrics = shared_metrics.clone();
            let stream_key = stream_key.to_string();

            tokio::spawn(async move {
                let redis_client = RedisStreamClient::new("redis://localhost:6379")?;

                for msg_id in 0..messages_per_producer {
                    let operation_start = Instant::now();

                    let message_data = json!({
                        "producer_id": producer_id,
                        "message_id": msg_id,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "event_type": "concurrent.stream.test",
                        "payload": format!("concurrent-message-{}-{}", producer_id, msg_id),
                        "data_size": "x".repeat((msg_id % 10) * 100) // Variable message sizes
                    });

                    match redis_client.xadd(&stream_key, "*", &message_data).await {
                        Ok(_) => {
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_operation(
                                "concurrent_write",
                                operation_start.elapsed(),
                                true,
                            );
                            metrics_lock.record_message_size(
                                serde_json::to_string(&message_data)
                                    .unwrap_or_default()
                                    .len(),
                            );
                        }
                        Err(e) => {
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_operation(
                                "concurrent_write",
                                operation_start.elapsed(),
                                false,
                            );
                            println!("Producer {} message {} failed: {}", producer_id, msg_id, e);
                        }
                    }

                    // Small delay between messages
                    tokio::time::sleep(StdDuration::from_millis(2)).await;
                }

                println!("  Producer {} completed", producer_id);
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
        })
        .collect::<Vec<_>>();

    // Start concurrent consumers
    let consumer_handles = (0..consumer_count)
        .map(|consumer_id| {
            let metrics = shared_metrics.clone();
            let stream_key = stream_key.to_string();
            let consumer_group = consumer_group.to_string();

            tokio::spawn(async move {
                let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
                let consumer_name = format!("concurrent-consumer-{}", consumer_id);
                let mut messages_consumed = 0;

                // Consumer runs for a fixed duration
                let consumer_duration = StdDuration::from_secs(20);
                let start_time = Instant::now();

                while start_time.elapsed() < consumer_duration {
                    let operation_start = Instant::now();

                    match cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(&consumer_group)
                        .arg(&consumer_name)
                        .arg("COUNT")
                        .arg(20)
                        .arg("STREAMS")
                        .arg(&stream_key)
                        .arg(">")
                        .query_async(&mut redis_client)
                        .await
                    {
                        Ok(messages) => {
                            let read_duration = operation_start.elapsed();

                            if !messages.keys.is_empty() {
                                // Acknowledge messages
                                for message in &messages {
                                    let _ = redis_client
                                        .xack(&stream_key, &consumer_group, &message.id)
                                        .await;
                                }

                                messages_consumed += messages.keys.len();

                                let mut metrics_lock = metrics.lock().await;
                                metrics_lock.record_operation(
                                    "concurrent_read",
                                    read_duration,
                                    true,
                                );
                            }
                        }
                        Err(e) => {
                            let mut metrics_lock = metrics.lock().await;
                            metrics_lock.record_operation(
                                "concurrent_read",
                                operation_start.elapsed(),
                                false,
                            );
                            println!("Consumer {} read failed: {}", consumer_id, e);
                        }
                    }

                    // Small delay between reads
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                }

                println!(
                    "  Consumer {} completed: {} messages consumed",
                    consumer_id, messages_consumed
                );
                Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(messages_consumed)
            })
        })
        .collect::<Vec<_>>();

    // Wait for all producers to complete
    let producer_results = futures::future::join_all(producer_handles).await;
    println!("  All producers completed");

    // Give consumers a bit more time to process
    tokio::time::sleep(StdDuration::from_secs(3)).await;

    // Wait for consumers to complete
    let consumer_results = futures::future::join_all(consumer_handles).await;

    let total_consumed: usize = consumer_results
        .into_iter()
        .filter_map(|r| r.ok().and_then(|inner| inner.ok()))
        .sum();

    println!("  Total messages consumed: {}", total_consumed);

    let final_metrics = shared_metrics.lock().await;
    final_metrics.print_summary();

    // Verify stream state
    let final_stream_length = redis_client.xlen::<_, usize>(stream_key).await?;
    println!("🔍 Final stream length: {}", final_stream_length);

    // Performance assertions
    assert!(
        final_metrics.calculate_throughput("concurrent_write") > 200.0,
        "Concurrent write throughput should be > 200 messages/sec"
    );
    assert!(
        final_metrics.calculate_throughput("concurrent_read") > 50.0,
        "Concurrent read throughput should be > 50 operations/sec"
    );
    assert!(
        final_metrics.average_latency("concurrent_write") < StdDuration::from_millis(10),
        "Concurrent write latency should be < 10ms"
    );
    assert!(
        final_metrics.average_latency("concurrent_read") < StdDuration::from_millis(100),
        "Concurrent read latency should be < 100ms"
    );
    assert!(
        final_metrics.success_rate("concurrent_write") > 95.0,
        "Concurrent write success rate should be > 95%"
    );
    assert!(
        final_metrics.success_rate("concurrent_read") > 90.0,
        "Concurrent read success rate should be > 90%"
    );

    println!("✅ Concurrent stream performance test passed");
    Ok(())
}

/// Test stream performance with varying message sizes
#[sinex_bench]
async fn test_variable_message_size_performance(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let redis_client = RedisStreamClient::new("redis://localhost:6379")?;
    let mut metrics = StreamMetrics::new();

    let stream_key = "sinex:performance:variable-size-test";

    println!("📏 Testing stream performance with variable message sizes");

    // Clean up stream
    let _ = redis_client.del(stream_key).await;

    // Test different message sizes
    let size_tests = vec![
        (100, "Small (100B)"),
        (1_000, "Medium (1KB)"),
        (10_000, "Large (10KB)"),
        (100_000, "Extra Large (100KB)"),
    ];

    let messages_per_size = 100;

    for (size_bytes, size_label) in size_tests {
        println!("\n📦 Testing {} messages", size_label);

        let payload_data = "x".repeat(size_bytes);

        for i in 0..messages_per_size {
            let operation_start = Instant::now();

            let message_data = json!({
                "message_id": i,
                "size_category": size_label,
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "event_type": "variable.size.test",
                "payload_data": payload_data,
                "metadata": {
                    "size_bytes": size_bytes,
                    "test_iteration": i
                }
            });

            let message_json = serde_json::to_string(&message_data).unwrap_or_default();
            metrics.record_message_size(message_json.len());

            let operation_key = format!("write_{}", size_label);

            match redis_client.xadd(stream_key, "*", &message_data).await {
                Ok(_) => {
                    metrics.record_operation(&operation_key, operation_start.elapsed(), true);
                }
                Err(e) => {
                    metrics.record_operation(&operation_key, operation_start.elapsed(), false);
                    println!("  Message {} failed: {}", i, e);
                }
            }

            if i % 20 == 0 {
                println!("    Processed {} {} messages", i + 1, size_label);
            }
        }

        println!("  {} messages completed", size_label);
        println!(
            "    Average latency: {:?}",
            metrics.average_latency(&format!("write_{}", size_label))
        );
        println!(
            "    P95 latency: {:?}",
            metrics.percentile_latency(&format!("write_{}", size_label), 95.0)
        );
    }

    metrics.print_summary();

    // Test reading variable size messages
    println!("\n📖 Testing read performance for variable sizes");

    let consumer_group = "variable-size-group";
    let consumer_name = "variable-size-consumer";

    match redis_client
        .xgroup_create(stream_key, consumer_group, "0", true)
        .await
    {
        Ok(_) => println!("  Created consumer group"),
        Err(e) => println!("  Consumer group: {}", e),
    }

    let mut total_read = 0;
    let batch_size = 10;

    while total_read < size_tests.len() * messages_per_size {
        let operation_start = Instant::now();

        match cmd("XREADGROUP")
            .arg("GROUP")
            .arg(consumer_group)
            .arg(consumer_name)
            .arg("COUNT")
            .arg(batch_size)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(">")
            .query_async::<_, redis::streams::StreamReadReply>(&mut redis_client)
            .await
        {
            Ok(messages) => {
                if messages.keys.is_empty() {
                    break;
                }

                metrics.record_operation("read_variable", operation_start.elapsed(), true);

                // ACK messages
                for message in &messages {
                    let _ = redis_client
                        .xack(stream_key, consumer_group, &message.id)
                        .await;
                }

                total_read += messages.keys.len();

                if total_read % 50 == 0 {
                    println!("    Read {} variable size messages", total_read);
                }
            }
            Err(e) => {
                metrics.record_operation("read_variable", operation_start.elapsed(), false);
                println!("  Read failed: {}", e);
                break;
            }
        }
    }

    println!("  Total variable size messages read: {}", total_read);

    metrics.print_summary();

    // Performance assertions
    // Small messages should be faster than large messages
    let small_latency = metrics.average_latency("write_Small (100B)");
    let large_latency = metrics.average_latency("write_Extra Large (100KB)");

    println!("📊 Size comparison:");
    println!("  Small message latency: {:?}", small_latency);
    println!("  Large message latency: {:?}", large_latency);

    assert!(
        large_latency > small_latency,
        "Large messages should have higher latency than small messages"
    );

    // All sizes should maintain reasonable performance
    for (_, size_label) in &size_tests {
        let operation_key = format!("write_{}", size_label);
        assert!(
            metrics.success_rate(&operation_key) > 95.0,
            "{} success rate should be > 95%",
            size_label
        );
        assert!(
            metrics.average_latency(&operation_key) < StdDuration::from_millis(50),
            "{} average latency should be < 50ms",
            size_label
        );
    }

    assert!(
        metrics.success_rate("read_variable") > 95.0,
        "Variable size read success rate should be > 95%"
    );

    println!("✅ Variable message size performance test passed");
    Ok(())
}
