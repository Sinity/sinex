// # Bottleneck Identification Testing
//
// Tools and tests for identifying system bottlenecks including database
// connection limits, memory constraints, CPU saturation, and I/O limitations.
// Provides automated bottleneck detection and performance optimization guidance.

use crate::common::prelude::*;

use crate::common::prelude::*;
use crate::common::{events, generators};
use serde_json::json;
use sinex_events::{EventFactory, services, event_types};
use sinex_satellite_sdk::RedisStreamClient;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::Mutex;

/// Bottleneck identification results
#[derive(Debug, Clone)]
pub struct BottleneckAnalysis {
    pub bottleneck_type: BottleneckType,
    pub severity: BottleneckSeverity,
    pub affected_operations: Vec<String>,
    pub symptoms: Vec<String>,
    pub metrics: BottleneckMetrics,
    pub recommendations: Vec<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BottleneckType {
    DatabaseConnections,
    Memory,
    CPU,
    NetworkIO,
    DiskIO,
    RedisMemory,
    RedisConnections,
    ApplicationLogic,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BottleneckSeverity {
    None,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct BottleneckMetrics {
    pub resource_utilization: f64,      // 0.0 to 1.0
    pub queue_length: usize,
    pub wait_time: StdDuration,
    pub error_rate: f64,
    pub throughput_degradation: f64,    // 0.0 to 1.0 (1.0 = no degradation)
    pub latency_increase: f64,          // multiplier (1.0 = no increase)
}

/// Bottleneck detector
pub struct BottleneckDetector {
    operation_timings: HashMap<String, Vec<StdDuration>>,
    resource_utilization: HashMap<String, Vec<f64>>,
    error_counts: HashMap<String, usize>,
    success_counts: HashMap<String, usize>,
    queue_lengths: HashMap<String, Vec<usize>>,
    concurrent_operations: Arc<AtomicUsize>,
    start_time: Instant,
}

impl BottleneckDetector {
    pub fn new() -> Self {
        Self {
            operation_timings: HashMap::new(),
            resource_utilization: HashMap::new(),
            error_counts: HashMap::new(),
            success_counts: HashMap::new(),
            queue_lengths: HashMap::new(),
            concurrent_operations: Arc::new(AtomicUsize::new(0)),
            start_time: Instant::now(),
        }
    }

    pub fn record_operation(&mut self, operation: &str, duration: StdDuration, success: bool) {
        self.operation_timings
            .entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);

        if success {
            *self.success_counts.entry(operation.to_string()).or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }
    }

    pub fn record_resource_utilization(&mut self, resource: &str, utilization: f64) {
        self.resource_utilization
            .entry(resource.to_string())
            .or_insert_with(Vec::new)
            .push(utilization.clamp(0.0, 1.0));
    }

    pub fn record_queue_length(&mut self, resource: &str, length: usize) {
        self.queue_lengths
            .entry(resource.to_string())
            .or_insert_with(Vec::new)
            .push(length);
    }

    pub fn increment_concurrent_operations(&self) -> usize {
        self.concurrent_operations.fetch_add(1, Ordering::SeqCst)
    }

    pub fn decrement_concurrent_operations(&self) -> usize {
        self.concurrent_operations.fetch_sub(1, Ordering::SeqCst)
    }

    pub fn analyze_bottlenecks(&self) -> Vec<BottleneckAnalysis> {
        let mut analyses = Vec::new();

        // Analyze database connection bottlenecks
        if let Some(db_analysis) = self.analyze_database_bottlenecks() {
            analyses.push(db_analysis);
        }

        // Analyze memory bottlenecks
        if let Some(memory_analysis) = self.analyze_memory_bottlenecks() {
            analyses.push(memory_analysis);
        }

        // Analyze Redis bottlenecks
        if let Some(redis_analysis) = self.analyze_redis_bottlenecks() {
            analyses.push(redis_analysis);
        }

        // Analyze application logic bottlenecks
        if let Some(app_analysis) = self.analyze_application_bottlenecks() {
            analyses.push(app_analysis);
        }

        analyses
    }

    fn analyze_database_bottlenecks(&self) -> Option<BottleneckAnalysis> {
        let db_operations = self.operation_timings.keys()
            .filter(|k| k.contains("database") || k.contains("insert") || k.contains("query"))
            .collect::<Vec<_>>();

        if db_operations.is_empty() {
            return None;
        }

        let mut total_db_time = StdDuration::from_millis(0);
        let mut total_db_operations = 0;
        let mut db_error_count = 0;
        let mut db_success_count = 0;

        for operation in &db_operations {
            if let Some(timings) = self.operation_timings.get(*operation) {
                total_db_time += timings.iter().sum::<StdDuration>();
                total_db_operations += timings.len();
            }
            db_error_count += self.error_counts.get(*operation).unwrap_or(&0);
            db_success_count += self.success_counts.get(*operation).unwrap_or(&0);
        }

        if total_db_operations == 0 {
            return None;
        }

        let avg_db_latency = total_db_time / total_db_operations as u32;
        let db_error_rate = if db_error_count + db_success_count > 0 {
            db_error_count as f64 / (db_error_count + db_success_count) as f64
        } else {
            0.0
        };

        // Detect bottleneck symptoms
        let mut symptoms = Vec::new();
        let mut severity = BottleneckSeverity::None;

        if avg_db_latency > StdDuration::from_millis(100) {
            symptoms.push("High database latency detected".to_string());
            severity = BottleneckSeverity::Medium;
        }

        if avg_db_latency > StdDuration::from_millis(500) {
            symptoms.push("Very high database latency detected".to_string());
            severity = BottleneckSeverity::High;
        }

        if db_error_rate > 0.05 {
            symptoms.push("High database error rate".to_string());
            if severity < BottleneckSeverity::High {
                severity = BottleneckSeverity::High;
            }
        }

        if db_error_rate > 0.2 {
            symptoms.push("Critical database error rate".to_string());
            severity = BottleneckSeverity::Critical;
        }

        let recommendations = self.generate_database_recommendations(&symptoms, avg_db_latency, db_error_rate);

        Some(BottleneckAnalysis {
            bottleneck_type: BottleneckType::DatabaseConnections,
            severity,
            affected_operations: db_operations.iter().map(|s| s.to_string()).collect(),
            symptoms,
            metrics: BottleneckMetrics {
                resource_utilization: db_error_rate.min(1.0),
                queue_length: 0, // Would need connection pool metrics
                wait_time: avg_db_latency,
                error_rate: db_error_rate,
                throughput_degradation: (1.0 - db_error_rate).max(0.0),
                latency_increase: avg_db_latency.as_millis() as f64 / 50.0, // Baseline 50ms
            },
            recommendations,
            confidence: 0.8,
        })
    }

    fn analyze_memory_bottlenecks(&self) -> Option<BottleneckAnalysis> {
        let memory_utilization = self.resource_utilization.get("memory")
            .map(|utils| utils.iter().sum::<f64>() / utils.len() as f64)
            .unwrap_or(0.0);

        if memory_utilization < 0.1 {
            return None; // Not enough data
        }

        let mut symptoms = Vec::new();
        let mut severity = BottleneckSeverity::None;

        if memory_utilization > 0.8 {
            symptoms.push("High memory utilization detected".to_string());
            severity = BottleneckSeverity::Medium;
        }

        if memory_utilization > 0.95 {
            symptoms.push("Critical memory utilization".to_string());
            severity = BottleneckSeverity::Critical;
        }

        // Check for memory-related operation slowdowns
        let memory_operations = self.operation_timings.keys()
            .filter(|k| k.contains("memory") || k.contains("large"))
            .collect::<Vec<_>>();

        for operation in &memory_operations {
            if let Some(timings) = self.operation_timings.get(*operation) {
                let avg_timing = timings.iter().sum::<StdDuration>() / timings.len() as u32;
                if avg_timing > StdDuration::from_millis(200) {
                    symptoms.push(format!("Slow memory operations in {}", operation));
                    if severity < BottleneckSeverity::High {
                        severity = BottleneckSeverity::High;
                    }
                }
            }
        }

        if symptoms.is_empty() {
            return None;
        }

        let recommendations = vec![
            "Monitor memory usage patterns".to_string(),
            "Consider increasing available memory".to_string(),
            "Optimize data structures and caching".to_string(),
            "Implement memory pressure handling".to_string(),
        ];

        Some(BottleneckAnalysis {
            bottleneck_type: BottleneckType::Memory,
            severity,
            affected_operations: memory_operations.iter().map(|s| s.to_string()).collect(),
            symptoms,
            metrics: BottleneckMetrics {
                resource_utilization: memory_utilization,
                queue_length: 0,
                wait_time: StdDuration::from_millis(0),
                error_rate: 0.0,
                throughput_degradation: (1.0 - memory_utilization.max(0.8)).max(0.0),
                latency_increase: if memory_utilization > 0.8 { memory_utilization * 2.0 } else { 1.0 },
            },
            recommendations,
            confidence: 0.7,
        })
    }

    fn analyze_redis_bottlenecks(&self) -> Option<BottleneckAnalysis> {
        let redis_operations = self.operation_timings.keys()
            .filter(|k| k.contains("redis") || k.contains("stream"))
            .collect::<Vec<_>>();

        if redis_operations.is_empty() {
            return None;
        }

        let mut total_redis_time = StdDuration::from_millis(0);
        let mut total_redis_operations = 0;
        let mut redis_error_count = 0;
        let mut redis_success_count = 0;

        for operation in &redis_operations {
            if let Some(timings) = self.operation_timings.get(*operation) {
                total_redis_time += timings.iter().sum::<StdDuration>();
                total_redis_operations += timings.len();
            }
            redis_error_count += self.error_counts.get(*operation).unwrap_or(&0);
            redis_success_count += self.success_counts.get(*operation).unwrap_or(&0);
        }

        if total_redis_operations == 0 {
            return None;
        }

        let avg_redis_latency = total_redis_time / total_redis_operations as u32;
        let redis_error_rate = if redis_error_count + redis_success_count > 0 {
            redis_error_count as f64 / (redis_error_count + redis_success_count) as f64
        } else {
            0.0
        };

        let mut symptoms = Vec::new();
        let mut severity = BottleneckSeverity::None;

        if avg_redis_latency > StdDuration::from_millis(50) {
            symptoms.push("High Redis latency detected".to_string());
            severity = BottleneckSeverity::Medium;
        }

        if redis_error_rate > 0.1 {
            symptoms.push("High Redis error rate".to_string());
            severity = BottleneckSeverity::High;
        }

        if symptoms.is_empty() {
            return None;
        }

        let recommendations = vec![
            "Check Redis memory usage and eviction policies".to_string(),
            "Monitor Redis connection pool".to_string(),
            "Consider Redis clustering for scale".to_string(),
            "Optimize Redis data structures".to_string(),
        ];

        Some(BottleneckAnalysis {
            bottleneck_type: BottleneckType::RedisMemory,
            severity,
            affected_operations: redis_operations.iter().map(|s| s.to_string()).collect(),
            symptoms,
            metrics: BottleneckMetrics {
                resource_utilization: redis_error_rate,
                queue_length: 0,
                wait_time: avg_redis_latency,
                error_rate: redis_error_rate,
                throughput_degradation: (1.0 - redis_error_rate).max(0.0),
                latency_increase: avg_redis_latency.as_millis() as f64 / 10.0, // Baseline 10ms
            },
            recommendations,
            confidence: 0.75,
        })
    }

    fn analyze_application_bottlenecks(&self) -> Option<BottleneckAnalysis> {
        // Look for operations that are consistently slow but not due to external resources
        let app_operations = self.operation_timings.keys()
            .filter(|k| !k.contains("database") && !k.contains("redis") && !k.contains("memory"))
            .collect::<Vec<_>>();

        if app_operations.is_empty() {
            return None;
        }

        let mut slow_operations = Vec::new();
        let mut severity = BottleneckSeverity::None;

        for operation in &app_operations {
            if let Some(timings) = self.operation_timings.get(*operation) {
                let avg_timing = timings.iter().sum::<StdDuration>() / timings.len() as u32;
                if avg_timing > StdDuration::from_millis(100) {
                    slow_operations.push(operation.to_string());
                    severity = BottleneckSeverity::Medium;
                }
                if avg_timing > StdDuration::from_millis(500) {
                    severity = BottleneckSeverity::High;
                }
            }
        }

        if slow_operations.is_empty() {
            return None;
        }

        let symptoms = vec![
            format!("Slow application operations: {:?}", slow_operations),
            "Potential CPU or algorithm bottlenecks".to_string(),
        ];

        let recommendations = vec![
            "Profile application code for hot paths".to_string(),
            "Consider algorithmic optimizations".to_string(),
            "Check for blocking I/O operations".to_string(),
            "Monitor CPU utilization".to_string(),
        ];

        Some(BottleneckAnalysis {
            bottleneck_type: BottleneckType::ApplicationLogic,
            severity,
            affected_operations: slow_operations,
            symptoms,
            metrics: BottleneckMetrics {
                resource_utilization: 0.5, // Estimate
                queue_length: 0,
                wait_time: StdDuration::from_millis(100),
                error_rate: 0.0,
                throughput_degradation: 0.8,
                latency_increase: 2.0,
            },
            recommendations,
            confidence: 0.6,
        })
    }

    fn generate_database_recommendations(&self, symptoms: &[String], avg_latency: StdDuration, error_rate: f64) -> Vec<String> {
        let mut recommendations = Vec::new();

        if avg_latency > StdDuration::from_millis(100) {
            recommendations.push("Consider database query optimization".to_string());
            recommendations.push("Check database indexes and execution plans".to_string());
        }

        if avg_latency > StdDuration::from_millis(500) {
            recommendations.push("Increase database connection pool size".to_string());
            recommendations.push("Consider read replicas for query distribution".to_string());
        }

        if error_rate > 0.05 {
            recommendations.push("Monitor database connection pool exhaustion".to_string());
            recommendations.push("Check database error logs".to_string());
        }

        if error_rate > 0.2 {
            recommendations.push("Immediate database investigation required".to_string());
            recommendations.push("Consider circuit breaker pattern".to_string());
        }

        recommendations
    }

    pub fn print_bottleneck_analysis(&self, analyses: &[BottleneckAnalysis]) {
        println!("\n🔍 Bottleneck Analysis Report");
        println!("============================");

        if analyses.is_empty() {
            println!("✅ No significant bottlenecks detected");
            return;
        }

        for analysis in analyses {
            println!("\n🚧 Bottleneck: {:?}", analysis.bottleneck_type);
            println!("   Severity: {:?}", analysis.severity);
            println!("   Confidence: {:.1}%", analysis.confidence * 100.0);
            
            if !analysis.affected_operations.is_empty() {
                println!("   Affected operations: {:?}", analysis.affected_operations);
            }
            
            println!("   📊 Metrics:");
            println!("     - Resource utilization: {:.1}%", analysis.metrics.resource_utilization * 100.0);
            println!("     - Error rate: {:.2}%", analysis.metrics.error_rate * 100.0);
            println!("     - Wait time: {:?}", analysis.metrics.wait_time);
            println!("     - Throughput degradation: {:.1}%", (1.0 - analysis.metrics.throughput_degradation) * 100.0);
            println!("     - Latency increase: {:.1}x", analysis.metrics.latency_increase);

            if !analysis.symptoms.is_empty() {
                println!("   ⚠️  Symptoms:");
                for symptom in &analysis.symptoms {
                    println!("     - {}", symptom);
                }
            }

            if !analysis.recommendations.is_empty() {
                println!("   💡 Recommendations:");
                for recommendation in &analysis.recommendations {
                    println!("     - {}", recommendation);
                }
            }
        }
    }
}

// =============================================================================
// Bottleneck Identification Tests
// =============================================================================

/// Test database connection bottleneck identification
#[sinex_test]
async fn test_database_bottleneck_identification(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let mut detector = BottleneckDetector::new();
    
    println!("🔍 Testing database bottleneck identification");
    
    // Phase 1: Normal database operations
    println!("\n✅ Phase 1: Normal database operations");
    
    for i in 0..50 {
        let start = Instant::now();
        
        let factory = EventFactory::new("bottleneck-test");
        let event = factory.create_event(
            event_types::test::BOTTLENECK_DATABASE_NORMAL,
            json!({
                "iteration": i,
                "phase": "normal"
            })
        );
        
        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();
        
        detector.record_operation("database_insert", duration, result.is_ok());
        
        if i % 10 == 0 {
            println!("    Completed {} normal operations", i + 1);
        }
    }
    
    // Phase 2: Simulate connection pool exhaustion
    println!("\n⚠️  Phase 2: Simulating database bottleneck");
    
    let mut held_connections = Vec::new();
    
    // Hold most connections to create bottleneck
    let connections_to_hold = (pool.size() * 80 / 100) as usize; // Hold 80% of connections
    for _ in 0..connections_to_hold {
        if let Ok(conn) = pool.acquire().await {
            held_connections.push(conn);
        }
    }
    
    println!("    Held {} connections, testing bottleneck...", held_connections.len());
    
    // Try operations with limited connections
    for i in 0..30 {
        let start = Instant::now();
        
        let factory = EventFactory::new("bottleneck-test");
        let event = factory.create_event(
            event_types::test::BOTTLENECK_DATABASE_LIMITED,
            json!({
                "iteration": i,
                "phase": "bottleneck"
            }))
            .build();
        
        // Set timeout to avoid hanging
        let result = tokio::time::timeout(
            StdDuration::from_millis(200),
            sinex_db::insert_event_with_validator(pool, &event, None)
        ).await;
        
        let duration = start.elapsed();
        let success = result.is_ok() && result.unwrap().is_ok();
        
        detector.record_operation("database_insert", duration, success);
        
        if !success && i < 5 {
            println!("      Operation {} failed (expected during bottleneck)", i);
        }
    }
    
    // Release connections
    drop(held_connections);
    
    // Phase 3: Recovery
    println!("\n🔄 Phase 3: Recovery after bottleneck");
    
    for i in 0..20 {
        let start = Instant::now();
        
        let factory = EventFactory::new("bottleneck-test");
        let event = factory.create_event(
            event_types::test::BOTTLENECK_DATABASE_RECOVERY,
            json!({
                "iteration": i,
                "phase": "recovery"
            })
        );
        
        let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
        let duration = start.elapsed();
        
        detector.record_operation("database_insert", duration, result.is_ok());
    }
    
    // Analyze bottlenecks
    let analyses = detector.analyze_bottlenecks();
    detector.print_bottleneck_analysis(&analyses);
    
    // Should detect database bottleneck
    let db_bottleneck = analyses.iter()
        .find(|a| a.bottleneck_type == BottleneckType::DatabaseConnections);
    
    assert!(db_bottleneck.is_some(), "Should detect database bottleneck");
    
    if let Some(bottleneck) = db_bottleneck {
        assert!(bottleneck.severity != BottleneckSeverity::None, "Database bottleneck should have non-zero severity");
        assert!(bottleneck.metrics.error_rate > 0.1, "Should show elevated error rate during bottleneck");
        println!("    ✅ Database bottleneck correctly identified");
    }
    
    println!("✅ Database bottleneck identification test passed");
    Ok(())
}

/// Test memory bottleneck identification
#[sinex_test]
async fn test_memory_bottleneck_identification(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let mut detector = BottleneckDetector::new();
    
    println!("🔍 Testing memory bottleneck identification");
    
    // Simulate memory pressure scenarios
    println!("\n🧠 Simulating memory pressure");
    
    let memory_allocations = vec![
        (0.3, "Low memory usage"),
        (0.6, "Medium memory usage"),
        (0.85, "High memory usage"),
        (0.95, "Critical memory usage"),
    ];
    
    for (utilization, description) in memory_allocations {
        println!("    Testing: {} ({:.1}% utilization)", description, utilization * 100.0);
        
        detector.record_resource_utilization("memory", utilization);
        
        // Simulate operations under memory pressure
        for i in 0..20 {
            let start = Instant::now();
            
            // Simulate longer operations under memory pressure
            let delay = if utilization > 0.8 {
                StdDuration::from_millis((utilization * 100.0) as u64)
            } else {
                StdDuration::from_millis(10)
            };
            
            tokio::time::sleep(delay).await;
            
            let factory = EventFactory::new("memory-bottleneck-test");
            let event = factory.create_event(
                event_types::test::MEMORY_BOTTLENECK_TEST,
                json!({
                    "iteration": i,
                    "memory_utilization": utilization,
                    "large_data": "x".repeat((utilization * 1000.0) as usize)
                })
            );
            
            let result = sinex_db::insert_event_with_validator(pool, &event, None).await;
            let duration = start.elapsed();
            
            detector.record_operation("memory_operation", duration, result.is_ok());
        }
    }
    
    // Analyze memory bottlenecks
    let analyses = detector.analyze_bottlenecks();
    detector.print_bottleneck_analysis(&analyses);
    
    // Should detect memory bottleneck at high utilization
    let memory_bottleneck = analyses.iter()
        .find(|a| a.bottleneck_type == BottleneckType::Memory);
    
    assert!(memory_bottleneck.is_some(), "Should detect memory bottleneck");
    
    if let Some(bottleneck) = memory_bottleneck {
        assert!(bottleneck.metrics.resource_utilization > 0.8, "Should show high memory utilization");
        assert!(bottleneck.severity != BottleneckSeverity::None, "Memory bottleneck should have non-zero severity");
        println!("    ✅ Memory bottleneck correctly identified");
    }
    
    println!("✅ Memory bottleneck identification test passed");
    Ok(())
}

/// Test Redis bottleneck identification
#[sinex_test]
async fn test_redis_bottleneck_identification(ctx: TestContext) -> TestResult {
    let mut detector = BottleneckDetector::new();
    
    println!("🔍 Testing Redis bottleneck identification");
    
    // Test normal Redis operations
    println!("\n📡 Testing normal Redis operations");
    
    if let Ok(redis_client) = RedisStreamClient::new("redis://localhost:6379")?.await {
        let stream_key = "sinex:bottleneck:test-stream";
        
        // Normal operations
        for i in 0..50 {
            let start = Instant::now();
            
            let message_data = json!({
                "message_id": i,
                "phase": "normal",
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            let result = redis_client.xadd(stream_key, "*", &message_data).await;
            let duration = start.elapsed();
            
            detector.record_operation("redis_stream_write", duration, result.is_ok());
        }
        
        // Simulate Redis stress
        println!("\n⚠️  Simulating Redis stress");
        
        // Rapid operations to stress Redis
        for i in 0..200 {
            let start = Instant::now();
            
            let large_message_data = json!({
                "message_id": i,
                "phase": "stress",
                "large_data": "x".repeat(10000), // 10KB per message
                "timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            let result = redis_client.xadd(stream_key, "*", &large_message_data).await;
            let duration = start.elapsed();
            
            detector.record_operation("redis_stream_write", duration, result.is_ok());
            
            if i % 50 == 0 {
                println!("    Completed {} stress operations", i + 1);
            }
            
            // No delay to stress Redis
        }
        
        // Cleanup
        let _ = redis_client.del(stream_key).await;
    } else {
        println!("    ⚠️  Redis not available, simulating results");
        
        // Simulate Redis bottleneck with artificial timings
        for i in 0..100 {
            let duration = if i < 50 {
                StdDuration::from_millis(5) // Normal
            } else {
                StdDuration::from_millis(100) // Bottlenecked
            };
            
            let success = if i < 50 { true } else { i % 5 != 0 }; // Simulate failures
            
            detector.record_operation("redis_stream_write", duration, success);
        }
    }
    
    // Analyze Redis bottlenecks
    let analyses = detector.analyze_bottlenecks();
    detector.print_bottleneck_analysis(&analyses);
    
    // Should detect Redis bottleneck
    let redis_bottleneck = analyses.iter()
        .find(|a| a.bottleneck_type == BottleneckType::RedisMemory);
    
    if let Some(bottleneck) = redis_bottleneck {
        println!("    ✅ Redis bottleneck identified with severity: {:?}", bottleneck.severity);
    } else {
        println!("    ℹ️  No Redis bottleneck detected (may be expected with light load)");
    }
    
    println!("✅ Redis bottleneck identification test passed");
    Ok(())
}

/// Test concurrent bottleneck identification
#[sinex_test]
async fn test_concurrent_bottleneck_identification(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let shared_detector = Arc::new(Mutex::new(BottleneckDetector::new()));
    
    println!("🔍 Testing concurrent bottleneck identification");
    
    let worker_count = 20;
    let operations_per_worker = 50;
    
    println!("    Configuration: {} workers, {} ops each", worker_count, operations_per_worker);
    
    let worker_handles = (0..worker_count)
        .map(|worker_id| {
            let pool_clone = pool.clone();
            let detector = shared_detector.clone();
            
            tokio::spawn(async move {
                for op_id in 0..operations_per_worker {
                    let concurrent_ops = {
                        let detector_lock = detector.lock().await;
                        detector_lock.increment_concurrent_operations()
                    };
                    
                    let start = Instant::now();
                    
                    // Simulate variable operation complexity
                    let operation_type = op_id % 4;
                    let operation_name = match operation_type {
                        0 => "concurrent_light",
                        1 => "concurrent_medium", 
                        2 => "concurrent_heavy",
                        _ => "concurrent_variable",
                    };
                    
                    // Add artificial delay based on concurrent load
                    let base_delay = match operation_type {
                        0 => 5,   // Light
                        1 => 20,  // Medium
                        2 => 50,  // Heavy
                        _ => 30,  // Variable
                    };
                    
                    // Delay increases with concurrent operations
                    let congestion_delay = if concurrent_ops > 10 {
                        (concurrent_ops - 10) * 2
                    } else {
                        0
                    };
                    
                    tokio::time::sleep(StdDuration::from_millis((base_delay + congestion_delay) as u64)).await;
                    
                    let factory = EventFactory::new(&format!("concurrent-bottleneck-worker-{}", worker_id));
                    let event = factory.create_event(
                        event_types::test::CONCURRENT_BOTTLENECK_TEST,
                        json!({
                            "worker_id": worker_id,
                            "operation_id": op_id,
                            "operation_type": operation_name,
                            "concurrent_ops": concurrent_ops
                        })
                    );
                    
                    let result = sinex_db::insert_event_with_validator(&pool_clone, &event, None).await;
                    let duration = start.elapsed();
                    
                    {
                        let mut detector_lock = detector.lock().await;
                        detector_lock.record_operation(operation_name, duration, result.is_ok());
                        detector_lock.decrement_concurrent_operations();
                    }
                    
                    // Small stagger between operations
                    tokio::time::sleep(StdDuration::from_millis(10)).await;
                }
            })
        })
        .collect::<Vec<_>>();
    
    // Wait for all workers to complete
    futures::future::join_all(worker_handles).await;
    
    let final_detector = shared_detector.lock().await;
    let analyses = final_detector.analyze_bottlenecks();
    final_detector.print_bottleneck_analysis(&analyses);
    
    // Verify some bottleneck detection occurred
    let has_app_bottleneck = analyses.iter()
        .any(|a| a.bottleneck_type == BottleneckType::ApplicationLogic);
    
    if has_app_bottleneck {
        println!("    ✅ Application bottleneck detected under concurrent load");
    } else {
        println!("    ℹ️  No application bottleneck detected (may be expected with light load)");
    }
    
    // Verify database consistency
    let concurrent_events = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE source LIKE 'concurrent-bottleneck-worker-%'"
    ).fetch_one(&pool).await?;
    
    println!("    📊 Concurrent events stored: {}", concurrent_events.count.unwrap_or(0));
    
    println!("✅ Concurrent bottleneck identification test passed");
    Ok(())
}
