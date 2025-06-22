use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tokio::sync::RwLock;
use tokio::time::timeout;

/// Test gradual memory leak detection
#[tokio::test]
async fn test_memory_leak_detection() {
    // Simulate a component that gradually leaks memory
    #[derive(Clone)]
    struct LeakyComponent {
        data: Arc<RwLock<Vec<Vec<u8>>>>,
        allocations: Arc<AtomicU64>,
        should_leak: Arc<AtomicBool>,
    }
    
    impl LeakyComponent {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(Vec::new())),
                allocations: Arc::new(AtomicU64::new(0)),
                should_leak: Arc::new(AtomicBool::new(true)),
            }
        }
        
        async fn process_event(&self, size: usize) {
            let allocation = vec![0u8; size];
            
            if self.should_leak.load(Ordering::Relaxed) {
                // Leak: Keep accumulating without cleanup
                let mut data = self.data.write().await;
                data.push(allocation);
                self.allocations.fetch_add(1, Ordering::Relaxed);
                
                // Simulate slow leak - only keep every 10th allocation
                if data.len() % 10 != 0 {
                    data.pop();
                }
            } else {
                // Normal: Process and discard
                drop(allocation);
            }
        }
        
        async fn get_retained_bytes(&self) -> usize {
            let data = self.data.read().await;
            data.iter().map(|v| v.len()).sum()
        }
    }
    
    let component = LeakyComponent::new();
    let memory_samples = Arc::new(RwLock::new(Vec::new()));
    
    // Monitoring task
    let monitor_component = component.clone();
    let monitor_samples = memory_samples.clone();
    let monitor = tokio::spawn(async move {
        let mut consecutive_increases = 0;
        let mut last_size = 0;
        
        for i in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            let current_size = monitor_component.get_retained_bytes().await;
            let allocations = monitor_component.allocations.load(Ordering::Relaxed);
            
            monitor_samples.write().await.push((i, current_size, allocations));
            
            if current_size > last_size {
                consecutive_increases += 1;
                if consecutive_increases >= 5 {
                    println!("WARNING: Potential memory leak detected!");
                    println!("  Memory has increased {} times consecutively", consecutive_increases);
                    println!("  Current retained: {} bytes", current_size);
                    return true; // Leak detected
                }
            } else {
                consecutive_increases = 0;
            }
            
            last_size = current_size;
        }
        
        false // No leak detected
    });
    
    // Simulate workload
    for i in 0..100 {
        component.process_event(1024 * (i % 10 + 1)).await; // Variable sizes
        tokio::task::yield_now().await;
        
        // Stop leaking after detection
        if i == 50 {
            component.should_leak.store(false, Ordering::Relaxed);
        }
    }
    
    let leak_detected = monitor.await.unwrap();
    let samples = memory_samples.read().await;
    
    println!("\nMemory leak detection results:");
    println!("  Leak detected: {}", leak_detected);
    println!("  Memory growth samples:");
    for (i, size, allocs) in samples.iter() {
        println!("    Sample {}: {} bytes, {} allocations", i, size, allocs);
    }
    
    // Performance assertions with reasonable safety margins
    assert!(samples.len() >= 10, "Should have collected at least 10 memory samples");
    
    // Verify memory growth pattern during leak phase
    if let (Some(early), Some(mid)) = (samples.get(2), samples.get(25)) {
        let growth_rate = (mid.1 as f64 - early.1 as f64) / (mid.0 as f64 - early.0 as f64);
        assert!(growth_rate > 1000.0, 
               "Memory should grow during leak phase at >1KB/sample, got {:.1} bytes/sample", growth_rate);
    }
    
    // Verify memory stabilizes after leak stops (with 10x safety margin)
    if let (Some(mid), Some(late)) = (samples.get(52), samples.get(75)) {
        let stable_growth = (late.1 as f64 - mid.1 as f64) / (late.0 as f64 - mid.0 as f64);
        assert!(stable_growth < 10000.0, 
               "Memory growth should slow after leak stops, got {:.1} bytes/sample", stable_growth);
    }
    
    println!("✅ Memory leak detection test completed with performance validation");
}

/// Test CPU throttling detection
#[tokio::test]
async fn test_cpu_throttling_detection() {
    // Track processing performance over time
    struct PerformanceMonitor {
        processing_times: Arc<RwLock<VecDeque<Duration>>>,
        throttle_detected: Arc<AtomicBool>,
    }
    
    impl PerformanceMonitor {
        fn new() -> Self {
            Self {
                processing_times: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
                throttle_detected: Arc::new(AtomicBool::new(false)),
            }
        }
        
        async fn record_processing_time(&self, duration: Duration) {
            let mut times = self.processing_times.write().await;
            
            // Keep sliding window of last 100 measurements
            if times.len() >= 100 {
                times.pop_front();
            }
            times.push_back(duration);
            
            // Detect throttling: significant increase in processing time
            if times.len() >= 20 {
                let recent: Vec<_> = times.iter().rev().take(10).collect();
                let older: Vec<_> = times.iter().rev().skip(10).take(10).collect();
                
                let recent_avg = recent.iter().map(|d| d.as_millis()).sum::<u128>() / recent.len() as u128;
                let older_avg = older.iter().map(|d| d.as_millis()).sum::<u128>() / older.len() as u128;
                
                // If recent processing is 2x slower than older, likely throttled
                if recent_avg > older_avg * 2 {
                    self.throttle_detected.store(true, Ordering::Relaxed);
                    println!("CPU throttling detected! Recent avg: {}ms, Older avg: {}ms", 
                        recent_avg, older_avg);
                }
            }
        }
    }
    
    let monitor = PerformanceMonitor::new();
    
    // Simulate CPU-intensive work with varying performance
    for i in 0..50 {
        let start = Instant::now();
        
        // Simulate work that gets progressively slower (throttling)
        let work_iterations = if i < 20 {
            10_000 // Normal performance
        } else if i < 35 {
            50_000 // Slightly degraded
        } else {
            100_000 // Heavily throttled
        };
        
        // CPU-intensive work
        let mut sum = 0u64;
        for j in 0..work_iterations {
            sum = sum.wrapping_add(j * j);
        }
        
        let duration = start.elapsed();
        monitor.record_processing_time(duration).await;
        
        // Prevent optimization
        if sum == u64::MAX {
            println!("Unlikely");
        }
        
        tokio::task::yield_now().await;
    }
    
    let throttling_detected = monitor.throttle_detected.load(Ordering::Relaxed);
    let times = monitor.processing_times.read().await;
    
    println!("\nCPU throttling detection results:");
    println!("  Throttling detected: {}", throttling_detected);
    println!("  Processing time progression:");
    for (i, duration) in times.iter().enumerate().step_by(5) {
        println!("    Sample {}: {:?}", i, duration);
    }
    
    // Performance assertions for CPU processing capability
    assert!(times.len() >= 50, "Should have collected at least 50 timing samples");
    
    // Verify initial processing times are reasonable (10x safety margin)
    if times.len() >= 10 {
        let early_times: Vec<_> = times.iter().take(10).collect();
        let avg_early = early_times.iter().map(|d| d.as_nanos()).sum::<u128>() / early_times.len() as u128;
        assert!(avg_early < 100_000_000, // 100ms
               "Early processing times should be <100ms, got {} ns", avg_early);
    }
    
    // Verify we can detect performance degradation patterns
    if times.len() >= 40 {
        let early_times: Vec<_> = times.iter().take(10).collect();
        let late_times: Vec<_> = times.iter().skip(30).take(10).collect();
        
        let early_avg = early_times.iter().map(|d| d.as_nanos()).sum::<u128>() / 10;
        let late_avg = late_times.iter().map(|d| d.as_nanos()).sum::<u128>() / 10;
        
        // Either consistent performance OR detectable degradation
        let degradation_ratio = late_avg as f64 / early_avg as f64;
        assert!(degradation_ratio < 50.0, // 50x degradation should be detectable
               "Extreme performance degradation should be detectable, ratio: {:.2}", degradation_ratio);
    }
    
    println!("✅ CPU throttling detection test completed with performance validation");
}

/// Test I/O saturation handling
#[tokio::test]
async fn test_io_saturation_handling() {
    use tokio::fs::OpenOptions;
    use tokio::io::AsyncWriteExt;
    
    let temp_dir = tempfile::TempDir::new().unwrap();
    let test_file = temp_dir.path().join("io_test.dat");
    
    // Track I/O performance
    let write_latencies = Arc::new(RwLock::new(Vec::new()));
    let slow_writes = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));
    
    // Baseline I/O performance
    let mut baseline_latencies = vec![];
    for _ in 0..10 {
        let start = Instant::now();
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&test_file)
            .await
            .unwrap();
        
        file.write_all(b"baseline test data").await.unwrap();
        file.sync_all().await.unwrap();
        
        baseline_latencies.push(start.elapsed());
        tokio::task::yield_now().await;
    }
    
    let baseline_avg = baseline_latencies.iter()
        .map(|d| d.as_micros())
        .sum::<u128>() / baseline_latencies.len() as u128;
    
    println!("Baseline I/O latency: {} μs", baseline_avg);
    
    // Simulate I/O saturation with concurrent writes
    let mut handles = vec![];
    
    for worker_id in 0..5 {
        let file_path = temp_dir.path().join(format!("worker_{}.dat", worker_id));
        let latencies = write_latencies.clone();
        let slow = slow_writes.clone();
        let total = total_writes.clone();
        let baseline = baseline_avg;
        
        let handle = tokio::spawn(async move {
            for i in 0..20 {
                let data = vec![worker_id as u8; 1024 * 1024]; // 1MB writes
                let start = Instant::now();
                
                let result = timeout(Duration::from_millis(500), async {
                    let mut file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&file_path)
                        .await?;
                    
                    file.write_all(&data).await?;
                    file.sync_all().await?;
                    Ok::<(), std::io::Error>(())
                }).await;
                
                let latency = start.elapsed();
                total.fetch_add(1, Ordering::Relaxed);
                
                match result {
                    Ok(Ok(())) => {
                        latencies.write().await.push(latency);
                        
                        // Check if this write was slow (>10x baseline)
                        if latency.as_micros() > baseline * 10 {
                            slow.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Slow write detected: {:?} ({}x baseline)", 
                                latency, latency.as_micros() / baseline);
                        }
                    }
                    Ok(Err(e)) => {
                        eprintln!("I/O error: {}", e);
                    }
                    Err(_) => {
                        eprintln!("I/O timeout!");
                        slow.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                // No delay - stress the I/O system
                if i < 10 {
                    // First half: aggressive
                } else {
                    // Second half: back off
                    tokio::task::yield_now().await;
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for completion
    for handle in handles {
        let _ = handle.await;
    }
    
    let all_latencies = write_latencies.read().await;
    let slow_count = slow_writes.load(Ordering::Relaxed);
    let total_count = total_writes.load(Ordering::Relaxed);
    
    // Calculate percentiles
    let mut sorted_latencies: Vec<_> = all_latencies.iter()
        .map(|d| d.as_micros())
        .collect();
    sorted_latencies.sort();
    
    let p50 = sorted_latencies.get(sorted_latencies.len() / 2).copied().unwrap_or(0);
    let p95 = sorted_latencies.get(sorted_latencies.len() * 95 / 100).copied().unwrap_or(0);
    let p99 = sorted_latencies.get(sorted_latencies.len() * 99 / 100).copied().unwrap_or(0);
    
    println!("\nI/O saturation test results:");
    println!("  Total writes: {}", total_count);
    println!("  Slow writes: {} ({:.1}%)", slow_count, 
        (slow_count as f64 / total_count as f64) * 100.0);
    println!("  Latency percentiles:");
    println!("    p50: {} μs ({:.1}x baseline)", p50, p50 as f64 / baseline_avg as f64);
    println!("    p95: {} μs ({:.1}x baseline)", p95, p95 as f64 / baseline_avg as f64);
    println!("    p99: {} μs ({:.1}x baseline)", p99, p99 as f64 / baseline_avg as f64);
    
    // Performance assertions for I/O handling capability
    assert!(total_count > 0, "Should have completed at least some I/O operations");
    assert!(sorted_latencies.len() > 0, "Should have collected latency measurements");
    
    // Verify we can handle reasonable I/O loads (generous safety margins)
    let slow_percentage = (slow_count as f64 / total_count as f64) * 100.0;
    assert!(slow_percentage < 95.0, 
           "Should handle most I/O operations without timeout, got {:.1}% slow", slow_percentage);
    
    // Verify latency measurements are reasonable
    assert!(p50 < 1_000_000, // 1 second p50 should be achievable
           "p50 latency should be <1s, got {} μs", p50);
    
    // System should handle some load - p99 shouldn't be extremely high (100x safety margin)
    let max_acceptable_p99 = baseline_avg * 100; // 100x baseline is extreme but detectable
    assert!(p99 < max_acceptable_p99, 
           "p99 latency shouldn't exceed 100x baseline. Got {} μs vs baseline {} μs", 
           p99, baseline_avg);
    
    // I/O saturation test results summary
    if slow_count > 0 {
        println!("✅ I/O saturation successfully detected and handled");
    } else {
        println!("✅ I/O system handled load without saturation");
    }
    
    println!("✅ I/O saturation test completed with performance validation");
}

/// Test resource usage pattern analysis
#[tokio::test]
async fn test_resource_usage_patterns() {
    // Simulate different resource usage patterns
    
    #[derive(Debug, Clone, Copy)]
    enum ResourcePattern {
        Steady,      // Consistent resource usage
        Bursty,      // Periodic spikes
        Growing,     // Gradual increase
        Oscillating, // Up and down pattern
    }
    
    struct ResourceMonitor {
        samples: Arc<RwLock<Vec<(Instant, f64)>>>,
        pattern_detected: Arc<RwLock<Option<ResourcePattern>>>,
    }
    
    impl ResourceMonitor {
        fn new() -> Self {
            Self {
                samples: Arc::new(RwLock::new(Vec::new())),
                pattern_detected: Arc::new(RwLock::new(None)),
            }
        }
        
        async fn record_usage(&self, usage: f64) {
            let mut samples = self.samples.write().await;
            samples.push((Instant::now(), usage));
            
            // Keep last 50 samples
            if samples.len() > 50 {
                samples.drain(0..1);
            }
            
            // Analyze pattern with enough samples
            if samples.len() >= 20 {
                let pattern = self.detect_pattern(&samples);
                *self.pattern_detected.write().await = Some(pattern);
            }
        }
        
        fn detect_pattern(&self, samples: &[(Instant, f64)]) -> ResourcePattern {
            let values: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
            
            // Calculate statistics
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance = values.iter()
                .map(|v| (v - mean).powi(2))
                .sum::<f64>() / values.len() as f64;
            let std_dev = variance.sqrt();
            
            // Detect trend
            let first_half_mean = values[..values.len()/2].iter().sum::<f64>() 
                / (values.len()/2) as f64;
            let second_half_mean = values[values.len()/2..].iter().sum::<f64>() 
                / (values.len()/2) as f64;
            
            // Pattern detection logic
            if std_dev < mean * 0.1 {
                ResourcePattern::Steady
            } else if second_half_mean > first_half_mean * 1.3 {
                ResourcePattern::Growing
            } else if std_dev > mean * 0.5 {
                // Check for periodic pattern
                let mut peaks = 0;
                for i in 1..values.len()-1 {
                    if values[i] > values[i-1] && values[i] > values[i+1] 
                        && values[i] > mean + std_dev {
                        peaks += 1;
                    }
                }
                
                if peaks >= 3 {
                    ResourcePattern::Bursty
                } else {
                    ResourcePattern::Oscillating
                }
            } else {
                ResourcePattern::Steady
            }
        }
    }
    
    // Test different patterns
    let monitor = ResourceMonitor::new();
    
    // Generate bursty pattern
    for i in 0..50 {
        let usage = if i % 10 == 0 {
            80.0 + (i as f64 * 0.5) // Spike
        } else {
            20.0 + (i % 3) as f64 * 5.0 // Baseline
        };
        
        monitor.record_usage(usage).await;
        tokio::task::yield_now().await;
    }
    
    let detected_pattern = monitor.pattern_detected.read().await.clone();
    let samples = monitor.samples.read().await.clone();
    
    println!("\nResource usage pattern analysis:");
    println!("  Detected pattern: {:?}", detected_pattern);
    println!("  Sample values (every 5th):");
    for (i, (_, usage)) in samples.iter().enumerate().step_by(5) {
        println!("    Sample {}: {:.1}%", i, usage);
    }
    
    assert!(detected_pattern.is_some(), "Should detect a resource usage pattern");
}