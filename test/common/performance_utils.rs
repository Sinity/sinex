//! Test performance measurement utilities
//! 
//! This module provides utilities for measuring and optimizing test performance,
//! including timing measurements, batch operation optimization, and parallelization helpers.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

/// Test timer for measuring operation performance
#[derive(Debug, Clone)]
pub struct TestTimer {
    name: String,
    start_time: Instant,
    checkpoint_times: Arc<Mutex<Vec<(String, Duration)>>>,
    total_operations: Arc<AtomicU64>,
}

impl TestTimer {
    /// Create a new test timer
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start_time: Instant::now(),
            checkpoint_times: Arc::new(Mutex::new(Vec::new())),
            total_operations: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Mark a checkpoint with a name
    pub async fn checkpoint(&self, checkpoint_name: impl Into<String>) {
        let elapsed = self.start_time.elapsed();
        let mut checkpoints = self.checkpoint_times.lock().await;
        checkpoints.push((checkpoint_name.into(), elapsed));
    }

    /// Increment operation counter
    pub fn record_operation(&self) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record multiple operations
    pub fn record_operations(&self, count: u64) {
        self.total_operations.fetch_add(count, Ordering::Relaxed);
    }

    /// Get elapsed time since start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get operations per second
    pub fn ops_per_second(&self) -> f64 {
        let ops = self.total_operations.load(Ordering::Relaxed) as f64;
        let secs = self.elapsed().as_secs_f64();
        if secs > 0.0 {
            ops / secs
        } else {
            0.0
        }
    }

    /// Generate performance report
    pub async fn report(&self) -> PerformanceReport {
        let checkpoints = self.checkpoint_times.lock().await;
        let mut checkpoint_durations = Vec::new();
        let mut last_time = Duration::ZERO;

        for (name, time) in checkpoints.iter() {
            let duration = *time - last_time;
            checkpoint_durations.push((name.clone(), duration));
            last_time = *time;
        }

        PerformanceReport {
            test_name: self.name.clone(),
            total_duration: self.elapsed(),
            checkpoint_durations,
            total_operations: self.total_operations.load(Ordering::Relaxed),
            ops_per_second: self.ops_per_second(),
        }
    }
}

/// Performance report for a test
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub test_name: String,
    pub total_duration: Duration,
    pub checkpoint_durations: Vec<(String, Duration)>,
    pub total_operations: u64,
    pub ops_per_second: f64,
}

impl PerformanceReport {
    /// Print the report to stdout
    pub fn print(&self) {
        println!("\n=== Performance Report: {} ===", self.test_name);
        println!("Total Duration: {:?}", self.total_duration);
        println!("Total Operations: {}", self.total_operations);
        println!("Operations/sec: {:.2}", self.ops_per_second);
        
        if !self.checkpoint_durations.is_empty() {
            println!("\nCheckpoints:");
            for (name, duration) in &self.checkpoint_durations {
                println!("  {} -> {:?}", name, duration);
            }
        }
        println!();
    }

    /// Check if performance meets threshold
    pub fn meets_threshold(&self, max_duration: Duration, min_ops_per_sec: f64) -> bool {
        self.total_duration <= max_duration && self.ops_per_second >= min_ops_per_sec
    }
}

/// Batch performance analyzer for bulk operations
pub struct BatchPerformanceAnalyzer {
    batch_sizes: Vec<usize>,
    results: Arc<Mutex<Vec<BatchResult>>>,
}

#[derive(Debug, Clone)]
pub struct BatchResult {
    pub batch_size: usize,
    pub duration: Duration,
    pub items_per_second: f64,
    pub success_count: usize,
    pub error_count: usize,
}

impl BatchPerformanceAnalyzer {
    /// Create analyzer with predefined batch sizes to test
    pub fn new(batch_sizes: Vec<usize>) -> Self {
        Self {
            batch_sizes,
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Analyze batch operation performance
    pub async fn analyze_batch_operation<F, T, Fut>(
        &self,
        operation: F,
    ) -> AnyhowResult<OptimalBatchSize>
    where
        F: Fn(usize) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = AnyhowResult<Vec<T>>> + Send,
        T: Send,
    {
        for &batch_size in &self.batch_sizes {
            let start = Instant::now();
            match operation(batch_size).await {
                Ok(results) => {
                    let duration = start.elapsed();
                    let items_per_second = batch_size as f64 / duration.as_secs_f64();
                    
                    let result = BatchResult {
                        batch_size,
                        duration,
                        items_per_second,
                        success_count: results.len(),
                        error_count: 0,
                    };
                    
                    self.results.lock().await.push(result);
                }
                Err(_) => {
                    let result = BatchResult {
                        batch_size,
                        duration: start.elapsed(),
                        items_per_second: 0.0,
                        success_count: 0,
                        error_count: batch_size,
                    };
                    
                    self.results.lock().await.push(result);
                }
            }
        }

        self.find_optimal_batch_size().await
    }

    /// Find optimal batch size based on results
    async fn find_optimal_batch_size(&self) -> AnyhowResult<OptimalBatchSize> {
        let results = self.results.lock().await;
        
        let optimal = results
            .iter()
            .filter(|r| r.error_count == 0)
            .max_by(|a, b| {
                a.items_per_second
                    .partial_cmp(&b.items_per_second)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| anyhow::anyhow!("No successful batch operations"))?;

        Ok(OptimalBatchSize {
            size: optimal.batch_size,
            items_per_second: optimal.items_per_second,
            all_results: results.clone(),
        })
    }
}

/// Optimal batch size recommendation
#[derive(Debug)]
pub struct OptimalBatchSize {
    pub size: usize,
    pub items_per_second: f64,
    pub all_results: Vec<BatchResult>,
}

impl OptimalBatchSize {
    /// Print analysis results
    pub fn print_analysis(&self) {
        println!("\n=== Batch Size Analysis ===");
        println!("Optimal batch size: {}", self.size);
        println!("Peak throughput: {:.2} items/sec", self.items_per_second);
        
        println!("\nAll results:");
        for result in &self.all_results {
            println!(
                "  Batch {}: {:.2} items/sec ({:?})",
                result.batch_size, result.items_per_second, result.duration
            );
        }
    }
}

/// Connection pool optimizer for database tests
pub struct ConnectionPoolOptimizer {
    pool_sizes: Vec<u32>,
    results: Arc<Mutex<Vec<PoolTestResult>>>,
}

#[derive(Debug, Clone)]
pub struct PoolTestResult {
    pub pool_size: u32,
    pub duration: Duration,
    pub queries_per_second: f64,
    pub error_rate: f64,
}

impl ConnectionPoolOptimizer {
    /// Create optimizer with pool sizes to test
    pub fn new(pool_sizes: Vec<u32>) -> Self {
        Self {
            pool_sizes,
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Find optimal pool size for workload
    pub async fn optimize_for_workload<F, Fut>(
        &self,
        create_pool: F,
        query_count: usize,
        concurrent_workers: usize,
    ) -> AnyhowResult<OptimalPoolSize>
    where
        F: Fn(u32) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = AnyhowResult<DbPool>> + Send,
    {
        for &pool_size in &self.pool_sizes {
            let pool = create_pool(pool_size).await?;
            let result = self
                .test_pool_performance(pool, pool_size, query_count, concurrent_workers)
                .await?;
            
            self.results.lock().await.push(result);
        }

        self.find_optimal_pool_size().await
    }

    /// Test performance of a specific pool configuration
    async fn test_pool_performance(
        &self,
        pool: DbPool,
        pool_size: u32,
        query_count: usize,
        concurrent_workers: usize,
    ) -> AnyhowResult<PoolTestResult> {
        let pool = Arc::new(pool);
        let queries_per_worker = query_count / concurrent_workers;
        let start = Instant::now();
        let error_count = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();
        for _ in 0..concurrent_workers {
            let pool_clone = pool.clone();
            let error_counter = error_count.clone();
            
            let handle = tokio::spawn(async move {
                for _ in 0..queries_per_worker {
                    if let Err(_) = sqlx::query("SELECT 1")
                        .fetch_one(pool_clone.as_ref())
                        .await
                    {
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            
            handles.push(handle);
        }

        // Wait for all workers
        for handle in handles {
            handle.await?;
        }

        let duration = start.elapsed();
        let queries_per_second = query_count as f64 / duration.as_secs_f64();
        let errors = error_count.load(Ordering::Relaxed) as f64;
        let error_rate = errors / query_count as f64;

        Ok(PoolTestResult {
            pool_size,
            duration,
            queries_per_second,
            error_rate,
        })
    }

    /// Find optimal pool size
    async fn find_optimal_pool_size(&self) -> AnyhowResult<OptimalPoolSize> {
        let results = self.results.lock().await;
        
        // Filter out configurations with high error rates
        let acceptable_results: Vec<_> = results
            .iter()
            .filter(|r| r.error_rate < 0.01) // Less than 1% errors
            .collect();

        let optimal = acceptable_results
            .iter()
            .max_by(|a, b| {
                a.queries_per_second
                    .partial_cmp(&b.queries_per_second)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| anyhow::anyhow!("No acceptable pool configurations found"))?;

        Ok(OptimalPoolSize {
            size: optimal.pool_size,
            queries_per_second: optimal.queries_per_second,
            error_rate: optimal.error_rate,
            all_results: results.clone(),
        })
    }
}

/// Optimal connection pool size
#[derive(Debug)]
pub struct OptimalPoolSize {
    pub size: u32,
    pub queries_per_second: f64,
    pub error_rate: f64,
    pub all_results: Vec<PoolTestResult>,
}

impl OptimalPoolSize {
    /// Print optimization results
    pub fn print_analysis(&self) {
        println!("\n=== Connection Pool Analysis ===");
        println!("Optimal pool size: {}", self.size);
        println!("Peak throughput: {:.2} queries/sec", self.queries_per_second);
        println!("Error rate: {:.2}%", self.error_rate * 100.0);
        
        println!("\nAll results:");
        for result in &self.all_results {
            println!(
                "  Pool {}: {:.2} queries/sec, {:.2}% errors ({:?})",
                result.pool_size,
                result.queries_per_second,
                result.error_rate * 100.0,
                result.duration
            );
        }
    }
}

/// Parallel test runner for concurrent test execution
pub struct ParallelTestRunner {
    max_concurrent: usize,
    results: Arc<Mutex<Vec<TestRunResult>>>,
}

#[derive(Debug, Clone)]
pub struct TestRunResult {
    pub test_name: String,
    pub duration: Duration,
    pub success: bool,
    pub error_message: Option<String>,
}

impl ParallelTestRunner {
    /// Create a new parallel test runner
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Run tests in parallel with optimal concurrency
    pub async fn run_tests<F, Fut>(
        &self,
        tests: Vec<(String, F)>,
    ) -> AnyhowResult<TestRunSummary>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = AnyhowResult<()>> + Send,
    {
        let mut join_set = JoinSet::new();
        let results = self.results.clone();
        let mut pending = tests.into_iter();

        // Start initial batch
        for _ in 0..self.max_concurrent {
            if let Some((name, test_fn)) = pending.next() {
                let results_clone = results.clone();
                join_set.spawn(async move {
                    let start = Instant::now();
                    let result = match test_fn().await {
                        Ok(()) => TestRunResult {
                            test_name: name,
                            duration: start.elapsed(),
                            success: true,
                            error_message: None,
                        },
                        Err(e) => TestRunResult {
                            test_name: name,
                            duration: start.elapsed(),
                            success: false,
                            error_message: Some(e.to_string()),
                        },
                    };
                    results_clone.lock().await.push(result);
                });
            }
        }

        // Process completions and start new tests
        while let Some(_) = join_set.join_next().await {
            if let Some((name, test_fn)) = pending.next() {
                let results_clone = results.clone();
                join_set.spawn(async move {
                    let start = Instant::now();
                    let result = match test_fn().await {
                        Ok(()) => TestRunResult {
                            test_name: name,
                            duration: start.elapsed(),
                            success: true,
                            error_message: None,
                        },
                        Err(e) => TestRunResult {
                            test_name: name,
                            duration: start.elapsed(),
                            success: false,
                            error_message: Some(e.to_string()),
                        },
                    };
                    results_clone.lock().await.push(result);
                });
            }
        }

        self.generate_summary().await
    }

    /// Generate test run summary
    async fn generate_summary(&self) -> AnyhowResult<TestRunSummary> {
        let results = self.results.lock().await;
        
        let total_tests = results.len();
        let passed_tests = results.iter().filter(|r| r.success).count();
        let failed_tests = total_tests - passed_tests;
        
        let total_duration: Duration = results.iter().map(|r| r.duration).sum();
        let avg_duration = if total_tests > 0 {
            total_duration / total_tests as u32
        } else {
            Duration::ZERO
        };

        let slowest_tests: Vec<_> = {
            let mut sorted = results.clone();
            sorted.sort_by(|a, b| b.duration.cmp(&a.duration));
            sorted.into_iter().take(10).collect()
        };

        Ok(TestRunSummary {
            total_tests,
            passed_tests,
            failed_tests,
            total_duration,
            avg_duration,
            slowest_tests,
            all_results: results.clone(),
        })
    }
}

/// Test run summary
#[derive(Debug)]
pub struct TestRunSummary {
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub total_duration: Duration,
    pub avg_duration: Duration,
    pub slowest_tests: Vec<TestRunResult>,
    pub all_results: Vec<TestRunResult>,
}

impl TestRunSummary {
    /// Print test run summary
    pub fn print_summary(&self) {
        println!("\n=== Test Run Summary ===");
        println!("Total tests: {}", self.total_tests);
        println!("Passed: {} ✓", self.passed_tests);
        println!("Failed: {} ✗", self.failed_tests);
        println!("Total duration: {:?}", self.total_duration);
        println!("Average duration: {:?}", self.avg_duration);
        
        if !self.slowest_tests.is_empty() {
            println!("\nSlowest tests:");
            for test in &self.slowest_tests {
                println!("  {} - {:?}", test.test_name, test.duration);
            }
        }

        if self.failed_tests > 0 {
            println!("\nFailed tests:");
            for test in &self.all_results {
                if !test.success {
                    println!("  {} - {}", test.test_name, test.error_message.as_ref().unwrap());
                }
            }
        }
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_tests > 0 {
            self.passed_tests as f64 / self.total_tests as f64
        } else {
            0.0
        }
    }
}

/// Helper functions for common performance patterns
pub mod helpers {
    use super::*;

    /// Measure database insert performance
    pub async fn measure_insert_performance(
        pool: &DbPool,
        events: Vec<RawEvent>,
        batch_size: usize,
    ) -> AnyhowResult<PerformanceReport> {
        let timer = TestTimer::new("database_insert_performance");
        
        timer.checkpoint("start").await;
        
        for (i, chunk) in events.chunks(batch_size).enumerate() {
            for event in chunk {
                sinex_db::insert_event_with_validator(pool, event, None).await?;
                timer.record_operation();
            }
            
            if i % 10 == 0 {
                timer.checkpoint(format!("batch_{}", i)).await;
            }
        }
        
        timer.checkpoint("complete").await;
        Ok(timer.report().await)
    }

    /// Measure query performance
    pub async fn measure_query_performance<F, T, Fut>(
        name: impl Into<String>,
        query_fn: F,
        iterations: usize,
    ) -> AnyhowResult<PerformanceReport>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = AnyhowResult<T>>,
    {
        let timer = TestTimer::new(name);
        
        for i in 0..iterations {
            query_fn().await?;
            timer.record_operation();
            
            if i % 100 == 0 && i > 0 {
                timer.checkpoint(format!("iteration_{}", i)).await;
            }
        }
        
        Ok(timer.report().await)
    }

    /// Find optimal concurrency level for a workload
    pub async fn find_optimal_concurrency<F, Fut>(
        workload: F,
        concurrency_levels: Vec<usize>,
    ) -> AnyhowResult<usize>
    where
        F: Fn(usize) -> Fut + Clone,
        Fut: std::future::Future<Output = AnyhowResult<Duration>>,
    {
        let mut best_concurrency = 1;
        let mut best_time = Duration::MAX;

        for level in concurrency_levels {
            let duration = workload.clone()(level).await?;
            
            if duration < best_time {
                best_time = duration;
                best_concurrency = level;
            }
        }

        Ok(best_concurrency)
    }
}

/// Macros for performance testing
#[macro_export]
macro_rules! time_operation {
    ($name:expr, $op:expr) => {{
        let start = std::time::Instant::now();
        let result = $op;
        let duration = start.elapsed();
        tracing::info!("{} took {:?}", $name, duration);
        result
    }};
}

#[macro_export]
macro_rules! assert_performance {
    ($timer:expr, max_duration = $max_dur:expr, min_ops = $min_ops:expr) => {{
        let report = $timer.report().await;
        assert!(
            report.meets_threshold($max_dur, $min_ops),
            "Performance threshold not met: {:?} (expected < {:?}, > {} ops/sec)",
            report.total_duration,
            $max_dur,
            $min_ops
        );
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timer_basic() {
        let timer = TestTimer::new("test_timer");
        
        timer.checkpoint("start").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        timer.record_operations(100);
        timer.checkpoint("middle").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        timer.record_operations(100);
        timer.checkpoint("end").await;
        
        let report = timer.report().await;
        assert_eq!(report.total_operations, 200);
        assert!(report.ops_per_second > 0.0);
        assert_eq!(report.checkpoint_durations.len(), 3);
    }

    #[tokio::test]
    async fn test_batch_analyzer() {
        let analyzer = BatchPerformanceAnalyzer::new(vec![10, 50, 100, 200]);
        
        let optimal = analyzer
            .analyze_batch_operation(|size| async move {
                // Simulate batch operation
                tokio::time::sleep(Duration::from_millis(size as u64 / 10)).await;
                Ok(vec![(); size])
            })
            .await
            .unwrap();

        assert!(optimal.size > 0);
        assert!(optimal.items_per_second > 0.0);
    }
}