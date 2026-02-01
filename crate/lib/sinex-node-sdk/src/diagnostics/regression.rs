use bon::Builder;
use std::collections::HashMap;
use std::time::{Duration as StdDuration, Instant};

/// Metadata captured alongside baseline measurements (historical compatibility).
#[derive(Debug, Clone)]
pub struct EnvironmentInfo {
    pub test_data_size: usize,
    pub concurrent_operations: usize,
    pub database_pool_size: usize,
    pub system_load: String,
}

/// Snapshot of historical performance for a specific operation.
#[derive(Debug, Clone)]
pub struct PerformanceBaseline {
    pub operation_name: String,
    pub average_latency: StdDuration,
    pub percentile_95_latency: StdDuration,
    pub throughput: f64,
    pub success_rate: f64,
    pub sample_size: usize,
    pub environment: EnvironmentInfo,
}

/// Helper for accumulating measurements and computing baselines.
pub struct BaselineTracker {
    measurements: HashMap<String, Vec<StdDuration>>,
    success_counts: HashMap<String, usize>,
    error_counts: HashMap<String, usize>,
    baselines: HashMap<String, PerformanceBaseline>,
    start_time: Instant,
}

impl Default for BaselineTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BaselineTracker {
    pub fn new() -> Self {
        Self {
            measurements: HashMap::new(),
            success_counts: HashMap::new(),
            error_counts: HashMap::new(),
            baselines: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn record_measurement(&mut self, operation: &str, duration: StdDuration, success: bool) {
        self.measurements
            .entry(operation.to_string())
            .or_default()
            .push(duration);

        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }
    }

    pub fn calculate_baseline(
        &mut self,
        operation: &str,
        environment: EnvironmentInfo,
    ) -> Option<PerformanceBaseline> {
        let measurements = self.measurements.get(operation)?;
        if measurements.len() < 10 {
            return None;
        }

        let mut sorted = measurements.clone();
        sorted.sort();

        let average_latency = measurements.iter().sum::<StdDuration>() / measurements.len() as u32;
        let p95_index = (sorted.len() as f64 * 0.95) as usize;
        let percentile_95 = sorted[p95_index.min(sorted.len() - 1)];

        let success = self.success_counts.get(operation).copied().unwrap_or(0);
        let errors = self.error_counts.get(operation).copied().unwrap_or(0);
        let total = success + errors;
        let success_rate = if total > 0 {
            success as f64 / total as f64 * 100.0
        } else {
            0.0
        };

        let throughput = success as f64 / self.start_time.elapsed().as_secs_f64();

        let baseline = PerformanceBaseline {
            operation_name: operation.to_string(),
            average_latency,
            percentile_95_latency: percentile_95,
            throughput,
            success_rate,
            sample_size: measurements.len(),
            environment,
        };

        self.baselines
            .insert(operation.to_string(), baseline.clone());
        Some(baseline)
    }

    pub fn get_baseline(&self, operation: &str) -> Option<&PerformanceBaseline> {
        self.baselines.get(operation)
    }
}

/// Performance regression detection results
#[derive(Debug, Clone, Builder)]
pub struct RegressionDetectionResult {
    pub operation_name: String,
    pub baseline_performance: PerformanceBaseline,
    pub current_performance: PerformanceMeasurement,
    pub regression_detected: bool,
    pub regression_severity: RegressionSeverity,
    pub affected_metrics: Vec<String>,
    pub confidence_level: f64,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Builder)]
pub struct PerformanceMeasurement {
    pub average_latency: StdDuration,
    pub percentile_95_latency: StdDuration,
    pub percentile_99_latency: StdDuration,
    pub throughput: f64,
    pub success_rate: f64,
    pub sample_size: usize,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum RegressionSeverity {
    None,
    Minor,    // 10-25% degradation
    Moderate, // 25-50% degradation
    Severe,   // 50-100% degradation
    Critical, // >100% degradation or functionality broken
}

/// Regression detection engine
#[derive(Builder)]
pub struct RegressionDetector {
    #[builder(default)]
    baselines: HashMap<String, PerformanceBaseline>,
    #[builder(default)]
    thresholds: RegressionThresholds,
    #[builder(default)]
    measurements: HashMap<String, Vec<StdDuration>>,
    #[builder(default)]
    success_counts: HashMap<String, usize>,
    #[builder(default)]
    error_counts: HashMap<String, usize>,
    #[builder(default = Instant::now())]
    start_time: Instant,
}

#[derive(Debug, Clone, Builder)]
pub struct RegressionThresholds {
    #[builder(default = 1.1)]
    pub latency_minor_threshold: f64, // 1.1 = 10% increase
    #[builder(default = 1.25)]
    pub latency_moderate_threshold: f64, // 1.25 = 25% increase
    #[builder(default = 1.5)]
    pub latency_severe_threshold: f64, // 1.5 = 50% increase
    #[builder(default = 2.0)]
    pub latency_critical_threshold: f64, // 2.0 = 100% increase
    #[builder(default = 0.9)]
    pub throughput_minor_threshold: f64, // 0.9 = 10% decrease
    #[builder(default = 0.75)]
    pub throughput_moderate_threshold: f64, // 0.75 = 25% decrease
    #[builder(default = 0.5)]
    pub throughput_severe_threshold: f64, // 0.5 = 50% decrease
    #[builder(default = 0.95)]
    pub success_rate_threshold: f64, // 0.95 = 95% success rate
    #[builder(default = 0.8)]
    pub minimum_confidence: f64, // 0.8 = 80% confidence
}

impl Default for RegressionThresholds {
    fn default() -> Self {
        Self {
            latency_minor_threshold: 1.1,
            latency_moderate_threshold: 1.25,
            latency_severe_threshold: 1.5,
            latency_critical_threshold: 2.0,
            throughput_minor_threshold: 0.9,
            throughput_moderate_threshold: 0.75,
            throughput_severe_threshold: 0.5,
            success_rate_threshold: 0.95,
            minimum_confidence: 0.8,
        }
    }
}

impl Default for RegressionDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl RegressionDetector {
    pub fn new() -> Self {
        Self {
            baselines: HashMap::new(),
            thresholds: RegressionThresholds::default(),
            measurements: HashMap::new(),
            success_counts: HashMap::new(),
            error_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn with_thresholds(thresholds: RegressionThresholds) -> Self {
        Self {
            baselines: HashMap::new(),
            thresholds,
            measurements: HashMap::new(),
            success_counts: HashMap::new(),
            error_counts: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    pub fn set_baseline(&mut self, baseline: PerformanceBaseline) {
        self.baselines
            .insert(baseline.operation_name.clone(), baseline);
    }

    pub fn record_measurement(&mut self, operation: &str, duration: StdDuration, success: bool) {
        self.measurements
            .entry(operation.to_string())
            .or_default()
            .push(duration);

        if success {
            *self
                .success_counts
                .entry(operation.to_string())
                .or_insert(0) += 1;
        } else {
            *self.error_counts.entry(operation.to_string()).or_insert(0) += 1;
        }
    }

    pub fn calculate_current_performance(&self, operation: &str) -> Option<PerformanceMeasurement> {
        if let Some(measurements) = self.measurements.get(operation) {
            if measurements.len() < 10 {
                return None; // Not enough samples
            }

            let mut sorted_measurements = measurements.clone();
            sorted_measurements.sort();

            let average_latency =
                measurements.iter().sum::<StdDuration>() / measurements.len() as u32;

            let p95_index = (measurements.len() as f64 * 0.95) as usize;
            let p99_index = (measurements.len() as f64 * 0.99) as usize;

            let percentile_95_latency =
                sorted_measurements[p95_index.min(sorted_measurements.len() - 1)];
            let percentile_99_latency =
                sorted_measurements[p99_index.min(sorted_measurements.len() - 1)];

            let success_count = self.success_counts.get(operation).unwrap_or(&0);
            let error_count = self.error_counts.get(operation).unwrap_or(&0);
            let total_operations = success_count + error_count;

            let success_rate = if total_operations > 0 {
                *success_count as f64 / total_operations as f64 * 100.0
            } else {
                0.0
            };

            let throughput = *success_count as f64 / self.start_time.elapsed().as_secs_f64();

            Some(PerformanceMeasurement {
                average_latency,
                percentile_95_latency,
                percentile_99_latency,
                throughput,
                success_rate,
                sample_size: measurements.len(),
            })
        } else {
            None
        }
    }

    pub fn detect_regression(&self, operation: &str) -> Option<RegressionDetectionResult> {
        let baseline = self.baselines.get(operation)?;
        let current = self.calculate_current_performance(operation)?;

        let mut affected_metrics = Vec::new();
        let mut regression_severity = RegressionSeverity::None;

        // Check latency regression
        let latency_ratio =
            current.average_latency.as_secs_f64() / baseline.average_latency.as_secs_f64();
        if latency_ratio >= self.thresholds.latency_critical_threshold {
            regression_severity = RegressionSeverity::Critical;
            affected_metrics.push("latency".to_string());
        } else if latency_ratio >= self.thresholds.latency_severe_threshold {
            if regression_severity < RegressionSeverity::Severe {
                regression_severity = RegressionSeverity::Severe;
            }
            affected_metrics.push("latency".to_string());
        } else if latency_ratio >= self.thresholds.latency_moderate_threshold {
            if regression_severity < RegressionSeverity::Moderate {
                regression_severity = RegressionSeverity::Moderate;
            }
            affected_metrics.push("latency".to_string());
        } else if latency_ratio >= self.thresholds.latency_minor_threshold {
            if regression_severity < RegressionSeverity::Minor {
                regression_severity = RegressionSeverity::Minor;
            }
            affected_metrics.push("latency".to_string());
        }

        // Check throughput regression
        let throughput_ratio = current.throughput / baseline.throughput;
        if throughput_ratio <= self.thresholds.throughput_severe_threshold {
            if regression_severity < RegressionSeverity::Severe {
                regression_severity = RegressionSeverity::Severe;
            }
            affected_metrics.push("throughput".to_string());
        } else if throughput_ratio <= self.thresholds.throughput_moderate_threshold {
            if regression_severity < RegressionSeverity::Moderate {
                regression_severity = RegressionSeverity::Moderate;
            }
            affected_metrics.push("throughput".to_string());
        } else if throughput_ratio <= self.thresholds.throughput_minor_threshold {
            if regression_severity < RegressionSeverity::Minor {
                regression_severity = RegressionSeverity::Minor;
            }
            affected_metrics.push("throughput".to_string());
        }

        // Check success rate regression
        if current.success_rate < baseline.success_rate * self.thresholds.success_rate_threshold {
            if regression_severity < RegressionSeverity::Critical {
                regression_severity = RegressionSeverity::Critical;
            }
            affected_metrics.push("success_rate".to_string());
        }

        // Calculate confidence level based on sample sizes
        let confidence_level = self.calculate_confidence_level(baseline, &current);

        let regression_detected = regression_severity != RegressionSeverity::None
            && confidence_level >= self.thresholds.minimum_confidence;

        let recommendations = self.generate_recommendations(
            &regression_severity,
            &affected_metrics,
            latency_ratio,
            throughput_ratio,
        );

        Some(RegressionDetectionResult {
            operation_name: operation.to_string(),
            baseline_performance: baseline.clone(),
            current_performance: current,
            regression_detected,
            regression_severity,
            affected_metrics,
            confidence_level,
            recommendations,
        })
    }

    fn calculate_confidence_level(
        &self,
        baseline: &PerformanceBaseline,
        current: &PerformanceMeasurement,
    ) -> f64 {
        // Simple confidence calculation based on sample sizes
        // In practice, would use proper statistical tests (t-test, Welch's test, etc.)
        let min_samples = baseline.sample_size.min(current.sample_size) as f64;
        let confidence = (min_samples / 100.0).min(1.0); // More samples = higher confidence
        confidence * 0.8 + 0.2 // Minimum 20% confidence, max approaches 100%
    }

    fn generate_recommendations(
        &self,
        severity: &RegressionSeverity,
        affected_metrics: &[String],
        latency_ratio: f64,
        throughput_ratio: f64,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        match severity {
            RegressionSeverity::Critical => {
                recommendations.push("🚨 CRITICAL: Immediate investigation required".to_string());
                recommendations.push("Consider rolling back recent changes".to_string());
                recommendations
                    .push("Check for resource exhaustion or system failures".to_string());
            }
            RegressionSeverity::Severe => {
                recommendations.push("⚠️  SEVERE: High priority investigation needed".to_string());
                recommendations.push("Review recent code changes and deployment".to_string());
                recommendations
                    .push("Monitor system resources and database performance".to_string());
            }
            RegressionSeverity::Moderate => {
                recommendations.push("⚡ MODERATE: Investigation recommended".to_string());
                recommendations.push("Profile performance in affected operations".to_string());
            }
            RegressionSeverity::Minor => {
                recommendations.push("📊 MINOR: Monitor for trending issues".to_string());
                recommendations.push("Consider optimization opportunities".to_string());
            }
            RegressionSeverity::None => {
                recommendations.push("✅ No significant regression detected".to_string());
            }
        }

        if affected_metrics.contains(&"latency".to_string()) {
            recommendations.push(format!(
                "Latency increased by {:.1}%",
                (latency_ratio - 1.0) * 100.0
            ));
            recommendations.push("Check database query performance and indexing".to_string());
        }

        if affected_metrics.contains(&"throughput".to_string()) {
            recommendations.push(format!(
                "Throughput decreased by {:.1}%",
                (1.0 - throughput_ratio) * 100.0
            ));
            recommendations.push("Check for bottlenecks in concurrent processing".to_string());
        }

        if affected_metrics.contains(&"success_rate".to_string()) {
            recommendations.push("Success rate degradation detected".to_string());
            recommendations.push("Check error logs and exception patterns".to_string());
        }

        recommendations
    }

    pub fn print_regression_report(&self, results: &[RegressionDetectionResult]) {
        println!("\n🔍 Performance Regression Detection Report");
        println!("==========================================");

        let mut critical_count = 0;
        let mut severe_count = 0;
        let mut moderate_count = 0;
        let mut minor_count = 0;
        let mut clean_count = 0;

        for result in results {
            match result.regression_severity {
                RegressionSeverity::Critical => critical_count += 1,
                RegressionSeverity::Severe => severe_count += 1,
                RegressionSeverity::Moderate => moderate_count += 1,
                RegressionSeverity::Minor => minor_count += 1,
                RegressionSeverity::None => clean_count += 1,
            }
        }

        println!("\n📊 Summary:");
        println!("  🚨 Critical: {}", critical_count);
        println!("  ⚠️  Severe: {}", severe_count);
        println!("  ⚡ Moderate: {}", moderate_count);
        println!("  📊 Minor: {}", minor_count);
        println!("  ✅ Clean: {}", clean_count);

        for result in results {
            if result.regression_detected {
                println!("\n🔍 Operation: {}", result.operation_name);
                println!("  Severity: {:?}", result.regression_severity);
                println!("  Confidence: {:.1}%", result.confidence_level * 100.0);
                println!("  Affected metrics: {:?}", result.affected_metrics);

                println!("  📈 Performance comparison:");
                println!(
                    "    Baseline latency: {:?} -> Current: {:?}",
                    result.baseline_performance.average_latency,
                    result.current_performance.average_latency
                );
                println!(
                    "    Baseline throughput: {:.2} -> Current: {:.2}",
                    result.baseline_performance.throughput, result.current_performance.throughput
                );

                println!("  💡 Recommendations:");
                for recommendation in &result.recommendations {
                    println!("    - {}", recommendation);
                }
            }
        }
    }
}
