// Failure injection system for testing
//
// Provides sophisticated failure simulation capabilities for testing system resilience

use crate::common::prelude::*;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Configuration for failure injection
#[derive(Debug, Clone)]
pub struct FailureConfig {
    pub patterns: Vec<FailurePattern>,
    pub enabled: bool,
}

/// Different types of failure patterns
#[derive(Debug, Clone)]
pub enum FailurePattern {
    /// Always fail specific operation
    Permanent { operation: String },
    /// Fail operation with given probability
    Probabilistic {
        operation: String,
        failure_rate: f64,
    },
    /// Fail operation for a duration
    Temporary {
        operation: String,
        failure_rate: f64,
        duration: Duration,
    },
    /// Fail operation intermittently
    Intermittent {
        operation: String,
        failure_rate: f64,
        interval: Duration,
    },
    /// Fail operation under specific conditions
    Conditional {
        operation: String,
        condition: FailureCondition,
    },
    /// Cascade failures (one failure triggers others)
    Cascade {
        trigger_operation: String,
        cascade_operations: Vec<String>,
        cascade_delay: Duration,
    },
}

/// Conditions for conditional failures
#[derive(Debug, Clone)]
pub enum FailureCondition {
    /// Fail after N operations
    AfterCount(usize),
    /// Fail during time window
    TimeWindow { start: Duration, end: Duration },
    /// Fail based on system load
    LoadThreshold(f64),
    /// Fail based on memory usage
    MemoryThreshold(usize),
    /// Custom condition based on state
    Custom(String),
}

/// Failure injection engine
pub struct FailureInjector {
    config: FailureConfig,
    patterns: RwLock<Vec<ActiveFailurePattern>>,
    operation_counts: RwLock<HashMap<String, usize>>,
    start_time: Instant,
}

#[derive(Debug)]
struct ActiveFailurePattern {
    pattern: FailurePattern,
    started_at: Instant,
    triggered_count: usize,
    last_trigger: Option<Instant>,
}

impl FailureInjector {
    pub fn new(config: FailureConfig) -> Self {
        let patterns = config
            .patterns
            .iter()
            .map(|p| ActiveFailurePattern {
                pattern: p.clone(),
                started_at: Instant::now(),
                triggered_count: 0,
                last_trigger: None,
            })
            .collect();

        Self {
            config,
            patterns: RwLock::new(patterns),
            operation_counts: RwLock::new(HashMap::new()),
            start_time: Instant::now(),
        }
    }

    pub async fn should_fail(&mut self, operation: &str) -> bool {
        if !self.config.enabled {
            return false;
        }

        // Update operation count
        {
            let mut counts = self.operation_counts.write().await;
            *counts.entry(operation.to_string()).or_insert(0) += 1;
        }

        let mut patterns = self.patterns.write().await;
        let now = Instant::now();
        let uptime = now.duration_since(self.start_time);

        for active_pattern in patterns.iter_mut() {
            if self.pattern_matches(operation, &active_pattern.pattern) {
                if self
                    .should_trigger_pattern(&active_pattern.pattern, operation, now, uptime)
                    .await
                {
                    active_pattern.triggered_count += 1;
                    active_pattern.last_trigger = Some(now);
                    return true;
                }
            }
        }

        false
    }

    pub async fn add_pattern(&mut self, pattern: FailurePattern) {
        let mut patterns = self.patterns.write().await;
        patterns.push(ActiveFailurePattern {
            pattern,
            started_at: Instant::now(),
            triggered_count: 0,
            last_trigger: None,
        });
    }

    pub async fn remove_pattern(&mut self, operation: &str) {
        let mut patterns = self.patterns.write().await;
        patterns.retain(|p| !self.pattern_matches(operation, &p.pattern));
    }

    pub async fn clear_patterns(&mut self) {
        let mut patterns = self.patterns.write().await;
        patterns.clear();
    }

    pub async fn get_stats(&self) -> FailureStats {
        let patterns = self.patterns.read().await;
        let operation_counts = self.operation_counts.read().await;

        FailureStats {
            active_patterns: patterns.len(),
            total_operations: operation_counts.values().sum(),
            operation_counts: operation_counts.clone(),
            uptime: self.start_time.elapsed(),
        }
    }

    fn pattern_matches(&self, operation: &str, pattern: &FailurePattern) -> bool {
        match pattern {
            FailurePattern::Permanent { operation: op } => op == operation || op == "*",
            FailurePattern::Probabilistic { operation: op, .. } => op == operation || op == "*",
            FailurePattern::Temporary { operation: op, .. } => op == operation || op == "*",
            FailurePattern::Intermittent { operation: op, .. } => op == operation || op == "*",
            FailurePattern::Conditional { operation: op, .. } => op == operation || op == "*",
            FailurePattern::Cascade {
                trigger_operation,
                cascade_operations,
                ..
            } => {
                trigger_operation == operation
                    || cascade_operations.contains(&operation.to_string())
            }
        }
    }

    async fn should_trigger_pattern(
        &self,
        pattern: &FailurePattern,
        operation: &str,
        now: Instant,
        uptime: Duration,
    ) -> bool {
        match pattern {
            FailurePattern::Permanent { .. } => true,

            FailurePattern::Probabilistic { failure_rate, .. } => fastrand::f64() < *failure_rate,

            FailurePattern::Temporary {
                failure_rate,
                duration,
                ..
            } => {
                if uptime < *duration {
                    fastrand::f64() < *failure_rate
                } else {
                    false
                }
            }

            FailurePattern::Intermittent {
                failure_rate,
                interval,
                ..
            } => {
                let cycle_position = uptime.as_secs_f64() % interval.as_secs_f64();
                let failure_window = interval.as_secs_f64() * failure_rate;
                cycle_position < failure_window
            }

            FailurePattern::Conditional { condition, .. } => {
                self.evaluate_condition(condition, operation, uptime).await
            }

            FailurePattern::Cascade {
                trigger_operation,
                cascade_operations,
                cascade_delay,
                ..
            } => {
                if trigger_operation == operation {
                    true // Trigger always fails
                } else if cascade_operations.contains(&operation.to_string()) {
                    // Check if trigger was recently activated
                    let patterns = self.patterns.read().await;
                    patterns.iter().any(|p| {
                        if let FailurePattern::Cascade {
                            trigger_operation: trigger,
                            ..
                        } = &p.pattern
                        {
                            trigger == trigger_operation
                                && p.last_trigger
                                    .map_or(false, |t| now.duration_since(t) < *cascade_delay)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            }
        }
    }

    async fn evaluate_condition(
        &self,
        condition: &FailureCondition,
        operation: &str,
        uptime: Duration,
    ) -> bool {
        match condition {
            FailureCondition::AfterCount(threshold) => {
                let counts = self.operation_counts.read().await;
                counts.get(operation).copied().unwrap_or(0) >= *threshold
            }

            FailureCondition::TimeWindow { start, end } => uptime >= *start && uptime <= *end,

            FailureCondition::LoadThreshold(threshold) => {
                // In a real implementation, this would check actual system load
                // For testing, we'll simulate based on operation count
                let counts = self.operation_counts.read().await;
                let total_ops = counts.values().sum::<usize>();
                (total_ops as f64 / 100.0) > *threshold
            }

            FailureCondition::MemoryThreshold(threshold) => {
                // In a real implementation, this would check actual memory usage
                // For testing, we'll simulate based on time
                uptime.as_secs() as usize * 1024 > *threshold
            }

            FailureCondition::Custom(condition_name) => {
                // Custom conditions can be implemented as needed
                match condition_name.as_str() {
                    "test_condition" => true,
                    _ => false,
                }
            }
        }
    }

    /// Simulate network partition
    pub async fn simulate_partition(&mut self, operations: &[&str], duration: Duration) {
        for operation in operations {
            let pattern = FailurePattern::Temporary {
                operation: operation.to_string(),
                failure_rate: 1.0,
                duration,
            };
            self.add_pattern(pattern).await;
        }
    }

    /// Simulate cascading failures
    pub async fn simulate_cascade(&mut self, trigger: &str, cascades: &[&str], delay: Duration) {
        let pattern = FailurePattern::Cascade {
            trigger_operation: trigger.to_string(),
            cascade_operations: cascades.iter().map(|s| s.to_string()).collect(),
            cascade_delay: delay,
        };
        self.add_pattern(pattern).await;
    }

    /// Simulate intermittent failures
    pub async fn simulate_intermittent(
        &mut self,
        operation: &str,
        failure_rate: f64,
        interval: Duration,
    ) {
        let pattern = FailurePattern::Intermittent {
            operation: operation.to_string(),
            failure_rate,
            interval,
        };
        self.add_pattern(pattern).await;
    }

    /// Simulate resource exhaustion
    pub async fn simulate_resource_exhaustion(&mut self, operation: &str, threshold: usize) {
        let pattern = FailurePattern::Conditional {
            operation: operation.to_string(),
            condition: FailureCondition::AfterCount(threshold),
        };
        self.add_pattern(pattern).await;
    }
}

/// Statistics about failure injection
#[derive(Debug, Clone)]
pub struct FailureStats {
    pub active_patterns: usize,
    pub total_operations: usize,
    pub operation_counts: HashMap<String, usize>,
    pub uptime: Duration,
}

/// Builder for creating failure patterns
pub struct FailurePatternBuilder {
    operation: String,
}

impl FailurePatternBuilder {
    pub fn new(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
        }
    }

    pub fn permanent(self) -> FailurePattern {
        FailurePattern::Permanent {
            operation: self.operation,
        }
    }

    pub fn probabilistic(self, failure_rate: f64) -> FailurePattern {
        FailurePattern::Probabilistic {
            operation: self.operation,
            failure_rate,
        }
    }

    pub fn temporary(self, failure_rate: f64, duration: Duration) -> FailurePattern {
        FailurePattern::Temporary {
            operation: self.operation,
            failure_rate,
            duration,
        }
    }

    pub fn intermittent(self, failure_rate: f64, interval: Duration) -> FailurePattern {
        FailurePattern::Intermittent {
            operation: self.operation,
            failure_rate,
            interval,
        }
    }

    pub fn after_count(self, threshold: usize) -> FailurePattern {
        FailurePattern::Conditional {
            operation: self.operation,
            condition: FailureCondition::AfterCount(threshold),
        }
    }

    pub fn time_window(self, start: Duration, end: Duration) -> FailurePattern {
        FailurePattern::Conditional {
            operation: self.operation,
            condition: FailureCondition::TimeWindow { start, end },
        }
    }
}

/// Convenience function for creating failure patterns
pub fn failure_pattern(operation: &str) -> FailurePatternBuilder {
    FailurePatternBuilder::new(operation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_permanent_failure() {
        let config = FailureConfig {
            patterns: vec![FailurePattern::Permanent {
                operation: "test".to_string(),
            }],
            enabled: true,
        };

        let mut injector = FailureInjector::new(config);
        assert!(injector.should_fail("test").await);
        assert!(injector.should_fail("test").await);
        assert!(!injector.should_fail("other").await);
    }

    #[tokio::test]
    async fn test_probabilistic_failure() {
        let config = FailureConfig {
            patterns: vec![FailurePattern::Probabilistic {
                operation: "test".to_string(),
                failure_rate: 0.5,
            }],
            enabled: true,
        };

        let mut injector = FailureInjector::new(config);

        // Test multiple times to verify probabilistic behavior
        let mut failures = 0;
        let iterations = 1000;
        for _ in 0..iterations {
            if injector.should_fail("test").await {
                failures += 1;
            }
        }

        // Should be roughly 50% failure rate (allow for variance)
        let failure_rate = failures as f64 / iterations as f64;
        assert!(failure_rate > 0.4 && failure_rate < 0.6);
    }

    #[tokio::test]
    async fn test_temporary_failure() {
        let config = FailureConfig {
            patterns: vec![FailurePattern::Temporary {
                operation: "test".to_string(),
                failure_rate: 1.0,
                duration: Duration::from_millis(100),
            }],
            enabled: true,
        };

        let mut injector = FailureInjector::new(config);

        // Should fail initially
        assert!(injector.should_fail("test").await);

        // Wait for pattern to expire
        sleep(Duration::from_millis(150)).await;

        // Should not fail after expiration
        assert!(!injector.should_fail("test").await);
    }

    #[tokio::test]
    async fn test_conditional_failure() {
        let config = FailureConfig {
            patterns: vec![FailurePattern::Conditional {
                operation: "test".to_string(),
                condition: FailureCondition::AfterCount(3),
            }],
            enabled: true,
        };

        let mut injector = FailureInjector::new(config);

        // Should not fail initially
        assert!(!injector.should_fail("test").await);
        assert!(!injector.should_fail("test").await);
        assert!(!injector.should_fail("test").await);

        // Should fail after threshold
        assert!(injector.should_fail("test").await);
        assert!(injector.should_fail("test").await);
    }

    #[tokio::test]
    async fn test_cascade_failure() {
        let config = FailureConfig {
            patterns: vec![FailurePattern::Cascade {
                trigger_operation: "trigger".to_string(),
                cascade_operations: vec!["cascade1".to_string(), "cascade2".to_string()],
                cascade_delay: Duration::from_millis(100),
            }],
            enabled: true,
        };

        let mut injector = FailureInjector::new(config);

        // Trigger should always fail
        assert!(injector.should_fail("trigger").await);

        // Cascade operations should fail within delay window
        assert!(injector.should_fail("cascade1").await);
        assert!(injector.should_fail("cascade2").await);

        // Wait for cascade to expire
        sleep(Duration::from_millis(150)).await;

        // Cascade operations should not fail after delay
        assert!(!injector.should_fail("cascade1").await);
        assert!(!injector.should_fail("cascade2").await);
    }
}
