use std::time::Duration;

/// Retry configuration for transient RPC failures
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum retry attempts
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set initial backoff duration
    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// Set maximum backoff duration
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Set backoff multiplier
    pub fn with_backoff_multiplier(mut self, backoff_multiplier: f32) -> Self {
        self.backoff_multiplier = backoff_multiplier;
        self
    }

    /// Calculate backoff duration for a given attempt number
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return self.initial_backoff;
        }

        let multiplier = self.backoff_multiplier.powi(attempt as i32 - 1);
        let backoff = self.initial_backoff.mul_f32(multiplier);

        // Cap at max_backoff
        if backoff > self.max_backoff {
            self.max_backoff
        } else {
            backoff
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_secs(10));
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_builder_pattern() {
        let config = RetryConfig::new()
            .with_max_attempts(5)
            .with_initial_backoff(Duration::from_millis(50))
            .with_max_backoff(Duration::from_secs(5))
            .with_backoff_multiplier(1.5);

        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(50));
        assert_eq!(config.max_backoff, Duration::from_secs(5));
        assert_eq!(config.backoff_multiplier, 1.5);
    }

    #[test]
    fn test_exponential_backoff() {
        let config = RetryConfig::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_backoff_multiplier(2.0);

        // Use approximate comparison for floating point precision
        let assert_approx = |actual: Duration, expected_ms: u64| {
            let diff = (actual.as_millis() as i64 - expected_ms as i64).abs();
            assert!(diff < 2, "Expected ~{}ms, got {:?}", expected_ms, actual);
        };

        // Attempt 0: 100ms
        assert_approx(config.backoff_for_attempt(0), 100);
        // Attempt 1: 100ms * 2^0 = 100ms
        assert_approx(config.backoff_for_attempt(1), 100);
        // Attempt 2: 100ms * 2^1 = 200ms
        assert_approx(config.backoff_for_attempt(2), 200);
        // Attempt 3: 100ms * 2^2 = 400ms
        assert_approx(config.backoff_for_attempt(3), 400);
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let config = RetryConfig::new()
            .with_initial_backoff(Duration::from_secs(1))
            .with_max_backoff(Duration::from_secs(5))
            .with_backoff_multiplier(10.0);

        // Should be capped at 5 seconds
        let backoff = config.backoff_for_attempt(5);
        assert!(backoff <= Duration::from_secs(5));
    }
}
