pub mod metrics;
pub mod worker;

use anyhow::Result;
use async_trait::async_trait;
use sinex_db::models::PromotionQueueItem;
use sqlx::PgPool;

/// Trait for implementing agent-specific processing logic
#[async_trait]
pub trait EventProcessor: Send + Sync {
    /// Process a single event from the promotion queue
    async fn process_event(
        &self,
        pool: &PgPool,
        item: &PromotionQueueItem,
    ) -> Result<()>;

    /// Get the agent name this processor handles
    fn agent_name(&self) -> &str;

    /// Get the batch size for processing
    fn batch_size(&self) -> i32 {
        10
    }

    /// Get the poll interval in seconds when no work is available
    fn poll_interval_secs(&self) -> u64 {
        1
    }
}

/// Calculate exponential backoff with jitter
pub fn calculate_backoff_secs(attempts: i32) -> f64 {
    use rand::Rng;
    
    let base_delay_secs = 60.0;
    let delay_secs = base_delay_secs * (2.0_f64.powi(attempts));
    let jitter_factor = rand::thread_rng().gen_range(0.8..=1.2);
    let final_delay_secs = (delay_secs * jitter_factor).max(1.0).min(24.0 * 3600.0);
    
    final_delay_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_backoff_basic() {
        // Test that backoff increases exponentially
        let backoff_0 = calculate_backoff_secs(0);
        let backoff_1 = calculate_backoff_secs(1);
        let backoff_2 = calculate_backoff_secs(2);
        
        // Should be roughly 60s, 120s, 240s (with jitter)
        assert!(backoff_0 >= 48.0 && backoff_0 <= 72.0); // 60 * 0.8 to 60 * 1.2
        assert!(backoff_1 >= 96.0 && backoff_1 <= 144.0); // 120 * 0.8 to 120 * 1.2
        assert!(backoff_2 >= 192.0 && backoff_2 <= 288.0); // 240 * 0.8 to 240 * 1.2
    }

    #[test]
    fn test_calculate_backoff_min_max() {
        // Test minimum bound
        let backoff_negative = calculate_backoff_secs(-10);
        assert!(backoff_negative >= 1.0);
        
        // Test maximum bound (should cap at 24 hours)
        let backoff_large = calculate_backoff_secs(20);
        assert!(backoff_large <= 24.0 * 3600.0);
    }

    #[test]
    fn test_calculate_backoff_jitter() {
        // Test that jitter produces different values
        let mut values = std::collections::HashSet::new();
        for _ in 0..10 {
            values.insert((calculate_backoff_secs(1) * 1000.0) as i64);
        }
        // With jitter, we should get at least 2 different values
        assert!(values.len() >= 2);
    }
}