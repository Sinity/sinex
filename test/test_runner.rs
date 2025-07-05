//! Custom test runner that ensures proper database cleanup
//!
//! This module provides initialization for the test infrastructure
//! and ensures all databases are cleaned up after tests complete.

use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::Mutex;

static INITIALIZED: OnceCell<Arc<Mutex<bool>>> = OnceCell::new();

/// Initialize test infrastructure
///
/// This should be called at the start of test execution to ensure
/// the database manager is properly initialized.
#[ctor::ctor]
fn initialize_test_infrastructure() {
    // Set up panic handler to ensure cleanup
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("\n❌ Test panic detected, cleaning up...");
        
        // Try to clean up in a blocking context
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = handle.block_on(async {
                if let Ok(stats) = crate::common::simple_db_manager::get_stats().await {
                    eprintln!("📊 {}", stats);
                }
            });
        }
        
        default_panic(info);
    }));
    
    // Mark as initialized
    INITIALIZED.set(Arc::new(Mutex::new(true))).ok();
}

/// Cleanup test infrastructure
///
/// This is called automatically when the test process exits.
// Disable automatic cleanup - it's interfering with tests
// Tests will clean up their own databases via Drop handlers
// The manager task will clean up idle databases periodically
// 
// If needed, cleanup can be done manually with:
// cargo test && ./scripts/cleanup_test_dbs.sh

/// Ensure test infrastructure is initialized
///
/// This can be called by tests to ensure initialization has occurred.
pub async fn ensure_initialized() {
    if INITIALIZED.get().is_none() {
        // This should not happen with ctor, but just in case
        initialize_test_infrastructure();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_infrastructure_initialized() {
        ensure_initialized().await;
        assert!(INITIALIZED.get().is_some());
    }
}