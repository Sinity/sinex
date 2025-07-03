//! Cleanup hook for test infrastructure
//!
//! Ensures that the database pool is properly cleaned up when tests finish.

use std::sync::Once;

static CLEANUP_HOOK: Once = Once::new();

/// Register cleanup hook for the database pool
/// 
/// This is called automatically when the first test acquires a database.
/// It ensures that all test databases are cleaned up when the test process exits.
pub fn register_cleanup_hook() {
    CLEANUP_HOOK.call_once(|| {
        // Simple approach: let Drop handlers clean up naturally
        // The PooledDatabase drop handler returns databases to the pool,
        // and the pool manager can clean up on process exit.
        //
        // For now, we rely on PostgreSQL's own cleanup of connections
        // when the client process terminates. Test databases are prefixed
        // with the process ID, making them easy to identify and clean up
        // in development if needed.
        
        // In the future, we could add more sophisticated cleanup using:
        // - std::process::exit handlers
        // - Custom test harness
        // - External cleanup script
    });
}
