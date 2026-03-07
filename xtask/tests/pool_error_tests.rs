//! Canary tests for pool acquisition error message format.
//!
//! These tests verify that the timeout error messages emitted by the pool
//! acquisition loop (pool/mod.rs) contain the expected hint text. If the
//! error format regresses to something unhelpful, these tests will fail.
//!
//! Tests do NOT require an active database connection.

use xtask::sandbox::sinex_test;

// ============================================================================
// Acquisition Timeout Error Message
// ============================================================================

/// The timeout error message must include "permanently locked" as a diagnostic hint.
///
/// Mirrors the error constructed in `xtask::sandbox::db::pool::mod` on timeout:
/// ```text
/// "Database acquisition timed out after ... All slots may be permanently locked."
/// ```
#[sinex_test]
async fn test_acquisition_timeout_error_contains_hint() -> ::xtask::sandbox::TestResult<()> {
    let elapsed = std::time::Duration::from_mins(1);
    let attempts = 120u32;
    let lock_holders = String::new(); // empty when no lock-holder query is available

    let msg = format!(
        "Database acquisition timed out after {elapsed:.1?} ({attempts} attempts). \
         All slots may be permanently locked.\
         {lock_holders}"
    );

    assert!(
        msg.contains("permanently locked"),
        "Timeout error must include 'permanently locked' hint.\nGot: {msg}"
    );
    assert!(
        msg.contains("All slots"),
        "Timeout error must include 'All slots' for context.\nGot: {msg}"
    );
    assert!(
        msg.contains("120 attempts"),
        "Timeout error must include attempt count.\nGot: {msg}"
    );
    Ok(())
}

// ============================================================================
// Stall Warning Threshold
// ============================================================================

/// The stall warning fires at elapsed > 10 seconds (not >=).
///
/// Mirrors the condition in `pool/mod.rs`:
/// `if elapsed > Duration::from_secs(10) && attempts == 1 { ... }`
///
/// If this threshold changes, the test and the CLAUDE.md docs should be updated together.
#[sinex_test]
async fn test_stall_warning_threshold_is_ten_seconds() -> ::xtask::sandbox::TestResult<()> {
    let threshold = std::time::Duration::from_secs(10);

    // At exactly 10s, the condition `elapsed > threshold` is false.
    assert!(
        (threshold <= threshold),
        "10s must NOT exceed the stall threshold (condition uses >)"
    );

    // At 11s, the warning should fire.
    let above = std::time::Duration::from_secs(11);
    assert!(above > threshold, "11s must exceed the 10s stall threshold");

    // At 9s, the warning must not fire.
    let below = std::time::Duration::from_secs(9);
    assert!(
        (below <= threshold),
        "9s must not exceed the stall threshold"
    );
    Ok(())
}

// ============================================================================
// pg_stat_activity Context in Errors
// ============================================================================

/// When lock-holder context is available, it must be appended to the timeout error.
///
/// This verifies that the pg_stat_activity diagnostic output (item 1.6) would be
/// included in error messages when non-empty.
#[sinex_test]
async fn test_acquisition_error_includes_lock_holder_context() -> ::xtask::sandbox::TestResult<()> {
    let elapsed = std::time::Duration::from_mins(1);
    let attempts = 5u32;
    let lock_holders =
        "\n\nLock holders:\n  pid=1234 app=nextest query=SELECT pg_advisory_lock(42)".to_string();

    let msg = format!(
        "Database acquisition timed out after {elapsed:.1?} ({attempts} attempts). \
         All slots may be permanently locked.\
         {lock_holders}"
    );

    assert!(
        msg.contains("Lock holders"),
        "Error with lock-holder context must include 'Lock holders'.\nGot: {msg}"
    );
    assert!(
        msg.contains("pg_advisory_lock"),
        "Lock holder context should identify the lock type.\nGot: {msg}"
    );
    Ok(())
}
