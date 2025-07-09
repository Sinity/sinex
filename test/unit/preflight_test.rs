//! Preflight Unit Tests - Basic functionality testing

use crate::common::prelude::*;
use sinex_preflight::VerificationStatus;

/// Test basic VerificationStatus functionality
#[sinex_test]
async fn test_verification_status_basic(ctx: TestContext) -> TestResult {
    // Test that VerificationStatus enum works correctly
    assert_eq!(VerificationStatus::Pass, VerificationStatus::Pass);
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Fail);
    
    // Test enum variants exist
    let _pass = VerificationStatus::Pass;
    let _warn = VerificationStatus::Warning;
    let _fail = VerificationStatus::Fail;
    
    Ok(())
}

/// Test verification status comparisons
#[sinex_test]
async fn test_verification_status_comparisons(ctx: TestContext) -> TestResult {
    // Test basic equality
    assert_eq!(VerificationStatus::Pass, VerificationStatus::Pass);
    assert_eq!(VerificationStatus::Warning, VerificationStatus::Warning);
    assert_eq!(VerificationStatus::Fail, VerificationStatus::Fail);
    
    // Test inequality
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Warning);
    assert_ne!(VerificationStatus::Warning, VerificationStatus::Fail);
    assert_ne!(VerificationStatus::Pass, VerificationStatus::Fail);
    
    Ok(())
}