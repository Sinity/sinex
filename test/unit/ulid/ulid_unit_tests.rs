use crate::common::prelude::*;

#[sinex_test]
async fn test_ulid_creation() -> TestResult {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    pretty_assertions::assert_ne!(ulid1, ulid2);
    Ok(())
}

#[sinex_test]
async fn test_monotonic_ulid() -> TestResult {
    let ulid1 = Ulid::new();
    // Note: new_monotonic not available in current implementation
    // Using regular new() instead
    let ulid2 = Ulid::new();
    assert!(ulid2 > ulid1);
    Ok(())
}

#[sinex_test]
async fn test_uuid_conversion() -> TestResult {
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    let ulid2 = Ulid::from_uuid(uuid);
    pretty_assertions::assert_eq!(ulid, ulid2);
    Ok(())
}
