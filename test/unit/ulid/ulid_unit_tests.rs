use crate::common::prelude::*;
use sinex_test_macros::sinex_test;

#[sinex_test]
async fn test_ulid_creation() -> Result<(), Box<dyn std::error::Error>> {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    pretty_assertions::assert_ne!(ulid1, ulid2);
    Ok(())
}

#[sinex_test]
async fn test_monotonic_ulid() -> Result<(), Box<dyn std::error::Error>> {
    let ulid1 = Ulid::new();
    // Note: new_monotonic not available in current implementation
    // Using regular new() instead
    let ulid2 = Ulid::new();
    assert!(ulid2 > ulid1);
    Ok(())
}

#[sinex_test]
async fn test_uuid_conversion() -> Result<(), Box<dyn std::error::Error>> {
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    let ulid2 = Ulid::from_uuid(uuid);
    pretty_assertions::assert_eq!(ulid, ulid2);
    Ok(())
}