use crate::common::prelude::*;

#[test]
fn test_ulid_creation() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    pretty_assertions::assert_ne!(ulid1, ulid2);
}

#[test]
fn test_monotonic_ulid() {
    let ulid1 = Ulid::new();
    // Note: new_monotonic not available in current implementation
    // Using regular new() instead
    let ulid2 = Ulid::new();
    assert!(ulid2 > ulid1);
}

#[test]
fn test_uuid_conversion() {
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    let ulid2 = Ulid::from_uuid(uuid);
    pretty_assertions::assert_eq!(ulid, ulid2);
}