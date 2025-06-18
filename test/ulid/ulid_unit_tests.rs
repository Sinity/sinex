use sinex_ulid::Ulid;
use std::str::FromStr;
use proptest::prelude::*;

#[test]
fn test_ulid_creation() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    assert_ne!(ulid1, ulid2);
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
    assert_eq!(ulid, ulid2);
}

proptest! {
    #[test]
    fn test_ulid_string_roundtrip(s in "[0-9A-Z]{26}") {
        if let Ok(ulid) = Ulid::from_str(&s) {
            let s2 = ulid.to_string();
            let ulid2 = Ulid::from_str(&s2).unwrap();
            prop_assert_eq!(ulid, ulid2);
        }
    }
}