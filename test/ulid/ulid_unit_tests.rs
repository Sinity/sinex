use sinex_ulid::Ulid;
use std::str::FromStr;
use proptest::prelude::*;

// Removed basic ULID library tests - testing library functionality, not Sinex logic

#[test]
fn test_monotonic_ulid() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new_monotonic(Some(&ulid1));
    assert!(ulid2 > ulid1);
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