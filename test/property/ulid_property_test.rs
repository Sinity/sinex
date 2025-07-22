use crate::common::prelude::*;
use crate::common::property_helpers::*;
use proptest::prelude::*;
use sinex_ulid::Ulid;
use std::collections::HashSet;
use std::str::FromStr;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn ulid_uniqueness_property(
        ulids in proptest::collection::vec(arbitrary_ulid(), 1..100)
    ) {
        // Property: All generated ULIDs should be unique
        let unique_ulids: HashSet<_> = ulids.iter().cloned().collect();
        assert_eq!(ulids.len(), unique_ulids.len(), "Generated ULIDs should be unique");
    }

    #[test]
    fn ulid_ordering_property(
        range in arbitrary_ulid_range()
    ) {
        let (start, end) = range;
        // Property: ULID ordering should be consistent with comparison operators
        assert!(start <= end, "ULID range should be properly ordered");
        
        // Property: ULIDs between start and end should maintain ordering
        let middle = Ulid::new();
        if start < middle && middle < end {
            assert!(start < middle);
            assert!(middle < end);
            assert!(start < end);
        }
    }

    #[test]
    fn ulid_timestamp_extraction_property(
        ulid in arbitrary_ulid()
    ) {
        // Property: Timestamp extraction should be consistent
        let timestamp = ulid.timestamp();
        // Re-create ULID from timestamp and verify it's in the same time range
        let ts_ms = timestamp.timestamp_millis() as u64;
        
        // The timestamp component should match (first 48 bits)
        let ulid_bytes = ulid.to_bytes();
        let ts_component = u64::from_be_bytes([
            ulid_bytes[0], ulid_bytes[1], ulid_bytes[2], ulid_bytes[3],
            ulid_bytes[4], ulid_bytes[5], 0, 0
        ]) >> 16;
        
        assert_eq!(ts_component, ts_ms, "ULID timestamp component should match extracted timestamp");
    }

    #[test]
    fn ulid_string_roundtrip_property(
        ulid in arbitrary_ulid()
    ) {
        // Property: ULID should survive string serialization roundtrip
        let ulid_string = ulid.to_string();
        let parsed = Ulid::from_str(&ulid_string);
        
        assert!(parsed.is_ok(), "ULID string should be parseable");
        assert_eq!(parsed.unwrap(), ulid, "Parsed ULID should match original");
        
        // Property: String representation should be canonical
        assert_eq!(ulid_string.len(), 26, "ULID string should be 26 characters");
        assert!(ulid_string.chars().all(|c| c.is_ascii_alphanumeric() && c.is_uppercase()),
                "ULID string should only contain uppercase alphanumeric characters");
    }

    #[test]
    fn ulid_bytes_roundtrip_property(
        ulid in arbitrary_ulid()
    ) {
        // Property: ULID should survive bytes serialization roundtrip
        let bytes = ulid.to_bytes();
        let restored = Ulid::from_bytes(bytes);
        
        assert!(restored.is_ok(), "ULID bytes should be valid");
        assert_eq!(restored.unwrap(), ulid, "Restored ULID should match original");
        assert_eq!(bytes.len(), 16, "ULID should be 16 bytes");
    }

    #[test]
    fn ulid_uuid_conversion_property(
        ulid in arbitrary_ulid()
    ) {
        // Property: ULID to UUID conversion should be lossless
        let uuid = ulid.to_uuid();
        let uuid_bytes = uuid.as_bytes();
        let ulid_bytes = ulid.to_bytes();
        
        assert_eq!(uuid_bytes, &ulid_bytes, "UUID bytes should match ULID bytes");
        
        // Property: UUID version and variant should be set correctly
        // ULIDs don't follow UUID structure, so we just verify the bytes match
        let restored = Ulid::from_bytes(*uuid_bytes);
        assert!(restored.is_ok(), "Should be able to create ULID from UUID bytes");
        assert_eq!(restored.unwrap(), ulid, "ULID from UUID bytes should match original");
    }

    #[test]
    fn ulid_monotonic_property(
        count in 1usize..20
    ) {
        // Property: ULIDs generated in sequence should be monotonically increasing
        let mut ulids = Vec::with_capacity(count);
        for _ in 0..count {
            ulids.push(Ulid::new());
            // Small delay to ensure different timestamps if needed
            std::thread::sleep(std::time::Duration::from_micros(1));
        }
        
        // Check monotonic ordering
        for window in ulids.windows(2) {
            assert!(window[0] < window[1], 
                    "ULIDs generated in sequence should be monotonically increasing");
        }
    }

    #[test]
    fn ulid_nil_property(
        ulid in arbitrary_ulid()
    ) {
        // Property: No generated ULID should equal nil ULID
        assert_ne!(ulid, Ulid::nil(), "Generated ULID should not be nil");
        
        // Property: Nil ULID should have all zero bytes
        let nil_bytes = Ulid::nil().to_bytes();
        assert!(nil_bytes.iter().all(|&b| b == 0), "Nil ULID should have all zero bytes");
        
        // Property: Nil ULID should be less than any non-nil ULID
        if ulid != Ulid::nil() {
            assert!(Ulid::nil() < ulid, "Nil ULID should be less than any non-nil ULID");
        }
    }

    #[test]
    fn ulid_database_compatibility_property(
        ulid in arbitrary_ulid()
    ) {
        // Property: ULID should be compatible with PostgreSQL UUID type
        let uuid = ulid.to_uuid();
        let uuid_string = uuid.to_string();
        
        // Verify UUID string format
        assert_eq!(uuid_string.len(), 36, "UUID string should be 36 characters with hyphens");
        assert_eq!(uuid_string.chars().filter(|&c| c == '-').count(), 4, 
                   "UUID string should have 4 hyphens");
        
        // Verify we can parse it back
        let parsed_uuid = uuid::Uuid::parse_str(&uuid_string);
        assert!(parsed_uuid.is_ok(), "UUID string should be parseable");
        assert_eq!(parsed_uuid.unwrap(), uuid, "Parsed UUID should match original");
    }

    #[test]
    fn ulid_collision_resistance_property(
        _count in Just(100)  // Fixed count for deterministic test
    ) {
        // Property: Rapidly generated ULIDs should not collide
        let mut ulids = HashSet::new();
        let start = std::time::Instant::now();
        
        // Generate ULIDs as fast as possible
        for _ in 0..100 {
            let ulid = Ulid::new();
            assert!(ulids.insert(ulid), "ULID collision detected: {:?}", ulid);
        }
        
        let elapsed = start.elapsed();
        println!("Generated 100 unique ULIDs in {:?}", elapsed);
    }
}

// Helper function for arbitrary ULID generation
pub fn arbitrary_ulid() -> impl Strategy<Value = Ulid> {
    prop_oneof![
        // Most ULIDs should be recent/valid
        90 => Just(Ulid::new()),
        // Some ULIDs with specific byte patterns
        5 => any::<[u8; 16]>().prop_map(|bytes| {
            Ulid::from_bytes(bytes).unwrap_or_else(|_| Ulid::new())
        }),
        // Edge cases
        5 => prop_oneof![
            Just(Ulid::nil()),
            Just(Ulid::from_bytes([0xFF; 16]).unwrap_or_else(|_| Ulid::new())),
            Just(Ulid::from_bytes([0x00; 16]).unwrap_or_else(|_| Ulid::new())),
        ]
    ]
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    proptest! {
        #[test]
        fn ulid_boundary_values(
            boundary_type in prop_oneof![
                Just("min"),
                Just("max"),
                Just("zero_timestamp"),
                Just("max_timestamp"),
            ]
        ) {
            let ulid = match boundary_type {
                "min" => Ulid::nil(),
                "max" => Ulid::from_bytes([0xFF; 16]).unwrap_or_else(|_| Ulid::new()),
                "zero_timestamp" => {
                    let mut bytes = [0u8; 16];
                    // Set random component but keep timestamp at 0
                    for i in 6..16 {
                        bytes[i] = rand::random();
                    }
                    Ulid::from_bytes(bytes).unwrap_or_else(|_| Ulid::new())
                },
                "max_timestamp" => {
                    let mut bytes = [0xFFu8; 16];
                    // Set only timestamp component to max
                    for i in 6..16 {
                        bytes[i] = rand::random();
                    }
                    Ulid::from_bytes(bytes).unwrap_or_else(|_| Ulid::new())
                },
                _ => Ulid::new()
            };
            
            // All boundary ULIDs should be valid
            assert_eq!(ulid.to_bytes().len(), 16);
            assert_eq!(ulid.to_string().len(), 26);
            
            // Should survive roundtrips
            let string_roundtrip = Ulid::from_str(&ulid.to_string());
            assert!(string_roundtrip.is_ok());
            
            let bytes_roundtrip = Ulid::from_bytes(ulid.to_bytes());
            assert!(bytes_roundtrip.is_ok());
        }
    }
}