//! Tests for ULID conversion utilities
//!
//! These tests validate the database boundary conversion functions
//! that handle ULID <-> UUID transformations for PostgreSQL operations.

use rstest::*;
use sinex_schema::ulid::Ulid;
use sinex_schema::ulid_conversions::*;
use sqlx::types::Uuid as SqlxUuid;
use std::collections::HashMap;

#[cfg(test)]
mod conversion_function_tests {
    use super::*;

    #[test]
    fn test_basic_conversions() {
        let ulid = Ulid::new();

        // Test basic conversion functions
        let db_uuid = ulid_to_uuid(ulid);
        let restored_ulid = uuid_to_ulid(db_uuid);

        assert_eq!(ulid, restored_ulid);
    }

    #[test]
    fn test_convenience_aliases() {
        let ulid = Ulid::new();

        // Test that aliases work the same as main functions
        let db_uuid1 = to_db(ulid);
        let db_uuid2 = ulid_to_uuid(ulid);
        assert_eq!(db_uuid1, db_uuid2);

        let restored1 = from_db(db_uuid1);
        let restored2 = uuid_to_ulid(db_uuid1);
        assert_eq!(restored1, restored2);
        assert_eq!(restored1, ulid);
    }

    #[test]
    fn test_optional_conversions() {
        let ulid = Ulid::new();

        // Test Some case
        let some_db_uuid = opt_to_db(Some(ulid));
        assert!(some_db_uuid.is_some());
        assert_eq!(some_db_uuid.unwrap(), ulid_to_uuid(ulid));

        let restored = opt_from_db(some_db_uuid);
        assert_eq!(restored, Some(ulid));

        // Test None case
        assert_eq!(opt_to_db(None), None);
        assert_eq!(opt_from_db(None), None);
    }

    #[test]
    fn test_vector_conversions() {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];

        // Test vector conversion
        let db_uuids = ulids.to_uuid_vec();
        assert_eq!(db_uuids.len(), ulids.len());

        // Verify each conversion
        for (ulid, db_uuid) in ulids.iter().zip(db_uuids.iter()) {
            assert_eq!(*db_uuid, ulid_to_uuid(*ulid));
        }

        // Test round-trip
        let restored_ulids = db_uuids.to_ulid_vec();
        assert_eq!(ulids, restored_ulids);
    }

    #[test]
    fn test_optional_vector_conversions() {
        let ulids = vec![Ulid::new(), Ulid::new()];

        // Test Some case
        let some_uuids = opt_vec_to_db(Some(ulids.clone()));
        assert!(some_uuids.is_some());

        let restored = opt_vec_from_db(some_uuids);
        assert_eq!(restored, Some(ulids));

        // Test None case
        assert_eq!(opt_vec_to_db(None), None);
        assert_eq!(opt_vec_from_db(None), None);

        // Test empty vector
        let empty_ulids: Vec<Ulid> = vec![];
        let empty_uuids = opt_vec_to_db(Some(empty_ulids.clone()));
        assert_eq!(empty_uuids, Some(vec![]));

        let restored_empty = opt_vec_from_db(empty_uuids);
        assert_eq!(restored_empty, Some(empty_ulids));
    }
}

#[cfg(test)]
mod extension_trait_tests {
    use super::*;

    #[test]
    fn test_ulid_ext_trait() {
        let ulid = Ulid::new();

        // Test to_db method
        let db_uuid = ulid.to_db();
        assert_eq!(db_uuid, ulid_to_uuid(ulid));

        // Test to_db_opt static method
        let some_result = Ulid::to_db_opt(Some(ulid));
        assert_eq!(some_result, Some(db_uuid));

        let none_result = Ulid::to_db_opt(None);
        assert_eq!(none_result, None);
    }

    #[test]
    fn test_db_uuid_ext_trait() {
        let ulid = Ulid::new();
        let db_uuid = ulid.to_db();

        // Test to_ulid method
        let restored = db_uuid.to_ulid();
        assert_eq!(restored, ulid);
    }

    #[test]
    fn test_ulid_array_ext_trait() {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];

        // Test to_uuid_vec method
        let uuids = ulids.to_uuid_vec();
        assert_eq!(uuids.len(), ulids.len());

        // Test to_db_vec alias
        let uuids2 = ulids.to_db_vec();
        assert_eq!(uuids, uuids2);

        // Test with slice
        let slice_uuids = ulids.as_slice().to_uuid_vec();
        assert_eq!(uuids, slice_uuids);

        // Test with array
        let array_ulids: [Ulid; 3] = [ulids[0], ulids[1], ulids[2]];
        let array_uuids = array_ulids.to_uuid_vec();
        assert_eq!(uuids, array_uuids);
    }

    #[test]
    fn test_db_uuid_collection_ext_trait() {
        let ulids = vec![Ulid::new(), Ulid::new()];
        let uuids = ulids.to_uuid_vec();

        // Test Vec<SqlxUuid> implementation
        let restored = uuids.clone().to_ulid_vec();
        assert_eq!(restored, ulids);

        // Test Option<Vec<SqlxUuid>> implementation
        let some_uuids = Some(uuids.clone());
        let restored_some = some_uuids.to_ulid_vec();
        assert_eq!(restored_some, ulids);

        let none_uuids: Option<Vec<SqlxUuid>> = None;
        let restored_none = none_uuids.to_ulid_vec();
        assert_eq!(restored_none, Vec::<Ulid>::new());
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_collections() {
        let empty_ulids: Vec<Ulid> = vec![];
        let empty_uuids = empty_ulids.to_uuid_vec();
        assert!(empty_uuids.is_empty());

        let restored = empty_uuids.to_ulid_vec();
        assert!(restored.is_empty());
    }

    #[test]
    fn test_large_collections() {
        // Test with a large collection to ensure performance is reasonable
        let large_ulids: Vec<Ulid> = (0..10000).map(|_| Ulid::new()).collect();

        let start = std::time::Instant::now();
        let large_uuids = large_ulids.to_uuid_vec();
        let convert_duration = start.elapsed();

        let start = std::time::Instant::now();
        let restored = large_uuids.to_ulid_vec();
        let restore_duration = start.elapsed();

        assert_eq!(large_ulids, restored);

        // Conversions should be reasonably fast (under 100ms for 10k items)
        assert!(
            convert_duration.as_millis() < 100,
            "Conversion took {:?}",
            convert_duration
        );
        assert!(
            restore_duration.as_millis() < 100,
            "Restoration took {:?}",
            restore_duration
        );
    }

    #[test]
    fn test_nil_ulid_conversion() {
        let nil_ulid = Ulid::nil();
        let nil_uuid = nil_ulid.to_db();
        let restored = nil_uuid.to_ulid();

        assert_eq!(nil_ulid, restored);
        assert!(restored.is_nil());
    }

    #[test]
    fn test_ordering_preservation() {
        let mut ulids = vec![Ulid::new(); 100];
        ulids.sort();

        let mut uuids = ulids.to_uuid_vec();
        uuids.sort();

        let restored_ulids = uuids.to_ulid_vec();

        // Original order should be preserved
        assert_eq!(ulids, restored_ulids);
    }
}

#[cfg(test)]
mod type_compatibility_tests {
    use super::*;

    #[test]
    fn test_sqlx_uuid_compatibility() {
        let ulid = Ulid::new();

        // Test that our SqlxUuid is compatible with standard UUID operations
        let sqlx_uuid = ulid.to_db();

        // Should be able to convert to standard UUID
        let std_uuid = uuid::Uuid::from_bytes(*sqlx_uuid.as_bytes());
        assert_eq!(std_uuid, ulid.to_uuid());

        // Should be able to create SqlxUuid from standard UUID
        let sqlx_from_std = SqlxUuid::from_bytes(*std_uuid.as_bytes());
        assert_eq!(sqlx_uuid, sqlx_from_std);
    }

    #[test]
    fn test_hash_map_usage() {
        // Test that converted UUIDs can be used as HashMap keys
        let mut map: HashMap<SqlxUuid, String> = HashMap::new();

        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];

        for (i, ulid) in ulids.iter().enumerate() {
            map.insert(ulid.to_db(), format!("value_{}", i));
        }

        assert_eq!(map.len(), 3);

        // Verify we can look up by converted ULID
        for (i, ulid) in ulids.iter().enumerate() {
            let key = ulid.to_db();
            assert_eq!(map.get(&key), Some(&format!("value_{}", i)));
        }
    }

    #[test]
    fn test_serialization_compatibility() {
        // Test that SqlxUuid from ULID serializes the same as direct UUID
        let ulid = Ulid::new();
        let direct_uuid = ulid.to_uuid();
        let sqlx_uuid = ulid.to_db();

        // Both should have the same byte representation
        assert_eq!(direct_uuid.as_bytes(), sqlx_uuid.as_bytes());

        // Both should have the same string representation
        assert_eq!(direct_uuid.to_string(), sqlx_uuid.to_string());
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;

    #[test]
    fn test_conversion_performance() {
        let ulids: Vec<Ulid> = (0..1000).map(|_| Ulid::new()).collect();

        // Measure conversion to DB format
        let start = std::time::Instant::now();
        let _uuids = ulids.to_uuid_vec();
        let to_db_duration = start.elapsed();

        // Measure individual conversions
        let start = std::time::Instant::now();
        for ulid in &ulids {
            let _uuid = ulid.to_db();
        }
        let individual_duration = start.elapsed();

        println!("Batch conversion: {:?}", to_db_duration);
        println!("Individual conversions: {:?}", individual_duration);

        // Batch should not be significantly slower than individual
        // (allows for some overhead but shouldn't be orders of magnitude different)
        assert!(to_db_duration <= individual_duration * 5);
    }

    #[test]
    fn test_optional_overhead() {
        let ulid = Ulid::new();

        // Measure direct conversion
        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let _uuid = ulid_to_uuid(ulid);
        }
        let direct_duration = start.elapsed();

        // Measure optional conversion
        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let _uuid = opt_to_db(Some(ulid));
        }
        let optional_duration = start.elapsed();

        println!("Direct conversion: {:?}", direct_duration);
        println!("Optional conversion: {:?}", optional_duration);

        // Optional should not add significant overhead
        assert!(optional_duration <= direct_duration * 2);
    }
}
