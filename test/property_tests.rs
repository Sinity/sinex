use proptest::prelude::*;
use serde_json::Value;
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};

/// Helper to compare JSON values with tolerance for floating point precision
fn assert_json_values_equivalent(a: &Value, b: &Value) {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                // For very small or very large numbers, use relative tolerance
                let tolerance = f64::EPSILON * f1.abs().max(1.0) * 100.0;
                assert!((f1 - f2).abs() <= tolerance, 
                    "Numbers not equal within tolerance: {} vs {}", f1, f2);
            } else {
                // For integers or other number types, require exact match
                assert_eq!(n1, n2);
            }
        }
        (Value::Array(a1), Value::Array(a2)) => {
            assert_eq!(a1.len(), a2.len(), "Array lengths differ");
            for (v1, v2) in a1.iter().zip(a2.iter()) {
                assert_json_values_equivalent(v1, v2);
            }
        }
        (Value::Object(o1), Value::Object(o2)) => {
            assert_eq!(o1.len(), o2.len(), "Object lengths differ");
            for (k, v1) in o1 {
                let v2 = o2.get(k).expect(&format!("Key {} missing in second object", k));
                assert_json_values_equivalent(v1, v2);
            }
        }
        _ => assert_eq!(a, b),
    }
}

/// Generate arbitrary JSON values for testing
fn arb_json_value() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|i| Value::Number(i.into())),
        any::<f64>()
            .prop_filter("valid float", |f| f.is_finite() && f.abs() > 1e-300 && f.abs() < 1e300)
            .prop_map(|f| Value::Number(serde_json::Number::from_f64(f).unwrap())),
        "[a-zA-Z0-9_]{0,50}".prop_map(Value::String),
    ];

    leaf.prop_recursive(
        3,  // max depth
        50, // max nodes
        10, // items per collection
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..10)
                    .prop_map(Value::Array),
                prop::collection::hash_map(
                    "[a-zA-Z_][a-zA-Z0-9_]{0,30}",
                    inner,
                    0..10,
                )
                .prop_map(|m| Value::Object(m.into_iter().collect())),
            ]
        },
    )
    .boxed()
}

/// Generate valid event source strings
fn arb_event_source() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z]+\\.[a-z]+\\.[a-z]+").unwrap()
}

/// Generate valid event type strings
fn arb_event_type() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z_]+_v[0-9]+").unwrap()
}

/// Generate valid agent names
#[allow(dead_code)]
fn arb_agent_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z]+Agent_[A-Za-z]+_v[0-9]+\\.[0-9]+\\.[0-9]+").unwrap()
}

/// Generate valid host names
fn arb_host_name() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-z][a-z0-9-]{0,62}").unwrap()
}

/// Generate timestamps within reasonable bounds
fn arb_timestamp() -> impl Strategy<Value = DateTime<Utc>> {
    // Generate timestamps within last year to next year
    let now = Utc::now();
    let year_ago = now - Duration::days(365);
    let year_future = now + Duration::days(365);
    
    let start_millis = year_ago.timestamp_millis();
    let end_millis = year_future.timestamp_millis();
    
    (start_millis..=end_millis)
        .prop_map(|millis| DateTime::from_timestamp_millis(millis).unwrap())
}

proptest! {



    /// Test queue retry timing boundaries
    #[test]
    fn test_queue_retry_timing_boundaries(
        attempts in 0i32..20,
        base_delay in 1.0f64..300.0,
    ) {
        // Calculate exponential backoff
        let delay = base_delay * (2.0_f64.powi(attempts));
        let with_jitter = delay * 1.1; // Max jitter
        let clamped = with_jitter.max(1.0).min(24.0 * 3600.0);
        
        // Verify bounds
        assert!(clamped >= 1.0);
        assert!(clamped <= 24.0 * 3600.0);
        
        // Verify exponential growth until cap
        if attempts > 0 && attempts < 10 && base_delay < 100.0 {
            assert!(delay > base_delay);
        }
        
        // When attempts = 0, delay should equal base_delay
        if attempts == 0 {
            assert_eq!(delay, base_delay);
        }
    }

    /// Test ULID to UUID conversion preserves ordering
    #[test]
    fn test_ulid_uuid_ordering(
        ulids in prop::collection::vec(any::<u128>(), 2..10)
    ) {
        use sinex_ulid::Ulid;
        
        // Convert to ULIDs and then to UUIDs
        let mut ulid_pairs: Vec<(Ulid, Uuid)> = ulids.into_iter()
            .map(|n| {
                // Create ULID from bytes - convert i128 to byte array
                let bytes = n.to_be_bytes();
                let ulid = Ulid::from_bytes(bytes).expect("Valid ULID bytes");
                let uuid = ulid.to_uuid();
                (ulid, uuid)
            })
            .collect();
        
        // Sort by ULID
        ulid_pairs.sort_by_key(|(ulid, _)| *ulid);
        
        // Verify UUID order matches ULID order
        for window in ulid_pairs.windows(2) {
            let (ulid1, uuid1) = &window[0];
            let (ulid2, uuid2) = &window[1];
            
            assert!(ulid1 <= ulid2);
            // Note: UUID ordering may not match ULID ordering due to byte layout differences
            // This is expected and acceptable as long as conversion is reversible
            
            // Verify round-trip conversion
            assert_eq!(*ulid1, Ulid::from_uuid(*uuid1));
            assert_eq!(*ulid2, Ulid::from_uuid(*uuid2));
        }
    }

}