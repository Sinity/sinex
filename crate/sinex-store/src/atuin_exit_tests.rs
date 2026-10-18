use super::*;

fn row(exit: i64, duration: i64) -> Value {
    json!({"id":"signed","timestamp":1_700_000_000_123_456_789i64,"duration":duration,"exit":exit,"command":"synthetic signed status","cwd":"/synthetic","session":"s","hostname":"host:user","deleted_at":null})
}

#[test]
fn signed_status_and_duration_are_independent_evidence() {
    for exit in [i64::MIN, -2, -1, 0, 1, i64::MAX] {
        for duration in [-1, 0, 7, i64::MAX] {
            let parsed = parse(&row(exit, duration)).unwrap();
            assert_eq!(
                parsed.payload["exit_code"],
                if exit == -1 { Value::Null } else { json!(exit) }
            );
            assert_eq!(
                parsed.payload["duration_ns"],
                if duration == -1 {
                    Value::Null
                } else {
                    json!(duration)
                }
            );
            assert_eq!(
                parsed.payload["completion"],
                if duration >= 0 || exit != -1 {
                    "ended"
                } else {
                    "end_not_recorded"
                }
            );
            let encoded = serde_json::to_string(&parsed.payload).unwrap();
            let decoded: Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, parsed.payload);
        }
    }
}

#[test]
fn malformed_numeric_evidence_remains_a_record_failure() {
    assert!(parse(&row(-2, -2)).is_err());
    for exit in [json!("-2"), json!(null), json!(1.5), json!(u64::MAX)] {
        let mut malformed = row(0, 1);
        malformed["exit"] = exit;
        assert!(parse(&malformed).is_err());
    }
}
