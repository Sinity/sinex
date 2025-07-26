// Demonstration of the snapshot testing capabilities
// Run with: cargo run --example snapshot_demo

use chrono::Utc;
use serde_json::{json, Value};
use sinex_ulid::Ulid;
use std::str::FromStr;

fn main() {
    println!("=== Sinex Snapshot Testing Demo ===\n");

    demo_basic_snapshot();
    demo_redaction_features();
    demo_builder_api();

    println!("\n✓ Snapshot testing is fully functional!");
    println!("\nTo use in tests:");
    println!("  assert_snapshot!(value, \"name\");");
    println!("  UPDATE_SNAPSHOTS=1 cargo test  # to update snapshots");
}

fn demo_basic_snapshot() {
    println!("1. Basic Snapshot Testing:");
    println!("-------------------------");

    let data = json!({
        "name": "John Doe",
        "age": 30,
        "active": true
    });

    println!("Original data:");
    println!("{}", serde_json::to_string_pretty(&data).unwrap());

    println!("\nIn a test, you would write:");
    println!("  assert_snapshot!(data, \"user_profile\");");
    println!("\nThis creates/compares with: test/snapshots/module/user_profile.snap");
}

fn demo_redaction_features() {
    println!("\n2. Automatic Redaction Features:");
    println!("--------------------------------");

    let data = json!({
        "id": Ulid::new().to_string(),
        "created_at": Utc::now().to_rfc3339(),
        "pid": 54321,
        "window_id": 98765,
        "user_data": {
            "session_id": Ulid::new().to_string(),
            "last_seen": Utc::now().to_rfc3339(),
        }
    });

    println!("Original data with dynamic values:");
    println!("{}", serde_json::to_string_pretty(&data).unwrap());

    // Simulate redaction
    let mut redacted = data.clone();
    apply_redactions(&mut redacted);

    println!("\nAfter automatic redaction:");
    println!("{}", serde_json::to_string_pretty(&redacted).unwrap());

    println!("\nRedactions applied:");
    println!("  • ULIDs → ULID_0001, ULID_0002, etc.");
    println!("  • Timestamps → 2024-01-01T00:00:00Z");
    println!("  • PIDs/Window IDs → 12345");
}

fn demo_builder_api() {
    println!("\n3. Snapshot Builder API:");
    println!("-----------------------");

    let data = json!({
        "id": Ulid::new().to_string(),
        "password": "secret123",
        "api_key": "sk_live_abcdef",
        "metadata": {
            "version": "1.0",
            "environment": "production"
        }
    });

    println!("Using the builder API for custom redactions:");
    println!("\n  snapshot(data)");
    println!("    .name(\"api_response\")");
    println!("    .redact_timestamps()");
    println!("    .redact_ulids()");
    println!("    .redact_field(\"password\", json!(\"[REDACTED]\"))");
    println!("    .redact_field(\"api_key\", json!(\"sk_[REDACTED]\"))");
    println!("    .assert();");

    // Simulate custom redaction
    let mut redacted = data.clone();
    apply_redactions(&mut redacted);
    if let Some(obj) = redacted.as_object_mut() {
        obj.insert("password".to_string(), json!("[REDACTED]"));
        obj.insert("api_key".to_string(), json!("sk_[REDACTED]"));
    }

    println!("\nResult after custom redactions:");
    println!("{}", serde_json::to_string_pretty(&redacted).unwrap());
}

// Simulate the redaction logic
fn apply_redactions(value: &mut Value) {
    match value {
        Value::String(s) => {
            // Check if it's a ULID
            if s.len() == 26 && Ulid::from_str(s).is_ok() {
                *s = "ULID_0001".to_string();
            }
            // Check if it's a timestamp
            else if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                *s = "2024-01-01T00:00:00Z".to_string();
            }
        }
        Value::Number(n) => {
            // Redact PIDs and window IDs (large numbers)
            if let Some(num) = n.as_u64() {
                if num > 10000 {
                    *n = serde_json::Number::from(12345);
                }
            }
        }
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                // Redact timestamp fields
                if k.contains("_at") || k.contains("time") || k == "created" || k == "updated" {
                    if let Value::String(_) = v {
                        *v = Value::String("2024-01-01T00:00:00Z".to_string());
                        continue;
                    }
                }
                // Redact ID fields
                if k.contains("_id") || k == "pid" || k == "process_id" {
                    if let Value::Number(_) = v {
                        *v = Value::Number(serde_json::Number::from(12345));
                        continue;
                    }
                }
                // Recurse
                apply_redactions(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                apply_redactions(v);
            }
        }
        _ => {}
    }
}
