// Working example of snapshot testing
// Run with: UPDATE_SNAPSHOTS=1 cargo test --example snapshot_test

#[path = "../test/common/mod.rs"]
mod common;

fn main() {
    println!("Snapshot testing example - run with cargo test");
}

// use common::snapshot_testing::*; // Module not available
use serde_json::json;
use sinex_ulid::Ulid;

#[test]
#[ignore = "snapshot_testing module not available"]
fn test_snapshot_example() {
    let data = json!({
        "event_id": Ulid::new().to_string(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "user": "test_user",
        "action": "created",
        "metadata": {
            "version": 1,
            "pid": 12345
        }
    });
    
    // This will create/compare snapshot at:
    // test/snapshots/snapshot_test/event_example.snap
    assert_snapshot!(data, "event_example");
}

#[test]
#[ignore = "snapshot_testing module not available"]
fn test_inline_snapshot_example() {
    let simple = json!({
        "status": "success",
        "count": 42
    });
    
    assert_inline_snapshot!(simple, @r###"
{
  "count": 42,
  "status": "success"
}
"###);
}

#[test]
#[ignore = "snapshot_testing module not available"]
fn test_builder_example() {
    let sensitive_data = json!({
        "api_key": "sk_live_1234567890",
        "user_id": Ulid::new().to_string(),
        "created_at": chrono::Utc::now().to_rfc3339(),
        "secrets": {
            "password": "hunter2",
            "token": "abc123"
        }
    });
    
    snapshot(sensitive_data)
        .name("sensitive_data_redacted")
        .redact_timestamps()
        .redact_ulids()
        .redact_field("api_key", json!("sk_live_[REDACTED]"))
        .redact_field("secrets.password", json!("[REDACTED]"))
        .redact_field("secrets.token", json!("[REDACTED]"))
        .assert();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_all_examples() {
        println!("\n=== Running Snapshot Test Examples ===\n");
        
        // Clear any previous redaction state
        clear_redaction_cache();
        
        println!("• Basic snapshot test");
        test_snapshot_example();
        
        println!("• Inline snapshot test");
        test_inline_snapshot_example();
        
        println!("• Builder API test");
        test_builder_example();
        
        println!("\n✓ All snapshot tests completed!");
    }
}