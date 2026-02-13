//! JetStream large payload handling tests.
//!
//! Ensures that sizeable messages can be published, stored, and consumed
//! without fragmentation issues.

use xtask::sandbox::prelude::*;

/// Test publishing and persisting a large (~100KB) JSON payload.
///
/// Verifies:
/// - Large payloads can be published successfully
/// - Payload round-trips correctly through the system
/// - Deep nesting, large arrays, and long strings are preserved
#[sinex_test]
async fn test_large_json_payload_persistence(ctx: TestContext) -> TestResult<()> {
    // Create a large payload (~100KB):
    // - Deeply nested objects (10 levels)
    // - Large array with 1000 elements
    // - Long string (50000 chars)

    let long_string = "x".repeat(50000);

    // Build nested objects
    let mut nested = serde_json::json!({ "level": 10, "data": long_string.clone() });
    for level in (1..10).rev() {
        nested = serde_json::json!({
            "level": level,
            "nested": nested,
            "marker": format!("level_{level}")
        });
    }

    // Build large array (1000 elements)
    let mut large_array = Vec::new();
    for i in 0..1000 {
        large_array.push(json!({
            "index": i,
            "value": format!("item_{i}"),
            "nested": {
                "data": format!("nested_data_{i}"),
            }
        }));
    }

    // Combine into payload
    let large_payload = json!({
        "nested_structure": nested,
        "large_array": large_array,
        "metadata": {
            "size_kb": 100,
            "type": "large_payload_test",
            "long_string": long_string
        }
    });

    // Publish the large event
    let published_event = ctx
        .publish(DynamicPayload::new(
            "large-payload-test",
            "payload.large",
            large_payload.clone(),
        ))
        .await?;

    // Verify the event was stored
    let event_id = published_event
        .id
        .expect("Published event should have an ID");
    assert!(event_id.as_ulid().to_string().len() > 0);

    // Verify payload round-trips correctly
    let stored_payload = published_event.payload.clone();
    assert_eq!(
        stored_payload, large_payload,
        "Payload should round-trip correctly"
    );

    // Verify specific payload structure
    let nested_val = stored_payload
        .get("nested_structure")
        .expect("nested_structure should exist");
    assert_eq!(nested_val["level"], 1);
    assert!(nested_val.to_string().contains("level_5"));

    let array_val = stored_payload
        .get("large_array")
        .expect("large_array should exist");
    assert!(array_val.is_array());
    let array_len = array_val.as_array().expect("should be array").len();
    assert_eq!(array_len, 1000, "Array should have 1000 elements");

    // Verify long string is preserved
    let metadata = stored_payload
        .get("metadata")
        .expect("metadata should exist");
    let stored_long_string = metadata["long_string"]
        .as_str()
        .expect("long_string should exist");
    assert_eq!(
        stored_long_string.len(),
        50000,
        "Long string should be 50000 chars"
    );

    Ok(())
}

/// Test batch publishing of multiple large payloads.
///
/// Verifies:
/// - Multiple ~10KB payloads can be published via publish_many()
/// - All payloads persist correctly
/// - Each payload's key fields are preserved
#[sinex_test]
async fn test_batch_large_payloads(ctx: TestContext) -> TestResult<()> {
    const PAYLOAD_COUNT: usize = 20;
    const PAYLOAD_SIZE_KB: usize = 10;

    // Build 20 payloads, each ~10KB
    let mut payloads = Vec::new();

    for batch_num in 0..PAYLOAD_COUNT {
        // Create a ~10KB payload with nested structure
        let mut nested = json!({
            "batch": batch_num,
            "level": 0,
            "data": "x".repeat(2000) // ~2KB of data
        });

        // Add nesting to reach ~10KB total
        for level in 1..3 {
            nested = json!({
                "level": level,
                "batch": batch_num,
                "nested_data": nested,
                "padding": "y".repeat(3000) // Additional padding
            });
        }

        let payload_json = json!({
            "batch_id": batch_num,
            "batch_timestamp": Timestamp::now().to_string(),
            "batch_type": "large_batch_test",
            "batch_size": PAYLOAD_SIZE_KB,
            "nested": nested,
            "sequence": batch_num,
            "metadata": {
                "test_id": "batch_large_payloads",
                "payload_index": batch_num
            }
        });

        let payload = DynamicPayload::new("batch-large-test", "payload.batch_large", payload_json);

        payloads.push(payload);
    }

    // Publish all payloads in batch
    let published_events = ctx.publish_many(payloads).await?;

    // Verify all events were published
    assert_eq!(
        published_events.len(),
        PAYLOAD_COUNT,
        "All {PAYLOAD_COUNT} payloads should be published"
    );

    // Verify each payload's key fields
    for (idx, event) in published_events.iter().enumerate() {
        let event_id = event.id.expect("Published event should have an ID");
        assert!(event_id.as_ulid().to_string().len() > 0);

        let payload = &event.payload;
        assert_eq!(
            payload["batch_id"].as_u64(),
            Some(idx as u64),
            "Batch ID {idx} should match"
        );

        assert_eq!(
            payload["sequence"].as_u64(),
            Some(idx as u64),
            "Sequence {idx} should match"
        );

        assert_eq!(
            payload["batch_type"].as_str(),
            Some("large_batch_test"),
            "Batch type should be preserved"
        );

        assert_eq!(
            payload["batch_size"].as_u64(),
            Some(PAYLOAD_SIZE_KB as u64),
            "Batch size should be {PAYLOAD_SIZE_KB}KB"
        );

        assert_eq!(
            payload["metadata"]["payload_index"].as_u64(),
            Some(idx as u64),
            "Payload index {idx} should be in metadata"
        );

        // Verify nested structure exists and has data
        assert!(
            payload["nested"].is_object(),
            "Nested structure should exist for payload {idx}"
        );
        assert!(payload["nested"]["nested_data"].is_object());
    }

    Ok(())
}
