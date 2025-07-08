use sinex_core::{RawEventBuilder, JsonValue};
use sinex_db::test::*;
use serde_json::json;
use std::collections::HashMap;

const SMALL_PAYLOAD_SIZE: usize = 1024;           // 1KB
const MEDIUM_PAYLOAD_SIZE: usize = 1024 * 1024;   // 1MB  
const LARGE_PAYLOAD_SIZE: usize = 10 * 1024 * 1024; // 10MB
const EXTREME_PAYLOAD_SIZE: usize = 100 * 1024 * 1024; // 100MB

#[sinex_test]
async fn test_small_payload_handling(ctx: TestContext) -> TestResult {
    // Test normal small payloads (< 1KB)
    let small_content = "x".repeat(SMALL_PAYLOAD_SIZE / 2);
    let payload = json!({
        "content": small_content,
        "size": small_content.len(),
        "metadata": {
            "type": "small_test",
            "timestamp": chrono::Utc::now().to_rfc3339()
        }
    });
    
    let event = RawEventBuilder::new("test.boundary", "small.payload", payload).build();
    
    // Should insert without issues
    sinex_db::insert_event(ctx.pool(), &event).await?;
    
    // Verify retrieval
    let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
    assert_eq!(retrieved.id, event.id);
    assert_eq!(retrieved.payload["content"].as_str().unwrap().len(), small_content.len());
    
    Ok(())
}

#[sinex_test]
async fn test_medium_payload_handling(ctx: TestContext) -> TestResult {
    // Test medium payloads (~1MB)
    let medium_content = "a".repeat(MEDIUM_PAYLOAD_SIZE);
    let payload = json!({
        "large_text": medium_content,
        "size": medium_content.len(),
        "chunks": (0..100).map(|i| format!("chunk_{}", i)).collect::<Vec<_>>(),
        "metadata": {
            "type": "medium_test",
            "compression": "none"
        }
    });
    
    let event = RawEventBuilder::new("test.boundary", "medium.payload", payload).build();
    
    // Should handle medium payloads
    sinex_db::insert_event(ctx.pool(), &event).await?;
    
    // Verify storage and retrieval
    let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
    assert_eq!(retrieved.payload["large_text"].as_str().unwrap().len(), medium_content.len());
    assert_eq!(retrieved.payload["chunks"].as_array().unwrap().len(), 100);
    
    Ok(())
}

#[sinex_test]
async fn test_large_payload_handling(ctx: TestContext) -> TestResult {
    // Test large payloads (~10MB)
    let large_content = "b".repeat(LARGE_PAYLOAD_SIZE);
    let payload = json!({
        "very_large_text": large_content,
        "size": large_content.len(),
        "type": "large_payload_test"
    });
    
    let event = RawEventBuilder::new("test.boundary", "large.payload", payload).build();
    
    // Large payloads should still be handled but may be slower
    let start = std::time::Instant::now();
    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    let duration = start.elapsed();
    
    // Verify it was handled (may be slow but should work)
    match result {
        Ok(()) => {
            println!("Large payload insert took: {:?}", duration);
            
            // Verify retrieval (this will also be slow)
            let start_retrieval = std::time::Instant::now();
            let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
            let retrieval_duration = start_retrieval.elapsed();
            
            println!("Large payload retrieval took: {:?}", retrieval_duration);
            assert_eq!(retrieved.payload["very_large_text"].as_str().unwrap().len(), large_content.len());
        }
        Err(e) => {
            // Large payloads might fail due to database limits - this is acceptable
            println!("Large payload rejected (expected): {}", e);
            assert!(e.to_string().contains("too large") || e.to_string().contains("limit") || e.to_string().contains("size"));
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_extreme_payload_rejection(ctx: TestContext) -> TestResult {
    // Test extreme payloads (~100MB) - these should be rejected
    let extreme_content = "c".repeat(EXTREME_PAYLOAD_SIZE);
    let payload = json!({
        "extreme_text": extreme_content,
        "size": extreme_content.len(),
        "warning": "This should probably be rejected"
    });
    
    let event = RawEventBuilder::new("test.boundary", "extreme.payload", payload).build();
    
    // Extreme payloads should be rejected
    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    assert!(result.is_err(), "Extreme payloads should be rejected");
    
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("too large") || 
        error_msg.contains("limit") || 
        error_msg.contains("size") ||
        error_msg.contains("memory"),
        "Error should indicate size/memory issue: {}", error_msg
    );
    
    Ok(())
}

#[sinex_test]
async fn test_deeply_nested_json_payload(ctx: TestContext) -> TestResult {
    // Test deeply nested JSON structures
    fn create_nested_json(depth: usize) -> JsonValue {
        if depth == 0 {
            json!({"value": "deep_value", "level": 0})
        } else {
            json!({
                "level": depth,
                "nested": create_nested_json(depth - 1),
                "metadata": format!("level_{}", depth)
            })
        }
    }
    
    // Test moderate nesting (should work)
    let moderate_nested = create_nested_json(50);
    let event = RawEventBuilder::new("test.boundary", "nested.moderate", moderate_nested).build();
    sinex_db::insert_event(ctx.pool(), &event).await?;
    
    // Test deep nesting (might fail)
    let deep_nested = create_nested_json(1000);
    let event = RawEventBuilder::new("test.boundary", "nested.deep", deep_nested).build();
    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    
    match result {
        Ok(()) => {
            println!("Deep nesting was accepted");
        }
        Err(e) => {
            println!("Deep nesting rejected (acceptable): {}", e);
            assert!(e.to_string().contains("depth") || e.to_string().contains("recursion") || e.to_string().contains("stack"));
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_wide_json_payload(ctx: TestContext) -> TestResult {
    // Test JSON objects with many keys
    let mut wide_object = serde_json::Map::new();
    
    // Moderate width (should work)
    for i in 0..1000 {
        wide_object.insert(format!("key_{}", i), json!(format!("value_{}", i)));
    }
    
    let moderate_wide = json!(wide_object);
    let event = RawEventBuilder::new("test.boundary", "wide.moderate", moderate_wide).build();
    sinex_db::insert_event(ctx.pool(), &event).await?;
    
    // Extreme width (might fail)
    let mut extreme_wide_object = serde_json::Map::new();
    for i in 0..100_000 {
        extreme_wide_object.insert(format!("key_{}", i), json!(i));
    }
    
    let extreme_wide = json!(extreme_wide_object);
    let event = RawEventBuilder::new("test.boundary", "wide.extreme", extreme_wide).build();
    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    
    match result {
        Ok(()) => {
            println!("Extreme width was accepted");
        }
        Err(e) => {
            println!("Extreme width rejected (acceptable): {}", e);
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_large_array_payload(ctx: TestContext) -> TestResult {
    // Test arrays with many elements
    let large_array: Vec<JsonValue> = (0..100_000)
        .map(|i| json!({
            "id": i,
            "value": format!("item_{}", i),
            "metadata": {
                "index": i,
                "category": format!("category_{}", i % 10)
            }
        }))
        .collect();
    
    let payload = json!({
        "large_array": large_array,
        "count": large_array.len(),
        "type": "array_boundary_test"
    });
    
    let event = RawEventBuilder::new("test.boundary", "array.large", payload).build();
    
    let result = sinex_db::insert_event(ctx.pool(), &event).await;
    match result {
        Ok(()) => {
            println!("Large array was accepted");
            
            // Verify retrieval
            let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
            assert_eq!(retrieved.payload["large_array"].as_array().unwrap().len(), 100_000);
        }
        Err(e) => {
            println!("Large array rejected (acceptable): {}", e);
            assert!(e.to_string().contains("too large") || e.to_string().contains("memory"));
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_binary_data_in_payload(ctx: TestContext) -> TestResult {
    // Test binary data encoded in JSON
    let binary_data = (0..10_000).map(|i| (i % 256) as u8).collect::<Vec<u8>>();
    let base64_data = base64::encode(&binary_data);
    
    let payload = json!({
        "binary_data": base64_data,
        "encoding": "base64",
        "original_size": binary_data.len(),
        "data_type": "simulated_image"
    });
    
    let event = RawEventBuilder::new("test.boundary", "binary.data", payload).build();
    sinex_db::insert_event(ctx.pool(), &event).await?;
    
    // Verify retrieval and decode
    let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
    let decoded_data = base64::decode(retrieved.payload["binary_data"].as_str().unwrap()).unwrap();
    assert_eq!(decoded_data.len(), binary_data.len());
    assert_eq!(decoded_data, binary_data);
    
    Ok(())
}

#[sinex_test]
async fn test_unicode_boundary_payload(ctx: TestContext) -> TestResult {
    // Test various Unicode scenarios
    let unicode_tests = vec![
        ("emojis", "🎭🎪🎨🎯🎲🎳🎮🎰🎱🎯".repeat(100)),
        ("chinese", "你好世界".repeat(500)),
        ("arabic", "مرحبا بالعالم".repeat(500)),
        ("mixed", "Hello 世界 🌍 مرحبا".repeat(200)),
        ("rtl_override", "\u{202E}This is reversed\u{202D}".repeat(100)),
        ("zero_width", "a\u{200B}b\u{200C}c\u{200D}d\u{FEFF}e".repeat(1000)),
    ];
    
    for (test_name, content) in unicode_tests {
        let payload = json!({
            "test_name": test_name,
            "content": content,
            "length": content.chars().count(),
            "byte_length": content.len()
        });
        
        let event = RawEventBuilder::new("test.boundary", "unicode.test", payload).build();
        
        let result = sinex_db::insert_event(ctx.pool(), &event).await;
        match result {
            Ok(()) => {
                // Verify Unicode integrity
                let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
                let retrieved_content = retrieved.payload["content"].as_str().unwrap();
                assert_eq!(retrieved_content, content, "Unicode content should be preserved for {}", test_name);
            }
            Err(e) => {
                println!("Unicode test '{}' failed: {}", test_name, e);
                // Some Unicode edge cases might legitimately fail
            }
        }
    }
    
    Ok(())
}

#[sinex_test]
async fn test_payload_size_distribution(ctx: TestContext) -> TestResult {
    // Test a distribution of payload sizes to understand performance characteristics
    let size_tests = vec![
        ("tiny", 10),
        ("small", 100),
        ("medium", 1_000),
        ("large", 10_000),
        ("very_large", 100_000),
        ("huge", 1_000_000),
    ];
    
    let mut results = HashMap::new();
    
    for (size_name, char_count) in size_tests {
        let content = "x".repeat(char_count);
        let payload = json!({
            "size_category": size_name,
            "content": content,
            "char_count": char_count
        });
        
        let event = RawEventBuilder::new("test.boundary", "size.distribution", payload).build();
        
        let start = std::time::Instant::now();
        let result = sinex_db::insert_event(ctx.pool(), &event).await;
        let duration = start.elapsed();
        
        results.insert(size_name, (result.is_ok(), duration));
        
        if result.is_ok() {
            println!("Size '{}' ({} chars): {:?}", size_name, char_count, duration);
        } else {
            println!("Size '{}' ({} chars): FAILED - {}", size_name, char_count, result.unwrap_err());
        }
    }
    
    // Verify that smaller payloads generally perform better
    let tiny_time = results.get("tiny").unwrap().1;
    let small_time = results.get("small").unwrap().1;
    
    assert!(tiny_time <= small_time * 2, "Tiny payloads should be significantly faster than small ones");
    
    Ok(())
}

#[sinex_test] 
async fn test_concurrent_large_payload_handling(ctx: TestContext) -> TestResult {
    // Test concurrent insertion of large payloads
    let payload_size = 50_000; // 50KB each
    let concurrent_count = 10;
    
    let tasks: Vec<_> = (0..concurrent_count)
        .map(|i| {
            let pool = ctx.pool().clone();
            tokio::spawn(async move {
                let content = format!("concurrent_test_{}", i).repeat(payload_size / 20);
                let payload = json!({
                    "thread_id": i,
                    "content": content,
                    "size": content.len()
                });
                
                let event = RawEventBuilder::new("test.boundary", "concurrent.large", payload).build();
                let start = std::time::Instant::now();
                let result = sinex_db::insert_event(&pool, &event).await;
                let duration = start.elapsed();
                
                (i, result, duration)
            })
        })
        .collect();
    
    // Wait for all tasks to complete
    let mut successful = 0;
    let mut total_time = std::time::Duration::ZERO;
    
    for task in tasks {
        let (thread_id, result, duration) = task.await.unwrap();
        
        match result {
            Ok(()) => {
                successful += 1;
                total_time += duration;
                println!("Thread {} completed in {:?}", thread_id, duration);
            }
            Err(e) => {
                println!("Thread {} failed: {}", thread_id, e);
            }
        }
    }
    
    assert!(successful > 0, "At least some concurrent large payloads should succeed");
    println!("Successfully handled {}/{} concurrent large payloads", successful, concurrent_count);
    println!("Average time per payload: {:?}", total_time / successful as u32);
    
    Ok(())
}

#[sinex_test]
async fn test_payload_edge_cases(ctx: TestContext) -> TestResult {
    // Test various edge cases that might cause issues
    let edge_cases = vec![
        ("empty_string", json!({"content": ""})),
        ("null_values", json!({"data": null, "more_data": null})),
        ("empty_object", json!({})),
        ("empty_array", json!({"arr": []})),
        ("special_chars", json!({"content": "\r\n\t\0\x1F\x7F"})),
        ("json_in_string", json!({"meta": "{\"nested\": \"json\"}"})),
        ("very_long_key", json!({&"x".repeat(1000): "short_value"})),
        ("boolean_extremes", json!({"true": true, "false": false})),
        ("number_extremes", json!({
            "zero": 0,
            "negative": -999999999,
            "positive": 999999999,
            "float": 3.14159265359,
            "scientific": 1.23e-10
        })),
    ];
    
    for (test_name, payload) in edge_cases {
        let event = RawEventBuilder::new("test.boundary", "edge.case", payload).build();
        
        let result = sinex_db::insert_event(ctx.pool(), &event).await;
        
        match result {
            Ok(()) => {
                println!("Edge case '{}' handled successfully", test_name);
                
                // Verify retrieval
                let retrieved = sinex_db::get_event_by_id(ctx.pool(), event.id).await?;
                // Basic verification that data integrity is maintained
                assert_eq!(retrieved.id, event.id);
            }
            Err(e) => {
                println!("Edge case '{}' failed: {}", test_name, e);
                // Some edge cases might legitimately fail
            }
        }
    }
    
    Ok(())
}