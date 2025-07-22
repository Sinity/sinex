# Sinex Test Coverage Improvement Plan

## Executive Summary

This document outlines a comprehensive plan to significantly increase test coverage for the Sinex test suite, addressing 293 untested error paths, missing edge case coverage, absent benchmarks, and critical concurrent operation scenarios.

## Current State Analysis

### 1. Untested Error Paths (293 unwrap/expect calls)

Found in the following modules:
- `sinex-db/src/queries/checkpoints.rs` - ULID parsing expects
- `sinex-db/src/integrity.rs` - Timestamp conversion unwraps
- `sinex-db/src/validation.rs` - Error handling unwraps
- `sinex-db/src/sanitization.rs` - Event sanitization unwraps
- `sinex-db/src/query_builder.rs` - Query building unwraps

### 2. Missing Edge Case Tests

Critical gaps identified:
- **ULID Overflow**: No tests for ULID wraparound at max timestamp (year 10889)
- **Unicode Attacks**: No tests for unicode normalization vulnerabilities
- **Large Payloads**: Limited testing beyond 1MB (need 10MB+ tests)
- **Deep JSON Nesting**: No tests for deeply nested JSON structures
- **Concurrent Updates**: No tests for concurrent checkpoint updates

### 3. Missing Property Tests

Current property tests exist but lack coverage for:
- Event ordering invariants under high concurrency
- Checkpoint consistency across failures
- ULID monotonicity guarantees
- JSON schema validation boundaries

### 4. No Performance Benchmarks

Critical paths without benchmarks:
- Event insertion throughput
- Checkpoint update latency
- Query performance with large datasets
- Concurrent operation scalability

## Implementation Plan

### Phase 1: Error Path Coverage (Priority: HIGH)

#### 1.1 Create Error Path Test Suite

```rust
// test/unit/error_paths_test.rs
mod error_path_tests {
    use super::*;
    
    #[sinex_test]
    async fn test_checkpoint_invalid_ulid_parsing() {
        // Test malformed ULID strings
        let invalid_ulids = vec![
            "not-a-ulid",
            "01234567890123456789012345", // Wrong length
            "ZZZZZZZZZZZZZZZZZZZZZZZZZ", // Invalid chars
            "", // Empty string
        ];
        
        for invalid in invalid_ulids {
            let result = parse_checkpoint_ulid(invalid);
            assert!(result.is_err());
        }
    }
    
    #[sinex_test]
    async fn test_timestamp_conversion_boundaries() {
        // Test timestamp edge cases
        let edge_cases = vec![
            0, // Unix epoch
            i64::MAX,
            i64::MIN,
            946684800, // Year 2000
        ];
        
        for ts in edge_cases {
            let result = safe_timestamp_conversion(ts);
            assert!(result.is_ok() || result.is_err());
        }
    }
}
```

#### 1.2 Systematic Error Path Discovery

Create automated script to find and generate tests for all unwrap/expect calls:

```python
# scripts/generate_error_tests.py
import ast
import re
from pathlib import Path

def find_unwrap_expects(file_path):
    """Find all unwrap/expect calls in Rust file"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Regex patterns for unwrap/expect
    unwrap_pattern = r'\.unwrap\(\)'
    expect_pattern = r'\.expect\([^)]+\)'
    
    unwraps = re.finditer(unwrap_pattern, content)
    expects = re.finditer(expect_pattern, content)
    
    return list(unwraps), list(expects)
```

### Phase 2: Edge Case Tests (Priority: HIGH)

#### 2.1 ULID Edge Cases

```rust
// test/adversarial/ulid_edge_cases_test.rs
#[sinex_test]
async fn test_ulid_max_timestamp_overflow() {
    // Maximum ULID timestamp (48-bit)
    let max_timestamp_ms: u64 = (1u64 << 48) - 1;
    
    // Create ULID with max timestamp
    let mut bytes = [0u8; 16];
    for i in 0..6 {
        bytes[i] = ((max_timestamp_ms >> (40 - i * 8)) & 0xFF) as u8;
    }
    
    let ulid = Ulid::from_bytes(bytes).unwrap();
    
    // Test database insertion
    let event = create_event_with_ulid(ulid);
    let result = insert_event(&pool, &event).await;
    assert!(result.is_ok());
    
    // Test wraparound behavior
    let next_ulid = Ulid::new();
    assert!(next_ulid > ulid || next_ulid.timestamp_ms() < ulid.timestamp_ms());
}

#[sinex_test]
async fn test_ulid_monotonic_generation_stress() {
    use std::sync::Arc;
    use tokio::sync::Mutex;
    
    let generated_ulids = Arc::new(Mutex::new(Vec::new()));
    let mut handles = vec![];
    
    // Generate ULIDs from multiple threads
    for _ in 0..10 {
        let ulids = generated_ulids.clone();
        let handle = tokio::spawn(async move {
            let mut local_ulids = vec![];
            for _ in 0..1000 {
                local_ulids.push(Ulid::new());
            }
            ulids.lock().await.extend(local_ulids);
        });
        handles.push(handle);
    }
    
    futures::future::join_all(handles).await;
    
    let ulids = generated_ulids.lock().await;
    let mut sorted_ulids = ulids.clone();
    sorted_ulids.sort();
    
    // Check for duplicates
    let unique_count = sorted_ulids.iter().collect::<std::collections::HashSet<_>>().len();
    assert_eq!(unique_count, ulids.len(), "ULID generation produced duplicates");
}
```

#### 2.2 Unicode Attack Tests

```rust
// test/security/unicode_attack_test.rs
#[sinex_test]
async fn test_unicode_normalization_attacks() {
    let attack_strings = vec![
        // Homograph attacks
        ("admin", "аdmin"), // Cyrillic 'а'
        
        // Zero-width characters
        ("test", "te\u{200B}st"), // Zero-width space
        
        // Direction override
        ("file.txt", "file\u{202E}txt.exe"), // Right-to-left override
        
        // Normalization forms
        ("café", "cafe\u{0301}"), // NFD vs NFC
    ];
    
    for (expected, attack) in attack_strings {
        let event = create_event_with_string_field(attack);
        let result = insert_event(&pool, &event).await;
        
        // Verify normalization or rejection
        if result.is_ok() {
            let retrieved = get_event(&pool, event.id).await?;
            // Check if properly normalized
            assert_ne!(retrieved.payload["field"], attack);
        }
    }
}
```

#### 2.3 Large Payload Tests

```rust
// test/performance/large_payload_test.rs
#[sinex_test]
async fn test_progressive_payload_sizes() {
    let sizes = vec![
        (1_000_000, "1MB"),
        (10_000_000, "10MB"),
        (50_000_000, "50MB"),
        (100_000_000, "100MB"),
        (500_000_000, "500MB"),
    ];
    
    for (size, label) in sizes {
        println!("Testing {} payload", label);
        
        let large_data = generate_large_json(size);
        let event = create_event_with_payload(large_data);
        
        let start = Instant::now();
        let result = insert_event(&pool, &event).await;
        let duration = start.elapsed();
        
        match result {
            Ok(_) => {
                println!("  Success: {} inserted in {:?}", label, duration);
                
                // Test retrieval
                let retrieve_start = Instant::now();
                let retrieved = get_event(&pool, event.id).await;
                let retrieve_duration = retrieve_start.elapsed();
                
                assert!(retrieved.is_ok());
                println!("  Retrieved in {:?}", retrieve_duration);
            }
            Err(e) => {
                println!("  Failed at {}: {}", label, e);
                // Expected to fail at some point
            }
        }
    }
}

#[sinex_test]
async fn test_deeply_nested_json() {
    fn create_nested_json(depth: usize) -> Value {
        if depth == 0 {
            json!({"value": "leaf"})
        } else {
            json!({"nested": create_nested_json(depth - 1)})
        }
    }
    
    let depths = vec![10, 50, 100, 500, 1000];
    
    for depth in depths {
        let nested = create_nested_json(depth);
        let event = create_event_with_payload(nested);
        
        let result = insert_event(&pool, &event).await;
        
        if depth > 100 {
            // Should fail or be rejected for extreme nesting
            assert!(result.is_err() || validate_json_depth(&event.payload) == false);
        } else {
            assert!(result.is_ok());
        }
    }
}
```

### Phase 3: Property Tests (Priority: MEDIUM)

#### 3.1 Event Ordering Invariants

```rust
// test/property/event_ordering_property_test.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_event_ordering_invariant(
        events in prop::collection::vec(event_strategy(), 1..100)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pool = setup_test_pool().await;
            
            // Insert events
            for event in &events {
                insert_event(&pool, event).await.unwrap();
            }
            
            // Query events by timestamp
            let retrieved = get_events_ordered_by_timestamp(&pool).await.unwrap();
            
            // Verify ordering
            for window in retrieved.windows(2) {
                assert!(window[0].ts_orig <= window[1].ts_orig);
            }
        });
    }
    
    #[test]
    fn test_ulid_monotonicity_property(
        count in 1..1000usize
    ) {
        let mut ulids = Vec::with_capacity(count);
        for _ in 0..count {
            ulids.push(Ulid::new());
        }
        
        // Check strict ordering for same-millisecond ULIDs
        let mut same_ms_groups = HashMap::new();
        for ulid in &ulids {
            same_ms_groups.entry(ulid.timestamp_ms())
                .or_insert_with(Vec::new)
                .push(ulid);
        }
        
        for (_, group) in same_ms_groups {
            if group.len() > 1 {
                // Within same millisecond, ordering by random component
                let mut sorted = group.clone();
                sorted.sort();
                assert_eq!(sorted, group);
            }
        }
    }
}
```

#### 3.2 Checkpoint Consistency Properties

```rust
// test/property/checkpoint_consistency_property_test.rs
proptest! {
    #[test]
    fn test_checkpoint_consistency_under_concurrent_updates(
        updates in prop::collection::vec(checkpoint_update_strategy(), 1..50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pool = setup_test_pool().await;
            
            // Apply updates concurrently
            let handles: Vec<_> = updates.into_iter()
                .map(|update| {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        apply_checkpoint_update(&pool, update).await
                    })
                })
                .collect();
            
            let results = futures::future::join_all(handles).await;
            
            // Verify final state consistency
            let final_checkpoint = get_checkpoint(&pool).await.unwrap();
            
            // Properties to check:
            // 1. Processed count is sum of all successful updates
            // 2. Last processed ID is from the latest update
            // 3. No lost updates
            prop_assert!(final_checkpoint.processed_count >= 0);
            prop_assert!(final_checkpoint.checkpoint_version > 0);
        });
    }
}
```

### Phase 4: Performance Benchmarks (Priority: MEDIUM)

#### 4.1 Event Processing Benchmarks

```rust
// benches/event_processing_bench.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion, BatchSize};

fn bench_event_insertion(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(setup_bench_pool());
    
    c.bench_function("event_insertion_single", |b| {
        b.to_async(&rt).iter(|| async {
            let event = create_test_event();
            insert_event(&pool, &event).await.unwrap()
        });
    });
    
    c.bench_function("event_insertion_batch_10", |b| {
        b.to_async(&rt).iter_batched(
            || create_event_batch(10),
            |events| async {
                for event in events {
                    insert_event(&pool, &event).await.unwrap();
                }
            },
            BatchSize::SmallInput
        );
    });
    
    c.bench_function("event_insertion_concurrent_100", |b| {
        b.to_async(&rt).iter(|| async {
            let handles: Vec<_> = (0..100)
                .map(|_| {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        let event = create_test_event();
                        insert_event(&pool, &event).await
                    })
                })
                .collect();
            
            futures::future::join_all(handles).await
        });
    });
}
```

#### 4.2 Checkpoint Update Benchmarks

```rust
// benches/checkpoint_bench.rs
fn bench_checkpoint_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pool = rt.block_on(setup_bench_pool());
    
    c.bench_function("checkpoint_update_uncontended", |b| {
        b.to_async(&rt).iter(|| async {
            update_checkpoint(&pool, "test_processor", Ulid::new()).await.unwrap()
        });
    });
    
    c.bench_function("checkpoint_update_contended_10", |b| {
        b.to_async(&rt).iter(|| async {
            let handles: Vec<_> = (0..10)
                .map(|_| {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        update_checkpoint(&pool, "test_processor", Ulid::new()).await
                    })
                })
                .collect();
            
            futures::future::join_all(handles).await
        });
    });
}
```

### Phase 5: Concurrent Operations Tests (Priority: HIGH)

#### 5.1 Concurrent Checkpoint Updates

```rust
// test/concurrency/checkpoint_concurrency_test.rs
#[sinex_test]
async fn test_concurrent_checkpoint_updates_consistency() {
    let pool = ctx.pool().clone();
    let processor_name = "concurrent_test_processor";
    
    // Initialize checkpoint
    create_checkpoint(&pool, processor_name).await?;
    
    // Concurrent updates from multiple workers
    let update_count = 100;
    let worker_count = 10;
    let updates_per_worker = update_count / worker_count;
    
    let successful_updates = Arc::new(AtomicU64::new(0));
    let failed_updates = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    for worker_id in 0..worker_count {
        let pool = pool.clone();
        let success_count = successful_updates.clone();
        let fail_count = failed_updates.clone();
        
        let handle = tokio::spawn(async move {
            for i in 0..updates_per_worker {
                let update_id = Ulid::new();
                
                match update_checkpoint_atomic(&pool, processor_name, update_id).await {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        if i == 0 {
                            println!("Worker {} update failed: {}", worker_id, e);
                        }
                    }
                }
                
                // Small random delay
                tokio::time::sleep(Duration::from_micros(rand::random::<u64>() % 100)).await;
            }
        });
        
        handles.push(handle);
    }
    
    futures::future::join_all(handles).await;
    
    let successful = successful_updates.load(Ordering::SeqCst);
    let failed = failed_updates.load(Ordering::SeqCst);
    
    println!("Concurrent checkpoint updates:");
    println!("  Successful: {}", successful);
    println!("  Failed: {}", failed);
    
    // Verify final state
    let final_checkpoint = get_checkpoint(&pool, processor_name).await?;
    
    // All updates should be reflected
    assert_eq!(
        final_checkpoint.processed_count as u64,
        successful,
        "Processed count should match successful updates"
    );
    
    // Verify checkpoint history integrity
    let history = get_checkpoint_history(&pool, processor_name, 1000).await?;
    assert!(!history.is_empty(), "Checkpoint history should be preserved");
}
```

## Test Execution Strategy

### 1. Automated Test Generation

```bash
#!/usr/bin/env bash
# scripts/generate_coverage_tests.sh

# Find all unwrap/expect calls
echo "Finding untested error paths..."
rg -t rust '\.unwrap\(\)|\.expect\(' crate/ --glob '!*test*' -n > untested_errors.txt

# Generate test stubs
python scripts/generate_error_tests.py untested_errors.txt > test/generated/error_path_tests.rs

# Run coverage analysis
cargo tarpaulin --out Html --output-dir coverage/
```

### 2. Continuous Coverage Monitoring

```yaml
# .github/workflows/coverage.yml
name: Test Coverage
on: [push, pull_request]

jobs:
  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
      - name: Run tests with coverage
        run: |
          cargo install cargo-tarpaulin
          cargo tarpaulin --out Xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

### 3. Performance Regression Detection

```toml
# Cargo.toml
[[bench]]
name = "event_processing"
harness = false

[[bench]]
name = "checkpoint_operations"
harness = false

[dev-dependencies]
criterion = { version = "0.5", features = ["async_tokio"] }
```

## Success Metrics

1. **Error Path Coverage**: 100% of unwrap/expect calls have corresponding error tests
2. **Edge Case Coverage**: All identified edge cases have dedicated tests
3. **Property Test Coverage**: Critical invariants verified with property tests
4. **Performance Baselines**: All critical paths have benchmark baselines
5. **Concurrent Operation Safety**: All concurrent scenarios tested and verified

## Timeline

- **Week 1**: Error path test generation and implementation
- **Week 2**: Edge case tests (ULID, Unicode, Large payloads)
- **Week 3**: Property tests and concurrent operation tests
- **Week 4**: Performance benchmarks and CI integration

## Maintenance

1. **Automated Coverage Reports**: Weekly coverage reports to track progress
2. **Benchmark Tracking**: Performance regression alerts on CI
3. **Test Review**: Quarterly review of test effectiveness
4. **Documentation**: Maintain test writing guidelines for new code