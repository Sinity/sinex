# Property Builders Guide

This guide explains how to use the enhanced property test builders that integrate proptest with the Sinex test framework.

## Overview

The property builders in `test/common/property_builders.rs` provide proptest strategies that generate test data using the same builder patterns as regular tests. This ensures consistency and reduces boilerplate in property-based tests.

## Core Strategies

### Event Generation

```rust
use crate::common::property_builders::*;

// Generate arbitrary valid events
proptest! {
    #[test]
    fn test_with_arbitrary_events(event in arbitrary_event()) {
        // event is a fully-formed RawEvent with valid structure
        assert!(!event.source.is_empty());
        assert!(!event.event_type.is_empty());
    }
}

// Generate batches of related events
proptest! {
    #[test]
    fn test_with_event_batches(batch in arbitrary_event_batch()) {
        // batch is Vec<RawEvent> with 1-50 events from same source
        let first_source = &batch[0].source;
        assert!(batch.iter().all(|e| &e.source == first_source));
    }
}
```

### Specialized Event Types

Generate specific types of events with proper field structure:

```rust
// Filesystem events
proptest! {
    #[test]
    fn test_filesystem_operations(fs_event in filesystem_event()) {
        assert_eq!(fs_event.source, sources::FS);
        let payload = fs_event.payload.as_object().unwrap();
        assert!(payload.contains_key("path"));
        assert!(payload.contains_key("size"));
    }
}

// Shell command events
proptest! {
    #[test]
    fn test_shell_commands(cmd_event in shell_command_event()) {
        assert_eq!(cmd_event.source, sources::SHELL_KITTY);
        assert_eq!(cmd_event.event_type, event_types::shell::COMMAND_EXECUTED);
        let payload = cmd_event.payload.as_object().unwrap();
        assert!(payload.contains_key("command"));
        assert!(payload.contains_key("exit_code"));
    }
}

// Window manager events
proptest! {
    #[test]
    fn test_window_events(window_event in window_event()) {
        assert_eq!(window_event.source, sources::WM_HYPRLAND);
        let payload = window_event.payload.as_object().unwrap();
        assert!(payload.contains_key("window_class"));
        assert!(payload.contains_key("window_title"));
    }
}

// Clipboard events
proptest! {
    #[test]
    fn test_clipboard_events(clip_event in clipboard_event()) {
        assert_eq!(clip_event.source, sources::CLIPBOARD);
        let payload = clip_event.payload.as_object().unwrap();
        assert!(payload.contains_key("content"));
    }
}

// Heartbeat events
proptest! {
    #[test]
    fn test_heartbeat_events(heartbeat in heartbeat_event()) {
        assert_eq!(heartbeat.source, sources::SINEX);
        assert_eq!(heartbeat.event_type, event_types::sinex::AUTOMATON_HEARTBEAT);
        let payload = heartbeat.payload.as_object().unwrap();
        assert!(payload.contains_key("automaton_name"));
        assert!(payload.contains_key("events_processed"));
    }
}
```

### Checkpoint Strategies

```rust
// Generate arbitrary checkpoints
proptest! {
    #[test]
    fn test_checkpoint_handling(checkpoint in arbitrary_checkpoint()) {
        match checkpoint {
            Checkpoint::None => { /* handle no checkpoint */ }
            Checkpoint::Stream { message_id, event_id } => {
                assert!(!message_id.is_empty());
            }
            Checkpoint::Database { event_id } => {
                assert!(event_id != Ulid::nil());
            }
            Checkpoint::Timestamp { timestamp } => {
                assert!(timestamp < Utc::now() + Duration::days(1));
            }
        }
    }
}
```

### Time-Based Strategies

```rust
// Generate ULID ranges for queries
proptest! {
    #[test]
    fn test_ulid_range_queries((start, end) in arbitrary_ulid_range()) {
        assert!(start <= end); // Always properly ordered
        // Use for range-based database queries
    }
}

// Generate time ranges
proptest! {
    #[test]
    fn test_time_range_queries((start_time, end_time) in arbitrary_time_range()) {
        assert!(start_time < end_time);
        let duration = end_time - start_time;
        assert!(duration >= Duration::seconds(1));
        assert!(duration <= Duration::days(1));
    }
}
```

### Invalid Event Generation

Test error handling with invalid events:

```rust
// Events with empty source
proptest! {
    #[test]
    fn test_empty_source_rejection(event in empty_source_event()) {
        assert!(event.source.is_empty());
        let result = validate_event(&event);
        assert!(result.is_err());
    }
}

// Events with massive payloads
proptest! {
    #[test]
    fn test_large_payload_handling(event in massive_payload_event()) {
        let payload_size = event.payload.to_string().len();
        assert!(payload_size >= 1_000_000);
        // Test system behavior with large payloads
    }
}

// Events with deeply nested JSON
proptest! {
    #[test]
    fn test_nested_payload_handling(event in deeply_nested_event()) {
        // Test JSON parsing limits
    }
}

// Events with extreme timestamps
proptest! {
    #[test]
    fn test_extreme_timestamp_handling(event in extreme_timestamp_event()) {
        let ts = event.ts_orig.unwrap();
        // Test handling of far past/future timestamps
    }
}
```

### Batch Pattern Strategies

Generate realistic event sequences:

```rust
// Time-ordered event batches
proptest! {
    #[test]
    fn test_temporal_ordering(batch in time_ordered_batch()) {
        // Events are guaranteed to be in chronological order
        for window in batch.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            if let (Some(t1), Some(t2)) = (prev.ts_orig, curr.ts_orig) {
                assert!(t1 <= t2);
            }
        }
    }
}

// Realistic user activity patterns
proptest! {
    #[test]
    fn test_user_activity_simulation(activity in user_activity_batch()) {
        // Generates sequence: shell command → file access → window switch → clipboard → more commands
        assert!(activity.len() >= 5);
        // Events simulate realistic user workflow
    }
}

// Related events (e.g., file lifecycle)
proptest! {
    #[test]
    fn test_related_event_tracking(related in related_events_batch()) {
        // Generates: file created → modified (2x) → deleted
        assert_eq!(related[0].event_type, event_types::filesystem::FILE_CREATED);
        assert_eq!(related.last().unwrap().event_type, event_types::filesystem::FILE_DELETED);
        
        // All events reference same file
        let file_path = related[0].payload.get("path").unwrap().as_str().unwrap();
        assert!(related.iter().all(|e| {
            e.payload.get("path").unwrap().as_str().unwrap() == file_path
        }));
    }
}
```

## Integration with Test Context

Use property builders with TestContext:

```rust
proptest! {
    #[test]
    fn test_database_operations(
        events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await.unwrap();
            let pool = ctx.pool();
            
            // Insert generated events
            for event in &events {
                sinex_db::insert_event_with_validator(&pool, event, None).await.unwrap();
            }
            
            // Use generated checkpoint
            let checkpoint_manager = CheckpointManager::new(
                pool.clone(),
                "test-automaton".to_string(),
                "test-group".to_string(),
                "test-consumer".to_string(),
            );
            
            let state = CheckpointState {
                checkpoint,
                processed_count: events.len() as u64,
                last_activity: chrono::Utc::now(),
                data: None,
                version: 2,
            };
            
            checkpoint_manager.save_checkpoint(&state).await.unwrap();
        });
    }
}
```

## Best Practices

1. **Use specific generators when possible**: `filesystem_event()` is better than `arbitrary_event()` when testing filesystem-specific logic.

2. **Combine strategies for complex scenarios**:
   ```rust
   proptest! {
       #[test]
       fn test_mixed_workload(
           fs_events in proptest::collection::vec(filesystem_event(), 1..=10),
           shell_events in proptest::collection::vec(shell_command_event(), 1..=10),
           heartbeats in proptest::collection::vec(heartbeat_event(), 1..=5),
       ) {
           // Test system under mixed workload
       }
   }
   ```

3. **Use batch generators for integration tests**: `time_ordered_batch()`, `user_activity_batch()`, and `related_events_batch()` generate realistic event sequences.

4. **Test error cases**: Use `empty_source_event()`, `massive_payload_event()`, etc. to verify error handling.

5. **Leverage type safety**: All generators produce properly typed `RawEvent` instances that work with the existing test infrastructure.

## Migration from Old Patterns

Before (manual construction):
```rust
proptest! {
    #[test]
    fn old_pattern(
        source in "[a-z]+",
        event_type in "event\\.[a-z]+",
        payload in any::<String>(),
    ) {
        let event = RawEvent {
            id: Ulid::new(),
            source,
            event_type,
            payload: json!({"data": payload}),
            // ... many more fields to set manually
        };
    }
}
```

After (using property builders):
```rust
proptest! {
    #[test]
    fn new_pattern(event in arbitrary_event()) {
        // event is fully constructed with all fields properly set
        // Just use it directly in tests
    }
}
```

## Advanced Usage

### Custom Strategy Composition

```rust
// Create domain-specific event generator
fn monitoring_event() -> impl Strategy<Value = RawEvent> {
    (
        heartbeat_event(),
        0u64..=100u64, // error count
        0u64..=1000u64, // warning count
    ).prop_map(|(mut event, errors, warnings)| {
        event.payload.as_object_mut().unwrap().insert(
            "errors".to_string(),
            json!(errors)
        );
        event.payload.as_object_mut().unwrap().insert(
            "warnings".to_string(), 
            json!(warnings)
        );
        event
    })
}
```

### Stateful Property Testing

```rust
proptest! {
    #[test]
    fn test_event_processor_state_machine(
        initial_events in arbitrary_event_batch(),
        checkpoint in arbitrary_checkpoint(),
        additional_events in arbitrary_event_batch(),
    ) {
        // Test state transitions:
        // 1. Process initial_events
        // 2. Save checkpoint
        // 3. Process additional_events
        // 4. Verify state consistency
    }
}
```

## Troubleshooting

- **Slow test generation**: Use more specific strategies instead of generic ones
- **Flaky tests**: Ensure you're not depending on non-deterministic event ordering
- **Memory issues**: Limit batch sizes in strategies: `arbitrary_event_batch().prop_map(|b| b.into_iter().take(10).collect())`