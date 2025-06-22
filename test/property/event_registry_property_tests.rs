use proptest::prelude::*;
use sinex_core::{EventRegistry, create_registry};
use std::sync::{Arc, Barrier};
use std::thread;
use std::collections::HashMap;

/// Generate arbitrary event type names that match registry patterns
fn arb_event_type() -> impl Strategy<Value = String> {
    prop_oneof![
        // Known event types from registry
        Just("file.created".to_string()),
        Just("file.modified".to_string()),
        Just("file.deleted".to_string()),
        Just("command.executed".to_string()),
        Just("window.focused".to_string()),
        Just("window.opened".to_string()),
        Just("workspace.changed".to_string()),
        Just("monitor.focused".to_string()),
        Just("shell.history.command".to_string()),
        Just("terminal.asciinema.session_started".to_string()),
        Just("dbus.signal".to_string()),
        Just("system.notification".to_string()),
        // Unknown event types (should not be found)
        Just("unknown.event".to_string()),
        Just("nonexistent.type".to_string()),
        Just("invalid.name".to_string()),
        // Randomly generated event types
        "[a-zA-Z][a-zA-Z0-9_-]{1,20}\\.[a-zA-Z][a-zA-Z0-9_-]{1,20}"
    ]
}

/// Generate arbitrary source names
fn arb_source_name() -> impl Strategy<Value = String> {
    prop_oneof![
        // Known source names from registry
        Just("filesystem".to_string()),
        Just("terminal_kitty".to_string()),
        Just("hyprland".to_string()),
        Just("shell_history".to_string()),
        Just("dbus".to_string()),
        // Unknown source names
        Just("unknown_source".to_string()),
        Just("nonexistent".to_string()),
        // Random source names
        "[a-zA-Z][a-zA-Z0-9_-]{1,30}"
    ]
}

/// Test concurrent access to EventRegistry methods
fn test_concurrent_registry_access<F>(
    num_threads: usize,
    operations_per_thread: usize,
    operation: F,
) where
    F: Fn(&EventRegistry, usize, usize) + Send + Sync + 'static,
{
    let registry = Arc::new(create_registry());
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();
    
    let operation = Arc::new(operation);
    
    for thread_id in 0..num_threads {
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        let operation = Arc::clone(&operation);
        
        let handle = thread::spawn(move || {
            barrier.wait();
            
            for op_id in 0..operations_per_thread {
                operation(&registry, thread_id, op_id);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread should complete successfully");
    }
}

proptest! {
    /// Property: EventRegistry should be thread-safe for concurrent reads
    #[test]
    fn test_event_registry_concurrent_reads(
        num_threads in 2usize..=10,
        operations_per_thread in 10usize..=50,
        event_types in prop::collection::vec(arb_event_type(), 5..=20)
    ) {
        test_concurrent_registry_access(
            num_threads,
            operations_per_thread,
            move |registry, thread_id, op_id| {
                let event_type = &event_types[op_id % event_types.len()];
                
                // These operations should be thread-safe
                let _ = registry.source_for_event(event_type);
                let _ = registry.has_event(event_type);
                let _ = registry.all_sources();
                let _ = registry.event_types.len();
                
                // Verify consistency across calls
                let has_event = registry.has_event(event_type);
                let source_option = registry.source_for_event(event_type);
                
                // If has_event is true, source_for_event should return Some
                if has_event {
                    assert!(source_option.is_some(), 
                        "Event {} should have a source if it exists", event_type);
                }
            }
        );
    }

    /// Property: EventRegistry schema generation should be thread-safe
    #[test]
    fn test_event_registry_concurrent_schema_access(
        num_threads in 2usize..=8,
        operations_per_thread in 5usize..=20
    ) {
        let known_events = vec![
            "file.created",
            "file.modified", 
            "command.executed",
            "window.focused",
            "unknown.event", // This should return None
        ];
        
        test_concurrent_registry_access(
            num_threads,
            operations_per_thread,
            move |registry, _thread_id, op_id| {
                let event_type = known_events[op_id % known_events.len()];
                
                // Schema access should be thread-safe
                let schema_result = registry.schema_for_event(event_type);
                
                // Verify consistency
                let has_event = registry.has_event(event_type);
                
                // Known events should have schemas, unknown should not
                if event_type == "unknown.event" {
                    assert!(!has_event);
                    assert!(schema_result.is_none());
                } else {
                    // For now, not all known events have schema generators
                    // but the call should still be thread-safe
                    let _ = schema_result;
                }
            }
        );
    }

    /// Property: EventRegistry lookup results should be consistent across threads
    #[test] 
    fn test_event_registry_lookup_consistency(
        num_threads in 3usize..=8,
        _lookups_per_thread in 20usize..=100
    ) {
        let registry = Arc::new(create_registry());
        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = Vec::new();
        
        // Collect results from each thread
        let results = Arc::new(std::sync::Mutex::new(Vec::new()));
        
        for thread_id in 0..num_threads {
            let registry = Arc::clone(&registry);
            let barrier = Arc::clone(&barrier);
            let results = Arc::clone(&results);
            
            let handle = thread::spawn(move || {
                barrier.wait();
                let mut thread_results = HashMap::new();
                
                // Test all known event types
                for &event_type in registry.event_types {
                    let source = registry.source_for_event(event_type);
                    let has_event = registry.has_event(event_type);
                    let events_for_source = if let Some(src) = source {
                        registry.events_for_source(src)
                    } else {
                        Vec::new()
                    };
                    
                    thread_results.insert(event_type, (source, has_event, events_for_source));
                }
                
                results.lock().unwrap().push((thread_id, thread_results));
            });
            handles.push(handle);
        }
        
        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify all threads got identical results
        let all_results = results.lock().unwrap();
        let first_results = &all_results[0].1;
        
        for (thread_id, thread_results) in all_results.iter().skip(1) {
            for event_type in first_results.keys() {
                let first_result = &first_results[event_type];
                let current_result = &thread_results[event_type];
                
                prop_assert_eq!(first_result.0, current_result.0, 
                    "Thread {} got different source for {}", thread_id, event_type);
                prop_assert_eq!(first_result.1, current_result.1,
                    "Thread {} got different has_event for {}", thread_id, event_type);
                prop_assert_eq!(&first_result.2, &current_result.2,
                    "Thread {} got different events_for_source for {}", thread_id, event_type);
            }
        }
    }

    /// Property: EventRegistry source mappings should be bidirectional
    #[test]
    fn test_event_registry_bidirectional_mappings(
        source_names in prop::collection::vec(arb_source_name(), 3..=10)
    ) {
        let registry = create_registry();
        
        // Test bidirectional consistency for all known mappings
        for &event_type in registry.event_types {
            if let Some(source) = registry.source_for_event(event_type) {
                let events_for_source = registry.events_for_source(source);
                prop_assert!(events_for_source.contains(&event_type),
                    "Event {} maps to source {} but source doesn't map back to event",
                    event_type, source);
            }
        }
        
        // Test with unknown sources
        for source_name in &source_names {
            let events = registry.events_for_source(source_name);
            
            // All events returned should actually map back to this source
            for event in events {
                let mapped_source = registry.source_for_event(event).unwrap();
                prop_assert_eq!(mapped_source, source_name,
                    "Event {} returned for source {} but maps to different source {}",
                    event, source_name, mapped_source);
            }
        }
    }

    /// Property: EventRegistry should handle edge cases gracefully
    #[test]
    fn test_event_registry_edge_cases(
        edge_case_inputs in prop::collection::vec(".*", 0..=10)
    ) {
        let registry = create_registry();
        
        let edge_cases = vec![
            "",
            " ",
            "  \t\n  ",
            "event.",
            ".type",
            "event..type",
            "UPPERCASE.EVENT",
            "event.type.with.many.dots",
            "event-with-dashes",
            "event_with_underscores",
            "123.numeric.start",
            "event.123",
            "special.chars!@#$",
            "very.long.event.name.that.might.cause.issues.with.storage.or.processing",
        ];
        
        // Combine generated and fixed edge cases
        let mut all_cases: Vec<String> = edge_cases.into_iter().map(|s| s.to_string()).collect();
        all_cases.extend(edge_case_inputs);
        
        for test_input in all_cases {
            // These calls should never panic, even with invalid inputs
            let source_result = registry.source_for_event(&test_input);
            let has_event_result = registry.has_event(&test_input);
            let events_for_source_result = registry.events_for_source(&test_input);
            
            // Results should be consistent
            if has_event_result {
                prop_assert!(source_result.is_some(),
                    "has_event returned true for {} but source_for_event returned None", test_input);
            }
            
            // events_for_source should always return a Vec (possibly empty)
            // This verifies the method doesn't panic on invalid input
            let _ = events_for_source_result.len();
        }
    }
}

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    
    #[test]
    fn test_registry_high_concurrency_stress() {
        const NUM_THREADS: usize = 50;
        const OPERATIONS_PER_THREAD: usize = 1000;
        const TOTAL_OPERATIONS: usize = NUM_THREADS * OPERATIONS_PER_THREAD;
        
        let _registry = Arc::new(create_registry());
        let operation_counter = Arc::new(AtomicUsize::new(0));
        let start_time = Instant::now();
        
        let counter_clone = Arc::clone(&operation_counter);
        test_concurrent_registry_access(
            NUM_THREADS,
            OPERATIONS_PER_THREAD,
            move |registry, _thread_id, op_id| {
                // Cycle through different operations
                match op_id % 5 {
                    0 => { let _ = registry.source_for_event("file.created"); },
                    1 => { let _ = registry.has_event("window.focused"); },
                    2 => { let _ = registry.events_for_source("filesystem"); },
                    3 => { let _ = registry.all_sources(); },
                    4 => { let _ = registry.schema_for_event("command.executed"); },
                    _ => unreachable!(),
                }
                
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }
        );
        
        let elapsed = start_time.elapsed();
        let final_count = operation_counter.load(Ordering::Relaxed);
        
        assert_eq!(final_count, TOTAL_OPERATIONS);
        println!("Completed {} operations in {:?} ({:.2} ops/sec)", 
                 final_count, elapsed, 
                 final_count as f64 / elapsed.as_secs_f64());
    }
    
    #[test]
    fn test_registry_memory_safety_under_stress() {
        const STRESS_DURATION_SECS: u64 = 2;
        const NUM_THREADS: usize = 20;
        
        let registry = Arc::new(create_registry());
        let should_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handles = Vec::new();
        
        // Start threads that continuously access the registry
        for _thread_id in 0..NUM_THREADS {
            let registry = Arc::clone(&registry);
            let should_stop = Arc::clone(&should_stop);
            
            let handle = thread::spawn(move || {
                let mut operation_count = 0;
                
                while !should_stop.load(Ordering::Relaxed) {
                    // Rapidly cycle through all registry operations
                    for &event_type in registry.event_types {
                        if should_stop.load(Ordering::Relaxed) {
                            break;
                        }
                        
                        let _ = registry.source_for_event(event_type);
                        let _ = registry.has_event(event_type);
                        
                        if let Some(source) = registry.source_for_event(event_type) {
                            let _ = registry.events_for_source(source);
                        }
                        
                        operation_count += 1;
                    }
                }
                
                operation_count
            });
            handles.push(handle);
        }
        
        // Let them run for a while
        thread::sleep(Duration::from_secs(STRESS_DURATION_SECS));
        should_stop.store(true, Ordering::Relaxed);
        
        // Collect results
        let mut total_operations = 0;
        for handle in handles {
            total_operations += handle.join().expect("Thread should complete");
        }
        
        println!("Memory safety stress test completed {} operations", total_operations);
        assert!(total_operations > 0);
    }
}