//! Unit tests for CoordinationPrimitive unified abstraction
//!
//! Tests all factory methods and behaviors:
//! - event_counter, barrier, synchronizer
//! - Thresholds, reset behaviors, atomic operations
//! - Backwards compatibility with EventCounter/ProgressTracker

use sinex_core_utils::{CoordinationPrimitive, EventCounter, ProgressTracker};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn test_event_counter_factory_method() {
    let counter = CoordinationPrimitive::event_counter(100, "test_events");
    
    // Initial state
    assert_eq!(counter.current_value(), 0);
    assert!(!counter.is_complete());
    assert_eq!(counter.name(), "test_events");
    assert_eq!(counter.threshold(), 100);
    
    // Increment operations
    counter.add(50);
    assert_eq!(counter.current_value(), 50);
    assert!(!counter.is_complete());
    
    counter.add(30);
    assert_eq!(counter.current_value(), 80);
    assert!(!counter.is_complete());
    
    // Reach threshold
    counter.add(20);
    assert_eq!(counter.current_value(), 100);
    assert!(counter.is_complete());
    
    // Event counter never resets automatically
    counter.add(10);
    assert_eq!(counter.current_value(), 110);
    assert!(counter.is_complete());
}

#[tokio::test]
async fn test_barrier_factory_method() {
    let barrier = CoordinationPrimitive::barrier(3, "worker_sync");
    
    // Initial state
    assert_eq!(barrier.current_value(), 0);
    assert!(!barrier.is_complete());
    assert_eq!(barrier.threshold(), 3);
    
    // Participants arriving
    barrier.add(1); // First worker
    assert_eq!(barrier.current_value(), 1);
    assert!(!barrier.is_complete());
    
    barrier.add(1); // Second worker  
    assert_eq!(barrier.current_value(), 2);
    assert!(!barrier.is_complete());
    
    barrier.add(1); // Third worker - barrier releases
    assert_eq!(barrier.current_value(), 3);
    assert!(barrier.is_complete());
    
    // Barrier resets automatically for reuse
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert_eq!(barrier.current_value(), 0);
    assert!(!barrier.is_complete());
}

#[tokio::test]  
async fn test_synchronizer_factory_method() {
    let sync = CoordinationPrimitive::synchronizer("service_ready");
    
    // Initial state
    assert_eq!(sync.current_value(), 0);
    assert!(!sync.is_complete());
    assert_eq!(sync.threshold(), 1);
    
    // Signal readiness
    sync.signal();
    assert_eq!(sync.current_value(), 1);
    assert!(sync.is_complete());
    
    // Synchronizer stays signaled
    sync.signal(); // Additional signals ignored
    assert_eq!(sync.current_value(), 1);
    assert!(sync.is_complete());
}

#[tokio::test]
async fn test_backwards_compatibility_type_aliases() {
    // EventCounter type alias should work exactly like factory method
    let counter1 = EventCounter::event_counter(50, "events");
    let counter2 = CoordinationPrimitive::event_counter(50, "events");
    
    counter1.add(25);
    counter2.add(25);
    
    assert_eq!(counter1.current_value(), counter2.current_value());
    assert_eq!(counter1.is_complete(), counter2.is_complete());
    
    // ProgressTracker type alias should work
    let tracker = ProgressTracker::barrier(5, "steps");
    tracker.add(3);
    assert_eq!(tracker.current_value(), 3);
    assert!(!tracker.is_complete());
}

#[tokio::test]
async fn test_reset_behaviors() {
    // Event counter - never resets
    let counter = CoordinationPrimitive::event_counter(10, "never_reset");
    counter.add(15);
    let old_value = counter.reset_and_get_previous();
    assert_eq!(old_value, 15);
    assert_eq!(counter.current_value(), 0); // Manual reset only
    
    // Barrier - auto resets
    let barrier = CoordinationPrimitive::barrier(2, "auto_reset");
    barrier.add(2);
    assert!(barrier.is_complete());
    
    // Wait for auto-reset
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(barrier.current_value(), 0);
    
    // Synchronizer - stays signaled
    let sync = CoordinationPrimitive::synchronizer("stays_signaled");
    sync.signal();
    assert!(sync.is_complete());
    
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(sync.is_complete()); // Still complete
}

#[tokio::test]
async fn test_concurrent_operations() {
    let counter = Arc::new(CoordinationPrimitive::event_counter(1000, "concurrent_test"));
    let mut handles = vec![];
    
    // Spawn 10 tasks adding 100 each = 1000 total
    for _ in 0..10 {
        let counter_clone = counter.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                counter_clone.add(1);
            }
        });
        handles.push(handle);
    }
    
    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }
    
    assert_eq!(counter.current_value(), 1000);
    assert!(counter.is_complete());
}

#[tokio::test]
async fn test_barrier_concurrent_workers() {
    let barrier = Arc::new(CoordinationPrimitive::barrier(5, "concurrent_barrier"));
    let mut handles = vec![];
    
    // Spawn 5 workers that all reach the barrier
    for worker_id in 0..5 {
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            // Simulate work
            tokio::time::sleep(Duration::from_millis(worker_id * 10)).await;
            
            // Reach barrier
            barrier_clone.add(1);
            
            // Check if we triggered completion (last worker)
            barrier_clone.is_complete()
        });
        handles.push(handle);
    }
    
    // Wait for all workers
    let results: Vec<bool> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    
    // Exactly one worker should have triggered completion
    let completers = results.iter().filter(|&&x| x).count();
    assert_eq!(completers, 1);
    
    // Barrier should be complete
    assert!(barrier.is_complete());
}

#[tokio::test]
async fn test_wait_for_completion() {
    let counter = Arc::new(CoordinationPrimitive::event_counter(10, "wait_test"));
    let counter_clone = counter.clone();
    
    // Start task that will complete the counter after delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        counter_clone.add(10);
    });
    
    // Wait for completion with timeout
    let result = timeout(Duration::from_millis(200), async {
        while !counter.is_complete() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    
    assert!(result.is_ok());
    assert!(counter.is_complete());
}

#[tokio::test]
async fn test_coordination_primitive_metadata() {
    let counter = CoordinationPrimitive::event_counter(100, "metadata_test");
    
    // Test name and threshold access
    assert_eq!(counter.name(), "metadata_test");
    assert_eq!(counter.threshold(), 100);
    
    // Test descriptive string
    let description = counter.description();
    assert!(description.contains("metadata_test"));
    assert!(description.contains("100"));
}

#[tokio::test]
async fn test_edge_cases() {
    // Zero threshold
    let zero_barrier = CoordinationPrimitive::barrier(0, "zero_test");
    assert!(zero_barrier.is_complete()); // Should be immediately complete
    
    // Large threshold
    let large_counter = CoordinationPrimitive::event_counter(usize::MAX, "large_test");
    large_counter.add(1000);
    assert!(!large_counter.is_complete());
    
    // Empty name
    let unnamed = CoordinationPrimitive::synchronizer("");
    assert_eq!(unnamed.name(), "");
    unnamed.signal();
    assert!(unnamed.is_complete());
}

#[tokio::test]
async fn test_multiple_coordination_patterns() {
    // Simulate complex coordination scenario
    let startup_barrier = Arc::new(CoordinationPrimitive::barrier(3, "startup"));
    let event_counter = Arc::new(CoordinationPrimitive::event_counter(100, "events"));
    let shutdown_signal = Arc::new(CoordinationPrimitive::synchronizer("shutdown"));
    
    // Three workers coordinate startup, process events, then shutdown
    let mut handles = vec![];
    
    for worker_id in 0..3 {
        let barrier = startup_barrier.clone();
        let counter = event_counter.clone();
        let shutdown = shutdown_signal.clone();
        
        let handle = tokio::spawn(async move {
            // Wait for all workers to start
            barrier.add(1);
            while !barrier.is_complete() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            
            // Process events
            for _ in 0..33 {
                counter.add(1);
                tokio::time::sleep(Duration::from_millis(1)).await;
                
                if shutdown.is_complete() {
                    break;
                }
            }
            
            worker_id
        });
        handles.push(handle);
    }
    
    // Let workers run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Signal shutdown
    shutdown_signal.signal();
    
    // Wait for all workers
    let worker_ids: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    
    assert_eq!(worker_ids, vec![0, 1, 2]);
    assert!(startup_barrier.is_complete());
    assert!(shutdown_signal.is_complete());
    // Event counter should have some events (workers might not reach 100 due to shutdown)
    assert!(event_counter.current_value() > 0);
}