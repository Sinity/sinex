//! Unit tests for ResourceGuard generic RAII pattern
//!
//! Tests automatic cleanup for various resource types:
//! - Basic cleanup functionality
//! - Async cleanup support  
//! - Panic safety
//! - Multiple resource types

use sinex_core_utils::ResourceGuard;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

#[tokio::test]
async fn test_basic_resource_cleanup() {
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let cleanup_called_clone = cleanup_called.clone();
    
    {
        let _guard = ResourceGuard::new("test_resource", move |_resource| async move {
            cleanup_called_clone.store(true, Ordering::SeqCst);
        });
        
        // Resource is held, cleanup not called yet
        assert!(!cleanup_called.load(Ordering::SeqCst));
    } // Guard drops here, should trigger cleanup
    
    // Give async cleanup time to run
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    assert!(cleanup_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_resource_access() {
    let guard = ResourceGuard::new("test_value", |_| async {});
    
    // Should be able to access the resource
    assert_eq!(*guard, "test_value");
    
    // Should work with complex types
    let data = vec![1, 2, 3, 4, 5];
    let guard = ResourceGuard::new(data, |_| async {});
    assert_eq!(guard.len(), 5);
    assert_eq!(guard[2], 3);
}

#[tokio::test]
async fn test_cleanup_with_resource_data() {
    let cleanup_data = Arc::new(Mutex::new(String::new()));
    let cleanup_data_clone = cleanup_data.clone();
    
    {
        let _guard = ResourceGuard::new("important_data", move |resource| {
            let data_clone = cleanup_data_clone.clone();
            async move {
                let mut data = data_clone.lock().unwrap();
                *data = format!("Cleaned up: {}", resource);
            }
        });
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    let data = cleanup_data.lock().unwrap();
    assert_eq!(*data, "Cleaned up: important_data");
}

#[tokio::test]
async fn test_multiple_guards_cleanup_order() {
    let cleanup_order = Arc::new(Mutex::new(Vec::new()));
    
    {
        let order1 = cleanup_order.clone();
        let _guard1 = ResourceGuard::new(1, move |resource| {
            let order = order1.clone();
            async move {
                order.lock().unwrap().push(resource);
            }
        });
        
        {
            let order2 = cleanup_order.clone();
            let _guard2 = ResourceGuard::new(2, move |resource| {
                let order = order2.clone();
                async move {
                    order.lock().unwrap().push(resource);
                }
            });
            
            let order3 = cleanup_order.clone();
            let _guard3 = ResourceGuard::new(3, move |resource| {
                let order = order3.clone();
                async move {
                    order.lock().unwrap().push(resource);
                }
            });
        } // guard3 and guard2 drop here
    } // guard1 drops here
    
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    
    let order = cleanup_order.lock().unwrap();
    // Should cleanup in reverse order (LIFO - like destructors)
    assert_eq!(*order, vec![3, 2, 1]);
}

#[tokio::test]
async fn test_panic_during_cleanup() {
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let cleanup_called_clone = cleanup_called.clone();
    
    {
        let _guard = ResourceGuard::new("panic_resource", move |_| {
            let called = cleanup_called_clone.clone();
            async move {
                called.store(true, Ordering::SeqCst);
                panic!("Cleanup panic!");
            }
        });
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    
    // Cleanup should still be called even if it panics
    assert!(cleanup_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_complex_resource_types() {
    // Test with file-like resource
    #[derive(Debug)]
    struct MockFile {
        name: String,
        is_closed: Arc<AtomicBool>,
    }
    
    impl MockFile {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                is_closed: Arc::new(AtomicBool::new(false)),
            }
        }
        
        fn close(&self) {
            self.is_closed.store(true, Ordering::SeqCst);
        }
        
        fn is_closed(&self) -> bool {
            self.is_closed.load(Ordering::SeqCst)
        }
    }
    
    let file = MockFile::new("test.txt");
    let closed_flag = file.is_closed.clone();
    
    {
        let _guard = ResourceGuard::new(file, |file| async move {
            file.close();
        });
        
        assert!(!closed_flag.load(Ordering::SeqCst));
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    assert!(closed_flag.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_database_connection_pattern() {
    // Simulate database connection cleanup
    #[derive(Debug)]
    struct MockConnection {
        id: u32,
        is_active: Arc<AtomicBool>,
        connection_count: Arc<AtomicU32>,
    }
    
    impl MockConnection {
        fn new(id: u32, counter: Arc<AtomicU32>) -> Self {
            counter.fetch_add(1, Ordering::SeqCst);
            Self {
                id,
                is_active: Arc::new(AtomicBool::new(true)),
                connection_count: counter,
            }
        }
        
        fn close(&self) {
            self.is_active.store(false, Ordering::SeqCst);
            self.connection_count.fetch_sub(1, Ordering::SeqCst);
        }
    }
    
    let connection_count = Arc::new(AtomicU32::new(0));
    
    {
        let counter = connection_count.clone();
        let _guard = ResourceGuard::new(
            MockConnection::new(1, counter.clone()), 
            move |conn| async move {
                conn.close();
            }
        );
        
        assert_eq!(connection_count.load(Ordering::SeqCst), 1);
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    assert_eq!(connection_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_lock_guard_pattern() {
    // Simulate advisory lock cleanup
    struct MockAdvisoryLock {
        lock_id: String,
        is_held: Arc<AtomicBool>,
    }
    
    impl MockAdvisoryLock {
        fn new(lock_id: &str) -> Self {
            Self {
                lock_id: lock_id.to_string(),
                is_held: Arc::new(AtomicBool::new(true)),
            }
        }
        
        async fn release(&self) {
            self.is_held.store(false, Ordering::SeqCst);
        }
    }
    
    let lock = MockAdvisoryLock::new("coordination_lock");
    let held_flag = lock.is_held.clone();
    
    {
        let _guard = ResourceGuard::new(lock, |lock| async move {
            lock.release().await;
        });
        
        assert!(held_flag.load(Ordering::SeqCst));
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    assert!(!held_flag.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_resource_guard_clone_move() {
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let cleanup_called_clone = cleanup_called.clone();
    
    let guard = ResourceGuard::new("moveable_resource", move |_| {
        let called = cleanup_called_clone.clone();
        async move {
            called.store(true, Ordering::SeqCst);
        }
    });
    
    // Move the guard
    let moved_guard = guard;
    
    // Drop the moved guard
    drop(moved_guard);
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    assert!(cleanup_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_async_cleanup_with_delay() {
    let cleanup_start = Arc::new(AtomicBool::new(false));
    let cleanup_complete = Arc::new(AtomicBool::new(false));
    
    let start_flag = cleanup_start.clone();
    let complete_flag = cleanup_complete.clone();
    
    {
        let _guard = ResourceGuard::new("delayed_cleanup", move |_| {
            let start = start_flag.clone();
            let complete = complete_flag.clone();
            async move {
                start.store(true, Ordering::SeqCst);
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                complete.store(true, Ordering::SeqCst);
            }
        });
    }
    
    // Give some time for cleanup to start
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    assert!(cleanup_start.load(Ordering::SeqCst));
    assert!(!cleanup_complete.load(Ordering::SeqCst));
    
    // Wait for cleanup to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;
    assert!(cleanup_complete.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_zero_sized_type() {
    let cleanup_called = Arc::new(AtomicBool::new(false));
    let cleanup_called_clone = cleanup_called.clone();
    
    {
        let _guard = ResourceGuard::new((), move |_| {
            let called = cleanup_called_clone.clone();
            async move {
                called.store(true, Ordering::SeqCst);
            }
        });
    }
    
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    assert!(cleanup_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_resource_guard_deref() {
    // Test that ResourceGuard properly derefs to the inner resource
    let data = vec![1, 2, 3, 4, 5];
    let guard = ResourceGuard::new(data, |_| async {});
    
    // Should be able to use Vec methods through deref
    assert_eq!(guard.len(), 5);
    assert_eq!(guard.first(), Some(&1));
    assert_eq!(guard.last(), Some(&5));
    
    // Should work with string
    let text = "Hello, World!".to_string();
    let guard = ResourceGuard::new(text, |_| async {});
    assert_eq!(guard.len(), 13);
    assert!(guard.contains("World"));
}