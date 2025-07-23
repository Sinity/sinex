//! Generic RAII pattern for automatic resource cleanup
//!
//! This module provides a unified RAII wrapper that can be used for any resource
//! requiring cleanup, including advisory locks, test fixtures, file handles, etc.

use std::sync::Arc;
use tokio::sync::Mutex;

/// Generic RAII wrapper for any resource that needs cleanup
pub struct ResourceGuard<T> 
where 
    T: Send + 'static,
{
    resource: Arc<Mutex<Option<T>>>,
    cleanup_sender: Option<tokio::sync::oneshot::Sender<T>>,
}

impl<T> ResourceGuard<T>
where
    T: Send + 'static,
{
    /// Create a new resource guard with async cleanup function
    pub fn new<F, Fut>(resource: T, cleanup: F) -> Self 
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let (cleanup_sender, cleanup_receiver) = tokio::sync::oneshot::channel();
        let resource_arc = Arc::new(Mutex::new(Some(resource)));
        
        // Spawn cleanup task
        tokio::spawn(async move {
            if let Ok(resource) = cleanup_receiver.await {
                cleanup(resource).await;
            }
        });
        
        Self {
            resource: resource_arc,
            cleanup_sender: Some(cleanup_sender),
        }
    }
    
    /// Create a new resource guard with sync cleanup function
    pub fn new_sync<F>(resource: T, cleanup: F) -> Self 
    where
        F: FnOnce(T) + Send + 'static,
    {
        Self::new(resource, move |r| async move { cleanup(r) })
    }
    
    /// Get reference to the resource
    pub async fn resource(&self) -> tokio::sync::MutexGuard<'_, Option<T>> {
        self.resource.lock().await
    }
    
    /// Take the resource, consuming the guard and running cleanup
    pub async fn take(mut self) -> Option<T> {
        let resource = self.resource.lock().await.take();
        if let (Some(resource), Some(sender)) = (resource, self.cleanup_sender.take()) {
            let _ = sender.send(resource);
        }
        None // Resource moved to cleanup
    }
    
    /// Release resource early without cleanup (for error cases)
    pub async fn release_without_cleanup(self) -> Option<T> {
        let resource = self.resource.lock().await.take();
        // Drop cleanup_sender without sending
        resource
    }
}

impl<T> Drop for ResourceGuard<T>
where
    T: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(sender) = self.cleanup_sender.take() {
            let resource_arc = self.resource.clone();
            tokio::spawn(async move {
                if let Some(resource) = resource_arc.lock().await.take() {
                    let _ = sender.send(resource);
                }
            });
        }
    }
}

/// Simple RAII guard for non-async cleanup
pub struct SimpleGuard<T, F> 
where 
    F: FnOnce(T),
    T: Send,
{
    resource: Option<T>,
    cleanup: Option<F>,
}

impl<T, F> SimpleGuard<T, F>
where
    F: FnOnce(T),
    T: Send,
{
    pub fn new(resource: T, cleanup: F) -> Self {
        Self {
            resource: Some(resource),
            cleanup: Some(cleanup),
        }
    }
    
    pub fn resource(&self) -> &T {
        self.resource.as_ref().expect("Resource already taken")
    }
    
    pub fn resource_mut(&mut self) -> &mut T {
        self.resource.as_mut().expect("Resource already taken")
    }
    
    /// Take resource and run cleanup immediately
    pub fn take(mut self) -> T {
        let resource = self.resource.take().expect("Resource already taken");
        if let Some(cleanup) = self.cleanup.take() {
            cleanup(resource);
        }
        panic!("Resource consumed by cleanup")
    }
    
    /// Release resource without cleanup
    pub fn release(mut self) -> T {
        self.resource.take().expect("Resource already taken")
    }
}

impl<T, F> Drop for SimpleGuard<T, F>
where
    F: FnOnce(T),
    T: Send,
{
    fn drop(&mut self) {
        if let (Some(resource), Some(cleanup)) = (self.resource.take(), self.cleanup.take()) {
            cleanup(resource);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_resource_guard_cleanup_on_drop() {
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_up_clone = cleaned_up.clone();
        
        {
            let _guard = ResourceGuard::new("test_resource", move |_resource| async move {
                cleaned_up_clone.store(true, Ordering::Relaxed);
            });
            
            // Guard should not be cleaned up yet
            sleep(Duration::from_millis(10)).await;
            assert!(!cleaned_up.load(Ordering::Relaxed));
        }
        
        // Give cleanup task time to run
        sleep(Duration::from_millis(50)).await;
        assert!(cleaned_up.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_simple_guard_cleanup() {
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let cleaned_up_clone = cleaned_up.clone();
        
        {
            let _guard = SimpleGuard::new("test_resource", move |_resource| {
                cleaned_up_clone.store(true, Ordering::Relaxed);
            });
        }
        
        assert!(cleaned_up.load(Ordering::Relaxed));
    }
}