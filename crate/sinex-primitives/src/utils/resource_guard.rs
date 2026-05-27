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
    cleanup_complete: Option<tokio::sync::oneshot::Receiver<()>>,
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
        let (cleanup_complete_sender, cleanup_complete_receiver) = tokio::sync::oneshot::channel();
        let resource_arc = Arc::new(Mutex::new(Some(resource)));

        // Spawn cleanup task
        tokio::spawn(async move {
            if let Ok(resource) = cleanup_receiver.await {
                cleanup(resource).await;
            }
            let _ = cleanup_complete_sender.send(());
        });

        Self {
            resource: resource_arc,
            cleanup_sender: Some(cleanup_sender),
            cleanup_complete: Some(cleanup_complete_receiver),
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

    /// Take the resource, consuming the guard and skipping cleanup.
    pub async fn take(mut self) -> Option<T> {
        self.cleanup_sender.take();
        self.cleanup_complete.take();
        self.resource.lock().await.take()
    }

    /// Release resource early without cleanup (for error cases).
    pub async fn release_without_cleanup(self) -> Option<T> {
        self.take().await
    }

    /// Trigger cleanup immediately and wait until the cleanup future completes.
    pub async fn cleanup_now(mut self) {
        if let Some(sender) = self.cleanup_sender.take()
            && let Some(resource) = self.resource.lock().await.take()
        {
            let _ = sender.send(resource);
        }

        if let Some(cleanup_complete) = self.cleanup_complete.take() {
            let _ = cleanup_complete.await;
        }
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
