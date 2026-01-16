//! Channel test support utilities for backpressure and monitoring.
//!
//! This module provides a compact but expressive API for exercising tokio mpsc
//! semantics in test harnesses. It favors explicit, structured helpers over
//! ad-hoc channel manipulation to keep backpressure and timeout behavior
//! consistent across suites.

use crate::Result;
use async_trait::async_trait;
use sinex_core::types::error::SinexError;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct ChannelStats {
    pub sent: u64,
    pub received: u64,
    pub errors: u64,
    pub backpressure: u64,
    pub timeouts: u64,
    pub dropped: u64,
    pub closed: u64,
    pub queue_depth: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Default)]
pub struct ChannelMonitor {
    sent: AtomicU64,
    received: AtomicU64,
    errors: AtomicU64,
    backpressure: AtomicU64,
    timeouts: AtomicU64,
    dropped: AtomicU64,
    closed: AtomicU64,
    last_error: RwLock<Option<String>>,
}

impl ChannelMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_send(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_receive(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self, error: impl Into<String>) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut last_error) = self.last_error.write() {
            *last_error = Some(error.into());
        }
    }

    pub fn record_backpressure(&self) {
        self.backpressure.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_drop(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_closed(&self) {
        self.closed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn queue_depth(&self) -> i64 {
        let sent = self.sent.load(Ordering::Relaxed) as i64;
        let received = self.received.load(Ordering::Relaxed) as i64;
        sent - received
    }

    pub fn stats(&self) -> ChannelStats {
        ChannelStats {
            sent: self.sent.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            backpressure: self.backpressure.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
            queue_depth: self.queue_depth(),
            last_error: self.last_error.read().ok().and_then(|e| e.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MonitoredSender<T> {
    inner: mpsc::Sender<T>,
    monitor: Arc<ChannelMonitor>,
}

impl<T> MonitoredSender<T> {
    pub fn new(inner: mpsc::Sender<T>, monitor: Arc<ChannelMonitor>) -> Self {
        Self { inner, monitor }
    }

    pub fn inner(&self) -> &mpsc::Sender<T> {
        &self.inner
    }

    pub fn monitor(&self) -> &ChannelMonitor {
        self.monitor.as_ref()
    }

    pub fn monitor_arc(&self) -> Arc<ChannelMonitor> {
        self.monitor.clone()
    }
}

#[derive(Debug)]
pub struct MonitoredReceiver<T> {
    inner: mpsc::Receiver<T>,
    monitor: Arc<ChannelMonitor>,
}

impl<T> MonitoredReceiver<T> {
    pub fn new(inner: mpsc::Receiver<T>, monitor: Arc<ChannelMonitor>) -> Self {
        Self { inner, monitor }
    }

    pub fn inner_mut(&mut self) -> &mut mpsc::Receiver<T> {
        &mut self.inner
    }

    pub fn monitor(&self) -> &ChannelMonitor {
        self.monitor.as_ref()
    }

    pub fn monitor_arc(&self) -> Arc<ChannelMonitor> {
        self.monitor.clone()
    }
}

#[derive(Debug)]
pub struct ChannelHarness<T> {
    pub sender: MonitoredSender<T>,
    pub receiver: MonitoredReceiver<T>,
    pub monitor: Arc<ChannelMonitor>,
}

impl<T> ChannelHarness<T> {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer_size);
        let monitor = Arc::new(ChannelMonitor::new());
        Self {
            sender: MonitoredSender::new(sender, monitor.clone()),
            receiver: MonitoredReceiver::new(receiver, monitor.clone()),
            monitor,
        }
    }

    pub fn small_capacity() -> Self {
        Self::new(1)
    }

    pub fn large_capacity() -> Self {
        Self::new(1024)
    }
}

#[async_trait]
pub trait ChannelSenderExt<T> {
    async fn send_or_log(&self, value: T, context: &str) -> Result<()>;
    async fn send_timeout(&self, value: T, wait: Duration) -> Result<()>;
    fn try_send_or_log(&self, value: T, context: &str) -> Result<()>;
}

#[async_trait]
impl<T: Send + Debug> ChannelSenderExt<T> for mpsc::Sender<T> {
    async fn send_or_log(&self, value: T, context: &str) -> Result<()> {
        self.send(value)
            .await
            .map_err(|err| SinexError::channel_send(format!("{context}: {err}")))
    }

    async fn send_timeout(&self, value: T, wait: Duration) -> Result<()> {
        match timeout(wait, self.send(value)).await {
            Ok(result) => result
                .map_err(|err| SinexError::channel_send(format!("send_timeout failed: {err}"))),
            Err(_) => Err(SinexError::timeout("send_timeout exceeded")),
        }
    }

    fn try_send_or_log(&self, value: T, context: &str) -> Result<()> {
        self.try_send(value)
            .map_err(|err| SinexError::channel_send(format!("{context}: try_send failed: {err}")))
    }
}

#[async_trait]
impl<T: Send + Debug> ChannelSenderExt<T> for MonitoredSender<T> {
    async fn send_or_log(&self, value: T, context: &str) -> Result<()> {
        match self.inner.send(value).await {
            Ok(()) => {
                self.monitor.record_send();
                Ok(())
            }
            Err(err) => {
                let message = format!("{context}: {err}");
                self.monitor.record_error(message.clone());
                self.monitor.record_closed();
                tracing::warn!(context, error = %message, "channel send failed");
                Err(SinexError::channel_send(message))
            }
        }
    }

    async fn send_timeout(&self, value: T, wait: Duration) -> Result<()> {
        match timeout(wait, self.inner.send(value)).await {
            Ok(Ok(())) => {
                self.monitor.record_send();
                Ok(())
            }
            Ok(Err(err)) => {
                let message = format!("send_timeout failed: {err}");
                self.monitor.record_error(message.clone());
                self.monitor.record_closed();
                Err(SinexError::channel_send(message))
            }
            Err(_) => {
                self.monitor.record_timeout();
                Err(SinexError::timeout("send_timeout exceeded"))
            }
        }
    }

    fn try_send_or_log(&self, value: T, context: &str) -> Result<()> {
        match self.inner.try_send(value) {
            Ok(()) => {
                self.monitor.record_send();
                Ok(())
            }
            Err(err) => {
                let message = format!("{context}: try_send failed: {err}");
                if matches!(err, mpsc::error::TrySendError::Full(_)) {
                    self.monitor.record_backpressure();
                } else {
                    self.monitor.record_closed();
                }
                self.monitor.record_error(message.clone());
                Err(SinexError::channel_send(message))
            }
        }
    }
}

#[async_trait]
pub trait ChannelReceiverExt<T> {
    async fn recv_timeout(&mut self, wait: Duration) -> std::result::Result<Option<T>, SinexError>;
    async fn recv_batch(&mut self, max_items: usize, wait: Duration) -> Vec<T>;
    async fn drain_all(&mut self) -> Vec<T>;
}

#[async_trait]
impl<T: Send> ChannelReceiverExt<T> for mpsc::Receiver<T> {
    async fn recv_timeout(&mut self, wait: Duration) -> std::result::Result<Option<T>, SinexError> {
        match timeout(wait, self.recv()).await {
            Ok(value) => Ok(value),
            Err(_) => Err(SinexError::timeout("recv_timeout exceeded")),
        }
    }

    async fn recv_batch(&mut self, max_items: usize, wait: Duration) -> Vec<T> {
        let mut items = Vec::with_capacity(max_items);
        if let Ok(Some(first)) = self.recv_timeout(wait).await {
            items.push(first);
        } else {
            return items;
        }

        while items.len() < max_items {
            match self.try_recv() {
                Ok(value) => items.push(value),
                Err(_) => break,
            }
        }
        items
    }

    async fn drain_all(&mut self) -> Vec<T> {
        let mut items = Vec::new();
        while let Ok(value) = self.try_recv() {
            items.push(value);
        }
        items
    }
}

#[async_trait]
impl<T: Send> ChannelReceiverExt<T> for MonitoredReceiver<T> {
    async fn recv_timeout(&mut self, wait: Duration) -> std::result::Result<Option<T>, SinexError> {
        match timeout(wait, self.inner.recv()).await {
            Ok(Some(value)) => {
                self.monitor.record_receive();
                Ok(Some(value))
            }
            Ok(None) => {
                self.monitor.record_closed();
                Ok(None)
            }
            Err(_) => {
                self.monitor.record_timeout();
                Err(SinexError::timeout("recv_timeout exceeded"))
            }
        }
    }

    async fn recv_batch(&mut self, max_items: usize, wait: Duration) -> Vec<T> {
        let mut items = Vec::with_capacity(max_items);
        match self.recv_timeout(wait).await {
            Ok(Some(first)) => items.push(first),
            _ => return items,
        }

        while items.len() < max_items {
            match self.inner.try_recv() {
                Ok(value) => {
                    self.monitor.record_receive();
                    items.push(value);
                }
                Err(_) => break,
            }
        }

        items
    }

    async fn drain_all(&mut self) -> Vec<T> {
        let mut items = Vec::new();
        while let Ok(value) = self.inner.try_recv() {
            self.monitor.record_receive();
            items.push(value);
        }
        items
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BackpressureStrategy {
    /// Wait for capacity up to the given timeout.
    Block(Duration),
    /// Buffer up to max_buffer items in memory.
    Buffer { max_buffer: usize },
    /// Drop the new item when the channel is full.
    DropNewest,
}

#[derive(Debug, Clone)]
pub enum BackpressureOutcome<T> {
    Sent,
    Buffered { buffered: usize },
    Dropped(T),
}

#[derive(Debug)]
pub struct BackpressureManager<T> {
    strategy: BackpressureStrategy,
    buffer: VecDeque<T>,
}

impl<T> BackpressureManager<T> {
    pub fn new(strategy: BackpressureStrategy) -> Self {
        Self {
            strategy,
            buffer: VecDeque::new(),
        }
    }

    pub fn buffering(max_buffer: usize) -> Self {
        Self::new(BackpressureStrategy::Buffer { max_buffer })
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    pub async fn send(
        &mut self,
        sender: &mpsc::Sender<T>,
        value: T,
        monitor: Option<&ChannelMonitor>,
    ) -> Result<BackpressureOutcome<T>>
    where
        T: Send,
    {
        match sender.try_send(value) {
            Ok(()) => {
                if let Some(monitor) = monitor {
                    monitor.record_send();
                }
                return Ok(BackpressureOutcome::Sent);
            }
            Err(mpsc::error::TrySendError::Full(value)) => {
                if let Some(monitor) = monitor {
                    monitor.record_backpressure();
                }
                match self.strategy {
                    BackpressureStrategy::Block(wait) => {
                        match timeout(wait, sender.send(value)).await {
                            Ok(Ok(())) => {
                                if let Some(monitor) = monitor {
                                    monitor.record_send();
                                }
                                Ok(BackpressureOutcome::Sent)
                            }
                            Ok(Err(err)) => {
                                if let Some(monitor) = monitor {
                                    monitor.record_error(err.to_string());
                                    monitor.record_closed();
                                }
                                Err(SinexError::channel_send(format!(
                                    "blocked send failed: {err}"
                                )))
                            }
                            Err(_) => {
                                if let Some(monitor) = monitor {
                                    monitor.record_timeout();
                                }
                                Err(SinexError::timeout("backpressure block timeout"))
                            }
                        }
                    }
                    BackpressureStrategy::Buffer { max_buffer } => {
                        if self.buffer.len() < max_buffer {
                            self.buffer.push_back(value);
                            Ok(BackpressureOutcome::Buffered {
                                buffered: self.buffer.len(),
                            })
                        } else {
                            if let Some(monitor) = monitor {
                                monitor.record_drop();
                            }
                            Ok(BackpressureOutcome::Dropped(value))
                        }
                    }
                    BackpressureStrategy::DropNewest => {
                        if let Some(monitor) = monitor {
                            monitor.record_drop();
                        }
                        Ok(BackpressureOutcome::Dropped(value))
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(_value)) => {
                if let Some(monitor) = monitor {
                    monitor.record_closed();
                }
                Err(SinexError::channel_send("channel closed before send"))
            }
        }
    }

    pub async fn send_monitored(
        &mut self,
        sender: &MonitoredSender<T>,
        value: T,
    ) -> Result<BackpressureOutcome<T>>
    where
        T: Send,
    {
        self.send(sender.inner(), value, Some(sender.monitor()))
            .await
    }

    pub fn flush_buffer(
        &mut self,
        sender: &mpsc::Sender<T>,
        monitor: Option<&ChannelMonitor>,
    ) -> Result<usize>
    where
        T: Send,
    {
        let mut flushed = 0;
        while let Some(value) = self.buffer.pop_front() {
            match sender.try_send(value) {
                Ok(()) => {
                    flushed += 1;
                    if let Some(monitor) = monitor {
                        monitor.record_send();
                    }
                }
                Err(mpsc::error::TrySendError::Full(value)) => {
                    self.buffer.push_front(value);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_value)) => {
                    if let Some(monitor) = monitor {
                        monitor.record_closed();
                        monitor.record_error("channel closed while flushing buffer");
                    }
                    return Err(SinexError::channel_send(
                        "channel closed while flushing buffer",
                    ));
                }
            }
        }
        Ok(flushed)
    }

    pub fn flush_monitored(&mut self, sender: &MonitoredSender<T>) -> Result<usize>
    where
        T: Send,
    {
        self.flush_buffer(sender.inner(), Some(sender.monitor()))
    }
}

pub mod behavior {
    use super::*;

    pub async fn assert_basic_send_receive<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        value: T,
        context: &str,
    ) -> Result<()>
    where
        T: Send + PartialEq + Debug + Clone,
    {
        let expected = value.clone();
        sender.send_or_log(value, context).await?;
        let received = receiver.recv_timeout(Duration::from_secs(1)).await?;
        let received = received
            .ok_or_else(|| SinexError::channel_receive(format!("channel closed in {context}")))?;
        if received != expected {
            return Err(SinexError::validation(format!(
                "value mismatch in {context}: got {received:?}, expected {expected:?}"
            )));
        }
        Ok(())
    }

    pub async fn assert_timeout<T>(
        receiver: &mut impl ChannelReceiverExt<T>,
        wait: Duration,
    ) -> Result<()>
    where
        T: Send,
    {
        match receiver.recv_timeout(wait).await {
            Ok(Some(_)) => Err(SinexError::validation(
                "expected timeout but received value",
            )),
            Ok(None) => Err(SinexError::validation(
                "expected timeout but channel closed",
            )),
            Err(err) => {
                if matches!(err, SinexError::Timeout(_)) {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    pub async fn assert_batch_receive<T>(
        sender: &impl ChannelSenderExt<T>,
        receiver: &mut impl ChannelReceiverExt<T>,
        items: Vec<T>,
        max_batch: usize,
        wait: Duration,
    ) -> Result<()>
    where
        T: Send + Clone + Debug,
    {
        for item in &items {
            sender.send_or_log(item.clone(), "batch_receive").await?;
        }

        let mut total = 0usize;
        let mut loops = 0usize;
        while total < items.len() {
            let batch = receiver.recv_batch(max_batch, wait).await;
            if batch.is_empty() {
                return Err(SinexError::validation(
                    "batch_receive stalled before all items were drained",
                ));
            }
            total += batch.len();
            loops += 1;
            if loops > 128 {
                return Err(SinexError::validation("batch_receive exceeded loop guard"));
            }
        }
        Ok(())
    }
}
