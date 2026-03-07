use crate::{DbusWatcher, UdevWatcher, UnifiedJournalWatcher, WatcherMaterialContext};
use async_trait::async_trait;
use sinex_db::models::Event;
use sinex_node_sdk::{NatsPublisher, NodeResult};
use sinex_primitives::JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc;

#[async_trait]
pub trait SystemWatcher: Send + Sync {
    async fn start_streaming(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()>;
}

struct RealDbusWatcher(DbusWatcher);

#[async_trait]
impl SystemWatcher for RealDbusWatcher {
    async fn start_streaming(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        self.0.start_streaming(tx, material).await
    }
}

pub struct RealUdevWatcher(UdevWatcher);

#[async_trait]
impl SystemWatcher for RealUdevWatcher {
    async fn start_streaming(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        self.0.start_streaming(tx, material).await
    }
}

pub struct RealJournalWatcher(UnifiedJournalWatcher);

#[async_trait]
pub trait JournalWatcherTrait: Send + Sync {
    async fn start_streaming_with_systemd(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        systemd_tx: Option<mpsc::Sender<Event<JsonValue>>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()>;
}

#[async_trait]
impl JournalWatcherTrait for RealJournalWatcher {
    async fn start_streaming_with_systemd(
        &mut self,
        tx: mpsc::Sender<Event<JsonValue>>,
        systemd_tx: Option<mpsc::Sender<Event<JsonValue>>>,
        material: WatcherMaterialContext,
    ) -> NodeResult<()> {
        self.0.start_streaming(tx, systemd_tx, material).await
    }
}

#[async_trait]
pub trait WatcherFactory: Send + Sync {
    async fn create_dbus_watcher(
        &self,
        config: crate::payloads::DbusConfig,
    ) -> NodeResult<Box<dyn SystemWatcher>>;
    async fn create_journal_watcher(
        &self,
        config: crate::payloads::JournalConfig,
        systemd_enabled: bool,
        dlq_publisher: Option<Arc<NatsPublisher>>,
    ) -> NodeResult<Box<dyn JournalWatcherTrait>>;
    async fn create_udev_watcher(
        &self,
        polling_fallback: bool,
    ) -> NodeResult<Box<dyn SystemWatcher>>;
}

pub struct RealWatcherFactory;

#[async_trait]
impl WatcherFactory for RealWatcherFactory {
    async fn create_dbus_watcher(
        &self,
        config: crate::payloads::DbusConfig,
    ) -> NodeResult<Box<dyn SystemWatcher>> {
        let w = DbusWatcher::new(config).await?;
        Ok(Box::new(RealDbusWatcher(w)))
    }

    async fn create_journal_watcher(
        &self,
        config: crate::payloads::JournalConfig,
        systemd_enabled: bool,
        dlq_publisher: Option<Arc<NatsPublisher>>,
    ) -> NodeResult<Box<dyn JournalWatcherTrait>> {
        let w = UnifiedJournalWatcher::new(config, systemd_enabled, dlq_publisher).await?;
        Ok(Box::new(RealJournalWatcher(w)))
    }

    async fn create_udev_watcher(
        &self,
        polling_fallback: bool,
    ) -> NodeResult<Box<dyn SystemWatcher>> {
        let w = UdevWatcher::new(polling_fallback).await?;
        Ok(Box::new(RealUdevWatcher(w)))
    }
}
