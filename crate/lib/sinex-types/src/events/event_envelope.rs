//! Type-safe event envelope for pattern matching
//!
//! This module provides the `EventEnvelope` enum that wraps all known event payload
//! types, enabling exhaustive pattern matching instead of fallible string-based
//! event type checking and deserialization.
//!
//! The envelope transforms event processing from:
//! ```ignore
//! match event.event_type.as_str() {
//!     "file.created" => {
//!         let payload: FileCreatedPayload = serde_json::from_value(event.payload)?;
//!         // handle payload
//!     }
//!     // ... many more string matches
//! }
//! ```
//!
//! To:
//! ```ignore
//! match event.to_envelope()? {
//!     EventEnvelope::FileCreated(payload) => {
//!         // handle payload directly
//!     }
//!     // ... exhaustive pattern matching
//!     EventEnvelope::Unknown(event) => {
//!         // handle unknown/new event types
//!     }
//! }
//! ```

use crate::error::SinexError;
use serde::{Deserialize, Serialize};

// Import all payload types
use crate::events::payloads::*;
use crate::events::EventPayload;

/// Type-safe envelope for all known event payload types
///
/// This enum provides exhaustive pattern matching over all known event types
/// in the Sinex system. When a new payload type is added, this enum must be
/// updated to include it, ensuring that all event processing code is updated
/// to handle the new type.
///
/// The `Unknown` variant provides forward compatibility for event types that
/// are not yet known to this version of the code.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum EventEnvelope {
    // Blob events
    BlobStored(BlobStoredPayload),
    BlobRetrieved(BlobRetrievedPayload),
    BlobDeleted(BlobDeletedPayload),
    BlobIngested(BlobIngestedPayload),
    BlobVerified(BlobVerifiedPayload),
    StorageStatistics(StorageStatisticsPayload),

    // Clipboard events
    ClipboardCopied(ClipboardCopiedPayload),
    ClipboardSelected(ClipboardSelectedPayload),

    // Desktop events
    DesktopMonitoringStarted(DesktopMonitoringStartedPayload),
    DesktopSnapshot(DesktopSnapshotPayload),
    ClipboardHistorical(ClipboardHistoricalPayload),
    WindowManagerHistorical(WindowManagerHistoricalPayload),

    // Document events
    DocumentIngested(DocumentIngestedPayload),

    // Filesystem events
    FileCreated(FileCreatedPayload),
    FileModified(FileModifiedPayload),
    FileDeleted(FileDeletedPayload),
    FileMoved(FileMovedPayload),
    DirCreated(DirCreatedPayload),
    DirDeleted(DirDeletedPayload),
    FileDiscovered(FileDiscoveredPayload),
    DirDiscovered(DirDiscoveredPayload),

    // Process events
    ProcessStarted(ProcessStartedPayload),
    ProcessHeartbeat(ProcessHeartbeatPayload),
    ProcessShutdown(ProcessShutdownPayload),
    AutomatonError(AutomatonErrorPayload),
    SensorActivated(SensorActivatedPayload),
    SensorDeactivated(SensorDeactivatedPayload),

    // RPC events
    RpcContentResponse(RpcContentResponsePayload),
    RpcPkmResponse(RpcPkmResponsePayload),

    // Shell events
    KittyCommandExecuted(KittyCommandExecutedPayload),
    KittyCommandCompleted(KittyCommandCompletedPayload),
    KittySessionStarted(KittySessionStartedPayload),
    KittySessionEnded(KittySessionEndedPayload),
    AtuinCommandExecuted(AtuinCommandExecutedPayload),
    AtuinCommandCompleted(AtuinCommandCompletedPayload),
    HistoryCommandImported(HistoryCommandImportedPayload),
    AtuinEntry(AtuinEntryPayload),
    CommandImported(CommandImportedPayload),
    BashHistoryEntry(BashHistoryEntryPayload),
    BashHistoricalCommand(BashHistoricalCommandPayload),
    ZshHistoricalCommand(ZshHistoricalCommandPayload),
    FishHistoricalCommand(FishHistoricalCommandPayload),
    TerminalMonitoringStarted(TerminalMonitoringStartedPayload),
    TerminalCommandHistorical(TerminalCommandHistoricalPayload),
    TerminalHistoryHistorical(TerminalHistoryHistoricalPayload),
    TerminalSnapshot(TerminalSnapshotPayload),
    KittyProcessChanged(KittyProcessChangedPayload),
    KittyTabFocused(KittyTabFocusedPayload),
    KittyContentStreamed(KittyContentStreamedPayload),
    CanonicalCommand(CanonicalCommandPayload),
    ShellOutputCaptured(ShellOutputCapturedPayload),
    AsciinemaSessionStarted(AsciinemaSessionStartedPayload),
    AsciinemaSessionEnded(AsciinemaSessionEndedPayload),

    // System events
    ScanStarted(ScanStartedPayload),
    ScanCompleted(ScanCompletedPayload),
    JournalEntry(JournalEntryPayload),
    JournalSyncCompleted(JournalSyncCompletedPayload),
    JournalEntryWritten(JournalEntryWrittenPayload),
    DbusSignal(DbusSignalPayload),
    DbusMethodCalled(DbusMethodCalledPayload),
    DbusNotificationSent(DbusNotificationSentPayload),
    DbusMediaStateChanged(DbusMediaStateChangedPayload),
    DbusPowerStateChanged(DbusPowerStateChangedPayload),
    DbusDeviceConnected(DbusDeviceConnectedPayload),
    DbusBluetoothDeviceChanged(DbusBluetoothDeviceChangedPayload),
    DbusNetworkStateChanged(DbusNetworkStateChangedPayload),
    DbusMountEvent(DbusMountEventPayload),
    SystemdUnitStarted(SystemdUnitStartedPayload),
    SystemdUnitStopped(SystemdUnitStoppedPayload),
    SystemdUnitStatus(SystemdUnitStatusPayload),
    SystemdUnitFailed(SystemdUnitFailedPayload),
    SystemdUnitReloaded(SystemdUnitReloadedPayload),
    SystemdTimerTriggered(SystemdTimerTriggeredPayload),
    SystemdUnitStarting(SystemdUnitStartingPayload),
    SystemdUnitStopping(SystemdUnitStoppingPayload),
    SystemdUnitStateChanged(SystemdUnitStateChangedPayload),
    UdevDeviceAdded(UdevDeviceAddedPayload),
    UdevDeviceRemoved(UdevDeviceRemovedPayload),
    UdevDeviceConnected(UdevDeviceConnectedPayload),
    UdevDeviceDisconnected(UdevDeviceDisconnectedPayload),
    UdevDeviceChanged(UdevDeviceChangedPayload),
    UdevDeviceDriverChanged(UdevDeviceDriverChangedPayload),
    UdevDeviceOther(UdevDeviceOtherPayload),
    LogLine(LogLinePayload),
    SystemHealthSummary(SystemHealthSummaryPayload),
    SatelliteHeartbeat(SatelliteHeartbeatPayload),
    SystemMonitoringStarted(SystemMonitoringStartedPayload),
    SystemSnapshot(SystemSnapshotPayload),
    JournaldHistorical(JournaldHistoricalPayload),
    SystemdUnitsHistorical(SystemdUnitsHistoricalPayload),
    UdevDeviceHistorical(UdevDeviceHistoricalPayload),

    // Telemetry events
    EventsProcessed(EventsProcessedPayload),
    ErrorsSummary(ErrorsSummaryPayload),
    SystemResources(SystemResourcesPayload),
    OperationPerformance(OperationPerformancePayload),
    ComponentResourceUsage(ComponentResourceUsagePayload),

    // Window events
    HyprlandWindowOpened(HyprlandWindowOpenedPayload),
    HyprlandWindowClosed(HyprlandWindowClosedPayload),
    HyprlandWindowFocused(HyprlandWindowFocusedPayload),
    HyprlandWorkspaceSwitched(HyprlandWorkspaceSwitchedPayload),
    HyprlandWindowMoved(HyprlandWindowMovedPayload),
    HyprlandMonitorFocused(HyprlandMonitorFocusedPayload),
    HyprlandStateCaptured(HyprlandStateCapturedPayload),

    /// Unknown or unsupported event type
    ///
    /// This variant is used when:
    /// - The event type is not recognized by this version of the code
    /// - Deserialization of the payload fails for a known type
    /// - Forward compatibility is needed for new event types
    Unknown(Box<UnknownEvent>),
}

/// Container for unknown event types
///
/// This preserves the original event data when we cannot parse it into
/// a known envelope variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnknownEvent {
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub reason: String,
}

impl EventEnvelope {
    /// Get the event source for any envelope variant
    pub fn source(&self) -> String {
        match self {
            // Blob events
            EventEnvelope::BlobStored(_) => BlobStoredPayload::SOURCE.to_string(),
            EventEnvelope::BlobRetrieved(_) => BlobRetrievedPayload::SOURCE.to_string(),
            EventEnvelope::BlobDeleted(_) => BlobDeletedPayload::SOURCE.to_string(),
            EventEnvelope::BlobIngested(_) => BlobIngestedPayload::SOURCE.to_string(),
            EventEnvelope::BlobVerified(_) => BlobVerifiedPayload::SOURCE.to_string(),
            EventEnvelope::StorageStatistics(_) => StorageStatisticsPayload::SOURCE.to_string(),

            // Clipboard events
            EventEnvelope::ClipboardCopied(_) => ClipboardCopiedPayload::SOURCE.to_string(),
            EventEnvelope::ClipboardSelected(_) => ClipboardSelectedPayload::SOURCE.to_string(),

            // Desktop events
            EventEnvelope::DesktopMonitoringStarted(_) => DesktopMonitoringStartedPayload::SOURCE.to_string(),
            EventEnvelope::DesktopSnapshot(_) => DesktopSnapshotPayload::SOURCE.to_string(),
            EventEnvelope::ClipboardHistorical(_) => ClipboardHistoricalPayload::SOURCE.to_string(),
            EventEnvelope::WindowManagerHistorical(_) => WindowManagerHistoricalPayload::SOURCE.to_string(),

            // Document events
            EventEnvelope::DocumentIngested(_) => DocumentIngestedPayload::SOURCE.to_string(),

            // Filesystem events
            EventEnvelope::FileCreated(_) => FileCreatedPayload::SOURCE.to_string(),
            EventEnvelope::FileModified(_) => FileModifiedPayload::SOURCE.to_string(),
            EventEnvelope::FileDeleted(_) => FileDeletedPayload::SOURCE.to_string(),
            EventEnvelope::FileMoved(_) => FileMovedPayload::SOURCE.to_string(),
            EventEnvelope::DirCreated(_) => DirCreatedPayload::SOURCE.to_string(),
            EventEnvelope::DirDeleted(_) => DirDeletedPayload::SOURCE.to_string(),
            EventEnvelope::FileDiscovered(_) => FileDiscoveredPayload::SOURCE.to_string(),
            EventEnvelope::DirDiscovered(_) => DirDiscoveredPayload::SOURCE.to_string(),

            // Process events
            EventEnvelope::ProcessStarted(_) => ProcessStartedPayload::SOURCE.to_string(),
            EventEnvelope::ProcessHeartbeat(_) => ProcessHeartbeatPayload::SOURCE.to_string(),
            EventEnvelope::ProcessShutdown(_) => ProcessShutdownPayload::SOURCE.to_string(),
            EventEnvelope::AutomatonError(_) => AutomatonErrorPayload::SOURCE.to_string(),
            EventEnvelope::SensorActivated(_) => SensorActivatedPayload::SOURCE.to_string(),
            EventEnvelope::SensorDeactivated(_) => SensorDeactivatedPayload::SOURCE.to_string(),

            // RPC events
            EventEnvelope::RpcContentResponse(_) => RpcContentResponsePayload::SOURCE.to_string(),
            EventEnvelope::RpcPkmResponse(_) => RpcPkmResponsePayload::SOURCE.to_string(),

            // Shell events
            EventEnvelope::KittyCommandExecuted(_) => KittyCommandExecutedPayload::SOURCE.to_string(),
            EventEnvelope::KittyCommandCompleted(_) => KittyCommandCompletedPayload::SOURCE.to_string(),
            EventEnvelope::KittySessionStarted(_) => KittySessionStartedPayload::SOURCE.to_string(),
            EventEnvelope::KittySessionEnded(_) => KittySessionEndedPayload::SOURCE.to_string(),
            EventEnvelope::AtuinCommandExecuted(_) => AtuinCommandExecutedPayload::SOURCE.to_string(),
            EventEnvelope::AtuinCommandCompleted(_) => AtuinCommandCompletedPayload::SOURCE.to_string(),
            EventEnvelope::HistoryCommandImported(_) => HistoryCommandImportedPayload::SOURCE.to_string(),
            EventEnvelope::AtuinEntry(_) => AtuinEntryPayload::SOURCE.to_string(),
            EventEnvelope::CommandImported(_) => CommandImportedPayload::SOURCE.to_string(),
            EventEnvelope::BashHistoryEntry(_) => BashHistoryEntryPayload::SOURCE.to_string(),
            EventEnvelope::BashHistoricalCommand(_) => BashHistoricalCommandPayload::SOURCE.to_string(),
            EventEnvelope::ZshHistoricalCommand(_) => ZshHistoricalCommandPayload::SOURCE.to_string(),
            EventEnvelope::FishHistoricalCommand(_) => FishHistoricalCommandPayload::SOURCE.to_string(),
            EventEnvelope::TerminalMonitoringStarted(_) => TerminalMonitoringStartedPayload::SOURCE.to_string(),
            EventEnvelope::TerminalCommandHistorical(_) => TerminalCommandHistoricalPayload::SOURCE.to_string(),
            EventEnvelope::TerminalHistoryHistorical(_) => TerminalHistoryHistoricalPayload::SOURCE.to_string(),
            EventEnvelope::TerminalSnapshot(_) => TerminalSnapshotPayload::SOURCE.to_string(),
            EventEnvelope::KittyProcessChanged(_) => KittyProcessChangedPayload::SOURCE.to_string(),
            EventEnvelope::KittyTabFocused(_) => KittyTabFocusedPayload::SOURCE.to_string(),
            EventEnvelope::KittyContentStreamed(_) => KittyContentStreamedPayload::SOURCE.to_string(),
            EventEnvelope::CanonicalCommand(_) => CanonicalCommandPayload::SOURCE.to_string(),
            EventEnvelope::ShellOutputCaptured(_) => ShellOutputCapturedPayload::SOURCE.to_string(),
            EventEnvelope::AsciinemaSessionStarted(_) => AsciinemaSessionStartedPayload::SOURCE.to_string(),
            EventEnvelope::AsciinemaSessionEnded(_) => AsciinemaSessionEndedPayload::SOURCE.to_string(),

            // System events
            EventEnvelope::ScanStarted(_) => ScanStartedPayload::SOURCE.to_string(),
            EventEnvelope::ScanCompleted(_) => ScanCompletedPayload::SOURCE.to_string(),
            EventEnvelope::JournalEntry(_) => JournalEntryPayload::SOURCE.to_string(),
            EventEnvelope::JournalSyncCompleted(_) => JournalSyncCompletedPayload::SOURCE.to_string(),
            EventEnvelope::JournalEntryWritten(_) => JournalEntryWrittenPayload::SOURCE.to_string(),
            EventEnvelope::DbusSignal(_) => DbusSignalPayload::SOURCE.to_string(),
            EventEnvelope::DbusMethodCalled(_) => DbusMethodCalledPayload::SOURCE.to_string(),
            EventEnvelope::DbusNotificationSent(_) => DbusNotificationSentPayload::SOURCE.to_string(),
            EventEnvelope::DbusMediaStateChanged(_) => DbusMediaStateChangedPayload::SOURCE.to_string(),
            EventEnvelope::DbusPowerStateChanged(_) => DbusPowerStateChangedPayload::SOURCE.to_string(),
            EventEnvelope::DbusDeviceConnected(_) => DbusDeviceConnectedPayload::SOURCE.to_string(),
            EventEnvelope::DbusBluetoothDeviceChanged(_) => DbusBluetoothDeviceChangedPayload::SOURCE.to_string(),
            EventEnvelope::DbusNetworkStateChanged(_) => DbusNetworkStateChangedPayload::SOURCE.to_string(),
            EventEnvelope::DbusMountEvent(_) => DbusMountEventPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitStarted(_) => SystemdUnitStartedPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitStopped(_) => SystemdUnitStoppedPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitStatus(_) => SystemdUnitStatusPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitFailed(_) => SystemdUnitFailedPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitReloaded(_) => SystemdUnitReloadedPayload::SOURCE.to_string(),
            EventEnvelope::SystemdTimerTriggered(_) => SystemdTimerTriggeredPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitStarting(_) => SystemdUnitStartingPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitStopping(_) => SystemdUnitStoppingPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitStateChanged(_) => SystemdUnitStateChangedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceAdded(_) => UdevDeviceAddedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceRemoved(_) => UdevDeviceRemovedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceConnected(_) => UdevDeviceConnectedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceDisconnected(_) => UdevDeviceDisconnectedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceChanged(_) => UdevDeviceChangedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceDriverChanged(_) => UdevDeviceDriverChangedPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceOther(_) => UdevDeviceOtherPayload::SOURCE.to_string(),
            EventEnvelope::LogLine(_) => LogLinePayload::SOURCE.to_string(),
            EventEnvelope::SystemHealthSummary(_) => SystemHealthSummaryPayload::SOURCE.to_string(),
            EventEnvelope::SatelliteHeartbeat(_) => SatelliteHeartbeatPayload::SOURCE.to_string(),
            EventEnvelope::SystemMonitoringStarted(_) => SystemMonitoringStartedPayload::SOURCE.to_string(),
            EventEnvelope::SystemSnapshot(_) => SystemSnapshotPayload::SOURCE.to_string(),
            EventEnvelope::JournaldHistorical(_) => JournaldHistoricalPayload::SOURCE.to_string(),
            EventEnvelope::SystemdUnitsHistorical(_) => SystemdUnitsHistoricalPayload::SOURCE.to_string(),
            EventEnvelope::UdevDeviceHistorical(_) => UdevDeviceHistoricalPayload::SOURCE.to_string(),

            // Telemetry events
            EventEnvelope::EventsProcessed(_) => EventsProcessedPayload::SOURCE.to_string(),
            EventEnvelope::ErrorsSummary(_) => ErrorsSummaryPayload::SOURCE.to_string(),
            EventEnvelope::SystemResources(_) => SystemResourcesPayload::SOURCE.to_string(),
            EventEnvelope::OperationPerformance(_) => OperationPerformancePayload::SOURCE.to_string(),
            EventEnvelope::ComponentResourceUsage(_) => ComponentResourceUsagePayload::SOURCE.to_string(),

            // Window events
            EventEnvelope::HyprlandWindowOpened(_) => HyprlandWindowOpenedPayload::SOURCE.to_string(),
            EventEnvelope::HyprlandWindowClosed(_) => HyprlandWindowClosedPayload::SOURCE.to_string(),
            EventEnvelope::HyprlandWindowFocused(_) => HyprlandWindowFocusedPayload::SOURCE.to_string(),
            EventEnvelope::HyprlandWorkspaceSwitched(_) => HyprlandWorkspaceSwitchedPayload::SOURCE.to_string(),
            EventEnvelope::HyprlandWindowMoved(_) => HyprlandWindowMovedPayload::SOURCE.to_string(),
            EventEnvelope::HyprlandMonitorFocused(_) => HyprlandMonitorFocusedPayload::SOURCE.to_string(),
            EventEnvelope::HyprlandStateCaptured(_) => HyprlandStateCapturedPayload::SOURCE.to_string(),

            EventEnvelope::Unknown(unknown) => unknown.source.clone(),
        }
    }

    /// Get the event type for any envelope variant
    pub fn event_type(&self) -> String {
        match self {
            // Blob events
            EventEnvelope::BlobStored(_) => BlobStoredPayload::EVENT_TYPE.to_string(),
            EventEnvelope::BlobRetrieved(_) => BlobRetrievedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::BlobDeleted(_) => BlobDeletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::BlobIngested(_) => BlobIngestedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::BlobVerified(_) => BlobVerifiedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::StorageStatistics(_) => StorageStatisticsPayload::EVENT_TYPE.to_string(),

            // Clipboard events
            EventEnvelope::ClipboardCopied(_) => ClipboardCopiedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ClipboardSelected(_) => ClipboardSelectedPayload::EVENT_TYPE.to_string(),

            // Desktop events
            EventEnvelope::DesktopMonitoringStarted(_) => DesktopMonitoringStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DesktopSnapshot(_) => DesktopSnapshotPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ClipboardHistorical(_) => ClipboardHistoricalPayload::EVENT_TYPE.to_string(),
            EventEnvelope::WindowManagerHistorical(_) => WindowManagerHistoricalPayload::EVENT_TYPE.to_string(),

            // Document events
            EventEnvelope::DocumentIngested(_) => DocumentIngestedPayload::EVENT_TYPE.to_string(),

            // Filesystem events
            EventEnvelope::FileCreated(_) => FileCreatedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::FileModified(_) => FileModifiedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::FileDeleted(_) => FileDeletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::FileMoved(_) => FileMovedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DirCreated(_) => DirCreatedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DirDeleted(_) => DirDeletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::FileDiscovered(_) => FileDiscoveredPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DirDiscovered(_) => DirDiscoveredPayload::EVENT_TYPE.to_string(),

            // Process events
            EventEnvelope::ProcessStarted(_) => ProcessStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ProcessHeartbeat(_) => ProcessHeartbeatPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ProcessShutdown(_) => ProcessShutdownPayload::EVENT_TYPE.to_string(),
            EventEnvelope::AutomatonError(_) => AutomatonErrorPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SensorActivated(_) => SensorActivatedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SensorDeactivated(_) => SensorDeactivatedPayload::EVENT_TYPE.to_string(),

            // RPC events
            EventEnvelope::RpcContentResponse(_) => RpcContentResponsePayload::EVENT_TYPE.to_string(),
            EventEnvelope::RpcPkmResponse(_) => RpcPkmResponsePayload::EVENT_TYPE.to_string(),

            // Shell events
            EventEnvelope::KittyCommandExecuted(_) => KittyCommandExecutedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::KittyCommandCompleted(_) => KittyCommandCompletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::KittySessionStarted(_) => KittySessionStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::KittySessionEnded(_) => KittySessionEndedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::AtuinCommandExecuted(_) => AtuinCommandExecutedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::AtuinCommandCompleted(_) => AtuinCommandCompletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HistoryCommandImported(_) => HistoryCommandImportedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::AtuinEntry(_) => AtuinEntryPayload::EVENT_TYPE.to_string(),
            EventEnvelope::CommandImported(_) => CommandImportedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::BashHistoryEntry(_) => BashHistoryEntryPayload::EVENT_TYPE.to_string(),
            EventEnvelope::BashHistoricalCommand(_) => BashHistoricalCommandPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ZshHistoricalCommand(_) => ZshHistoricalCommandPayload::EVENT_TYPE.to_string(),
            EventEnvelope::FishHistoricalCommand(_) => FishHistoricalCommandPayload::EVENT_TYPE.to_string(),
            EventEnvelope::TerminalMonitoringStarted(_) => TerminalMonitoringStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::TerminalCommandHistorical(_) => TerminalCommandHistoricalPayload::EVENT_TYPE.to_string(),
            EventEnvelope::TerminalHistoryHistorical(_) => TerminalHistoryHistoricalPayload::EVENT_TYPE.to_string(),
            EventEnvelope::TerminalSnapshot(_) => TerminalSnapshotPayload::EVENT_TYPE.to_string(),
            EventEnvelope::KittyProcessChanged(_) => KittyProcessChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::KittyTabFocused(_) => KittyTabFocusedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::KittyContentStreamed(_) => KittyContentStreamedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::CanonicalCommand(_) => CanonicalCommandPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ShellOutputCaptured(_) => ShellOutputCapturedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::AsciinemaSessionStarted(_) => AsciinemaSessionStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::AsciinemaSessionEnded(_) => AsciinemaSessionEndedPayload::EVENT_TYPE.to_string(),

            // System events
            EventEnvelope::ScanStarted(_) => ScanStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ScanCompleted(_) => ScanCompletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::JournalEntry(_) => JournalEntryPayload::EVENT_TYPE.to_string(),
            EventEnvelope::JournalSyncCompleted(_) => JournalSyncCompletedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::JournalEntryWritten(_) => JournalEntryWrittenPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusSignal(_) => DbusSignalPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusMethodCalled(_) => DbusMethodCalledPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusNotificationSent(_) => DbusNotificationSentPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusMediaStateChanged(_) => DbusMediaStateChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusPowerStateChanged(_) => DbusPowerStateChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusDeviceConnected(_) => DbusDeviceConnectedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusBluetoothDeviceChanged(_) => DbusBluetoothDeviceChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusNetworkStateChanged(_) => DbusNetworkStateChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::DbusMountEvent(_) => DbusMountEventPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitStarted(_) => SystemdUnitStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitStopped(_) => SystemdUnitStoppedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitStatus(_) => SystemdUnitStatusPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitFailed(_) => SystemdUnitFailedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitReloaded(_) => SystemdUnitReloadedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdTimerTriggered(_) => SystemdTimerTriggeredPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitStarting(_) => SystemdUnitStartingPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitStopping(_) => SystemdUnitStoppingPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitStateChanged(_) => SystemdUnitStateChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceAdded(_) => UdevDeviceAddedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceRemoved(_) => UdevDeviceRemovedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceConnected(_) => UdevDeviceConnectedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceDisconnected(_) => UdevDeviceDisconnectedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceChanged(_) => UdevDeviceChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceDriverChanged(_) => UdevDeviceDriverChangedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceOther(_) => UdevDeviceOtherPayload::EVENT_TYPE.to_string(),
            EventEnvelope::LogLine(_) => LogLinePayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemHealthSummary(_) => SystemHealthSummaryPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SatelliteHeartbeat(_) => SatelliteHeartbeatPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemMonitoringStarted(_) => SystemMonitoringStartedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemSnapshot(_) => SystemSnapshotPayload::EVENT_TYPE.to_string(),
            EventEnvelope::JournaldHistorical(_) => JournaldHistoricalPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemdUnitsHistorical(_) => SystemdUnitsHistoricalPayload::EVENT_TYPE.to_string(),
            EventEnvelope::UdevDeviceHistorical(_) => UdevDeviceHistoricalPayload::EVENT_TYPE.to_string(),

            // Telemetry events
            EventEnvelope::EventsProcessed(_) => EventsProcessedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::ErrorsSummary(_) => ErrorsSummaryPayload::EVENT_TYPE.to_string(),
            EventEnvelope::SystemResources(_) => SystemResourcesPayload::EVENT_TYPE.to_string(),
            EventEnvelope::OperationPerformance(_) => OperationPerformancePayload::EVENT_TYPE.to_string(),
            EventEnvelope::ComponentResourceUsage(_) => ComponentResourceUsagePayload::EVENT_TYPE.to_string(),

            // Window events
            EventEnvelope::HyprlandWindowOpened(_) => HyprlandWindowOpenedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HyprlandWindowClosed(_) => HyprlandWindowClosedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HyprlandWindowFocused(_) => HyprlandWindowFocusedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HyprlandWorkspaceSwitched(_) => HyprlandWorkspaceSwitchedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HyprlandWindowMoved(_) => HyprlandWindowMovedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HyprlandMonitorFocused(_) => HyprlandMonitorFocusedPayload::EVENT_TYPE.to_string(),
            EventEnvelope::HyprlandStateCaptured(_) => HyprlandStateCapturedPayload::EVENT_TYPE.to_string(),

            EventEnvelope::Unknown(unknown) => unknown.event_type.clone(),
        }
    }

    /// Try to parse an event from source, event_type, and payload JSON
    ///
    /// This method attempts to deserialize the payload JSON into the appropriate
    /// strongly-typed payload based on the source and event_type combination.
    /// If deserialization fails or the event type is unknown, returns the
    /// Unknown variant.
    pub fn from_parts(
        source: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Self {
        // Macro to attempt deserialization and return the appropriate variant
        macro_rules! try_deserialize {
            ($payload_type:ty, $variant:ident) => {
                match serde_json::from_value::<$payload_type>(payload.clone()) {
                    Ok(payload) => return EventEnvelope::$variant(payload),
                    Err(_) => {} // Continue to next option or Unknown
                }
            };
        }

        // Match on (source, event_type) combinations to determine payload type
        match (source, event_type) {
            // Blob events
            ("sinex-blob", "blob.stored") => try_deserialize!(BlobStoredPayload, BlobStored),
            ("sinex-blob", "blob.retrieved") => try_deserialize!(BlobRetrievedPayload, BlobRetrieved),
            ("sinex-blob", "blob.deleted") => try_deserialize!(BlobDeletedPayload, BlobDeleted),
            ("sinex-blob", "blob.ingested") => try_deserialize!(BlobIngestedPayload, BlobIngested),
            ("sinex-blob", "blob.verified") => try_deserialize!(BlobVerifiedPayload, BlobVerified),
            ("sinex-blob", "storage.statistics") => try_deserialize!(StorageStatisticsPayload, StorageStatistics),

            // Clipboard events  
            ("clipboard", "clipboard.copied") => try_deserialize!(ClipboardCopiedPayload, ClipboardCopied),
            ("clipboard", "clipboard.selected") => try_deserialize!(ClipboardSelectedPayload, ClipboardSelected),

            // Desktop events
            ("desktop", "desktop.monitoring_started") => try_deserialize!(DesktopMonitoringStartedPayload, DesktopMonitoringStarted),
            ("desktop", "desktop.snapshot") => try_deserialize!(DesktopSnapshotPayload, DesktopSnapshot),
            ("desktop", "clipboard.historical") => try_deserialize!(ClipboardHistoricalPayload, ClipboardHistorical),
            ("desktop", "window_manager.historical") => try_deserialize!(WindowManagerHistoricalPayload, WindowManagerHistorical),

            // Document events
            ("document-ingestor", "document.ingested") => try_deserialize!(DocumentIngestedPayload, DocumentIngested),

            // Filesystem events
            ("fs-watcher", "file.created") => try_deserialize!(FileCreatedPayload, FileCreated),
            ("fs-watcher", "file.modified") => try_deserialize!(FileModifiedPayload, FileModified),
            ("fs-watcher", "file.deleted") => try_deserialize!(FileDeletedPayload, FileDeleted),
            ("fs-watcher", "file.moved") => try_deserialize!(FileMovedPayload, FileMoved),
            ("fs-watcher", "dir.created") => try_deserialize!(DirCreatedPayload, DirCreated),
            ("fs-watcher", "dir.deleted") => try_deserialize!(DirDeletedPayload, DirDeleted),
            ("fs-watcher", "file.discovered") => try_deserialize!(FileDiscoveredPayload, FileDiscovered),
            ("fs-watcher", "dir.discovered") => try_deserialize!(DirDiscoveredPayload, DirDiscovered),

            // Process events
            ("sinex", "process.started") => try_deserialize!(ProcessStartedPayload, ProcessStarted),
            ("sinex", "process.heartbeat") => try_deserialize!(ProcessHeartbeatPayload, ProcessHeartbeat),
            ("sinex", "process.shutdown") => try_deserialize!(ProcessShutdownPayload, ProcessShutdown),
            ("sinex", "automaton.error") => try_deserialize!(AutomatonErrorPayload, AutomatonError),
            ("sinex", "sensor.activated") => try_deserialize!(SensorActivatedPayload, SensorActivated),
            ("sinex", "sensor.deactivated") => try_deserialize!(SensorDeactivatedPayload, SensorDeactivated),

            // RPC events
            ("rpc", "content.response") => try_deserialize!(RpcContentResponsePayload, RpcContentResponse),
            ("rpc", "pkm.response") => try_deserialize!(RpcPkmResponsePayload, RpcPkmResponse),

            // Shell events - Kitty
            ("shell.kitty", "command.executed") => try_deserialize!(KittyCommandExecutedPayload, KittyCommandExecuted),
            ("shell.kitty", "command.completed") => try_deserialize!(KittyCommandCompletedPayload, KittyCommandCompleted),
            ("terminal.kitty", "session.started") => try_deserialize!(KittySessionStartedPayload, KittySessionStarted),
            ("terminal.kitty", "session.ended") => try_deserialize!(KittySessionEndedPayload, KittySessionEnded),
            ("terminal.kitty", "process.changed") => try_deserialize!(KittyProcessChangedPayload, KittyProcessChanged),
            ("terminal.kitty", "tab.focused") => try_deserialize!(KittyTabFocusedPayload, KittyTabFocused),
            ("terminal.kitty", "content.streamed") => try_deserialize!(KittyContentStreamedPayload, KittyContentStreamed),

            // Shell events - Atuin
            ("shell.atuin", "command.executed") => try_deserialize!(AtuinCommandExecutedPayload, AtuinCommandExecuted),
            ("shell.atuin", "command.completed") => try_deserialize!(AtuinCommandCompletedPayload, AtuinCommandCompleted),
            ("shell.atuin", "entry") => try_deserialize!(AtuinEntryPayload, AtuinEntry),

            // Shell events - History/Terminal
            ("shell", "history.command_imported") => try_deserialize!(HistoryCommandImportedPayload, HistoryCommandImported),
            ("shell", "command.imported") => try_deserialize!(CommandImportedPayload, CommandImported),
            ("shell.bash", "history.entry") => try_deserialize!(BashHistoryEntryPayload, BashHistoryEntry),
            ("shell.bash", "command.historical") => try_deserialize!(BashHistoricalCommandPayload, BashHistoricalCommand),
            ("shell.zsh", "command.historical") => try_deserialize!(ZshHistoricalCommandPayload, ZshHistoricalCommand),
            ("shell.fish", "command.historical") => try_deserialize!(FishHistoricalCommandPayload, FishHistoricalCommand),
            ("terminal", "monitoring_started") => try_deserialize!(TerminalMonitoringStartedPayload, TerminalMonitoringStarted),
            ("terminal", "command.historical") => try_deserialize!(TerminalCommandHistoricalPayload, TerminalCommandHistorical),
            ("terminal", "history.historical") => try_deserialize!(TerminalHistoryHistoricalPayload, TerminalHistoryHistorical),
            ("terminal", "snapshot") => try_deserialize!(TerminalSnapshotPayload, TerminalSnapshot),
            ("terminal", "canonical.command") => try_deserialize!(CanonicalCommandPayload, CanonicalCommand),
            ("terminal", "shell.output_captured") => try_deserialize!(ShellOutputCapturedPayload, ShellOutputCaptured),

            // Asciinema events
            ("asciinema", "session.started") => try_deserialize!(AsciinemaSessionStartedPayload, AsciinemaSessionStarted),
            ("asciinema", "session.ended") => try_deserialize!(AsciinemaSessionEndedPayload, AsciinemaSessionEnded),

            // System events - Scanning
            ("system", "scan.started") => try_deserialize!(ScanStartedPayload, ScanStarted),
            ("system", "scan.completed") => try_deserialize!(ScanCompletedPayload, ScanCompleted),
            ("system", "monitoring_started") => try_deserialize!(SystemMonitoringStartedPayload, SystemMonitoringStarted),
            ("system", "snapshot") => try_deserialize!(SystemSnapshotPayload, SystemSnapshot),

            // System events - Journal
            ("system.journald", "entry") => try_deserialize!(JournalEntryPayload, JournalEntry),
            ("system.journald", "sync.completed") => try_deserialize!(JournalSyncCompletedPayload, JournalSyncCompleted),
            ("system.journald", "entry.written") => try_deserialize!(JournalEntryWrittenPayload, JournalEntryWritten),
            ("system.journald", "historical") => try_deserialize!(JournaldHistoricalPayload, JournaldHistorical),

            // System events - D-Bus
            ("system.dbus", "signal") => try_deserialize!(DbusSignalPayload, DbusSignal),
            ("system.dbus", "method.called") => try_deserialize!(DbusMethodCalledPayload, DbusMethodCalled),
            ("system.dbus", "notification.sent") => try_deserialize!(DbusNotificationSentPayload, DbusNotificationSent),
            ("system.dbus", "media.state_changed") => try_deserialize!(DbusMediaStateChangedPayload, DbusMediaStateChanged),
            ("system.dbus", "power.state_changed") => try_deserialize!(DbusPowerStateChangedPayload, DbusPowerStateChanged),
            ("system.dbus", "device.connected") => try_deserialize!(DbusDeviceConnectedPayload, DbusDeviceConnected),
            ("system.dbus", "bluetooth.device_changed") => try_deserialize!(DbusBluetoothDeviceChangedPayload, DbusBluetoothDeviceChanged),
            ("system.dbus", "network.state_changed") => try_deserialize!(DbusNetworkStateChangedPayload, DbusNetworkStateChanged),
            ("system.dbus", "mount.event") => try_deserialize!(DbusMountEventPayload, DbusMountEvent),

            // System events - systemd
            ("system.systemd", "unit.started") => try_deserialize!(SystemdUnitStartedPayload, SystemdUnitStarted),
            ("system.systemd", "unit.stopped") => try_deserialize!(SystemdUnitStoppedPayload, SystemdUnitStopped),
            ("system.systemd", "unit.status") => try_deserialize!(SystemdUnitStatusPayload, SystemdUnitStatus),
            ("system.systemd", "unit.failed") => try_deserialize!(SystemdUnitFailedPayload, SystemdUnitFailed),
            ("system.systemd", "unit.reloaded") => try_deserialize!(SystemdUnitReloadedPayload, SystemdUnitReloaded),
            ("system.systemd", "timer.triggered") => try_deserialize!(SystemdTimerTriggeredPayload, SystemdTimerTriggered),
            ("system.systemd", "unit.starting") => try_deserialize!(SystemdUnitStartingPayload, SystemdUnitStarting),
            ("system.systemd", "unit.stopping") => try_deserialize!(SystemdUnitStoppingPayload, SystemdUnitStopping),
            ("system.systemd", "unit.state_changed") => try_deserialize!(SystemdUnitStateChangedPayload, SystemdUnitStateChanged),
            ("system.systemd", "units.historical") => try_deserialize!(SystemdUnitsHistoricalPayload, SystemdUnitsHistorical),

            // System events - udev
            ("system.udev", "device.added") => try_deserialize!(UdevDeviceAddedPayload, UdevDeviceAdded),
            ("system.udev", "device.removed") => try_deserialize!(UdevDeviceRemovedPayload, UdevDeviceRemoved),
            ("system.udev", "device.connected") => try_deserialize!(UdevDeviceConnectedPayload, UdevDeviceConnected),
            ("system.udev", "device.disconnected") => try_deserialize!(UdevDeviceDisconnectedPayload, UdevDeviceDisconnected),
            ("system.udev", "device.changed") => try_deserialize!(UdevDeviceChangedPayload, UdevDeviceChanged),
            ("system.udev", "device.driver_changed") => try_deserialize!(UdevDeviceDriverChangedPayload, UdevDeviceDriverChanged),
            ("system.udev", "device.other") => try_deserialize!(UdevDeviceOtherPayload, UdevDeviceOther),
            ("system.udev", "device.historical") => try_deserialize!(UdevDeviceHistoricalPayload, UdevDeviceHistorical),

            // System events - Logs
            ("system.logs", "line") => try_deserialize!(LogLinePayload, LogLine),
            ("system", "health.summary") => try_deserialize!(SystemHealthSummaryPayload, SystemHealthSummary),
            ("system", "satellite.heartbeat") => try_deserialize!(SatelliteHeartbeatPayload, SatelliteHeartbeat),

            // Telemetry events
            ("telemetry", "events.processed") => try_deserialize!(EventsProcessedPayload, EventsProcessed),
            ("telemetry", "errors.summary") => try_deserialize!(ErrorsSummaryPayload, ErrorsSummary),
            ("telemetry", "system.resources") => try_deserialize!(SystemResourcesPayload, SystemResources),
            ("telemetry", "operation.performance") => try_deserialize!(OperationPerformancePayload, OperationPerformance),
            ("telemetry", "component.resource_usage") => try_deserialize!(ComponentResourceUsagePayload, ComponentResourceUsage),

            // Window events (Hyprland)
            ("hyprland", "window.opened") => try_deserialize!(HyprlandWindowOpenedPayload, HyprlandWindowOpened),
            ("hyprland", "window.closed") => try_deserialize!(HyprlandWindowClosedPayload, HyprlandWindowClosed),
            ("hyprland", "window.focused") => try_deserialize!(HyprlandWindowFocusedPayload, HyprlandWindowFocused),
            ("hyprland", "workspace.switched") => try_deserialize!(HyprlandWorkspaceSwitchedPayload, HyprlandWorkspaceSwitched),
            ("hyprland", "window.moved") => try_deserialize!(HyprlandWindowMovedPayload, HyprlandWindowMoved),
            ("hyprland", "monitor.focused") => try_deserialize!(HyprlandMonitorFocusedPayload, HyprlandMonitorFocused),
            ("hyprland", "state.captured") => try_deserialize!(HyprlandStateCapturedPayload, HyprlandStateCaptured),

            // Unknown event type - return Unknown variant
            _ => {}
        }

        // If we reach here, the event type wasn't recognized or deserialization failed
        EventEnvelope::Unknown(Box::new(UnknownEvent {
            source: source.to_string(),
            event_type: event_type.to_string(),
            payload,
            reason: "Unknown event type or deserialization failed".to_string(),
        }))
    }

    /// Check if this envelope represents a known event type
    pub fn is_known(&self) -> bool {
        !matches!(self, EventEnvelope::Unknown(_))
    }

    /// Check if this envelope represents an unknown event type
    pub fn is_unknown(&self) -> bool {
        matches!(self, EventEnvelope::Unknown(_))
    }

    /// Extract the underlying payload as JSON value for any variant
    pub fn to_json_value(&self) -> Result<serde_json::Value, SinexError> {
        match self {
            EventEnvelope::Unknown(unknown) => Ok(unknown.payload.clone()),
            _ => serde_json::to_value(self)
                .map_err(|e| SinexError::serialization(format!("Failed to serialize envelope: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_from_parts_known_event() {
        let payload = json!({
            "path": "/test/file.txt",
            "size": 1024,
            "created_at": "2024-01-01T00:00:00Z",
            "permissions": 644
        });

        let envelope = EventEnvelope::from_parts("fs-watcher", "file.created", payload);

        match envelope {
            EventEnvelope::FileCreated(payload) => {
                assert_eq!(payload.path, "/test/file.txt");
                assert_eq!(payload.size, 1024);
            }
            _ => panic!("Expected FileCreated variant"),
        }
    }

    #[test]
    fn test_from_parts_unknown_event() {
        let payload = json!({"unknown": "data"});
        let envelope = EventEnvelope::from_parts("unknown-source", "unknown.type", payload);

        match envelope {
            EventEnvelope::Unknown(unknown) => {
                assert_eq!(unknown.source, "unknown-source");
                assert_eq!(unknown.event_type, "unknown.type");
                assert_eq!(unknown.payload["unknown"], "data");
            }
            _ => panic!("Expected Unknown variant"),
        }
    }

    #[test]
    fn test_from_parts_deserialization_failure() {
        // Valid event type but invalid payload structure
        let payload = json!({"invalid": "structure"});
        let envelope = EventEnvelope::from_parts("fs-watcher", "file.created", payload);

        match envelope {
            EventEnvelope::Unknown(unknown) => {
                assert_eq!(unknown.source, "fs-watcher");
                assert_eq!(unknown.event_type, "file.created");
            }
            _ => panic!("Expected Unknown variant due to deserialization failure"),
        }
    }

    #[test]
    fn test_source_and_event_type_methods() {
        let payload = FileCreatedPayload {
            path: "/test".to_string(),
            size: 100,
            created_at: chrono::Utc::now(),
            permissions: Some(644),
        };
        let envelope = EventEnvelope::FileCreated(payload);

        assert_eq!(envelope.source(), "fs-watcher");
        assert_eq!(envelope.event_type(), "file.created");
    }

    #[test]
    fn test_is_known_and_is_unknown() {
        let known_envelope = EventEnvelope::FileCreated(FileCreatedPayload {
            path: "/test".to_string(),
            size: 100,
            created_at: chrono::Utc::now(),
            permissions: Some(644),
        });

        let unknown_envelope = EventEnvelope::Unknown(Box::new(UnknownEvent {
            source: "test".to_string(),
            event_type: "test.unknown".to_string(),
            payload: json!({}),
            reason: "test".to_string(),
        }));

        assert!(known_envelope.is_known());
        assert!(!known_envelope.is_unknown());
        assert!(!unknown_envelope.is_known());
        assert!(unknown_envelope.is_unknown());
    }

    #[test]
    fn test_to_json_value() {
        let payload = FileCreatedPayload {
            path: "/test".to_string(),
            size: 100,
            created_at: chrono::Utc::now(),
            permissions: Some(644),
        };
        let envelope = EventEnvelope::FileCreated(payload);

        let json_value = envelope.to_json_value().unwrap();
        // Should serialize the envelope with both type tag and payload
        assert!(json_value.get("type").is_some());
        assert!(json_value.get("payload").is_some());
    }
}