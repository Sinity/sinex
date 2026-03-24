//! Marker traits for event categorisation.
//!
//! These allow generic code to constrain payloads to specific domains.

// Marker traits are intentionally unused as bounds until callers need to
// filter on them. Implementations are wired for correctness now.
#![allow(dead_code)]

use crate::events::EventPayload;

/// Marker for shell/terminal related events.
pub trait ShellRelated: EventPayload {}

/// Marker for filesystem related events.
pub trait FilesystemRelated: EventPayload {}

/// Marker for system/hardware related events.
pub trait SystemRelated: EventPayload {}

/// Marker for desktop/UI related events.
pub trait DesktopRelated: EventPayload {}

/// Marker for document/content related events.
pub trait DocumentRelated: EventPayload {}

// Implementations

use super::payloads::{desktop, document, filesystem, shell, system};

// Shell
impl ShellRelated for shell::KittyCommandExecutedPayload {}
impl ShellRelated for shell::KittyCommandCompletedPayload {}
impl ShellRelated for shell::KittySessionStartedPayload {}
impl ShellRelated for shell::KittySessionEndedPayload {}
impl ShellRelated for shell::AtuinCommandExecutedPayload {}
impl ShellRelated for shell::AtuinCommandCompletedPayload {}
impl ShellRelated for shell::HistoryCommandImportedPayload {}
impl ShellRelated for shell::TerminalMonitoringStartedPayload {}
impl ShellRelated for shell::KittyProcessChangedPayload {}
impl ShellRelated for shell::KittyTabFocusedPayload {}
impl ShellRelated for shell::KittyContentStreamedPayload {}
impl ShellRelated for shell::CanonicalCommandPayload {}
impl ShellRelated for shell::ShellOutputCapturedPayload {}
impl ShellRelated for shell::AsciinemaSessionStartedPayload {}
impl ShellRelated for shell::AsciinemaSessionEndedPayload {}

// Filesystem
impl FilesystemRelated for filesystem::FileCreatedPayload {}
impl FilesystemRelated for filesystem::FileModifiedPayload {}
impl FilesystemRelated for filesystem::FileDeletedPayload {}
impl FilesystemRelated for filesystem::FileMovedPayload {}
impl FilesystemRelated for filesystem::DirCreatedPayload {}
impl FilesystemRelated for filesystem::DirDeletedPayload {}
impl FilesystemRelated for filesystem::FileDiscoveredPayload {}
impl FilesystemRelated for filesystem::DirDiscoveredPayload {}

// Desktop
impl DesktopRelated for desktop::DesktopMonitoringStartedPayload {}
impl DesktopRelated for desktop::DesktopSnapshotPayload {}
impl DesktopRelated for desktop::ClipboardHistoricalPayload {}
impl DesktopRelated for desktop::WindowManagerHistoricalPayload {}

// Document
impl DocumentRelated for document::DocumentIngestedPayload {}

// System
impl SystemRelated for system::ScanStartedPayload {}
impl SystemRelated for system::ScanCompletedPayload {}
impl SystemRelated for system::JournalEntryPayload {}
impl SystemRelated for system::JournalSyncCompletedPayload {}
impl SystemRelated for system::JournalEntryWrittenPayload {}
impl SystemRelated for system::DbusSignalPayload {}
impl SystemRelated for system::DbusMethodCalledPayload {}
impl SystemRelated for system::DbusNotificationSentPayload {}
impl SystemRelated for system::DbusMediaStateChangedPayload {}
impl SystemRelated for system::DbusPowerStateChangedPayload {}
impl SystemRelated for system::DbusDeviceConnectedPayload {}
impl SystemRelated for system::DbusBluetoothDeviceChangedPayload {}
impl SystemRelated for system::DbusNetworkStateChangedPayload {}
impl SystemRelated for system::DbusMountEventPayload {}
impl SystemRelated for system::SystemdUnitStartedPayload {}
impl SystemRelated for system::SystemdUnitStoppedPayload {}
impl SystemRelated for system::SystemdUnitStatusPayload {}
impl SystemRelated for system::SystemdUnitFailedPayload {}
impl SystemRelated for system::SystemdUnitReloadedPayload {}
impl SystemRelated for system::SystemdTimerTriggeredPayload {}
impl SystemRelated for system::SystemdUnitStartingPayload {}
impl SystemRelated for system::SystemdUnitStoppingPayload {}
impl SystemRelated for system::SystemdUnitStateChangedPayload {}
impl SystemRelated for system::UdevDeviceAddedPayload {}
impl SystemRelated for system::UdevDeviceRemovedPayload {}
impl SystemRelated for system::UdevDeviceConnectedPayload {}
impl SystemRelated for system::UdevDeviceDisconnectedPayload {}
impl SystemRelated for system::UdevDeviceChangedPayload {}
impl SystemRelated for system::UdevDeviceDriverChangedPayload {}
impl SystemRelated for system::UdevDeviceOtherPayload {}
impl SystemRelated for system::LogLinePayload {}
impl SystemRelated for system::SystemHealthSummaryPayload {}
impl SystemRelated for system::NodeHeartbeatPayload {}
impl SystemRelated for system::SystemMonitoringStartedPayload {}
impl SystemRelated for system::SystemSnapshotPayload {}
