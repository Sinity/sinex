pub mod dbus;
pub mod journal;

// Re-export system event types and payloads
pub use dbus::{
    BluetoothEvent, BluetoothEventPayload, DbusMethodCall, DbusMethodCallPayload, DbusSignal,
    DbusSignalPayload, HardwareEvent, HardwareEventPayload, MediaPlaybackChanged,
    MediaPlaybackPayload, MountEvent, MountEventPayload, NetworkEvent, NetworkEventPayload,
    NotificationPayload, PolicyKitEvent, PolicyKitEventPayload, PowerEvent, PowerEventPayload,
    ScreenSaverEvent, ScreenSaverEventPayload, SessionEvent, SessionEventPayload,
    SystemNotification,
};
pub use journal::{JournalEntry, JournalEntryPayload, JournalSync, JournalSyncPayload};

use sinex_core::register_events;

// Register all system event types using the macro
register_events! {
    "signal.received" => (dbus, DbusSignalPayload),
    "method.called" => (dbus, DbusMethodCallPayload),
    "notification.sent" => (dbus, NotificationPayload),
    "media.state_changed" => (dbus, MediaPlaybackPayload),
    "power.state_changed" => (dbus, PowerEventPayload),
    "device.connected" => (dbus, HardwareEventPayload),
    "session.state_changed" => (dbus, SessionEventPayload),
    "security.authorization" => (dbus, PolicyKitEventPayload),
    "bluetooth.device_changed" => (dbus, BluetoothEventPayload),
    "network.state_changed" => (dbus, NetworkEventPayload),
    "screensaver.state_changed" => (dbus, ScreenSaverEventPayload),
    "mount.changed" => (dbus, MountEventPayload),
    "entry.written" => (journald, JournalEntryPayload),
    "sync.completed" => (journald, JournalSyncPayload),
}
