pub mod dbus;
pub mod journal;

// Re-export system event types and payloads
pub use dbus::{
    BluetoothEvent, DbusMethodCall, DbusSignal, HardwareEvent, MediaPlaybackChanged, MountEvent,
    NetworkEvent, PolicyKitEvent, PowerEvent, ScreenSaverEvent, SessionEvent, SystemNotification,
    DbusSignalPayload, DbusMethodCallPayload, NotificationPayload, MediaPlaybackPayload,
    PowerEventPayload, HardwareEventPayload, SessionEventPayload, PolicyKitEventPayload,
    BluetoothEventPayload, NetworkEventPayload, ScreenSaverEventPayload, MountEventPayload,
};
pub use journal::{JournalEntry, JournalSync, JournalEntryPayload, JournalSyncPayload};

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
