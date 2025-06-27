pub mod dbus;
pub mod journal;

// Re-export system event types
pub use dbus::{
    BluetoothEvent, DbusMethodCall, DbusSignal, HardwareEvent, MediaPlaybackChanged, MountEvent,
    NetworkEvent, PolicyKitEvent, PowerEvent, ScreenSaverEvent, SessionEvent, SystemNotification,
};
pub use journal::{JournalEntry, JournalSync};