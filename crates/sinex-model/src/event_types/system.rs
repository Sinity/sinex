//! The machine's own state as the system reports it: systemd units, boots,
//! sleep and shutdown (read from the journal entries systemd and logind write
//! about them), devices (udev) and the system bus (network, Bluetooth,
//! batteries).
//!
//! An event read from a journal entry is keyed by that entry's cursor, so the
//! same entry read again (an overlapping capture, an archived journal) is a
//! repeat. Live device and bus records exist only while they happen; each is
//! its own record location, timed when Sinex received it.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A unit became active: a start job finished successfully.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "unit.started", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct UnitStarted {
    /// The journal entry's cursor.
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    pub unit: String,
    /// `system` for the system manager, `user` for a user's manager.
    pub manager: String,
    /// The manager's user ID: 0 for the system manager.
    pub uid: Option<i64>,
    /// The unit's invocation ID, shared by every entry of one run of it.
    pub invocation_id: Option<String>,
    /// The job type: `start`, or `restart` for the start half of a restart.
    pub job: Option<String>,
}

/// A unit stopped being active: a stop job finished, it exited on its own,
/// or it (or its start job) failed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "unit.stopped", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct UnitStopped {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    pub unit: String,
    pub manager: String,
    pub uid: Option<i64>,
    pub invocation_id: Option<String>,
    /// `stopped` (a stop job finished), `succeeded` (it exited on its own
    /// with success), `failed` (it failed) or `start_failed` (its start job
    /// did not succeed).
    pub outcome: String,
    /// systemd's result: the job's (`done`, `failed`, `dependency`,
    /// `timeout`…) or, for a failed unit, the unit's (`exit-code`, `signal`,
    /// `timeout`, `oom-kill`…).
    pub result: Option<String>,
}

/// A unit reloaded its configuration: a reload job finished.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "unit.reloaded", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct UnitReloaded {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    pub unit: String,
    pub manager: String,
    pub uid: Option<i64>,
    pub invocation_id: Option<String>,
    /// The reload job's result (`done`, `failed`…).
    pub result: Option<String>,
}

/// A boot, from the system manager's "startup finished" entry: the kernel
/// started that entry's time minus the kernel, initrd and userspace phases.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "boot.started", key = ["boot_id"], replace = repeat, time = started_at, quality = intrinsic)]
pub struct BootStarted {
    pub boot_id: String,
    /// When the kernel started; unknown when a phase is missing.
    pub started_at: Option<Timestamp>,
    /// When startup finished (the entry's time).
    pub finished_at: Option<Timestamp>,
    pub kernel_us: Option<i64>,
    pub initrd_us: Option<i64>,
    pub userspace_us: Option<i64>,
    /// The journal entry's cursor.
    pub cursor: String,
}

/// The system entered a sleep state (systemd-sleep).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "sleep.started", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct SleepStarted {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    /// `suspend`, `hibernate`, `hybrid-sleep` or `suspend-then-hibernate`.
    pub state: Option<String>,
}

/// The system left a sleep state (systemd-sleep).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "sleep.ended", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct SleepEnded {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    pub state: Option<String>,
}

/// A shutdown or reboot began (logind).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "shutdown.started", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct ShutdownStarted {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    /// `reboot`, `power-off`, `halt`, `kexec`…, as logind wrote it.
    pub kind: Option<String>,
}

/// A device event from udev, after its rules ran (`add`, `remove`, `change`,
/// `bind`, `unbind`, `move`, `online`, `offline`), or a device present when
/// capture started (`present`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "device.changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct DeviceChanged {
    pub received_at: Timestamp,
    pub action: String,
    /// The device's path under `/sys` (`DEVPATH`).
    pub device: String,
    pub subsystem: Option<String>,
    pub device_type: Option<String>,
    /// Its node under `/dev`, when it has one.
    pub node: Option<String>,
    pub driver: Option<String>,
    /// udev's event number; none for a device read at capture start.
    pub sequence: Option<u64>,
    /// From the hardware database or the device itself.
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub serial: Option<String>,
    /// Every property udev reported, by name.
    pub properties: Map<String, Value>,
}

/// A signal on the system bus that no more specific type reads (logind's
/// sessions and sleep, NetworkManager's properties, …).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "bus.signal", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct BusSignal {
    pub received_at: Timestamp,
    /// The sender's unique bus name.
    pub sender: String,
    pub path: String,
    pub interface: String,
    pub member: String,
    /// The signal's arguments, decoded.
    pub args: Vec<Value>,
}

/// NetworkManager's overall state changed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "network.changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct NetworkChanged {
    pub received_at: Timestamp,
    /// `asleep`, `disconnected`, `disconnecting`, `connecting`,
    /// `connected_local`, `connected_site`, `connected_global` or `unknown`.
    pub state: String,
    /// The state as NetworkManager numbers it.
    pub state_code: u32,
    /// Read when capture started, not a change.
    pub snapshot: bool,
}

/// A network device's state changed (NetworkManager), or was read when
/// capture started.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "network.device_changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct NetworkDeviceChanged {
    pub received_at: Timestamp,
    /// NetworkManager's object path for the device.
    pub device: String,
    /// The kernel interface (`enp4s0`), when capture has seen it.
    pub interface: Option<String>,
    /// `unmanaged`, `unavailable`, `disconnected`, `prepare`, `config`,
    /// `need_auth`, `ip_config`, `ip_check`, `secondaries`, `activated`,
    /// `deactivating`, `failed` or `unknown`.
    pub state: String,
    pub state_code: u32,
    pub previous_state: Option<String>,
    /// NetworkManager's reason code for the change.
    pub reason_code: Option<u32>,
    pub snapshot: bool,
}

/// A Bluetooth device appeared, went away or changed (BlueZ), or was read
/// when capture started.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "bluetooth.device_changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct BluetoothDeviceChanged {
    pub received_at: Timestamp,
    /// BlueZ's object path for the device.
    pub device: String,
    /// The device address, from its properties or its object path.
    pub address: Option<String>,
    /// `present` (read at capture start), `added`, `removed` or `changed`.
    pub change: String,
    pub name: Option<String>,
    pub connected: Option<bool>,
    pub paired: Option<bool>,
    /// Battery level in percent (`org.bluez.Battery1`).
    pub battery_percent: Option<u8>,
    /// The properties sent, by interface (`org.bluez.Device1`, …); for a
    /// removal, the interfaces removed.
    pub properties: Map<String, Value>,
}

/// A power source's state changed (UPower).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "battery.changed", key = location, replace = repeat, time = received_at, quality = captured)]
pub struct BatteryChanged {
    pub received_at: Timestamp,
    /// UPower's object path for the device.
    pub device: String,
    pub percentage: Option<f64>,
    /// `charging`, `discharging`, `empty`, `fully_charged`,
    /// `pending_charge`, `pending_discharge` or `unknown`.
    pub state: Option<String>,
    pub time_to_empty_s: Option<i64>,
    pub time_to_full_s: Option<i64>,
    /// Every property sent, by name.
    pub properties: Map<String, Value>,
}
