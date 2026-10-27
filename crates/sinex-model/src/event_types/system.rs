//! The machine's own state as the system reports it: systemd units, boots,
//! sleep and shutdown (read from the journal entries systemd and logind write
//! about them), and devices (udev).
//!
//! An event read from a journal entry is keyed by that entry's cursor, so the
//! same entry read again (an overlapping capture, an archived journal) is a
//! repeat. Live device records exist only while they happen; each is
//! its own record location, timed when Sinex received it.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A unit became active: a start job finished successfully.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "unit.started", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
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
#[event(version = 2, type = "unit.stopped", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
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
#[event(version = 2, type = "unit.reloaded", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
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
#[event(version = 2, type = "boot.started", key = ["boot_id"], replace = repeat, time = started_at, quality = intrinsic)]
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
#[event(version = 2, type = "sleep.started", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct SleepStarted {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    /// `suspend`, `hibernate`, `hybrid-sleep` or `suspend-then-hibernate`.
    pub state: Option<String>,
}

/// The system left a sleep state (systemd-sleep).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "sleep.ended", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
pub struct SleepEnded {
    pub cursor: String,
    pub logged_at: Option<Timestamp>,
    pub state: Option<String>,
}

/// A shutdown or reboot began (logind).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "shutdown.started", key = ["cursor"], replace = repeat, time = logged_at, quality = intrinsic)]
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
#[event(version = 2, type = "device.changed", key = location, replace = repeat, time = received_at, quality = captured)]
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
