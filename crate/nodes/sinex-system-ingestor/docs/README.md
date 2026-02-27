# sinex-system-ingestor

The system node unifies multiple system-level event sources (D-Bus,
journal, udev, systemd unit transitions) into a single `IngestorNode`.
It is responsible for:

- Capturing OS-level signals and normalising them into Sinex events.
- Maintaining checkpoints so restarts continue from the last processed marker.
- Publishing derived events consumed by gateways and health dashboards.

See `crate/lib/sinex-node-sdk/docs/overview.md` for the shared node
architecture and `docs/current/architecture/SystemOperations_And_Integrity_Architecture.md`
for downstream consumers.

## Watcher Overview

| Watcher  | Backing subsystem | What it captures | Key config knobs |
|----------|------------------|------------------|------------------|
| **D-Bus** | `dbus_tokio` + match rules | System/session signals (power, Bluetooth, notifications, etc.) | `SystemConfig.dbus_enabled`, `dbus_config.monitor_session`, `dbus_config.monitor_system`, interface allowlists |
| **Journal** | `journalctl --output=json` | Historical + live journal entries with cursor tracking | `SystemConfig.journal_enabled`, `journal_config.import_on_startup`, `journal_config.import_hours`, cursor file path |
| **udev** | `udev` monitor socket | Device attach/detach, block and network changes | `SystemConfig.udev_enabled`, future per-subsystem filters |
| **systemd** | `sd-bus` subscriptions | Unit state changes, failures, restarts | `SystemConfig.systemd_enabled`, `systemd_config.units` |

Each watcher exposes a readiness flag through `SystemNode::watcher_snapshot()` so
CLI commands and integration tests can assert wiring status. When running in
continuous mode the node stores the handle for every watcher so shutdown
hooks can cancel the background tasks cleanly; restart handling remains a
follow-on item.
