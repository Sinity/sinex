# Payloads

`payloads.rs` centralises the typed payload definitions emitted by the system
satellite.

- Provides serde-friendly structs for D-Bus, journal, systemd, and udev events.
- Encodes schema versions so downstream services can validate payloads.
- Shares conversion helpers for watchers to build consistent events.
