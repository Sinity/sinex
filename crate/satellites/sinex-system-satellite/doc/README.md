# sinex-system-satellite

The system satellite unifies multiple system-level event sources (D-Bus,
journal, udev, systemd unit transitions) into a single `StatefulStreamProcessor`.
It is responsible for:

- Capturing OS-level signals and normalising them into Sinex events.
- Maintaining checkpoints so restarts continue from the last processed marker.
- Publishing derived events consumed by gateways and health dashboards.

See `docs/architecture/satellite-implementation.md` for the shared processor
architecture and `docs/architecture/SystemOperations_And_Integrity_Architecture.md`
for downstream consumers.
