# sinex-fs-watcher

The filesystem watcher satellite monitors filesystem changes and emits events
into the Sinex pipeline. It utilises the unified processor architecture to
support snapshots, historical replays, and continuous monitoring.

- Watches configured roots for creations, modifications, and deletions.
- Produces enriched events that include provenance metadata and content hashes.
- Maintains checkpoints so the watcher can resume without gaps.

See `docs/architecture/satellite-implementation.md` for the shared pattern and
`docs/architecture/Core_Architecture.md` for downstream processing.
