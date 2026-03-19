# sinex-fs-ingestor

The filesystem watcher node monitors filesystem changes and emits events
into the Sinex pipeline. It utilises the unified node architecture to
support snapshots, historical replays, and continuous monitoring.

- Watches configured roots for creations, modifications, and deletions.
- Produces enriched events that include provenance metadata and content hashes.
- Maintains checkpoints so the watcher can resume without gaps.

See `crate/lib/sinex-node-sdk/docs/overview.md` for the shared lifecycle
pattern and `docs/architecture.md` for downstream processing.
