# Unified Processor

`unified_processor.rs` implements the `StatefulStreamProcessor` trait for the
system node. It merges signals from the individual watchers, maintains
checkpoint state, and emits events downstream.

- Coordinates watchers through async tasks and channels.
- Applies deduplication and ordering rules before forwarding events.
- Exposes health reporting APIs used by the gateway dashboards.
